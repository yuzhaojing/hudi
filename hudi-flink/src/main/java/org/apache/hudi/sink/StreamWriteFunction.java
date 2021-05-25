/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.flink.util.TimeUtils;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.event.BatchWriteSuccessEvent;
import org.apache.hudi.sink.event.InitWriterEvent;
import org.apache.hudi.table.action.commit.FlinkWriteHelper;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Sink function to write the data to the underneath filesystem.
 *
 * <p><h2>Work Flow</h2>
 *
 * <p>The function firstly buffers the data as a batch of {@link HoodieRecord}s,
 * It flushes(write) the records batch when the batch size exceeds the configured size {@link FlinkOptions#WRITE_BATCH_SIZE}
 * or the total buffer size exceeds the configured size {@link FlinkOptions#WRITE_TASK_MAX_SIZE}
 * or a Flink checkpoint starts. After a batch has been written successfully,
 * the function notifies its operator coordinator {@link StreamWriteOperatorCoordinator} to mark a successful write.
 *
 * <p><h2>The Semantics</h2>
 *
 * <p>The task implements exactly-once semantics by buffering the data between checkpoints. The operator coordinator
 * starts a new instant on the time line when a checkpoint triggers, the coordinator checkpoints always
 * start before its operator, so when this function starts a checkpoint, a REQUESTED instant already exists.
 *
 * <p>In order to improve the throughput, The function process thread does not block data buffering
 * after the checkpoint thread starts flushing the existing data buffer. So there is possibility that the next checkpoint
 * batch was written to current checkpoint. When a checkpoint failure triggers the write rollback, there may be some duplicate records
 * (e.g. the eager write batch), the semantics is still correct using the UPSERT operation.
 *
 * <p><h2>Fault Tolerance</h2>
 *
 * <p>The operator coordinator checks and commits the last instant then starts a new one when a checkpoint finished successfully.
 * The operator rolls back the written data and throws to trigger a failover when any error occurs.
 * This means one Hoodie instant may span one or more checkpoints(some checkpoints notifications may be skipped).
 * If a checkpoint timed out, the next checkpoint would help to rewrite the left buffer data (clean the buffer in the last
 * step of the #flushBuffer method).
 *
 * <p>The operator coordinator would try several times when committing the write status.
 *
 * <p>Note: The function task requires the input stream be shuffled by the file IDs.
 *
 * @param <I> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
public class StreamWriteFunction<K, I, O>
    extends KeyedProcessFunction<K, I, O>
    implements CheckpointedFunction, CheckpointListener {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteFunction.class);

  /**
   * Write buffer as buckets for a checkpoint. The key is bucket ID.
   */
  private transient Map<String, DataBucket> buckets;

  /**
   * Config options.
   */
  private final Configuration config;

  /**
   * Id of current subtask.
   */
  private int taskID;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  private transient BiFunction<List<HoodieRecord>, String, List<WriteStatus>> writeFunction;

  /**
   * The REQUESTED instant we write the data.
   */
  private volatile String currentInstant;

  /**
   * Gateway to send operator events to the operator coordinator.
   */
  private transient OperatorEventGateway eventGateway;

  /**
   * Commit action type.
   */
  private transient String actionType;

  /**
   * Total size tracer.
   */
  private transient TotalSizeTracer tracer;

  private transient ListState<WriteStatus> writeStatusState;

  private final List<WriteStatus> writeStatusOfCurrentCkpt = Lists.newArrayList();

  private transient boolean canFlush = true;

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param config The config options
   */
  public StreamWriteFunction(Configuration config) {
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    this.tracer = new TotalSizeTracer(this.config);
    initBuffer();
    initWriteFunction();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.writeClient = StreamerUtil.createWriteClient(this.config, getRuntimeContext());
    this.actionType = CommitUtils.getCommitActionType(
            WriteOperationType.fromValue(config.getString(FlinkOptions.OPERATION)),
            HoodieTableType.valueOf(config.getString(FlinkOptions.TABLE_TYPE)));

    this.writeStatusState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "hudi-write-status-state",
            TypeInformation.of(new TypeHint<WriteStatus>() {})
    ));

    final List<WriteStatus> writeStatuses = new ArrayList<>();

    if (context.isRestored()) {
      for (WriteStatus writeStatus : this.writeStatusState.get()) {
        writeStatuses.add(writeStatus);
      }

      // disable flush, until received event from coordinator
      this.canFlush = false;
    }

    final InitWriterEvent event = InitWriterEvent.builder()
            .taskID(taskID)
            .writeStatus(writeStatuses)
            .build();
    this.eventGateway.sendEventToCoordinator(event);

    LOG.info("Send init event to coordinator, task[{}].", taskID);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    // Based on the fact that the coordinator starts the checkpoint first,
    // it would check the validity.
    // wait for the buffer data flush out and request a new instant
    flushRemaining(false);

    // reset the snapshot state to the current state
    this.writeStatusState.clear();
    this.writeStatusState.addAll(writeStatusOfCurrentCkpt);

    // disable flush, until received event from coordinator
    this.canFlush = false;
  }

  @Override
  public void processElement(I value, KeyedProcessFunction<K, I, O>.Context ctx, Collector<O> out) throws Exception {
    bufferRecord((HoodieRecord<?>) value);
  }

  @Override
  public void close() {
    if (this.writeClient != null) {
      this.writeClient.cleanHandlesGracefully();
      this.writeClient.close();
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    this.writeClient.cleanHandles();
  }

  /**
   * End input action for batch source.
   */
  public void endInput() {
    flushRemaining(true);
    this.writeClient.cleanHandles();
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public Map<String, List<HoodieRecord>> getDataBuffer() {
    Map<String, List<HoodieRecord>> ret = new HashMap<>();
    for (Map.Entry<String, DataBucket> entry : buckets.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().writeBuffer());
    }
    return ret;
  }

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public HoodieFlinkWriteClient getWriteClient() {
    return writeClient;
  }

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    this.eventGateway = operatorEventGateway;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initBuffer() {
    this.buckets = new LinkedHashMap<>();
  }

  private void initWriteFunction() {
    final String writeOperation = this.config.get(FlinkOptions.OPERATION);
    switch (WriteOperationType.fromValue(writeOperation)) {
      case INSERT:
        this.writeFunction = (records, instantTime) -> this.writeClient.insert(records, instantTime);
        break;
      case UPSERT:
        this.writeFunction = (records, instantTime) -> this.writeClient.upsert(records, instantTime);
        break;
      case INSERT_OVERWRITE:
        this.writeFunction = (records, instantTime) -> this.writeClient.insertOverwrite(records, instantTime);
        break;
      case INSERT_OVERWRITE_TABLE:
        this.writeFunction = (records, instantTime) -> this.writeClient.insertOverwriteTable(records, instantTime);
        break;
      default:
        throw new RuntimeException("Unsupported write operation : " + writeOperation);
    }
  }

  public void handleOperatorEvent(OperatorEvent evt) {
    LOG.info("Received commit success from coordinator.");
    this.canFlush = true;
  }

  /**
   * Represents a data item in the buffer, this is needed to reduce the
   * memory footprint.
   *
   * <p>A {@link HoodieRecord} was firstly transformed into a {@link DataItem}
   * for buffering, it then transforms back to the {@link HoodieRecord} before flushing.
   */
  private static class DataItem {
    private final String key; // record key
    private final String instant; // 'U' or 'I'
    private final HoodieRecordPayload<?> data; // record payload

    private DataItem(String key, String instant, HoodieRecordPayload<?> data) {
      this.key = key;
      this.instant = instant;
      this.data = data;
    }

    public static DataItem fromHoodieRecord(HoodieRecord<?> record) {
      return new DataItem(
          record.getRecordKey(),
          record.getCurrentLocation().getInstantTime(),
          record.getData());
    }

    public HoodieRecord<?> toHoodieRecord(String partitionPath) {
      HoodieKey hoodieKey = new HoodieKey(this.key, partitionPath);
      HoodieRecord<?> record = new HoodieRecord<>(hoodieKey, data);
      HoodieRecordLocation loc = new HoodieRecordLocation(instant, null);
      record.setCurrentLocation(loc);
      return record;
    }
  }

  /**
   * Data bucket.
   */
  private static class DataBucket {
    private final List<DataItem> records;
    private final BufferSizeDetector detector;
    private final String partitionPath;
    private final String fileID;

    private DataBucket(Double batchSize, HoodieRecord<?> hoodieRecord) {
      this.records = new ArrayList<>();
      this.detector = new BufferSizeDetector(batchSize);
      this.partitionPath = hoodieRecord.getPartitionPath();
      this.fileID = hoodieRecord.getCurrentLocation().getFileId();
    }

    /**
     * Prepare the write data buffer: patch up all the records with correct partition path.
     */
    public List<HoodieRecord> writeBuffer() {
      // rewrite all the records with new record key
      return records.stream()
          .map(record -> record.toHoodieRecord(partitionPath))
          .collect(Collectors.toList());
    }

    /**
     * Sets up before flush: patch up the first record with correct partition path and fileID.
     *
     * <p>Note: the method may modify the given records {@code records}.
     */
    public void preWrite(List<HoodieRecord> records) {
      // rewrite the first record with expected fileID
      HoodieRecord<?> first = records.get(0);
      HoodieRecord<?> record = new HoodieRecord<>(first.getKey(), first.getData());
      HoodieRecordLocation newLoc = new HoodieRecordLocation(first.getCurrentLocation().getInstantTime(), fileID);
      record.setCurrentLocation(newLoc);

      records.set(0, record);
    }

    public void reset() {
      this.records.clear();
      this.detector.reset();
    }
  }

  /**
   * Tool to detect if to flush out the existing buffer.
   * Sampling the record to compute the size with 0.01 percentage.
   */
  private static class BufferSizeDetector {
    private final Random random = new Random(47);
    private static final int DENOMINATOR = 100;

    private final double batchSizeBytes;

    private long lastRecordSize = -1L;
    private long totalSize = 0L;

    BufferSizeDetector(double batchSizeMb) {
      this.batchSizeBytes = batchSizeMb * 1024 * 1024;
    }

    boolean detect(Object record) {
      if (lastRecordSize == -1 || sampling()) {
        lastRecordSize = ObjectSizeCalculator.getObjectSize(record);
      }
      totalSize += lastRecordSize;
      return totalSize > this.batchSizeBytes;
    }

    boolean sampling() {
      // 0.01 sampling percentage
      return random.nextInt(DENOMINATOR) == 1;
    }

    void reset() {
      this.lastRecordSize = -1L;
      this.totalSize = 0L;
    }
  }

  /**
   * Tool to trace the total buffer size. It computes the maximum buffer size,
   * if current buffer size is greater than the maximum buffer size, the data bucket
   * flush triggers.
   */
  private static class TotalSizeTracer {
    private long bufferSize = 0L;
    private final double maxBufferSize;

    TotalSizeTracer(Configuration conf) {
      long mergeReaderMem = 100; // constant 100MB
      long mergeMapMaxMem = conf.getInteger(FlinkOptions.WRITE_MERGE_MAX_MEMORY);
      this.maxBufferSize = (conf.getDouble(FlinkOptions.WRITE_TASK_MAX_SIZE) - mergeReaderMem - mergeMapMaxMem) * 1024 * 1024;
      final String errMsg = String.format("'%s' should be at least greater than '%s' plus merge reader memory(constant 100MB now)",
          FlinkOptions.WRITE_TASK_MAX_SIZE.key(), FlinkOptions.WRITE_MERGE_MAX_MEMORY.key());
      ValidationUtils.checkState(this.maxBufferSize > 0, errMsg);
    }

    /**
     * Trace the given record size {@code recordSize}.
     *
     * @param recordSize The record size
     * @return true if the buffer size exceeds the maximum buffer size
     */
    boolean trace(long recordSize) {
      this.bufferSize += recordSize;
      return this.bufferSize > this.maxBufferSize;
    }

    void countDown(long size) {
      this.bufferSize -= size;
    }

    public void reset() {
      this.bufferSize = 0;
    }
  }

  /**
   * Returns the bucket ID with the given value {@code value}.
   */
  private String getBucketID(HoodieRecord<?> record) {
    final String fileId = record.getCurrentLocation().getFileId();
    return StreamerUtil.generateBucketKey(record.getPartitionPath(), fileId);
  }

  /**
   * Buffers the given record.
   *
   * <p>Flush the data bucket first if the bucket records size is greater than
   * the configured value {@link FlinkOptions#WRITE_BATCH_SIZE}.
   *
   * <p>Flush the max size data bucket if the total buffer size exceeds the configured
   * threshold {@link FlinkOptions#WRITE_TASK_MAX_SIZE}.
   *
   * @param value HoodieRecord
   */
  private void bufferRecord(HoodieRecord<?> value) throws Exception {
    final String bucketID = getBucketID(value);

    DataBucket bucket = this.buckets.computeIfAbsent(bucketID,
        k -> new DataBucket(this.config.getDouble(FlinkOptions.WRITE_BATCH_SIZE), value));
    final DataItem item = DataItem.fromHoodieRecord(value);
    boolean flushBucket = bucket.detector.detect(item);
    boolean flushBuffer = this.tracer.trace(bucket.detector.lastRecordSize);

    // if current memorySize larger than maxBufferSize and no response message received from coordinator,
    // should blocking until the lastPendingInstant update to avoid oom.
    while (flushBuffer && !canFlush) {
      LOG.warn("Trigger the flushBuffer but no response message received from coordinator, blocking until the lastPendingInstant update.");
      final String instant = this.writeClient.getLastPendingInstant(this.actionType);
      if (instant.compareTo(this.currentInstant) > 0) {
        this.currentInstant = instant;
        this.canFlush = true;
        break;
      }

      TimeUnit.SECONDS.sleep(1);
    }

    if (flushBucket) {
      boolean flush = flushBucket(bucket);
      if (flush) {
        this.tracer.countDown(bucket.detector.totalSize);
        bucket.reset();
      }
    } else if (flushBuffer) {
      // find the max size bucket and flush it out
      List<DataBucket> sortedBuckets = this.buckets.values().stream()
          .sorted((b1, b2) -> Long.compare(b2.detector.totalSize, b1.detector.totalSize))
          .collect(Collectors.toList());
      final DataBucket bucketToFlush = sortedBuckets.get(0);
      boolean flush = flushBucket(bucketToFlush);
      if (flush) {
        this.tracer.countDown(bucketToFlush.detector.totalSize);
        bucketToFlush.reset();
      }
    }
    bucket.records.add(item);
  }

  @SuppressWarnings("unchecked, rawtypes")
  private boolean flushBucket(DataBucket bucket) {
    if (!canFlush) {
      // in case there are flush before last instant commit
      LOG.info("Last commit has not committed, skip.");
      return false;
    }

    final String instant = this.writeClient.getLastPendingInstant(this.actionType);

    if (instant == null) {
      // in case there are empty checkpoints that has no input data
      LOG.info("No inflight instant when flushing data, skip.");
      return false;
    }

    List<HoodieRecord> records = bucket.writeBuffer();
    ValidationUtils.checkState(records.size() > 0, "Data bucket to flush has no buffering records");
    if (config.getBoolean(FlinkOptions.INSERT_DROP_DUPS)) {
      records = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1);
    }
    bucket.preWrite(records);
    final List<WriteStatus> writeStatus = new ArrayList<>(writeFunction.apply(records, instant));
    final BatchWriteSuccessEvent event = BatchWriteSuccessEvent.builder()
        .taskID(taskID)
        .instantTime(instant) // the write instant may shift but the event still use the currentInstant.
        .writeStatus(writeStatus)
        .isLastBatch(false)
        .isEndInput(false)
        .build();

    writeStatusOfCurrentCkpt.addAll(writeStatus);
    this.eventGateway.sendEventToCoordinator(event);

    return true;
  }

  @SuppressWarnings("unchecked, rawtypes")
  private void flushRemaining(boolean isEndInput) {
    this.currentInstant = this.writeClient.getLastPendingInstant(this.actionType);

    if (this.currentInstant == null) {
      throw new HoodieException("No inflight instant when flushRemaining!");
    }

    final List<WriteStatus> writeStatus;
    if (buckets.size() > 0) {
      writeStatus = new ArrayList<>();
      this.buckets.values()
          // The records are partitioned by the bucket ID and each batch sent to
          // the writer belongs to one bucket.
          .forEach(bucket -> {
            List<HoodieRecord> records = bucket.writeBuffer();
            if (records.size() > 0) {
              if (config.getBoolean(FlinkOptions.INSERT_DROP_DUPS)) {
                records = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1);
              }
              bucket.preWrite(records);
              writeStatus.addAll(writeFunction.apply(records, currentInstant));
              bucket.reset();
            }
          });
    } else {
      LOG.info("No data to write in subtask [{}] for instant [{}]", taskID, currentInstant);
      writeStatus = Collections.emptyList();
    }
    final BatchWriteSuccessEvent event = BatchWriteSuccessEvent.builder()
        .taskID(taskID)
        .instantTime(currentInstant)
        .writeStatus(writeStatus)
        .isLastBatch(true)
        .isEndInput(isEndInput)
        .build();

    writeStatusOfCurrentCkpt.addAll(writeStatus);
    this.eventGateway.sendEventToCoordinator(event);
    this.buckets.clear();
    this.tracer.reset();
  }
}
