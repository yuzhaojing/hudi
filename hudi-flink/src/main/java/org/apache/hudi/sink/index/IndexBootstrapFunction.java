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

package org.apache.hudi.sink.index;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.sink.partitioner.BucketAssigner;
import org.apache.hudi.sink.partitioner.BucketAssigners;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * The function to build the write profile incrementally for records within a checkpoint,
 * it then assigns the bucket with ID using the {@link BucketAssigner}.
 *
 * <p>All the records are tagged with HoodieRecordLocation, instead of real instant time,
 * INSERT record uses "I" and UPSERT record uses "U" as instant time. There is no need to keep
 * the "real" instant time for each record, the bucket ID (partition path & fileID) actually decides
 * where the record should write to. The "I" and "U" tags are only used for downstream to decide whether
 * the data bucket is an INSERT or an UPSERT, we should factor the tags out when the underneath writer
 * supports specifying the bucket type explicitly.
 *
 * <p>The output records should then shuffle by the bucket ID and thus do scalable write.
 *
 * @see BucketAssigner
 */
public class IndexBootstrapFunction<I, O>
        extends ProcessFunction<I, O> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexBootstrapFunction.class);

    private HoodieFlinkEngineContext context;

    /**
     * Index cache(speed-up) state for the underneath file based(BloomFilter) indices.
     * When a record came in, we do these check:
     *
     * <ul>
     *   <li>Try to load all the records in the partition path where the record belongs to</li>
     *   <li>Checks whether the state contains the record key</li>
     *   <li>If it does, tag the record with the location</li>
     *   <li>If it does not, use the {@link BucketAssigner} to generate a new bucket ID</li>
     * </ul>
     */
    private Map<String, HoodieRecordGlobalLocation> keyToLoc;

    /**
     * Bucket assigner to assign new bucket IDs or reuse existing ones.
     */
    private BucketAssigner bucketAssigner;

    private final Configuration conf;

    private transient org.apache.hadoop.conf.Configuration hadoopConf;

    private final boolean bootstrapIndex;

    public IndexBootstrapFunction(Configuration conf) {
        this.conf = conf;
        this.bootstrapIndex = conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED);
        this.keyToLoc = new HashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        HoodieWriteConfig writeConfig = StreamerUtil.getHoodieClientConfig(this.conf);
        this.hadoopConf = StreamerUtil.getHadoopConf();
        this.context = new HoodieFlinkEngineContext(
                new SerializableConfiguration(this.hadoopConf),
                new FlinkTaskContextSupplier(getRuntimeContext()));
        this.bucketAssigner = BucketAssigners.create(
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getNumberOfParallelSubtasks(),
                WriteOperationType.isOverwrite(WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION))),
                HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE)),
                context,
                writeConfig);

        // The dataset may be huge, thus the processing would block for long,
        // disabled by default.
        if (bootstrapIndex) {
            loadRecords();
        }
    }

    /**
     * Initialize a spillable map for incoming records.
     */
    protected void initializeIncomingRecordsMap(HoodieTable table) {
        try {
            // Load the new records in a map
            long memoryForMerge = IOUtils.getMaxMemoryPerPartitionMerge(table.getTaskContextSupplier(), table.getConfig().getProps());
            LOG.info("MaxMemoryPerPartitionMerge => " + memoryForMerge);
            this.keyToLoc = new ExternalSpillableMap<>(memoryForMerge, table.getConfig().getSpillableMapBasePath(),
                    new DefaultSizeEstimator<>(), new DefaultSizeEstimator<>());
        } catch (IOException io) {
            throw new HoodieIOException("Cannot instantiate an ExternalSpillableMap", io);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processElement(I value, Context ctx, Collector<O> out) {
        if (bootstrapIndex && !keyToLoc.isEmpty()) {
            LOG.info("Start send index to BucketAssign, taskId = {}.", getRuntimeContext().getIndexOfThisSubtask());
            for (Map.Entry<String, HoodieRecordGlobalLocation> entry : keyToLoc.entrySet()) {
                HoodieIndexMessage hoodieIndexMessage = new HoodieIndexMessage(entry.getKey(), entry.getValue());
                out.collect((O) hoodieIndexMessage);
            }

            keyToLoc.clear();
            LOG.info("Finish send index to BucketAssign, taskId = {}.", getRuntimeContext().getIndexOfThisSubtask());
        }

        out.collect((O) value);
    }

    /**
     * Load all the indices of hoodieTable into the backup state.
     */
    private void loadRecords() throws IOException {
        bucketAssigner.refreshTable();
        HoodieTable<?, ?, ?, ?> hoodieTable = bucketAssigner.getTable();
        String basePath = hoodieTable.getMetaClient().getBasePath();
        LOG.info("Start loading records in table {} into the index state", basePath);
        BaseFileUtils fileUtils = BaseFileUtils.getInstance(hoodieTable.getBaseFileFormat());

        List<HoodieBaseFile> latestBaseFiles = Lists.newArrayList();
        for (String partitionPath : FSUtils.getAllFoldersWithPartitionMetaFile(
                FSUtils.getFs(basePath, new org.apache.hadoop.conf.Configuration()), basePath)) {
            latestBaseFiles.addAll(HoodieIndexUtils.getLatestBaseFilesForPartition(partitionPath, hoodieTable));
        }

        final int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        final int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
        final int taskID = getRuntimeContext().getIndexOfThisSubtask();
        for (HoodieBaseFile baseFile : latestBaseFiles) {
            boolean shouldLoad;

            try {
                shouldLoad = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                        baseFile.getFileId(), maxParallelism, parallelism) == taskID;
            } catch (Exception e) {
                LOG.error("Error when assign fileId into the state from file: {}", baseFile);
                throw e;
            }

            if (shouldLoad) {
                final List<HoodieKey> hoodieKeys;
                try {
                    hoodieKeys =
                            fileUtils.fetchRecordKeyPartitionPath(hadoopConf, new Path(baseFile.getPath()));
                } catch (Exception e) {
                    // in case there was some empty parquet file when the pipeline
                    // crushes exceptionally.
                    LOG.error("Error when loading record keys from file: {}", baseFile);
                    continue;
                }

                hoodieKeys.forEach(hoodieKey -> {
                    try {
                        this.keyToLoc.put(hoodieKey.getRecordKey(),
                                new HoodieRecordGlobalLocation(hoodieKey.getPartitionPath(), baseFile.getCommitTime(), baseFile.getFileId()));
                    } catch (Exception e) {
                        LOG.error("Error when putting record keys into the state from file: {}", baseFile);
                    }
                });
            }
        }

        LOG.info("Finish loading records in table {} into the index state", hoodieTable.getMetaClient().getBasePath());
    }
}
