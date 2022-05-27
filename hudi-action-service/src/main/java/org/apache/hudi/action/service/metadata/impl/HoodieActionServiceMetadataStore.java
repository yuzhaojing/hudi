/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.action.service.metadata.impl;

import org.apache.hudi.action.service.common.ActionServiceConfig;
import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;
import org.apache.hudi.action.service.payload.HoodieActionServicePayload;
import org.apache.hudi.avro.model.HoodieServiceRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import com.clearspring.analytics.util.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hudi.action.service.payload.HoodieActionServicePayload.EMPTY_PARTITION;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;

public class HoodieActionServiceMetadataStore extends ActionServiceMetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieActionServiceMetadataStore.class);

  private static final String SCHEMA = HoodieServiceRecord.getClassSchema().toString();

  // Virtual keys support for metadata table. This Field is
  // from the metadata payload schema.
  private static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

  private final ConcurrentHashMap<String, HoodieRecord<HoodieActionServicePayload>> keyToRecord;
  private HoodieJavaWriteClient<HoodieActionServicePayload> writeClient;

  public HoodieActionServiceMetadataStore(ActionServiceConfig serviceConfig, Configuration hadoopConf, Boolean isMetricsOn) {
    super(serviceConfig, hadoopConf, isMetricsOn);
    this.keyToRecord = new ConcurrentHashMap<>();
  }

  @Override
  public void init() {
    HoodieWriteConfig writeConfig = createMetadataWriteConfig();
    try {
      initTableIfNotExists(serviceConfig, hadoopConf);
      this.writeClient = createWriteClient(writeConfig, hadoopConf);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create write client", e);
    }
  }

  /**
   * Create a {@code HoodieWriteConfig} to use for the Metadata Table.
   */
  private HoodieWriteConfig createMetadataWriteConfig() {
    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withAutoCommit(true)
        .withAvroSchemaValidate(true)
        .withEmbeddedTimelineServerEnabled(false)
        .withMarkersType(MarkerType.DIRECT.name())
        .withRollbackUsingMarkers(false)
        .withPath(serviceConfig.basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.INMEMORY)
            .build())
        .withSchema(SCHEMA)
        .forTable(serviceConfig.metaTableName)
        .withParallelism(serviceConfig.parallelism, serviceConfig.parallelism)
        .withDeleteParallelism(serviceConfig.parallelism)
        .withRollbackParallelism(serviceConfig.parallelism)
        .withFinalizeWriteParallelism(serviceConfig.parallelism);

    return builder.build();
  }

  @Override
  public void saveInstance(Instance instance) {
    String recordKey = instance.getRecordKey();
    HoodieRecord<HoodieActionServicePayload> record = HoodieActionServicePayload.createInstanceRecord(instance);

    // deduplicate instance
    if (!keyToRecord.containsKey(recordKey)) {
      keyToRecord.put(recordKey, record);
    }
  }

  @Override
  public void commit() {
    if (!keyToRecord.isEmpty()) {
      synchronized (HoodieActionServiceMetadataStore.class) {
        if (!keyToRecord.isEmpty()) {
          LOG.info("Commit the " + keyToRecord.size() + " records in buffer.");
          List<HoodieRecord<HoodieActionServicePayload>> records = new ArrayList<>(keyToRecord.values());
          keyToRecord.clear();
          final String instant = HoodieActiveTimeline.createNewInstantTime();
          writeClient.startCommitWithTime(instant);
          List<WriteStatus> statuses = writeClient.upsert(records, instant);
          if (statuses != null) {
            LOG.info("Success to commit record, instant = " + instant);
          } else {
            LOG.info("Failed to commit record, instant = " + instant);
          }
        }
      }
    }
  }

  @Override
  public Instance getInstance(Instance instance) throws IOException {
    TableFileSystemView.BaseFileOnlyView baseFileOnlyView = writeClient.getHoodieTable().getBaseFileOnlyView();
    List<HoodieBaseFile> baseFiles = baseFileOnlyView.getAllBaseFiles(EMPTY_PARTITION).collect(Collectors.toList());
    for (HoodieBaseFile baseFile : baseFiles) {
      HoodieFileReader<GenericRecord> baseFileReader = HoodieFileReaderFactory.getFileReader(hadoopConf, new Path(baseFile.getPath()));
      LOG.info(String.format("Opened metadata base file from %s at instant %s", baseFile.getPath(),
          baseFile.getCommitTime()));
      Option<GenericRecord> recordOption = baseFileReader.getRecordByKey(instance.getRecordKey());
      if (recordOption.isPresent()) {
        return HoodieActionServicePayload.covertRecordToInstant(recordOption.get());
      }
    }

    return null;
  }

  @Override
  public List<Instance> getInstances(int status, int limit) throws IOException {
    List<Instance> instances = Lists.newArrayList();
    TableFileSystemView.BaseFileOnlyView baseFileOnlyView = writeClient.getHoodieTable().getBaseFileOnlyView();
    List<HoodieBaseFile> baseFiles = baseFileOnlyView.getAllBaseFiles(EMPTY_PARTITION).collect(Collectors.toList());
    for (HoodieBaseFile baseFile : baseFiles) {
      HoodieFileReader<GenericRecord> baseFileReader = HoodieFileReaderFactory.getFileReader(hadoopConf, new Path(baseFile.getPath()));
      LOG.info(String.format("Opened metadata base file from %s at instant %s", baseFile.getPath(),
          baseFile.getCommitTime()));
      Iterator<GenericRecord> iterator = baseFileReader.getRecordIterator();
      while (iterator.hasNext()) {
        Instance instance = HoodieActionServicePayload.covertRecordToInstant(iterator.next());
        if (instance.getStatus() == status) {
          instances.add(instance);
        }

        if (instances.size() == limit) {
          return instances;
        }
      }
    }

    return instances;
  }

  /**
   * Initialize the table if it does not exist.
   *
   * @throws IOException if errors happens when writing metadata
   */
  private static void initTableIfNotExists(ActionServiceConfig config, Configuration hadoopConf) throws IOException {
    if (!tableExists(config.basePath, hadoopConf)) {
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE)
          .setTableName(config.metaTableName)
          .setPayloadClassName(HoodieActionServicePayload.class.getName())
          .setBaseFileFormat(HoodieFileFormat.HFILE.toString())
          .setRecordKeyFields(RECORD_KEY_FIELD_NAME)
          .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
          .setTimelineLayoutVersion(1)
          .initTable(hadoopConf, config.basePath);
      LOG.info("Table initialized under base path {}", config.basePath);
    } else {
      LOG.info("Table [{}] already exists, no need to initialize the table", config.basePath);
    }
    // Do not close the filesystem in order to use the CACHE,
    // some of the filesystems release the handles in #close method.
  }

  private static HoodieJavaWriteClient<HoodieActionServicePayload> createWriteClient(HoodieWriteConfig writeConfig, Configuration conf) throws IOException {
    Path path = new Path(writeConfig.getBasePath());
    FileSystem fs = FSUtils.getFs(writeConfig.getBasePath(), conf);
    if (!fs.exists(path)) {
      throw new HoodieException("Invalid table path");
    }

    return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(conf), writeConfig);
  }

  /**
   * Returns whether the hoodie table exists under given path {@code basePath}.
   */
  private static boolean tableExists(String basePath, org.apache.hadoop.conf.Configuration hadoopConf) {
    // Hadoop FileSystem
    FileSystem fs = FSUtils.getFs(basePath, hadoopConf);
    try {
      return fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME));
    } catch (IOException e) {
      throw new HoodieException("Error while checking whether table exists under path:" + basePath, e);
    }
  }
}
