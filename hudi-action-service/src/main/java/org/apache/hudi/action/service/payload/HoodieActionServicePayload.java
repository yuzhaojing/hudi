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

package org.apache.hudi.action.service.payload;

import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.action.service.util.InstanceUtil;
import org.apache.hudi.avro.model.HoodieServiceRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.HoodieHFileReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Objects;

public class HoodieActionServicePayload implements HoodieRecordPayload<HoodieActionServicePayload> {

  public static final String EMPTY_PARTITION = "";

  // HoodieMetadata schema field ids
  public static final String KEY_FIELD_NAME = HoodieHFileReader.KEY_FIELD_NAME;

  private String key = null;
  private Instance instance = null;

  public HoodieActionServicePayload(GenericRecord record, Comparable<?> orderingVal) {
    this(Option.of(record));
  }

  public HoodieActionServicePayload(Option<GenericRecord> record) {
    if (record.isPresent()) {
      // This can be simplified using SpecificData.deepcopy once this bug is fixed
      // https://issues.apache.org/jira/browse/AVRO-1811
      GenericRecord gr = record.get();
      key = gr.get(KEY_FIELD_NAME).toString();
      instance = covertRecordToInstant(gr);
    }
  }

  private HoodieActionServicePayload(String key, Instance instance) {
    this.key = key;
    this.instance = instance;
  }

  @Override
  public HoodieActionServicePayload preCombine(HoodieActionServicePayload previousRecord) {
    return previousRecord;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRecord, Schema schema) throws IOException {
    HoodieActionServicePayload anotherPayload = new HoodieActionServicePayload(Option.of((GenericRecord) oldRecord));

    if (anotherPayload.instance.getRunTimes() > instance.getRunTimes()) {
      instance.setRunTimes(anotherPayload.instance.getRunTimes());
    }

    if (anotherPayload.instance.getStatus() > instance.getStatus()) {
      instance.setStatus(anotherPayload.instance.getStatus());
    }

    if (!Objects.equals(anotherPayload.instance.getApplicationId(), instance.getApplicationId())) {
      instance.setApplicationId(anotherPayload.instance.getApplicationId());
    }

    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (key == null) {
      return Option.empty();
    }

    GenericRecord record = covertInstantToRecord(instance);
    return Option.of(record);
  }

  public static HoodieRecord<HoodieActionServicePayload> createInstanceRecord(Instance instance) {
    HoodieKey key = new HoodieKey(instance.getRecordKey(), EMPTY_PARTITION);
    HoodieActionServicePayload payload = new HoodieActionServicePayload(instance.getRecordKey(), instance);
    return new HoodieAvroRecord<>(key, payload);
  }

  public static HoodieRecord<HoodieActionServicePayload> createInstanceRecord(String recordKey, HoodieActionServicePayload payload) {
    HoodieKey key = new HoodieKey(recordKey, EMPTY_PARTITION);
    return new HoodieAvroRecord<>(key, payload);
  }

  public static Instance covertRecordToInstant(GenericRecord gr) {
    return Instance.builder()
        .basePath(gr.get("basePath").toString())
        .action((Integer) gr.get("action"))
        .applicationId(gr.get("application_id") == null ? null : gr.get("application_id").toString())
        .instant(gr.get("instant").toString())
        .dbName(gr.get("dbName").toString())
        .tableName(gr.get("tableName").toString())
        .executionEngine(gr.get("executionEngine").toString())
        .isDeleted((Integer) gr.get("isDeleted") == 0)
        .status((Integer) gr.get("status"))
        .runTimes((Integer) gr.get("runTimes"))
        .build();
  }

  public static HoodieServiceRecord covertInstantToRecord(Instance instance) {
    return HoodieServiceRecord.newBuilder()
        .setKey(instance.getRecordKey())
        .setBasePath(instance.getBasePath())
        .setAction(instance.getAction())
        .setApplicationId(instance.getApplicationId())
        .setInstant(instance.getInstant())
        .setDbName(instance.getDbName())
        .setTableName(instance.getTableName())
        .setExecutionEngine(instance.getExecutionEngine())
        .setIsDeleted(instance.isDeleted() ? 0 : 1)
        .setStatus(instance.getStatus())
        .setRunTimes(instance.getRunTimes()).build();
  }
}
