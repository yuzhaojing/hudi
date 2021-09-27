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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * subclass of OverwriteWithLatestAvroPayload used for delta streamer.
 *
 * <ol>
 * <li>preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li>combineAndGetUpdateValue/getInsertValue - overwrite storage for specified fields
 * that doesn't equal defaultValue.
 * </ol>
 */
public class PartialUpdateAvroPayload extends OverwriteNonDefaultsWithLatestAvroPayload {
  public PartialUpdateAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialUpdateAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Schema schema, Properties properties) {
    Option<IndexedRecord> oldRecordOption;
    Option<IndexedRecord> currentRecordOption;
    try {
      oldRecordOption = oldValue.getInsertValue(schema);
      if (!oldRecordOption.isPresent()) {
        return this;
      }

      currentRecordOption = getInsertValue(schema);
      if (!currentRecordOption.isPresent()) {
        return oldValue;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to preCombine in OverwriteWithLatestAvroPayload.", e);
    }

    GenericRecord oldRecord = (GenericRecord) oldRecordOption.get();
    GenericRecord currentRecord = (GenericRecord) currentRecordOption.get();

    GenericRecord record = (GenericRecord) partialUpdate(oldRecord, currentRecord, schema).get();
    return new PartialUpdateAvroPayload(record, orderingVal.compareTo(oldValue.orderingVal) > 0 ? orderingVal : oldValue.orderingVal);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {

    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord insertRecord = (GenericRecord) recordOption.get();
    GenericRecord currentRecord = (GenericRecord) currentValue;
    if (isDeleteRecord(insertRecord)) {
      return Option.empty();
    }

    return partialUpdate(insertRecord, currentRecord, schema);
  }

  private Option<IndexedRecord> partialUpdate(GenericRecord oldRecord, GenericRecord currentRecord, Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    fields.forEach(field -> {
      Object value = oldRecord.get(field.name());
      value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
      Object defaultValue = field.defaultVal();
      if (!overwriteField(value, defaultValue)) {
        currentRecord.put(field.name(), value);
      }
    });
    return Option.of(currentRecord);
  }

  @Override
  public Boolean overwriteField(Object value, Object defaultValue) {
    return super.overwriteField(value, defaultValue) || (value == null && Objects.equals(defaultValue, JsonProperties.NULL_VALUE));
  }
}
