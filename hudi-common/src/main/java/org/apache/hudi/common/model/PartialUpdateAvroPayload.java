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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldVal;

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
        if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
          // pick the payload with greatest ordering value
          return oldValue;
        } else {
          return this;
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to preCombine in OverwriteWithLatestAvroPayload.", e);
    }

    GenericRecord oldRecord = (GenericRecord) oldRecordOption.get();
    GenericRecord currentRecord = (GenericRecord) currentRecordOption.get();

    GenericRecord record = (GenericRecord) partialUpdate(oldRecord, currentRecord, schema, properties).get();
    return new PartialUpdateAvroPayload(record, orderingVal.compareTo(oldValue.orderingVal) > 0 ? orderingVal : oldValue.orderingVal);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {

    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord insertRecord = (GenericRecord) recordOption.get();
    GenericRecord currentRecord = (GenericRecord) currentValue;
    if (isDeleteRecord(insertRecord)) {
      return Option.empty();
    }

    return partialUpdate(insertRecord, currentRecord, schema, properties);
  }

  private Option<IndexedRecord> partialUpdate(GenericRecord oldRecord, GenericRecord currentRecord, Schema schema, Properties properties) {
    List<Schema.Field> fields = schema.getFields();
    fields.forEach(field -> {
      Object value = currentRecord.get(field.name());
      value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
      Object defaultValue = field.defaultVal();
      if (!overwriteField(value, defaultValue) && needUpdatingPersistedRecord(oldRecord, currentRecord, properties)) {
        oldRecord.put(field.name(), value);
      }
    });

    System.out.println("partialUpdateRecord = " + oldRecord + " currentRecord = " + currentRecord);
    return Option.of(oldRecord);
  }

  @Override
  public Boolean overwriteField(Object value, Object defaultValue) {
    return super.overwriteField(value, defaultValue) || (value == null && Objects.equals(defaultValue, JsonProperties.NULL_VALUE));
  }

  protected boolean needUpdatingPersistedRecord(IndexedRecord currentValue,
                                                IndexedRecord incomingRecord, Properties properties) {
    /*
     * Combining strategy here returns currentValue on disk if incoming record is older.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older than the record in disk, the currentValue
     * in disk is returned (to be rewritten with new commit time).
     *
     * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
     * and need to be dealt with separately.
     */
    Object persistedOrderingVal = getNestedFieldVal((GenericRecord) currentValue,
        properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY), true);
    Comparable incomingOrderingVal = (Comparable) getNestedFieldVal((GenericRecord) incomingRecord,
        properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY), false);
    return persistedOrderingVal == null || ((Comparable) persistedOrderingVal).compareTo(incomingOrderingVal) <= 0;
  }
}
