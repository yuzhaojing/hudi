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

package org.apache.hudi.action.service.handlers;

import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * REST Handler servicing clustering requests.
 */
public class ClusteringHandler {
  private static Logger LOG = LoggerFactory.getLogger(ClusteringHandler.class);

  public void scheduleClustering(ActionServiceMetadataStore metadataStore, Instance instance) {
    LOG.info("Start register compaction instance: " + instance.getIdentifier());
    metadataStore.saveInstance(instance);
  }

  public void removeClustering(ActionServiceMetadataStore metadataStore, Instance instance) throws IOException {
    LOG.info("Start remove clustering instance: " + instance.getIdentifier());
    // 1. check instance exist
    Instance result = metadataStore.getInstance(instance);
    if (result == null) {
      throw new RuntimeException("Instance not exist: " + instance);
    }
    // 2. update instance
    metadataStore.saveInstance(instance);
  }
}
