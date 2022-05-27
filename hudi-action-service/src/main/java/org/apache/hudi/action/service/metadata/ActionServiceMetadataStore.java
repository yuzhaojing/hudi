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

package org.apache.hudi.action.service.metadata;

import java.io.IOException;
import java.util.List;

import org.apache.hudi.action.service.common.ActionServiceConfig;
import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieMetadataMetrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public abstract class ActionServiceMetadataStore {

  private static final Logger LOG = LogManager.getLogger(ActionServiceMetadataStore.class);

  protected Option<HoodieMetadataMetrics> metrics;
  protected final ActionServiceConfig serviceConfig;
  protected final Configuration hadoopConf;

  /**
   * Hudi backed table metadata writer.
   */
  public ActionServiceMetadataStore(ActionServiceConfig serviceConfig, Configuration hadoopConf, Boolean isMetricsOn) {
    this.serviceConfig = serviceConfig;
    this.hadoopConf = hadoopConf;
    initRegistry(isMetricsOn);
    init();
  }

  /**
   * register meta record.
   * @param instance
   */
  public abstract void saveInstance(Instance instance);

  public abstract void commit();

  public abstract void init();

  public abstract Instance getInstance(Instance instance) throws IOException;

  public abstract List<Instance> getInstances(int status, int limit) throws IOException;

  protected void initRegistry(boolean isMetricsOn) {
    if (isMetricsOn) {
      Registry registry = Registry.getRegistry("HoodieActionMetadata");
      this.metrics = Option.of(new HoodieMetadataMetrics(registry));
    } else {
      this.metrics = Option.empty();
    }
  }
}
