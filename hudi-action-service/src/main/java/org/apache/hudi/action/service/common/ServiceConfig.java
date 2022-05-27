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

package org.apache.hudi.action.service.common;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceConfig extends Properties {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceConfig.class);

  private static final ServiceConfig CONFIG = new ServiceConfig();

  /**
   * Constructor.
   */
  private ServiceConfig() {}

  public String getString(ServiceConfVars confVars) {
    return this.getProperty(confVars.key(), confVars.defVal());
  }

  public void setString(ServiceConfVars confVars, String value) {
    this.setProperty(confVars.key(), value);
  }

  public Boolean getBool(ServiceConfVars confVars) {
    return Boolean.valueOf(this.getProperty(confVars.key(), confVars.defVal()));
  }

  public int getInt(ServiceConfVars confVars) {
    return Integer.parseInt(this.getProperty(confVars.key(), confVars.defVal()));
  }

  public static ServiceConfig getInstance() {
    return CONFIG;
  }

  public enum ServiceConfVars {
    MetadataStoreClass("metadata.store.class", "org.apache.hudi.action.service.metadata.impl.HoodieActionServiceMetadataStore"),
    MetricsEnable("metrics.enable", "true"),
    RetryTimes("retry.times","5"),
    SparkYarnPriority("spark.yarn.priority","9"),
    SparkParallelism("spark.parallelism","1"),
    SparkMaster("spark.master","local[1]"),
    ExecutorMemory("executor.memory","20g"),
    ExecutorMemoryOverhead("executor.memory.overhead","5g"),
    ExecutorCores("executor.cores","1"),
    MinExecutors("min.executors","5"),
    MaxExecutors("max.executors","1000"),
    CoreExecuteSize("core.execute.size", "50"),
    MaxExecuteSize("max.execute.size", "100");

    private final String key;
    private final String defaultVal;

    ServiceConfVars(String key, String defaultVal) {
      this.key = key;
      this.defaultVal = defaultVal;
    }

    public String key() {
      return this.key;
    }

    public String defVal() {
      return this.defaultVal;
    }
  }

}
