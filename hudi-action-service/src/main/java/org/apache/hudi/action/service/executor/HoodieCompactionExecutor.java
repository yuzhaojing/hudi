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

package org.apache.hudi.action.service.executor;

import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.action.service.entity.InstanceStatus;
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;
import org.apache.hudi.action.service.executor.engine.ExecutionEngine;
import org.apache.hudi.action.service.executor.engine.SparkEngine;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;

public class HoodieCompactionExecutor extends HoodieBaseActionExecutor {

  private static final Logger LOG = LogManager.getLogger(HoodieCompactionExecutor.class);

  public static final String COMPACT_JOB_NAME = "Compact %s.%s %s";
  public static final ExecutionEngine engine = new SparkEngine();

  public HoodieCompactionExecutor(Instance instance, ActionServiceMetadataStore metadataStore) {
    super(instance, metadataStore);
  }

  @Override
  public boolean doExecute() {
    String jobName = getJobName(instance);
    try {
      LOG.info("Start exec : " + jobName);
      instance.setStatus(InstanceStatus.RUNNING.getStatus());
      metadataStore.saveInstance(instance);
      int exitCode = engine.execute(jobName, instance);
      if (exitCode != 0) {
        LOG.warn("Failed to run compaction for " + instance.getInstant());
        return false;
      }

      LOG.info("Compaction successfully completed for " + instance.getInstant());
      return true;
    } catch (Exception e) {
      LOG.error("Fail exec : " + jobName + ", errMsg: ", e);
      throw new RuntimeException("Fail exec : " + jobName);
    }
  }

  @Override
  public String getJobName(Instance instance) {
    return String.format(COMPACT_JOB_NAME, instance.getDbName(), instance.getTableName(),
        instance.getInstant());
  }

  @Override
  public Map<String, String> getJobParams(Instance instance) {
    Map<String, String> tqsParams = super.getJobParams(instance);
    tqsParams.put("compact.instance", instance.getInstant());
    return tqsParams;
  }

}
