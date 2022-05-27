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

import org.apache.hudi.action.service.common.ServiceConfig;
import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.action.service.entity.InstanceStatus;
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;

import com.esotericsoftware.minlog.Log;

import java.util.HashMap;
import java.util.Map;

public abstract class HoodieBaseActionExecutor extends BaseActionExecutor {

  protected ActionServiceMetadataStore metadataStore;

  public HoodieBaseActionExecutor(Instance instance, ActionServiceMetadataStore metadataStore) {
    super(instance);
    this.metadataStore = metadataStore;
  }

  @Override
  public void execute() {
    try {
      boolean success = doExecute();
      if (success) {
        instance.setStatus(InstanceStatus.COMPLETED.getStatus());
        Log.info("Success exec instance: " + instance.getIdentifier());
      } else {
        instance.setStatus(InstanceStatus.FAILED.getStatus());
        Log.info("Fail exec instance: " + instance.getIdentifier());
      }
    } catch (Exception e) {
      instance.setStatus(InstanceStatus.FAILED.getStatus());
      Log.error("Fail exec instance: " + instance.getIdentifier() + ", errMsg: ", e);
    }
    metadataStore.saveInstance(instance);
    metadataStore.commit();
  }

  @Override
  public Map<String, String> getJobParams(Instance instance) {
    Map<String, String> params = new HashMap<>();
    params.put("spark.yarn.priority",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkYarnPriority));
    params.put("mapreduce.job.queuename", instance.getFullTableName());
    params.put("spark.dynamicAllocation.maxExecutors",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.MaxExecutors));
    params.put("spark.dynamicAllocation.minExecutors",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.MinExecutors));
    params.put("spark.executor.cores",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.ExecutorCores));
    params.put("spark.executor.memory",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.ExecutorMemory));
    params.put("spark.executor.memoryOverhead", ServiceConfig.getInstance()
        .getString(ServiceConfig.ServiceConfVars.ExecutorMemoryOverhead));
    return params;
  }
}
