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

package org.apache.hudi.table.management.executor.submitter;

import org.apache.hudi.table.management.entity.Instance;
import org.apache.hudi.table.management.exception.HoodieTableManagementException;
import org.apache.hudi.table.management.store.impl.InstanceService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public abstract class ExecutionEngine {

  private static final Logger LOG = LogManager.getLogger(ExecutionEngine.class);

  protected final InstanceService instanceDao;

  public ExecutionEngine(InstanceService instanceDao) {
    this.instanceDao = instanceDao;
  }

  public void execute(String jobName, Instance instance) throws HoodieTableManagementException {
    try {
      LOG.info("Submitting instance {}:{}", jobName, instance.getIdentifier());
      launchJob(jobName, instance);
    } catch (Exception e) {
      throw new HoodieTableManagementException("Failed submit instance " + instance.getIdentifier(), e);
    }
  }

  public abstract void launchJob(String jobName, Instance instance);

  public abstract Map<String, String> getJobParams(Instance instance);
}
