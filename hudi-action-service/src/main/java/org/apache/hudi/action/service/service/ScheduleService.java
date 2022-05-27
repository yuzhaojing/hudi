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

package org.apache.hudi.action.service.service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hudi.action.service.entity.Action;
import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.action.service.entity.InstanceStatus;
import org.apache.hudi.action.service.executor.HoodieClusteringExecutor;
import org.apache.hudi.action.service.executor.HoodieCompactionExecutor;
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;
import org.apache.hudi.action.service.executor.BaseActionExecutor;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleService implements BaseService {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleService.class);

  private ScheduledExecutorService service;
  private ExecutorService executionService;
  private ActionServiceMetadataStore metadataStore;

  public ScheduleService(ExecutorService executionService, ActionServiceMetadataStore metadataStore) {
    this.executionService = executionService;
    this.metadataStore = metadataStore;
  }

  @Override
  public void init() {
    LOG.info("Init service: " + ScheduleService.class.getName());
    this.service = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void startService() {
    LOG.info("Start service: " + ScheduleService.class.getName());
    service.scheduleAtFixedRate(new ScheduleRunnable(), 30, 60, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    LOG.info("Stop service: " + ScheduleService.class.getName());
    if (service != null && !service.isShutdown()) {
      service.shutdown();
    }
  }

  private class ScheduleRunnable implements Runnable {

    @Override
    public void run() {
      try {
        submitReadyTask();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void submitReadyTask() throws IOException {
    int limitSize = executionService.getFreeSize();
    LOG.info("Start submit ready task, limitSize: " + limitSize);
    if (limitSize > 0) {
      List<Instance> readyInstances = metadataStore.getInstances(
          InstanceStatus.SCHEDULED.getStatus(), limitSize);
      LOG.info("Submit ready task, readyInstances: " + readyInstances);
      for (Instance readyInstance : readyInstances) {
        BaseActionExecutor executor = getActionExecutor(readyInstance);
        executionService.submitTask(executor);
      }
    }
  }

  protected BaseActionExecutor getActionExecutor(Instance instance) {
    if (instance.getAction() == Action.COMPACTION.getValue()) {
      return new HoodieCompactionExecutor(instance, metadataStore);
    } else {
      return new HoodieClusteringExecutor(instance, metadataStore);
    }
  }

}
