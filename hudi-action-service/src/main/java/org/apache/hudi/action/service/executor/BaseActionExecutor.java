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

import org.apache.hudi.action.service.common.ServiceContext;
import org.apache.hudi.action.service.entity.Instance;

import java.util.Map;

public abstract class BaseActionExecutor implements Runnable {

  protected Instance instance;

  public BaseActionExecutor(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void run() {
    ServiceContext.addRunningInstance(instance.getRecordKey(), getThreadIdentifier());
    try {
      execute();
    } finally {
      ServiceContext.removeRunningInstance(instance.getRecordKey());
    }
  }

  public abstract void execute();

  public abstract boolean doExecute() throws Exception;

  public abstract String getJobName(Instance instance);

  public abstract Map<String, String> getJobParams(Instance instance);

  public String getThreadIdentifier() {
    return Thread.currentThread().getId() + "." + Thread.currentThread().getName() + "."
        + Thread.currentThread().getState();
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ", instance: " + instance.getIdentifier();
  }

}
