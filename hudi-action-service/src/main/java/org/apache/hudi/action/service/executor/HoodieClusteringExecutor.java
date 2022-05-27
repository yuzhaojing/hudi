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
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;

import java.util.Map;

public class HoodieClusteringExecutor extends HoodieBaseActionExecutor {

  public HoodieClusteringExecutor(Instance instance, ActionServiceMetadataStore metadataStore) {
    super(instance, metadataStore);
  }

  @Override
  public boolean doExecute() {
    return true;
  }

  @Override
  public String getJobName(Instance instance) {
    return null;
  }

  @Override
  public Map<String, String> getJobParams(Instance instance) {
    Map<String, String> tqsParams = super.getJobParams(instance);
    tqsParams.put("cluster.instance", instance.getInstant());
    return tqsParams;
  }

}
