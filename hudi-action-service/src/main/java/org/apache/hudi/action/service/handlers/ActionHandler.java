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

import org.apache.hudi.action.service.common.ActionServiceConfig;
import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class ActionHandler implements AutoCloseable {
  private static Logger LOG = LoggerFactory.getLogger(ActionHandler.class);

  protected final Configuration conf;
  protected final FileSystem fileSystem;
  protected final ActionServiceMetadataStore metadataStore;

  private final CompactionHandler compactionHandler;
  private final ClusteringHandler clusteringHandler;
  private final Timer timer;

  public ActionHandler(Configuration conf,
                       ActionServiceConfig config,
                       ActionServiceMetadataStore metadataStore) throws IOException {
    this.conf = conf;
    this.fileSystem = FileSystem.get(conf);
    this.metadataStore = metadataStore;

    this.compactionHandler = new CompactionHandler();
    this.clusteringHandler = new ClusteringHandler();

    this.timer = new Timer("request-commit-thread", true);
    this.timer.scheduleAtFixedRate(new RequestCommitTask(metadataStore), 10 * 1000L, 10 * 1000L);
  }

  public void scheduleCompaction(Instance instance) throws IOException {
    compactionHandler.scheduleCompaction(metadataStore, instance);
  }

  public void removeCompaction(Instance instance) throws IOException {
    compactionHandler.removeCompaction(metadataStore, instance);
  }

  public void scheduleClustering(Instance instance) {
    clusteringHandler.scheduleClustering(metadataStore, instance);
  }

  public void removeClustering(Instance instance) throws IOException {
    clusteringHandler.removeClustering(metadataStore, instance);
  }

  @Override
  public void close() throws Exception {
    this.timer.cancel();
  }

  static class RequestCommitTask extends TimerTask {

    private final ActionServiceMetadataStore metadataStore;

    RequestCommitTask(ActionServiceMetadataStore metadataStore) {
      this.metadataStore = metadataStore;
    }

    @Override
    public void run() {
      metadataStore.commit();
    }
  }
}
