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

package org.apache.hudi.action.service;

import com.beust.jcommander.JCommander;
import io.javalin.Javalin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hudi.action.service.common.ActionServiceConfig;
import org.apache.hudi.action.service.common.ServiceConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;
import org.apache.hudi.action.service.service.BaseService;
import org.apache.hudi.action.service.service.ExecutorService;
import org.apache.hudi.action.service.service.MonitorService;
import org.apache.hudi.action.service.service.ScheduleService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A standalone action service.
 */
public class ActionService {

  private static final Logger LOG = LoggerFactory.getLogger(ActionService.class);
  private static final int START_SERVICE_MAX_RETRIES = 16;

  private int serverPort;
  private final Configuration conf;
  private final ActionServiceConfig config;
  private final transient FileSystem fs;
  private transient Javalin app = null;
  private List<BaseService> services;
  private ActionServiceMetadataStore metadataStore;

  public int getServerPort() {
    return serverPort;
  }

  public ActionService(int serverPort, Configuration conf, ActionServiceConfig config)
      throws IOException {
    this.config = config;
    this.conf = FSUtils.prepareHadoopConf(conf);
    this.fs = FileSystem.get(conf);
    this.serverPort = serverPort;
    this.metadataStore = initMetadataStore();
  }

  public ActionService(ActionServiceConfig config) throws IOException {
    this(config.serverPort, new Configuration(), config);
  }

  private int startServiceOnPort(int port) throws IOException {
    if (!(port == 0 || (1024 <= port && port < 65536))) {
      throw new IllegalArgumentException(String.format("startPort should be between 1024 and 65535 (inclusive), "
          + "or 0 for a random free port. but now is %s.", port));
    }
    for (int attempt = 0; attempt < START_SERVICE_MAX_RETRIES; attempt++) {
      // Returns port to try when trying to bind a service. Handles wrapping and skipping privileged ports.
      int tryPort = port == 0 ? port : (port + attempt - 1024) % (65536 - 1024) + 1024;
      try {
        app.start(tryPort);
        return app.port();
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("Failed to bind to")) {
          if (tryPort == 0) {
            LOG.warn("Action server could not bind on a random free port.");
          } else {
            LOG.warn(String.format("Action server could not bind on port %d. "
                + "Attempting port %d + 1.", tryPort, tryPort));
          }
        } else {
          LOG.warn(String.format("Action server start failed on port %d. Attempting port %d + 1.", tryPort, tryPort), e);
        }
      }
    }
    throw new IOException(String.format("Action server start failed on port %d, after retry %d times", port, START_SERVICE_MAX_RETRIES));
  }

  public void startService() throws IOException {
    this.metadataStore = (ActionServiceMetadataStore) ReflectionUtils
        .loadClass(ServiceConfig.getInstance()
                .getString(ServiceConfig.ServiceConfVars.MetadataStoreClass),
            new Class[] {ActionServiceConfig.class, Configuration.class, Boolean.class},
            config, conf,
            ServiceConfig.getInstance().getBool(ServiceConfig.ServiceConfVars.MetricsEnable));
    app = Javalin.create();
    RequestHandler requestHandler = new RequestHandler(app, conf, config, metadataStore);
    app.get("/", ctx -> ctx.result("Hello World"));
    requestHandler.register();
    int realServerPort = startServiceOnPort(serverPort);
    LOG.info("Starting action server on port :" + realServerPort);
    this.serverPort = realServerPort;
    registerService();
    initAndStartRegisterService();
  }

  private ActionServiceMetadataStore initMetadataStore() {
    ActionServiceMetadataStore metadataStore = (ActionServiceMetadataStore) ReflectionUtils
        .loadClass(ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.MetadataStoreClass),
            new Class[] {ActionServiceConfig.class, Configuration.class, Boolean.class}, config, conf,
            ServiceConfig.getInstance().getBool(ServiceConfig.ServiceConfVars.MetricsEnable));
    metadataStore.init();
    return metadataStore;
  }

  private void registerService() {
    services = new ArrayList<>();
    ExecutorService executorService = new ExecutorService();
    services.add(executorService);
    services.add(new ScheduleService(executorService, metadataStore));
    services.add(new MonitorService());
  }

  private void initAndStartRegisterService() {
    for (BaseService service : services) {
      service.init();
      service.startService();
    }
  }

  private void stopRegisterService() {
    for (BaseService service : services) {
      service.stop();
    }
  }

  public void run() throws IOException {
    startService();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println(
                      "*** shutting down action service since JVM is shutting down");
                  try {
                    ActionService.this.stop();
                  } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                  }
                  System.err.println("*** action service shut down");
                }));
  }

  /**
   * Stop serving requests and shutdown resources.
   */
  public void stop() throws InterruptedException {
    LOG.info("Stopping Action Service");
    this.app.stop();
    this.app = null;
    stopRegisterService();
    LOG.info("Stopped Action Service");
  }

  public Configuration getConf() {
    return conf;
  }

  public FileSystem getFs() {
    return fs;
  }

  public static void main(String[] args) throws Exception {
    final ActionServiceConfig cfg = new ActionServiceConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help) {
      cmd.usage();
      System.exit(1);
    }
    ActionService service = new ActionService(cfg);
    service.run();
  }
}
