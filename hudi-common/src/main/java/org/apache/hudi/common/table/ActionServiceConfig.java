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

package org.apache.hudi.common.table;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

/**
 * File System View Storage Configurations.
 */
public class ActionServiceConfig extends HoodieConfig {
  
  public static final ConfigProperty<String> ACTION_SERVICE_HOST = ConfigProperty
      .key("hoodie.action.service.host")
      .defaultValue("localhost")
      .withDocumentation("Action service host.");

  public static final ConfigProperty<Integer> ACTION_SERVICE_PORT = ConfigProperty
      .key("hoodie.action.service.port")
      .defaultValue(26755)
      .withDocumentation("Action service port.");

  public static final ConfigProperty<String> ACTION_SERVICE_CLIENT_OWNER = ConfigProperty
      .key("hoodie.action.service.client.owner")
      .defaultValue("default")
      .withDocumentation("Action service job owner.");

  public static final ConfigProperty<String> ACTION_SERVICE_CLIENT_QUEUE = ConfigProperty
      .key("hoodie.action.service.client.queue")
      .defaultValue("default")
      .withDocumentation("Action service job queue.");

  public static final ConfigProperty<Integer> ACTION_SERVICE_TIMEOUT_SECS = ConfigProperty
      .key("hoodie.action.server.timeout.secs")
      .defaultValue(300)
      .withDocumentation("Action service connect timeout.");

  public String getActionServiceHost() {
    return getString(ACTION_SERVICE_HOST).toUpperCase(Locale.ROOT);
  }

  public Integer getActionServicePort() {
    return getInt(ACTION_SERVICE_PORT);
  }

  public String getActionServiceClientOwner() {
    return getString(ACTION_SERVICE_CLIENT_OWNER).toUpperCase(Locale.ROOT);
  }

  public String getActionServiceClientQueue() {
    return getString(ACTION_SERVICE_CLIENT_QUEUE).toUpperCase(Locale.ROOT);
  }

  public Integer getActionServiceTimeoutSecs() {
    return getInt(ACTION_SERVICE_TIMEOUT_SECS);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private ActionServiceConfig() {
    super();
  }

  /**
   * The builder used to build {@link ActionServiceConfig}.
   */
  public static class Builder {

    private final ActionServiceConfig actionServiceConfig = new ActionServiceConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.actionServiceConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.actionServiceConfig.getProps().putAll(props);
      return this;
    }

    public Builder withActionServiceHost(String actionServerHost) {
      actionServiceConfig.setValue(ACTION_SERVICE_HOST, String.valueOf(actionServerHost));
      return this;
    }

    public Builder withActionServicePort(Integer actionServerPort) {
      actionServiceConfig.setValue(ACTION_SERVICE_PORT, actionServerPort.toString());
      return this;
    }

    public Builder withActionServiceClientOwner(String owner) {
      actionServiceConfig.setValue(ACTION_SERVICE_CLIENT_OWNER, owner);
      return this;
    }

    public Builder withActionServiceClientQueue(String queue) {
      actionServiceConfig.setValue(ACTION_SERVICE_CLIENT_QUEUE, queue);
      return this;
    }

    public Builder withActionServiceTimeoutSecs(Integer timeoutSecs) {
      actionServiceConfig.setValue(ACTION_SERVICE_TIMEOUT_SECS, timeoutSecs.toString());
      return this;
    }

    public ActionServiceConfig build() {
      actionServiceConfig.setDefaults(ActionServiceConfig.class.getName());
      return actionServiceConfig;
    }
  }

}
