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

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

public class ActionServiceConfig {
  @Parameter(names = {"--server-port", "-p"}, description = " Server Port")
  public Integer serverPort = 26755;

  @Parameter(names = {"--meta-table-name"}, description = "Name of the meta table in Hive.")
  public String metaTableName = "action_service_meta";

  @Parameter(names = {"--base-path"},
      description = "Base path for the hoodie meta table. "
          + "(Will be created if did not exist first time around. If exists, expected to be a hoodie table).",
      required = true)
  public String basePath;

  @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
      + "(using the CLI parameter \"--props\") can also be passed command line using this parameter.")
  public List<String> hoodieConfigs = new ArrayList<>();

  @Parameter(names = {"--insert-parallelism"}, description = "Parallelism for inserts to hoodie meta table")
  public Integer parallelism = 1;

  @Parameter(names = {"--help", "-h"})
  public Boolean help = false;
}
