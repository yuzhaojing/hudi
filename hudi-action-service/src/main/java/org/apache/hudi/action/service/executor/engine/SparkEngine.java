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

package org.apache.hudi.action.service.executor.engine;

import org.apache.hudi.action.service.common.ServiceConfig;
import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.cli.commands.SparkMain;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.util.StringUtils;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;

import java.io.File;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Objects;

public class SparkEngine extends ExecutionEngine {

  public Integer execute(String jobName, Instance instance) throws Exception {
    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
    SparkLauncher sparkLauncher = initLauncher(sparkPropertiesPath);

    String master = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkMaster);
    String sparkMemory = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.ExecutorMemory);
    String parallelism = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkParallelism);
    String retry = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.RetryTimes);

    sparkLauncher.addAppArgs("COMPACT_RUN", master, sparkMemory, instance.getBasePath(),
        instance.getTableName(), instance.getInstant(), parallelism, "", retry, "");

    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    return process.waitFor();
  }

  public static SparkLauncher initLauncher(String propertiesFile) throws URISyntaxException {
    String currentJar = new File(SparkUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath())
        .getAbsolutePath();

    HashMap<String, String> env = new HashMap<>(System.getenv());
    env.put("SPARK_HOME", "/usr/local/spark-3.1.2-bin-hadoop2.7");

    SparkLauncher sparkLauncher =
        new SparkLauncher(env).setAppResource(currentJar).setMainClass(SparkMain.class.getName());

    if (!StringUtils.isNullOrEmpty(propertiesFile)) {
      sparkLauncher.setPropertiesFile(propertiesFile);
    }

    File libDirectory = new File(new File(currentJar).getParent(), "lib");
    for (String library : Objects.requireNonNull(libDirectory.list())) {
      sparkLauncher.addJar(new File(libDirectory, library).getAbsolutePath());
    }
    return sparkLauncher;
  }
}
