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

import org.apache.hudi.cli.commands.SparkMain;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.table.management.common.ServiceConfig;
import org.apache.hudi.table.management.entity.Instance;
import org.apache.hudi.table.management.entity.InstanceStatus;
import org.apache.hudi.table.management.exception.HoodieTableManagementException;
import org.apache.hudi.table.management.store.impl.InstanceService;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.launcher.SparkAppHandle.State.FINISHED;
import static org.apache.spark.launcher.SparkAppHandle.State.SUBMITTED;

public class SparkEngine extends ExecutionEngine {

  private static final Logger LOG = LogManager.getLogger(SparkEngine.class);

  public SparkEngine(InstanceService instanceDao) {
    super(instanceDao);
  }

  @Override
  public Map<String, String> getJobParams(Instance instance) {
    Map<String, String> sparkParams = new HashMap<>();
    String parallelism = StringUtils.isNullOrEmpty(instance.getParallelism())
        ? ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.MaxExecutors)
        : instance.getParallelism();
    sparkParams.put("spark.dynamicAllocation.maxExecutors", parallelism);
    sparkParams.put("spark.dynamicAllocation.minExecutors",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.MinExecutors));
    sparkParams.put("spark.speculation",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkSpeculation));
    String driverResource;
    String executorResource;
    String resource = instance.getResource().trim();
    if (StringUtils.isNullOrEmpty(resource)) {
      driverResource = ServiceConfig.getInstance()
          .getString(ServiceConfig.ServiceConfVars.DriverMemory);
      executorResource = ServiceConfig.getInstance()
          .getString(ServiceConfig.ServiceConfVars.ExecutorMemory);
    } else {
      String[] resourceArray = resource.split(":");
      if (resourceArray.length == 1) {
        driverResource = resourceArray[0];
        executorResource = resourceArray[0];
      } else if (resourceArray.length == 2) {
        driverResource = resourceArray[0];
        executorResource = resourceArray[1];
      } else {
        throw new RuntimeException(
            "Invalid conf: " + instance.getIdentifier() + ", resource: " + resource);
      }
    }
    sparkParams.put("spark.executor.cores",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.ExecutorCores));
    sparkParams.put("spark.executor.memory", executorResource);
    sparkParams.put("spark.driver.memory", driverResource);
    sparkParams.put("spark.executor.memoryOverhead", ServiceConfig.getInstance()
        .getString(ServiceConfig.ServiceConfVars.ExecutorMemoryOverhead));

    return sparkParams;
  }

  @Override
  public void launchJob(String jobName, Instance instance) throws HoodieTableManagementException {
    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
    SparkLauncher sparkLauncher;
    try {
      sparkLauncher = initLauncher(sparkPropertiesPath, instance);
    } catch (URISyntaxException e) {
      LOG.error("Failed to init spark launcher");
      throw new HoodieTableManagementException("Failed to init spark launcher", e);
    }

    try {
      final boolean[] isFinished = new boolean[1];
      SparkAppHandle sparkAppHandle = sparkLauncher.startApplication(new SparkAppHandle.Listener() {
        @Override
        public void stateChanged(SparkAppHandle handle) {
          LOG.info("****************************");
          LOG.info("State Changed [state={}]", handle.getState());
          LOG.info("AppId={}", handle.getAppId());

          if (handle.getState() == SUBMITTED) {
            LOG.info("Submit job in application id: " + handle.getAppId());
            instance.setApplicationId(handle.getAppId());
            instanceDao.updateExecutionInfo(instance);
          } else if (isCompleted(handle.getState())) {
            isFinished[0] = true;
            LOG.info("Completed job in state: " + handle.getState());
            if (handle.getState() == FINISHED) {
              instance.setStatus(InstanceStatus.COMPLETED.getStatus());
            } else {
              instance.setStatus(InstanceStatus.FAILED.getStatus());
            }
            instanceDao.updateStatus(instance);
          }
        }

        @Override
        public void infoChanged(SparkAppHandle handle) {
          // no OP
        }
      });

      while (!isFinished[0]) {
        TimeUnit.SECONDS.sleep(5);
        LOG.info("Waiting for job {} finished.", jobName);
      }

      LOG.info("Stop job when job is finished.");
      sparkAppHandle.kill();
    } catch (Exception e) {
      LOG.error("Failed to launcher spark process");
      throw new HoodieTableManagementException("Failed to init spark launcher", e);
    }
  }

  private boolean isCompleted(SparkAppHandle.State state) {
    switch (state) {
      case FINISHED:
      case FAILED:
      case KILLED:
      case LOST:
        return true;
    }
    return false;
  }

  private SparkLauncher initLauncher(String propertiesFile, Instance instance) throws URISyntaxException {
    String currentJar = SparkEngine.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    System.out.println("currentJar = " + currentJar);
    Map<String, String> env = System.getenv();
    String master = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkMaster);

    SparkLauncher sparkLauncher =
        new SparkLauncher(env)
            .setDeployMode("cluster")
            .setMaster(master)
            .setAppResource(currentJar)
            .setMainClass(SparkMain.class.getName());

    if (!StringUtils.isNullOrEmpty(propertiesFile)) {
      sparkLauncher.setPropertiesFile(propertiesFile);
    }

    File libDirectory = new File(new File(currentJar).getParent(), "lib");
    // This lib directory may be not required, such as providing libraries through a bundle jar
    if (libDirectory.exists()) {
      Arrays.stream(libDirectory.list()).forEach(library -> {
        if (!library.startsWith("hadoop-hdfs")) {
          sparkLauncher.addJar(new File(libDirectory, library).getAbsolutePath());
        }
      });
    }

    Map<String, String> jobParams = getJobParams(instance);

    for (Map.Entry<String, String> entry : jobParams.entrySet()) {
      sparkLauncher.setConf(entry.getKey(), entry.getValue());
    }

    sparkLauncher.addSparkArg("--queue", instance.getQueue());
    String sparkMemory = jobParams.get("spark.executor.memory");
    String parallelism = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkParallelism);
    String maxRetryNum = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.MaxRetryNum);

    sparkLauncher.addAppArgs("COMPACT_RUN", master, sparkMemory, instance.getBasePath(),
        instance.getTableName(), instance.getInstant(), parallelism, "", maxRetryNum, "");

    return sparkLauncher;
  }
}
