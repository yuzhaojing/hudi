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

package org.apache.hudi.client.table.manager;

import org.apache.hudi.common.config.HoodieTableManagerConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HoodieTableManagerClient {

  private static final String BASE_URL = "/v1/hoodie/serivce";

  public static final String REGISTER = String.format("%s/%s", BASE_URL, "register");

  public static final String SUBMIT_COMPACTION = String.format("%s/%s", BASE_URL, "compact/submit");
  public static final String REMOVE_COMPACTION = String.format("%s/%s", BASE_URL, "compact/remove");

  public static final String SUBMIT_CLUSTERING = String.format("%s/%s", BASE_URL, "cluster/submit");
  public static final String REMOVE_CLUSTERING = String.format("%s/%s", BASE_URL, "cluster/remove");

  public static final String SUBMIT_CLEAN = String.format("%s/%s", BASE_URL, "clean/submit");
  public static final String REMOVE_CLEAN = String.format("%s/%s", BASE_URL, "clean/remove");

  public static final String DATABASE_NAME_PARAM = "db_name";
  public static final String TABLE_NAME_PARAM = "table_name";
  public static final String BASEPATH_PARAM = "basepath";
  public static final String INSTANT_PARAM = "instant";
  public static final String USERNAME = "username";
  public static final String CLUSTER = "cluster";
  public static final String QUEUE = "queue";
  public static final String RESOURCE = "resource";
  public static final String PARALLELISM = "parallelism";
  public static final String EXTRA_PARAMS = "extra_params";
  public static final String EXECUTION_ENGINE = "execution_engine";

  private final HoodieTableManagerConfig config;
  private final String host;
  private final int port;
  private final String basePath;
  private final String dbName;
  private final String tableName;

  private static int failTimes;

  private static final Logger LOG = LogManager.getLogger(HoodieTableManagerClient.class);

  public HoodieTableManagerClient(HoodieTableMetaClient metaClient, HoodieTableManagerConfig config) {
    this.basePath = metaClient.getBasePathV2().toString();
    this.dbName = metaClient.getTableConfig().getDatabaseName();
    this.tableName = metaClient.getTableConfig().getTableName();
    this.host = config.getTableManagerHost();
    this.port = config.getTableManagerPort();
    this.config = config;
  }

  private String executeRequest(String requestPath, String instantRange) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(host).setPort(port).setPath(requestPath).setScheme("http");

    Map<String, String> queryParameters = getParamsWithAdditionalParams(
        new String[] {DATABASE_NAME_PARAM, TABLE_NAME_PARAM, INSTANT_PARAM, USERNAME, QUEUE, RESOURCE, PARALLELISM, EXTRA_PARAMS},
        new String[] {dbName, tableName, instantRange, config.getDeployUsername(), config.getDeployQueue(),
            config.getDeployResource(), String.valueOf(config.getDeployParallelism()), config.getDeployExtraParams()});
    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    LOG.info("Sending request to table management service : (" + url + ")");
    Response response;
    int timeout = this.config.getConnectionTimeout() * 1000; // msec
    int requestRetryLimit = config.getConnectionRetryLimit();
    int retry = 0;

    while (retry < requestRetryLimit) {
      try {
        response = Request.Get(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        return response.returnContent().asString();
      } catch (IOException e) {
        retry++;
        LOG.warn(String.format("Failed request to server %s, will retry for %d times", url, requestRetryLimit - retry), e);
        if (requestRetryLimit == retry) {
          throw e;
        }
      }

      try {
        TimeUnit.SECONDS.sleep(config.getConnectionRetryDelay());
      } catch (InterruptedException e) {
        // ignore
      }
    }

    throw new IOException(String.format("Failed request to table management service %s after retry %d times", url, requestRetryLimit));
  }

  private Map<String, String> getParamsWithAdditionalParams(String[] paramNames, String[] paramVals) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    ValidationUtils.checkArgument(paramNames.length == paramVals.length);
    for (int i = 0; i < paramNames.length; i++) {
      paramsMap.put(paramNames[i], paramVals[i]);
    }
    return paramsMap;
  }

  public void submitCompaction(List<String> instantsToSubmit) {
    try {
      executeRequest(SUBMIT_COMPACTION, StringUtils.join(instantsToSubmit.toArray(new String[0]), ","));
    } catch (Exception e) {
      LOG.error("Failed to schedule compaction to service", e);
      failTimes++;
      checkTolerableSchedule(e);
      return;
    }

    failTimes = 0;
  }

  public void submitClean(List<String> instantsToSubmit) {
    try {
      executeRequest(SUBMIT_CLEAN, StringUtils.join(instantsToSubmit.toArray(new String[0]), ","));
    } catch (Exception e) {
      LOG.error("Failed to schedule clean to service", e);
      failTimes++;
      checkTolerableSchedule(e);
      return;
    }

    failTimes = 0;
  }

  public void submitClustering(List<String> instantsToSubmit) {
    try {
      executeRequest(SUBMIT_CLUSTERING, StringUtils.join(instantsToSubmit.toArray(new String[0]), ","));
    } catch (Exception e) {
      LOG.error("Failed to schedule clustering to service", e);
      failTimes++;
      checkTolerableSchedule(e);
      return;
    }

    failTimes = 0;
  }

  private void checkTolerableSchedule(Exception e) {
    if (failTimes > config.getConnectionTolerableNum()) {
      failTimes = 0;
      throw new HoodieException("The number of consecutive failures [" + failTimes + "] "
          + "exceeds the number of tolerances [" + config.getConnectionTolerableNum() + "]", e);
    }
  }
}
