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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieRemoteException;

import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ActionServiceClient {

  private static final String BASE_URL = "/v1/hoodie/serivce";

  public static final String SCHEDULE_COMPACTION = String.format("%s/%s", BASE_URL, "compact/schedule");
  public static final String REMOVE_COMPACTION = String.format("%s/%s", BASE_URL, "compact/remove");

  public static final String SCHEDULE_CLUSTERING = String.format("%s/%s", BASE_URL, "cluster/schedule");
  public static final String REMOVE_CLUSTERING = String.format("%s/%s", BASE_URL, "cluster/remove");

  public static final String DATABASE_NAME_PARAM = "db_name";
  public static final String TABLE_NAME_PARAM = "table_name";
  public static final String BASEPATH_PARAM = "basepath";
  public static final String INSTANT_PARAM = "instant";
  public static final String OWNER = "owner";
  public static final String QUEUE = "queue";
  public static final String EXECUTION_ENGINE = "execution_engine";

  private final int requestRetryLimit = 3;

  private final String serverHost;
  private final int serverPort;
  private final String basePath;
  private final int timeoutSecs;

  private static final Logger LOG = LogManager.getLogger(ActionServiceClient.class);

  public ActionServiceClient(String server, int port, String basePath) {
    this(server, port, basePath, 300);
  }

  public ActionServiceClient(String server, int port, String basePath, int timeoutSecs) {
    this.basePath = basePath;
    this.serverHost = server;
    this.serverPort = port;
    this.timeoutSecs = timeoutSecs;
  }

  private String executeRequest(String requestPath, Map<String, String> queryParameters) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(serverHost).setPort(serverPort).setPath(requestPath).setScheme("http");

    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    LOG.info("Sending request : (" + url + ")");
    Response response;
    int timeout = this.timeoutSecs * 1000; // msec
    int retry = 0;

    while (retry < requestRetryLimit) {
      try {
        response = Request.Get(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        return response.returnContent().asString();
      } catch (IOException e) {
        retry++;
        LOG.error(String.format("Failed request to server %s, will retry for %d times", url, requestRetryLimit - retry), e);
        if (requestRetryLimit == retry) {
          throw e;
        }
      }
    }

    throw new IOException(String.format("Failed request to server %s after retry %d times", url, requestRetryLimit));
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

  public String scheduleCompaction(String db, String tbl, String instant, String owner, String queue) {
    Map<String, String> paramsMap = getParamsWithAdditionalParams(
        new String[] {DATABASE_NAME_PARAM, TABLE_NAME_PARAM, INSTANT_PARAM, OWNER, QUEUE}, new String[] {db, tbl, instant, owner, queue});
    try {
      return executeRequest(SCHEDULE_COMPACTION, paramsMap);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  public String scheduleClustering(String db, String tbl, String instant, String owner, String queue) {
    Map<String, String> paramsMap = getParamsWithAdditionalParams(
        new String[] {DATABASE_NAME_PARAM, TABLE_NAME_PARAM, INSTANT_PARAM, OWNER, QUEUE}, new String[] {db, tbl, instant, owner, queue});
    try {
      return executeRequest(SCHEDULE_CLUSTERING, paramsMap);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }
}
