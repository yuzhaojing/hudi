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

import org.apache.hudi.action.service.common.ActionServiceConfig;
import org.apache.hudi.action.service.entity.Action;
import org.apache.hudi.action.service.entity.Instance;
import org.apache.hudi.action.service.entity.InstanceStatus;
import org.apache.hudi.action.service.handlers.ActionHandler;
import org.apache.hudi.action.service.metadata.ActionServiceMetadataStore;
import org.apache.hudi.action.service.util.InstanceUtil;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.ActionServiceClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.BadRequestResponse;
import io.javalin.Context;
import io.javalin.Handler;
import io.javalin.Javalin;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Main REST Handler class that handles and delegates calls to action relevant handlers.
 */
public class RequestHandler {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

  private final Javalin app;
  private final ActionHandler actionHandler;
  private final Registry metricsRegistry = Registry.getRegistry("ActionService");

  public RequestHandler(Javalin app,
                        Configuration conf,
                        ActionServiceConfig config,
                        ActionServiceMetadataStore metadataStore) throws IOException {
    this.app = app;
    this.actionHandler = new ActionHandler(conf, config, metadataStore);
  }

  public void register() {
    registerCompactionAPI();
    registerClusteringAPI();
  }

  private void writeValueAsString(Context ctx, Object obj) throws JsonProcessingException {
    boolean prettyPrint = ctx.queryParam("pretty") != null;
    long beginJsonTs = System.currentTimeMillis();
    String result =
        prettyPrint ? OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj) : OBJECT_MAPPER.writeValueAsString(obj);
    long endJsonTs = System.currentTimeMillis();
    LOG.debug("Jsonify TimeTaken=" + (endJsonTs - beginJsonTs));
    ctx.result(result);
  }

  /**
   * Register Compaction API calls.
   */
  private void registerCompactionAPI() {
    app.get(ActionServiceClient.SCHEDULE_COMPACTION, new DefaultHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(ActionServiceClient.BASEPATH_PARAM).getValue())
          .dbName(ctx.validatedQueryParam(ActionServiceClient.DATABASE_NAME_PARAM).getValue())
          .tableName(ctx.validatedQueryParam(ActionServiceClient.TABLE_NAME_PARAM).getValue())
          .action(Action.COMPACTION.getValue())
          .instant(ctx.validatedQueryParam(ActionServiceClient.INSTANT_PARAM).getValue())
          .executionEngine(ctx.validatedQueryParam(ActionServiceClient.EXECUTION_ENGINE).getValue())
          .owner(ctx.validatedQueryParam(ActionServiceClient.OWNER).getValue())
          .queue(ctx.validatedQueryParam(ActionServiceClient.QUEUE).getValue())
          .status(InstanceStatus.SCHEDULED.getStatus())
          .build();
      InstanceUtil.checkArgument(instance);
      actionHandler.scheduleCompaction(instance);
    }));

    app.get(ActionServiceClient.REMOVE_COMPACTION, new DefaultHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(ActionServiceClient.BASEPATH_PARAM).getValue())
          .dbName(ctx.validatedQueryParam(ActionServiceClient.DATABASE_NAME_PARAM).getValue())
          .tableName(ctx.validatedQueryParam(ActionServiceClient.TABLE_NAME_PARAM).getValue())
          .instant(ctx.validatedQueryParam(ActionServiceClient.INSTANT_PARAM).getValue())
          .status(InstanceStatus.INVALID.getStatus())
          .isDeleted(true)
          .build();
      actionHandler.removeCompaction(instance);
    }));
  }

  /**
   * Register Compaction API calls.
   */
  private void registerClusteringAPI() {
    app.get(ActionServiceClient.SCHEDULE_CLUSTERING, new DefaultHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(ActionServiceClient.BASEPATH_PARAM).getValue())
          .dbName(ctx.validatedQueryParam(ActionServiceClient.DATABASE_NAME_PARAM).getValue())
          .tableName(ctx.validatedQueryParam(ActionServiceClient.TABLE_NAME_PARAM).getValue())
          .action(Action.CLUSTERING.getValue())
          .instant(ctx.validatedQueryParam(ActionServiceClient.INSTANT_PARAM).getValue())
          .executionEngine(ctx.validatedQueryParam(ActionServiceClient.EXECUTION_ENGINE).getValue())
          .owner(ctx.validatedQueryParam(ActionServiceClient.OWNER).getValue())
          .queue(ctx.validatedQueryParam(ActionServiceClient.QUEUE).getValue())
          .status(InstanceStatus.SCHEDULED.getStatus())
          .build();
      InstanceUtil.checkArgument(instance);
      actionHandler.scheduleClustering(instance);
    }));

    app.get(ActionServiceClient.REMOVE_CLUSTERING, new DefaultHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(ActionServiceClient.BASEPATH_PARAM).getValue())
          .dbName(ctx.validatedQueryParam(ActionServiceClient.DATABASE_NAME_PARAM).getValue())
          .tableName(ctx.validatedQueryParam(ActionServiceClient.TABLE_NAME_PARAM).getValue())
          .instant(ctx.validatedQueryParam(ActionServiceClient.INSTANT_PARAM).getValue())
          .status(InstanceStatus.INVALID.getStatus())
          .isDeleted(true)
          .build();
      actionHandler.removeClustering(instance);
    }));
  }

  /**
   * Used for logging and performing refresh check.
   */
  private class DefaultHandler implements Handler {

    private final Handler handler;

    DefaultHandler(Handler handler) {
      this.handler = handler;
    }

    @Override
    public void handle(@NotNull Context context) throws Exception {
      boolean success = true;
      long beginTs = System.currentTimeMillis();
      long handleTimeTaken = 0;
      try {
        long handleBeginMs = System.currentTimeMillis();
        handler.handle(context);
        long handleEndMs = System.currentTimeMillis();
        handleTimeTaken = handleEndMs - handleBeginMs;
      } catch (RuntimeException re) {
        success = false;
        if (re instanceof BadRequestResponse) {
          LOG.warn("Bad request response due to client view behind server view. " + re.getMessage());
        } else {
          LOG.error("Got runtime exception servicing request " + context.queryString(), re);
        }
        throw re;
      } finally {
        long endTs = System.currentTimeMillis();
        long timeTakenMillis = endTs - beginTs;
        metricsRegistry.add("TOTAL_API_TIME", timeTakenMillis);
        metricsRegistry.add("TOTAL_HANDLE_TIME", handleTimeTaken);
        metricsRegistry.add("TOTAL_API_CALLS", 1);

        LOG.debug(String.format(
            "TimeTakenMillis[Total=%d, handle=%d], Success=%s, Query=%s, Host=%s",
            timeTakenMillis, handleTimeTaken, success, context.queryString(), context.host()));
      }
    }
  }
}
