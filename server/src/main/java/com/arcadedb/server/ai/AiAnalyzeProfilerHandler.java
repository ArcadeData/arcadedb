/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ai;

import com.arcadedb.Constants;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.logging.Level;

/**
 * POST /api/v1/ai/analyze-profiler - Sends profiler data to the AI gateway for analysis.
 */
public class AiAnalyzeProfilerHandler extends AbstractServerHttpHandler {
  private final AiConfiguration config;
  private final HttpClient      httpClient;

  public AiAnalyzeProfilerHandler(final HttpServer httpServer, final AiConfiguration config) {
    super(httpServer);
    this.config = config;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    if (!config.isConfigured())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "AI assistant is not configured. Please configure config/ai.json.").toString());

    if (payload == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Request body is required").toString());

    final JSONObject profilerData = payload.getJSONObject("profilerData", null);
    if (profilerData == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Profiler data is required").toString());

    try {
      final JSONObject gatewayRequest = new JSONObject();
      gatewayRequest.put("profilerData", profilerData);
      gatewayRequest.put("hardwareId", AiActivateHandler.getHardwareId());
      gatewayRequest.put("serverVersion", Constants.getVersion());

      final JSONObject gatewayResponse = callGateway(gatewayRequest);

      final JSONObject result = new JSONObject();
      result.put("response", gatewayResponse.getString("response", ""));
      if (gatewayResponse.has("commands"))
        result.put("commands", gatewayResponse.getJSONArray("commands"));

      return new ExecutionResponse(200, result.toString());

    } catch (final SecurityException e) {
      throw e;
    } catch (final AiTokenException e) {
      return new ExecutionResponse(e.getHttpStatus(), e.getJsonResponse());
    } catch (final ConnectException | HttpConnectTimeoutException e) {
      LogManager.instance().log(this, Level.WARNING, "AI gateway unreachable: %s", e.getMessage());
      return new ExecutionResponse(503, new JSONObject()//
          .put("error", "AI service is temporarily unreachable. Please try again later.")//
          .put("code", "gateway_unreachable").toString());
    } catch (final HttpTimeoutException e) {
      LogManager.instance().log(this, Level.WARNING, "AI gateway timeout: %s", e.getMessage());
      return new ExecutionResponse(504, new JSONObject()//
          .put("error", "AI service took too long to respond. Please try again later.")//
          .put("code", "gateway_timeout").toString());
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "AI gateway I/O error: %s", e.getMessage());
      return new ExecutionResponse(503, new JSONObject()//
          .put("error", "AI service is temporarily unavailable. Please try again later.")//
          .put("code", "gateway_unreachable").toString());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error processing profiler analysis request: %s", e.getMessage());
      return new ExecutionResponse(500, new JSONObject()//
          .put("error", "An unexpected error occurred. Please try again later.").toString());
    }
  }

  private JSONObject callGateway(final JSONObject requestBody) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()//
        .uri(URI.create(config.getGatewayUrl() + "/api/analyze-profiler"))//
        .header("Content-Type", "application/json")//
        .header("Authorization", "Bearer " + config.getSubscriptionToken())//
        .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))//
        .timeout(Duration.ofSeconds(120))//
        .build();

    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 401 || response.statusCode() == 403) {
      final JSONObject errBody = new JSONObject(response.body());
      final String code = errBody.getString("code", "token_invalid");
      final String errorMsg = errBody.getString("error", "Invalid or expired subscription token");
      final JSONObject errorResponse = new JSONObject();
      errorResponse.put("error", errorMsg);
      errorResponse.put("code", code);
      throw new AiTokenException(response.statusCode(), errorResponse.toString());
    }

    if (response.statusCode() != 200)
      throw new RuntimeException("Gateway returned status " + response.statusCode() + ": " + response.body());

    return new JSONObject(response.body());
  }
}
