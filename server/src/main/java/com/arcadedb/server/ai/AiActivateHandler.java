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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Enumeration;
import java.util.logging.Level;

/**
 * POST /api/v1/ai/activate - Activates an AI subscription.
 * Collects hardware fingerprint, validates the key against the gateway, and saves to config/ai.json.
 */
public class AiActivateHandler extends AbstractServerHttpHandler {
  private final AiConfiguration config;
  private final HttpClient      httpClient;

  public AiActivateHandler(final HttpServer httpServer, final AiConfiguration config) {
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
    if (payload == null)
      return new ExecutionResponse(400, errorJson("Request body is required"));

    final String subscriptionKey = payload.getString("subscriptionKey", "");
    if (subscriptionKey.isEmpty())
      return new ExecutionResponse(400, errorJson("Subscription key is required"));

    try {
      final String serverVersion = Constants.getVersion();
      final String hardwareId = getHardwareId();
      final String clientIp = getClientIp(exchange);

      // Validate the key against the gateway
      final JSONObject activationRequest = new JSONObject();
      activationRequest.put("subscriptionKey", subscriptionKey);
      activationRequest.put("serverVersion", serverVersion);
      activationRequest.put("hardwareId", hardwareId);

      final HttpRequest request = HttpRequest.newBuilder()//
          .uri(URI.create(config.getGatewayUrl() + "/api/activate"))//
          .header("Content-Type", "application/json")//
          .POST(HttpRequest.BodyPublishers.ofString(activationRequest.toString()))//
          .timeout(Duration.ofSeconds(15))//
          .build();

      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        String errorMsg = "Activation failed";
        try {
          final JSONObject errBody = new JSONObject(response.body());
          errorMsg = errBody.getString("error", errorMsg);
        } catch (final Exception ignored) {
        }
        return new ExecutionResponse(response.statusCode(), errorJson(errorMsg));
      }

      // Activation successful - save to config/ai.json
      config.activate(subscriptionKey, clientIp, hardwareId, serverVersion);

      LogManager.instance().log(this, Level.INFO, "AI subscription activated (user=%s, ip=%s)", user.getName(), clientIp);

      return new ExecutionResponse(200, new JSONObject().put("activated", true).toString());

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "AI activation error: %s", e.getMessage());
      final String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      return new ExecutionResponse(500, errorJson("Activation failed: " + msg));
    }
  }

  /**
   * Generates a hardware fingerprint by hashing MAC addresses + hostname.
   * This provides a stable identifier for the server without exposing raw MAC addresses.
   */
  static String getHardwareId() {
    try {
      final StringBuilder raw = new StringBuilder();

      // Collect all non-loopback MAC addresses
      final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        final NetworkInterface ni = interfaces.nextElement();
        if (ni.isLoopback() || ni.isVirtual())
          continue;
        final byte[] mac = ni.getHardwareAddress();
        if (mac != null) {
          for (final byte b : mac)
            raw.append(String.format("%02x", b));
          raw.append("|");
        }
      }

      // Add hostname for extra uniqueness
      raw.append(InetAddress.getLocalHost().getHostName());

      // Hash it to produce a stable, non-reversible fingerprint
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      final byte[] hash = digest.digest(raw.toString().getBytes());
      final StringBuilder hex = new StringBuilder();
      for (int i = 0; i < 16; i++) // Use first 16 bytes (128 bits) for a shorter ID
        hex.append(String.format("%02x", hash[i]));
      return hex.toString();
    } catch (final Exception e) {
      LogManager.instance().log(AiActivateHandler.class, Level.FINE, "Could not generate hardware ID: %s", e.getMessage());
      return "unknown";
    }
  }

  private static String getClientIp(final HttpServerExchange exchange) {
    // Check for X-Forwarded-For (reverse proxy)
    final String forwarded = exchange.getRequestHeaders().getFirst("X-Forwarded-For");
    if (forwarded != null && !forwarded.isEmpty())
      return forwarded.split(",")[0].trim();
    return exchange.getSourceAddress().getAddress().getHostAddress();
  }

  private static String errorJson(final String message) {
    return new JSONObject().put("error", message).toString();
  }
}
