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
package com.arcadedb.server.support;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.logging.Level;

/**
 * The server-side support API used by the Support tab of Studio ({@code /api/v1/server/support/...}). Every route is for the
 * server administrator (root) only, like the other server administration routes. The browser never sees the Client key: the
 * server holds it and proxies the portal.
 * <p>
 * Failures answer {@code {"error": <code>, "detail": <message>, "message": <message>}} with a status that is never 401 or 403
 * for a portal refusal (Studio reads a 401 as an expired session): see {@link SupportException#getStudioStatus()}.
 */
public class SupportHandler extends AbstractServerHttpHandler {
  public enum Action {
    STATUS, REGISTER, UNREGISTER, REGISTER_INSTALLATION, CONNECT_START, CONNECT_STATUS, CONNECT_CANCEL, PREVIEW, CREATE_ISSUE, LIST_ISSUES, GET_ISSUE, COMMENT, STAGE_SCREENSHOT, DISCARD_SCREENSHOT, ANSWER_REQUEST, ANSWER_REQUESTS, SET_OPEN, ATTACH, BUNDLE
  }

  private final Action action;

  public SupportHandler(final HttpServer httpServer, final Action action) {
    super(httpServer);
    this.action = action;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    // Server-wide administration that spends the credentials of the installation: root only
    checkRootUser(user);

    final SupportService service = httpServer.getServer().getSupportService();
    try {
      return switch (action) {
        case STATUS -> json(200, service.status("true".equals(getQueryParameter(exchange, "refresh"))));
        case REGISTER -> register(service, payload);
        case UNREGISTER -> {
          service.unregister();
          yield new ExecutionResponse(204, "");
        }
        case REGISTER_INSTALLATION -> new ExecutionResponse(200, service.registerInstallation());
        case CONNECT_START -> json(200, connectStart(service, payload));
        case CONNECT_STATUS -> json(200, service.getConnector().status());
        case CONNECT_CANCEL -> {
          service.getConnector().cancel();
          yield new ExecutionResponse(204, "");
        }
        case PREVIEW -> json(200, service.preview(required(payload)));
        case CREATE_ISSUE -> new ExecutionResponse(201, service.createIssue(required(payload)));
        case LIST_ISSUES -> new ExecutionResponse(200, service.listIssues(getQueryParameter(exchange, "status", "open")));
        case GET_ISSUE -> new ExecutionResponse(200, service.getIssue(number(exchange)));
        case COMMENT -> new ExecutionResponse(201, service.addComment(number(exchange), required(payload).getString("body", ""),
            SupportService.screenshotIdsOf(payload)));
        case STAGE_SCREENSHOT -> json(201, service.stageScreenshot(required(payload)));
        case DISCARD_SCREENSHOT -> {
          service.discardScreenshot(getQueryParameter(exchange, "id", ""));
          yield new ExecutionResponse(204, "");
        }
        case ANSWER_REQUEST -> new ExecutionResponse(201,
            service.answerRequest(number(exchange), getQueryParameter(exchange, "requestId", ""), required(payload)));
        case ANSWER_REQUESTS -> new ExecutionResponse(201, service.answerRequests(number(exchange), required(payload)));
        case SET_OPEN -> {
          final JSONObject body = required(payload);
          if (!body.has("open"))
            throw new SupportException("bad_request", "The field 'open' (true or false) is required");
          service.setOpen(number(exchange), body.getBoolean("open"));
          yield new ExecutionResponse(204, "");
        }
        case ATTACH -> new ExecutionResponse(200, service.addAttachments(number(exchange), required(payload).getString("previewId", ""),
            SupportService.screenshotIdsOf(payload)));
        case BUNDLE -> download(exchange, service, required(payload).getString("previewId", ""));
      };
    } catch (final SupportException e) {
      return error(e);
    } catch (final IllegalArgumentException e) {
      return error(new SupportException("bad_request", e.getMessage()));
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Support: %s failed", e, action);
      // A temp file, a zip or a read of the logs failed on this server: not the caller's request
      return error(new SupportException("internal_error", "The operation failed on the server: " + e.getClass().getSimpleName()));
    }
  }

  private JSONObject connectStart(final SupportService service, final JSONObject payload) {
    final String label = payload == null ? "" : payload.getString("label", "");
    final ArcadeDBServer server = httpServer.getServer();
    final HAServerPlugin ha = server.getHA();
    final JSONObject attributes = SupportConnector.attributesOf(SupportConnector.localHost(server.getServerName()), server.getServerName(),
        ha == null ? null : ha.getClusterName(), ha == null ? 0 : ha.getConfiguredServers());
    return service.getConnector().start(label, server.getInstanceId(), attributes);
  }

  private ExecutionResponse register(final SupportService service, final JSONObject payload) {
    final JSONObject body = required(payload);
    try {
      if (body.getBoolean("verifyOnly", false))
        return json(200, service.verify(body.getString("clientId", ""), body.getString("key", "")));
      return json(200, service.register(body.getString("clientId", ""), body.getString("key", "")));
    } catch (final SupportPortalException e) {
      // The contract answers 400 for a refused registration; never 401/403, see the class comment
      if (e.getCode().equals("invalid_key") || e.getCode().equals("client_mismatch") || e.getCode().equals("scope_denied"))
        return errorResponse(400, e);
      throw e;
    }
  }

  private ExecutionResponse download(final HttpServerExchange exchange, final SupportService service, final String previewId)
      throws IOException {
    // Leased while the file is built AND streamed: a slow client must not see the preview expire under the download
    try (final SupportBundleManager.Lease lease = service.getBundles().lease(previewId)) {
      final Path zip = service.buildDownload(lease.bundle());
      final long size = Files.size(zip);
      exchange.setStatusCode(200);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/zip");
      exchange.getResponseHeaders().put(Headers.CONTENT_DISPOSITION,
          "attachment; filename=\"arcadedb-support-" + Instant.now().toString().replaceAll("[^0-9TZ]", "") + ".zip\"");
      exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, size);
      exchange.getResponseHeaders().put(Headers.CACHE_CONTROL, "no-store");
      try (final OutputStream out = streamedResponseOutput(exchange, () -> "the support bundle download");
          final InputStream in = Files.newInputStream(zip)) {
        in.transferTo(out);
      }
    }
    // Written already
    return null;
  }

  private static JSONObject required(final JSONObject payload) {
    if (payload == null)
      throw new SupportException("bad_request", "A JSON request body is required");
    return payload;
  }

  private long number(final HttpServerExchange exchange) {
    final String value = getQueryParameter(exchange, "number", "");
    try {
      final long n = Long.parseLong(value);
      if (n <= 0)
        throw new NumberFormatException();
      return n;
    } catch (final NumberFormatException e) {
      throw new SupportException("bad_request", "The issue number is not valid");
    }
  }

  private static ExecutionResponse json(final int status, final JSONObject body) {
    return new ExecutionResponse(status, body.toString());
  }

  private static ExecutionResponse error(final SupportException e) {
    return errorResponse(e.getStudioStatus(), e);
  }

  private static ExecutionResponse errorResponse(final int status, final SupportException e) {
    final JSONObject body = new JSONObject().put("error", e.getCode()).put("detail", e.getMessage()).put("message", e.getMessage());
    if (e.getRetryAfterSeconds() > 0) {
      body.put("retryAfterSeconds", e.getRetryAfterSeconds());
      return new ExecutionResponse(status, body.toString()).setHeader("Retry-After", String.valueOf(e.getRetryAfterSeconds()));
    }
    return new ExecutionResponse(status, body.toString());
  }
}
