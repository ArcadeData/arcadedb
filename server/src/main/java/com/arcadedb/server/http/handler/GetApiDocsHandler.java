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
package com.arcadedb.server.http.handler;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.logging.Level;

/**
 * HTTP handler that serves the OpenAPI HTML documentation page at /api/v1/docs.
 * This endpoint provides an interactive Swagger UI for exploring the ArcadeDB HTTP API.
 */
public class GetApiDocsHandler extends AbstractServerHttpHandler {

  private static final String HTML_TEMPLATE = """
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ArcadeDB API Documentation</title>
        <link rel="stylesheet" type="text/css" href="/swagger-ui/swagger-ui.css">
        <link rel="icon" type="image/png" href="/swagger-ui/favicon-32x32.png" sizes="32x32" />
        <style>
          html {
            box-sizing: border-box;
            overflow: -moz-scrollbars-vertical;
            overflow-y: scroll;
          }
          *, *:before, *:after {
            box-sizing: inherit;
          }
          body {
            margin: 0;
            padding: 0;
          }
        </style>
      </head>
      <body>
        <div id="swagger-ui"></div>
        <script src="/swagger-ui/swagger-ui-bundle.js" charset="UTF-8"></script>
        <script src="/swagger-ui/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
        <script>
          window.onload = function() {
            const ui = SwaggerUIBundle({
              url: "/api/v1/openapi.json",
              dom_id: '#swagger-ui',
              deepLinking: true,
              displayOperationId: false,
              docExpansion: "list",
              filter: true,
              tryItOutEnabled: true,
              persistAuthorization: true,
              presets: [
                SwaggerUIBundle.presets.apis,
                SwaggerUIStandalonePreset
              ],
              plugins: [
                SwaggerUIBundle.plugins.DownloadUrl
              ],
              layout: "StandaloneLayout",
              requestInterceptor: (request) => {
                // Ensure credentials are included with requests
                request.credentials = 'same-origin';
                return request;
              }
            });
            window.ui = ui;
          };
        </script>
      </body>
      </html>
      """;

  // The page's one inline <script>, the Swagger UI bootstrap, is allowed by its hash rather than by 'unsafe-inline', so
  // the Content-Security-Policy still refuses any other inline script (issue #9681).
  private static final String INLINE_SCRIPT_HASH_SOURCE = "'sha256-" + sha256Base64(inlineScript(HTML_TEMPLATE)) + "'";

  // The page with its Swagger UI assets addressed by the version of their bytes (issue #9961), rendered on first use: the
  // assets are immutable for the life of the process
  private static volatile String versionedPage;

  public GetApiDocsHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload)
      throws Exception {

    Metrics.counter("http.api-docs").increment();

    try {
      // Set proper content type for HTML
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html; charset=UTF-8");
      setContentSecurityPolicy(exchange, INLINE_SCRIPT_HASH_SOURCE);
      exchange.getResponseHeaders().put(Headers.CACHE_CONTROL, GetDynamicContentHandler.CACHE_REVALIDATE);

      LogManager.instance().log(this, Level.FINE, "Serving OpenAPI documentation page");

      String page = versionedPage;
      if (page == null)
        versionedPage = page = GetDynamicContentHandler.stampAssetVersions(HTML_TEMPLATE, "");
      return new ExecutionResponse(200, page);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error serving OpenAPI documentation page: %s", e, e.getMessage());
      throw e;
    }
  }

  /**
   * The CSP source expression ({@code 'sha256-...'}) that allows the page's inline bootstrap script.
   */
  public static String inlineScriptHashSource() {
    return INLINE_SCRIPT_HASH_SOURCE;
  }

  private static String inlineScript(final String html) {
    final int start = html.indexOf("<script>");
    final int end = html.indexOf("</script>", start);
    if (start < 0 || end < 0 || html.indexOf("<script>", end) >= 0)
      throw new IllegalStateException("The API docs page must carry exactly one inline <script> block");
    return html.substring(start + "<script>".length(), end);
  }

  private static String sha256Base64(final String text) {
    try {
      return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (final NoSuchAlgorithmException e) {
      // SHA-256 is a mandatory algorithm of every Java platform
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean isRequireAuthentication() {
    // API documentation requires authentication for security
    return true;
  }
}
