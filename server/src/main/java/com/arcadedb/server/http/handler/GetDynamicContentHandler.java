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

import com.arcadedb.Constants;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class GetDynamicContentHandler extends AbstractServerHttpHandler {

  // Non-templated Studio assets (js/css/svg/fonts/json/ico) are immutable classpath resources for the
  // process lifetime. Cache their raw bytes so repeated requests skip the getResourceAsStream + full read
  // + toByteArray copy on every hit. Bounded so a pathological set of distinct URIs cannot grow it without
  // limit. Templated (.html) pages are intentionally NOT cached: they embed per-request dynamic content
  // (now, uuid, role-gated protectBegin sections).
  private static final int                     STATIC_CACHE_MAX_ENTRIES = 512;
  private static final Map<String, byte[]>     STATIC_CONTENT_CACHE     = new ConcurrentHashMap<>();

  // Shown at the server root when the Studio module is not on the classpath (e.g. the "base"/"headless"
  // distributions). Self-contained (no external assets) so it renders even without Studio installed.
  private static final String                  STUDIO_NOT_BUNDLED_PAGE  = """
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>ArcadeDB Server</title>
        <style>
          body { font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background: #1b1f24;
                 color: #e6e6e6; margin: 0; display: flex; min-height: 100vh; align-items: center; justify-content: center; }
          .card { max-width: 640px; padding: 2.5rem; background: #23282f; border-radius: 12px;
                  box-shadow: 0 8px 30px rgba(0,0,0,0.35); }
          h1 { margin: 0 0 .5rem; font-size: 1.5rem; }
          p { line-height: 1.55; }
          code { background: #12151a; padding: .15rem .4rem; border-radius: 4px; font-size: .9em; }
          a { color: #4ea1ff; }
          .muted { color: #9aa4af; font-size: .9rem; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>ArcadeDB Server is running</h1>
          <p>The REST API is available at <code>/api/v1</code>, but the <strong>Studio</strong> web console is
             not bundled in this distribution.</p>
          <p>To use Studio, either:</p>
          <ul>
            <li>Download the full distribution <code>arcadedb-&lt;version&gt;.tar.gz</code> (without the
                <code>-base</code>/<code>-headless</code> suffix), or</li>
            <li>Rebuild a custom distribution including Studio:
                <code>./arcadedb-builder.sh --modules=console,studio</code>.</li>
          </ul>
          <p class="muted">See <a href="https://docs.arcadedb.com">docs.arcadedb.com</a> for details.</p>
        </div>
      </body>
      </html>
      """;

  public GetDynamicContentHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public boolean isRequireAuthentication() {
    return false;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload)
      throws Exception {
    String uri = exchange.getRequestURI();

    if (uri.contains(".."))
      // UNAUTHORIZED ACCESS TO RELATIVE PATH
      return new ExecutionResponse(404, "Not Found");

    if (uri.isEmpty() || "/".equals(uri))
      uri = "/index.html";

    if (!uri.contains("."))
      // NO EXTENSION + HTML PAGES
      uri += ".html";

    boolean processTemplate = true;
    String contentType = "plain/text";
    if (uri.endsWith(".html") || uri.endsWith(".htm"))
      contentType = "text/html";
    else if (uri.endsWith(".js")) {
      contentType = "text/javascript";
      processTemplate = false;
    } else if (uri.endsWith(".json")) {
      contentType = "application/json";
      processTemplate = false;
    } else if (uri.endsWith(".css"))
      contentType = "text/css";
    else if (uri.endsWith(".svg")) {
      contentType = "image/svg+xml";
      processTemplate = false;
    } else if (uri.endsWith(".ico")) {
      contentType = "image/x-icon";
      processTemplate = false;
    } else if (uri.endsWith(".woff")) {
      contentType = "font/woff";
      processTemplate = false;
    } else if (uri.endsWith(".woff2")) {
      contentType = "font/woff2";
      processTemplate = false;
    } else if (uri.endsWith(".tff")) {
      contentType = "font/tff";
      processTemplate = false;
    } else
      processTemplate = false;

    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, contentType);

    Metrics.counter("http.static-content").increment();

    final String resourcePath = "static" + uri;
    LogManager.instance().log(this, Level.FINE, "Loading file %s ", "/" + resourcePath);

    final byte[] bytes;
    if (processTemplate) {
      // Templated pages are re-rendered per request (they embed now/uuid/role-gated sections) and are not cached.
      final InputStream file = getClass().getClassLoader().getResourceAsStream(resourcePath);
      if (file == null) {
        final byte[] fallback = missingResourceFallback(resourcePath);
        if (fallback != null)
          return new ExecutionResponse(200, fallback);
        return new ExecutionResponse(404, "Not Found");
      }
      final Binary fileContent = FileUtils.readStreamAsBinary(file);
      file.close();
      bytes = templating(exchange, new String(fileContent.toByteArray(), DatabaseFactory.getDefaultCharset()),
          new HashMap<>()).getBytes(DatabaseFactory.getDefaultCharset());
    } else {
      // Immutable static asset: served from the in-process cache after the first read.
      bytes = loadStaticResource(resourcePath);
      if (bytes == null)
        return new ExecutionResponse(404, "Not Found");
      exchange.getResponseHeaders().put(Headers.CACHE_CONTROL, "max-age=86400");
    }

    return new ExecutionResponse(200, bytes);
  }

  /**
   * Loads an immutable classpath static asset, caching its raw bytes for the process lifetime so repeated
   * requests avoid re-reading the stream and re-copying the bytes. Returns {@code null} when the resource
   * does not exist. The cache is bounded; once full, further distinct resources are still served but not
   * cached. Package-private for direct unit testing.
   */
  static byte[] loadStaticResource(final String resourcePath) throws IOException {
    final byte[] cached = STATIC_CONTENT_CACHE.get(resourcePath);
    if (cached != null)
      return cached;

    final InputStream file = GetDynamicContentHandler.class.getClassLoader().getResourceAsStream(resourcePath);
    if (file == null)
      return null;

    final byte[] bytes;
    try {
      bytes = FileUtils.readStreamAsBinary(file).toByteArray();
    } finally {
      file.close();
    }

    if (STATIC_CONTENT_CACHE.size() < STATIC_CACHE_MAX_ENTRIES) {
      final byte[] existing = STATIC_CONTENT_CACHE.putIfAbsent(resourcePath, bytes);
      return existing != null ? existing : bytes;
    }
    return bytes;
  }

  /**
   * Chooses the response body to serve when a requested Studio asset is not on the classpath. Studio is an
   * optional module: the "base"/"headless" distributions ship without it, so browsing to the server root would
   * otherwise return a bare "Not Found" that reads like a broken server. For the landing page request only
   * ({@code static/index.html}) this returns a small self-contained page explaining Studio is not bundled and
   * how to enable it; every other missing asset returns {@code null} so the caller emits a genuine 404.
   * Package-private for direct unit testing.
   */
  static byte[] missingResourceFallback(final String resourcePath) {
    if ("static/index.html".equals(resourcePath))
      return STUDIO_NOT_BUNDLED_PAGE.getBytes(DatabaseFactory.getDefaultCharset());
    return null;
  }

  protected String templating(final HttpServerExchange exchange, final String file, final Map<String, Object> variables)
      throws IOException {
    int beginTokenPos = file.indexOf("${");
    if (beginTokenPos < 0)
      return file;

    final StringBuilder buffer = new StringBuilder((int) (file.length() * 1.5));

    int pos = 0;
    while (pos < file.length() && beginTokenPos > -1) {
      buffer.append(file, pos, beginTokenPos);

      pos = beginTokenPos + "${".length();

      int endTokenPos = file.indexOf("}", pos);
      if (endTokenPos == -1) {
        LogManager.instance().log(this, Level.SEVERE, "Could not find end of token after position %d", beginTokenPos);
        return file;
      }

      final String command = file.substring(pos, endTokenPos);
      if (command.startsWith("include:")) {
        String include = file.substring(pos + "include:".length(), endTokenPos);

        final int sep = include.indexOf(' ');
        if (sep > -1) {
          final String params = include.substring(sep).trim();

          include = include.substring(0, sep);

          final String[] parameterPairs = params.split(" ");
          for (final String pair : parameterPairs) {
            final String[] kv = pair.split("=");
            variables.put(kv[0].trim(), kv[1].trim());
          }
        }

        final InputStream fis = getClass().getClassLoader().getResourceAsStream("static/" + include);
        if (fis == null) {
          LogManager.instance().log(this, Level.WARNING, "Could not find file to include '%s' to include", include);
          pos = endTokenPos + "}".length();
          beginTokenPos = file.indexOf("${", pos);
          continue;
        }

        byte[] includeBytes = FileUtils.readStreamAsBinary(fis).toByteArray();

        // RECURSIVE
        includeBytes = templating(exchange, new String(includeBytes, DatabaseFactory.getDefaultCharset()), variables).getBytes(
            DatabaseFactory.getDefaultCharset());

        buffer.append(new String(includeBytes, DatabaseFactory.getDefaultCharset()));

      } else if (command.startsWith("var:")) {
        final int assignmentPos = command.indexOf("=");
        if (assignmentPos < 0) {
          // READ
          final String variableName = command.substring("var:".length());
          if ("swVersion".equalsIgnoreCase(variableName))
            buffer.append(Constants.getVersion());
          else if ("now".equalsIgnoreCase(variableName))
            buffer.append(System.currentTimeMillis());
          else if ("uuid".equalsIgnoreCase(variableName))
            buffer.append(UUID.randomUUID());
          else
            buffer.append(variables.get(variableName));
        } else {
          // WRITE
          final String variableName = command.substring("var:".length(), assignmentPos);
          final String variableValue = command.substring(assignmentPos + 1);
          variables.put(variableName, variableValue);
        }
      } else if (command.startsWith("protectBegin:")) {
        final String protect = file.substring(pos + "protectBegin:".length(), endTokenPos);

        pos = endTokenPos + "}".length();
        beginTokenPos = file.indexOf("${protectEnd", pos);
        endTokenPos = file.indexOf("}", beginTokenPos);

        final Set<String> currentUserRoles = exchange.getSecurityContext().getAuthenticatedAccount().getRoles();

        for (final String role : protect.split(","))
          if (currentUserRoles.contains(role)) {
            final String subContent = file.substring(pos, beginTokenPos);
            buffer.append(templating(exchange, subContent, variables));
            break;
          }

      } else
        // IGNORE IT
        buffer.append(file, beginTokenPos, endTokenPos + "}".length());

      pos = endTokenPos + "}".length();
      beginTokenPos = file.indexOf("${", pos);
    }

    buffer.append(file.substring(pos));

    return buffer.toString();
  }
}
