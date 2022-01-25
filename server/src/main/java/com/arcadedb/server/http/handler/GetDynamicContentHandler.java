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
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class GetDynamicContentHandler extends AbstractHandler {

  public GetDynamicContentHandler(final HttpServer httpServer) {
    super(httpServer);
    setRequireAuthentication(false);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user) throws Exception {
    String uri = exchange.getRequestURI();

    if (uri.contains("..")) {
      // UNAUTHORIZED ACCESS TO RELATIVE PATH
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Not Found");
      return;
    }

    if (uri.isEmpty() || uri.equals("/"))
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

    LogManager.instance().log(this, Level.FINE, "Loading file %s ", "/static" + uri);

    final InputStream file = getClass().getClassLoader().getResourceAsStream("static" + uri);
    if (file == null) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Not Found");
      return;
    }

    final Binary fileContent = FileUtils.readStreamAsBinary(file);
    file.close();

    byte[] bytes = fileContent.toByteArray();

    if (processTemplate)
      bytes = templating(exchange, new String(bytes, DatabaseFactory.getDefaultCharset()), new HashMap<>()).getBytes(DatabaseFactory.getDefaultCharset());

    if (!processTemplate)
      exchange.getResponseHeaders().put(Headers.CACHE_CONTROL, "max-age=86400");

    exchange.setStatusCode(200);
    exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, bytes.length);
    exchange.getResponseSender().send(ByteBuffer.wrap(bytes));
  }

  protected String templating(final HttpServerExchange exchange, final String file, final Map<String, Object> variables) throws IOException {
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
          for (String pair : parameterPairs) {
            final String[] kv = pair.split("=");
            variables.put(kv[0].trim(), kv[1].trim());
          }
        }

        final InputStream fis = getClass().getClassLoader().getResourceAsStream("static/" + include);
        if (fis == null) {
          LogManager.instance().log(this, Level.SEVERE, "Could not find file to include '%s' to include", include);
          return file;
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
          if (variableName.equalsIgnoreCase("swVersion")) {
            buffer.append(Constants.getVersion());
          } else
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

        for (String role : protect.split(","))
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
