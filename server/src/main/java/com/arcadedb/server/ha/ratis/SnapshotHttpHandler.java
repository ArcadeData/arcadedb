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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * HTTP handler that serves a consistent database snapshot as a ZIP file.
 * Used by the Ratis snapshot installation mechanism: when a follower is too far behind,
 * it downloads the database from the leader via this endpoint.
 *
 * <p>The snapshot contains only data files and schema configuration - no WAL files,
 * no replication logs. After installation, Ratis replays log entries from the snapshot point.
 *
 * <p>Endpoint: GET /api/v1/ha/snapshot/{database}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SnapshotHttpHandler implements HttpHandler {

  private final HttpServer httpServer;

  public SnapshotHttpHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  @Override
  public void handleRequest(final HttpServerExchange exchange) throws Exception {
    if (exchange.isInIoThread()) {
      exchange.dispatch(this);
      return;
    }

    // Authenticate the request
    final ServerSecurityUser user = authenticate(exchange);
    if (user == null) {
      exchange.setStatusCode(401);
      exchange.getResponseHeaders().put(Headers.WWW_AUTHENTICATE, "Basic realm=\"ArcadeDB\"");
      exchange.getResponseSender().send("Unauthorized");
      return;
    }

    final String databaseName = exchange.getPathParameters().get("database").getFirst();

    if (databaseName == null || databaseName.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("Missing 'database' parameter");
      return;
    }

    final var server = httpServer.getServer();

    if (!server.existsDatabase(databaseName)) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Database '" + databaseName + "' not found");
      return;
    }

    LogManager.instance().log(this, Level.INFO, "Serving database snapshot for '%s'...", databaseName);

    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/zip");
    exchange.getResponseHeaders().put(Headers.CONTENT_DISPOSITION,
        "attachment; filename=\"" + databaseName + "-snapshot.zip\"");

    exchange.startBlocking();

    final DatabaseInternal db = server.getDatabase(databaseName);

    db.executeInReadLock(() -> {
      db.getPageManager().suspendFlushAndExecute(db, () -> {
        try (final OutputStream out = exchange.getOutputStream();
             final ZipOutputStream zipOut = new ZipOutputStream(out)) {

          final File configFile = ((LocalDatabase) db.getEmbedded()).getConfigurationFile();
          if (configFile.exists())
            addFileToZip(zipOut, configFile);

          final File schemaFile = ((LocalSchema) db.getSchema()).getConfigurationFile();
          if (schemaFile.exists())
            addFileToZip(zipOut, schemaFile);

          final Collection<ComponentFile> files = db.getFileManager().getFiles();
          for (final ComponentFile file : new ArrayList<>(files))
            if (file != null)
              addFileToZip(zipOut, file.getOSFile());

          zipOut.finish();
          LogManager.instance().log(this, Level.INFO, "Database snapshot for '%s' sent successfully", databaseName);

        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error serving snapshot for '%s'", e, databaseName);
          throw new RuntimeException(e);
        }
      });
      return null;
    });
  }

  private ServerSecurityUser authenticate(final HttpServerExchange exchange) {
    // Cluster token auth (inter-node communication)
    final HeaderValues clusterTokenHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Cluster-Token");
    if (clusterTokenHeader != null && !clusterTokenHeader.isEmpty()) {
      final var raftHA = httpServer.getServer().getHA();
      if (raftHA != null && clusterTokenHeader.getFirst().equals(raftHA.getClusterToken()))
        return httpServer.getServer().getSecurity().getUser("root");
    }

    // Basic auth
    final HeaderValues authHeader = exchange.getRequestHeaders().get(Headers.AUTHORIZATION);
    if (authHeader == null || authHeader.isEmpty())
      return null;

    final String auth = authHeader.getFirst();
    if (auth.startsWith("Basic ")) {
      final String decoded = new String(Base64.getDecoder().decode(auth.substring(6)));
      final int colonPos = decoded.indexOf(':');
      if (colonPos > 0) {
        final String userName = decoded.substring(0, colonPos);
        final String password = decoded.substring(colonPos + 1);
        try {
          return httpServer.getServer().getSecurity().authenticate(userName, password, null);
        } catch (final Exception e) {
          return null;
        }
      }
    }
    return null;
  }

  private void addFileToZip(final ZipOutputStream zipOut, final File inputFile) throws Exception {
    if (!inputFile.exists())
      return;

    final ZipEntry entry = new ZipEntry(inputFile.getName());
    zipOut.putNextEntry(entry);

    try (final FileInputStream fis = new FileInputStream(inputFile)) {
      fis.transferTo(zipOut);
    }

    zipOut.closeEntry();
  }
}
