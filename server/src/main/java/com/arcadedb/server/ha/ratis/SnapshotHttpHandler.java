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
import java.nio.file.Files;
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

    try {
      handleRequestInternal(exchange);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error in snapshot handler: %s", e, e.getMessage());
      if (!exchange.isResponseStarted()) {
        exchange.setStatusCode(500);
        exchange.getResponseSender().send("Internal error: " + e.getMessage());
      }
    }
  }

  private void handleRequestInternal(final HttpServerExchange exchange) throws Exception {
    // Authenticate the request
    final ServerSecurityUser user = authenticate(exchange);
    if (user == null) {
      exchange.setStatusCode(401);
      exchange.getResponseHeaders().put(Headers.WWW_AUTHENTICATE, "Basic realm=\"ArcadeDB\"");
      exchange.getResponseSender().send("Unauthorized");
      return;
    }

    // Only root user can download database snapshots
    if (!"root".equals(user.getName())) {
      exchange.setStatusCode(403);
      exchange.getResponseSender().send("Forbidden: only root user can download database snapshots");
      return;
    }

    // Extract database name from path parameters or URL path directly.
    // After dispatch(), path parameters may be empty in some Undertow versions.
    final var dbParam = exchange.getPathParameters().get("database");
    final String databaseName;
    if (dbParam != null && !dbParam.isEmpty())
      databaseName = dbParam.getFirst();
    else {
      // Fallback: extract from URL path (e.g., /api/v1/ha/snapshot/testdb -> testdb)
      final String path = exchange.getRelativePath();
      final int lastSlash = path.lastIndexOf('/');
      databaseName = lastSlash >= 0 ? path.substring(lastSlash + 1) : null;
    }

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
    // Sanitize database name for Content-Disposition to prevent header injection
    final String safeName = databaseName.replaceAll("[^a-zA-Z0-9._-]", "_");
    exchange.getResponseHeaders().put(Headers.CONTENT_DISPOSITION,
        "attachment; filename=\"" + safeName + "-snapshot.zip\"");

    exchange.startBlocking();

    final DatabaseInternal db = server.getDatabase(databaseName);
    // Unwrap ServerDatabase -> ReplicatedDatabase -> LocalDatabase for file access
    DatabaseInternal unwrapped = db.getEmbedded();
    if (unwrapped != null && !(unwrapped instanceof LocalDatabase))
      unwrapped = unwrapped.getEmbedded();
    final LocalDatabase localDb = (LocalDatabase) unwrapped;

    localDb.executeInReadLock(() -> {
      localDb.getPageManager().suspendFlushAndExecute(localDb, () -> {
        try (final OutputStream out = exchange.getOutputStream();
             final ZipOutputStream zipOut = new ZipOutputStream(out)) {

          final File configFile = localDb.getConfigurationFile();
          if (configFile.exists())
            addFileToZip(zipOut, configFile);

          final File schemaFile = ((LocalSchema) localDb.getSchema()).getConfigurationFile();
          if (schemaFile.exists())
            addFileToZip(zipOut, schemaFile);

          final Collection<ComponentFile> files = localDb.getFileManager().getFiles();
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
      // Constant-time comparison to prevent timing attacks (token is hex-encoded PBKDF2 output)
      if (raftHA != null && raftHA.getClusterToken() != null
          && java.security.MessageDigest.isEqual(
              raftHA.getClusterToken().getBytes(java.nio.charset.StandardCharsets.UTF_8),
              clusterTokenHeader.getFirst().getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
        final ServerSecurityUser rootUser = httpServer.getServer().getSecurity().getUser("root");
        if (rootUser == null) {
          LogManager.instance().log(this, Level.SEVERE, "Cluster token valid but 'root' user not found");
          return null;
        }
        return rootUser;
      }
      // Invalid cluster token: reject immediately, do not fall through to Basic auth
      LogManager.instance().log(this, Level.WARNING, "Invalid cluster token received from %s",
          exchange.getSourceAddress());
      return null;
    }

    // Basic auth
    final HeaderValues authHeader = exchange.getRequestHeaders().get(Headers.AUTHORIZATION);
    if (authHeader == null || authHeader.isEmpty())
      return null;

    final String auth = authHeader.getFirst();
    if (auth.startsWith("Basic ")) {
      try {
        final String decoded = new String(Base64.getDecoder().decode(auth.substring(6)));
        final int colonPos = decoded.indexOf(':');
        if (colonPos > 0) {
          final String userName = decoded.substring(0, colonPos);
          final String password = decoded.substring(colonPos + 1);
          return httpServer.getServer().getSecurity().authenticate(userName, password, null);
        }
      } catch (final Exception e) {
        return null;
      }
    }
    return null;
  }

  private void addFileToZip(final ZipOutputStream zipOut, final File inputFile) throws Exception {
    if (!inputFile.exists())
      return;

    // Security: skip symlinks to prevent path traversal via crafted symlinks
    if (Files.isSymbolicLink(inputFile.toPath())) {
      LogManager.instance().log(this, Level.WARNING, "Skipping symlinked file in snapshot: %s", inputFile.getAbsolutePath());
      return;
    }

    final ZipEntry entry = new ZipEntry(inputFile.getName());
    zipOut.putNextEntry(entry);

    try (final FileInputStream fis = new FileInputStream(inputFile)) {
      fis.transferTo(zipOut);
    }

    zipOut.closeEntry();
  }
}
