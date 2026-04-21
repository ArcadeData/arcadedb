/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * HTTP handler serving a consistent database snapshot as a ZIP file.
 * When a follower falls behind the compacted Raft log, it downloads
 * the database from the leader via this endpoint.
 * <p>
 * Endpoint: GET /api/v1/ha/snapshot/{database}
 */
public class SnapshotHttpHandler implements HttpHandler {

  private static final Semaphore CONCURRENCY_SEMAPHORE =
      new Semaphore(GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger(), true);

  private final ScheduledExecutorService watchdogExecutor =
      Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "arcadedb-snapshot-watchdog");
        t.setDaemon(true);
        return t;
      });

  private volatile boolean plainHttpWarningLogged = false;

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

    final ServerSecurityUser user = authenticate(exchange);
    if (user == null) {
      exchange.setStatusCode(401);
      exchange.getResponseHeaders().put(Headers.WWW_AUTHENTICATE, "Basic realm=\"ArcadeDB\"");
      exchange.getResponseSender().send("Unauthorized");
      return;
    }

    if (!"root".equals(user.getName())) {
      exchange.setStatusCode(403);
      exchange.getResponseSender().send("Only root user can download database snapshots");
      return;
    }

    if (!plainHttpWarningLogged && !httpServer.getServer().getConfiguration().getValueAsBoolean(
        GlobalConfiguration.NETWORK_USE_SSL)) {
      plainHttpWarningLogged = true;
      LogManager.instance().log(this, Level.WARNING,
          "Serving database snapshots over plain HTTP. Consider enabling SSL for production clusters.");
    }

    if (!CONCURRENCY_SEMAPHORE.tryAcquire()) {
      LogManager.instance().log(this, Level.WARNING, "Snapshot rejected: concurrency limit of %d reached",
          GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger());
      exchange.setStatusCode(503);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
      exchange.getResponseSender().send("{\"error\":\"Too many concurrent snapshots\"}");
      return;
    }
    try {
      // Extract database name from the path: /api/v1/ha/snapshot/{database}
      final String relativePath = exchange.getRelativePath();
      final String databaseName = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;

      // Check for checksums sub-path: /api/v1/ha/snapshot/{database}/checksums
      if (databaseName.endsWith("/checksums")) {
        final String dbName = databaseName.substring(0, databaseName.length() - "/checksums".length());
        handleChecksums(exchange, dbName);
        return;
      }

      if (databaseName.isEmpty()) {
        exchange.setStatusCode(400);
        exchange.getResponseSender().send("Missing database name in path");
        return;
      }

      if (databaseName.contains("/") || databaseName.contains("\\")
          || databaseName.contains("..") || databaseName.contains("\0")
          || !databaseName.chars().allMatch(c -> c >= 0x20 && c < 0x7F)) {
        exchange.setStatusCode(400);
        exchange.getResponseSender().send("Invalid database name");
        return;
      }

      final var server = httpServer.getServer();
      if (!server.existsDatabase(databaseName)) {
        exchange.setStatusCode(404);
        exchange.getResponseSender().send("Database '" + databaseName + "' not found");
        return;
      }

      LogManager.instance().log(this, Level.INFO, "Serving database snapshot for '%s'...", databaseName);

      final String safeName = databaseName.replaceAll("[^a-zA-Z0-9._-]", "_");
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/zip");
      exchange.getResponseHeaders().put(Headers.CONTENT_DISPOSITION,
          "attachment; filename=\"" + safeName + "-snapshot.zip\"");
      exchange.startBlocking();

      final DatabaseInternal db = server.getDatabase(databaseName);

      db.executeInReadLock(() -> {
        // suspendFlushAndExecute uses a putIfAbsent-based guard that only one concurrent
        // caller can "own" the suspend. If flush is already suspended (e.g. a concurrent
        // snapshot request), isPageFlushingSuspended() is true and it is safe to read files
        // directly since the flush thread is not writing. In both cases we serve the snapshot.
        final boolean[] executed = { false };
        db.getPageManager().suspendFlushAndExecute(db, () -> {
          executed[0] = true;
          serveSnapshotZip(exchange, db, databaseName);
        });
        if (!executed[0]) {
          // Flush was already suspended by another concurrent snapshot request; serve directly.
          serveSnapshotZip(exchange, db, databaseName);
        }
        return null;
      });
    } finally {
      CONCURRENCY_SEMAPHORE.release();
    }
  }

  private void handleChecksums(final HttpServerExchange exchange, final String databaseName) throws Exception {
    final var server = httpServer.getServer();
    if (!server.existsDatabase(databaseName)) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Database '" + databaseName + "' not found");
      return;
    }

    final DatabaseInternal db = server.getDatabase(databaseName);

    // Flush pages and hold a read lock to ensure a consistent point-in-time view of database files
    db.executeInReadLock(() -> {
      db.getPageManager().suspendFlushAndExecute(db, () -> {
        try {
          final File dbDir = new File(db.getDatabasePath());
          final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dbDir);
          final JSONObject response = new JSONObject();
          for (final var entry : checksums.entrySet())
            response.put(entry.getKey(), entry.getValue());

          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
          exchange.getResponseSender().send(response.toString());
        } catch (final Exception e) {
          exchange.setStatusCode(500);
          exchange.getResponseSender().send("Error computing checksums: " + e.getMessage());
        }
      });
      return null;
    });
  }

  private ServerSecurityUser authenticate(final HttpServerExchange exchange) {
    // Cluster token auth (inter-node communication)
    final HeaderValues clusterTokenHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Cluster-Token");
    if (clusterTokenHeader != null && !clusterTokenHeader.isEmpty()) {
      final var server = httpServer.getServer();
      final RaftHAPlugin haPlugin = server.getHA() instanceof RaftHAPlugin rp ? rp : null;
      final RaftHAServer raftHAServer = haPlugin != null ? haPlugin.getRaftHAServer() : null;
      final String expectedToken = raftHAServer != null ? raftHAServer.getClusterToken() : null;
      if (expectedToken != null && !expectedToken.isEmpty()
          && java.security.MessageDigest.isEqual(
          expectedToken.getBytes(), clusterTokenHeader.getFirst().getBytes())) {
        final ServerSecurityUser rootUser = server.getSecurity().getUser("root");
        if (rootUser == null) {
          LogManager.instance().log(this, Level.SEVERE, "Cluster token valid but 'root' user not found");
          return null;
        }
        return rootUser;
      }
    }

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

  private void serveSnapshotZip(final HttpServerExchange exchange, final DatabaseInternal db, final String databaseName) {
    final long writeTimeoutMs = httpServer.getServer().getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_WRITE_TIMEOUT);
    final AtomicBoolean completed = new AtomicBoolean(false);
    final ScheduledFuture<?> watchdog = watchdogExecutor.schedule(() -> {
      if (!completed.get()) {
        LogManager.instance().log(this, Level.WARNING,
            "Snapshot write for '%s' timed out after %dms, closing connection to release semaphore slot",
            databaseName, writeTimeoutMs);
        try {
          exchange.getConnection().close();
        } catch (final Exception ignored) {
        }
      }
    }, writeTimeoutMs, TimeUnit.MILLISECONDS);

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
      HALog.log(this, HALog.BASIC, "Database snapshot for '%s' sent successfully", databaseName);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error serving snapshot for '%s'", e, databaseName);
      throw new RuntimeException(e);
    } finally {
      completed.set(true);
      watchdog.cancel(false);
    }
  }

  private void addFileToZip(final ZipOutputStream zipOut, final File inputFile) throws Exception {
    if (!inputFile.exists())
      return;
    final Path filePath = inputFile.toPath();
    if (Files.isSymbolicLink(filePath)) {
      LogManager.instance().log(this, Level.WARNING, "Skipping symlink in snapshot: %s", filePath);
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
