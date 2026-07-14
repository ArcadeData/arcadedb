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
import com.arcadedb.engine.TransactionManager;
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
import io.undertow.util.HttpString;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
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

  // #5063 (review round 5) introduced this per-database lock because PageManagerFlushThread.setSuspended
  // was ownership-based (putIfAbsent): only the FIRST caller owned the suspend flag, so a second thread
  // streaming the same database could observe a resumed flush mid-read. Issue #5068 made the suspension
  // REFCOUNTED in the engine - flushing resumes only when the LAST suspender exits - so overlapping
  // suspendFlushAndExecute callers (this handler, SQL BACKUP DATABASE, database verify) each own their
  // whole window and the lock is NO LONGER needed for suspension correctness.
  // DECISION (#5068): the lock STAYS, for resource discipline rather than correctness. It serializes the
  // whole zip streaming per database: two followers resyncing the same multi-GB database at once would
  // double the read I/O and, with refcounted suspension, keep flushing suspended for the UNION of both
  // windows, growing the deferred-page backlog toward the #4728 backpressure cap and throttling commits
  // for longer. Serializing keeps each suspension window as short as possible. Requests beyond the
  // CONCURRENCY_SEMAPHORE limit (HA_SNAPSHOT_MAX_CONCURRENT, default 2) still get a fast 503.
  // Entries are never pruned by design: one small ReentrantLock per database NAME ever snapshotted, bounded
  // by the server's database count (a dropped-and-recreated database safely reuses its lock). Pruning on
  // drop would need lifecycle callbacks this handler does not have, for negligible memory.
  private final ConcurrentHashMap<String, ReentrantLock> perDatabaseSuspendLock = new ConcurrentHashMap<>();

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
      // Advertise that this stream ends with a completeness manifest (issue #4831) so the follower
      // requires it and rejects a download truncated at a ZIP-entry boundary.
      exchange.getResponseHeaders().put(new HttpString(SnapshotManager.MANIFEST_HEADER), "1");
      exchange.startBlocking();

      final DatabaseInternal db = server.getDatabase(databaseName);

      final ReentrantLock dbSuspendLock = suspendLockFor(databaseName);
      dbSuspendLock.lock();
      try {
        db.executeInReadLock(() -> {
          // The refcounted suspension (#5068) guarantees the flush thread is parked for this whole read;
          // perDatabaseSuspendLock additionally serializes same-database zip streams (see its comment).
          db.getPageManager().suspendFlushAndExecute(db, () -> serveSnapshotZip(exchange, db, databaseName));
          return null;
        });
      } finally {
        dbSuspendLock.unlock();
      }
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

    // Flush pages and hold a read lock to ensure a consistent point-in-time view of database files.
    // The refcounted suspension (#5068) guarantees ownership of the window; the per-database lock only
    // serializes same-database reads with the snapshot zip path (see perDatabaseSuspendLock).
    final ReentrantLock dbSuspendLock = suspendLockFor(databaseName);
    dbSuspendLock.lock();
    try {
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
    } finally {
      dbSuspendLock.unlock();
    }
  }

  /**
   * Returns the per-database lock serializing every entry into {@code suspendFlushAndExecute} made by
   * this handler (snapshot ZIP and checksums paths). Since the refcounted suspension of issue #5068 this
   * is not needed for correctness; it is retained to serialize same-database zip streaming and keep each
   * suspension window short (see {@link #perDatabaseSuspendLock}). One lock instance per database name
   * for the lifetime of the handler. Package-private for unit testing.
   */
  ReentrantLock suspendLockFor(final String databaseName) {
    return perDatabaseSuspendLock.computeIfAbsent(databaseName, k -> new ReentrantLock());
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
          && MessageDigest.isEqual(
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

    // Track the wall-clock time of the last byte written to the follower. The watchdog uses this to
    // detect a *stalled* transfer (no progress within HA_SNAPSHOT_WRITE_TIMEOUT) rather than killing a
    // slow-but-healthy one. A large database can legitimately take longer than the timeout to stream over
    // the network; an absolute deadline would force-close a perfectly fine transfer, surfacing as
    // "Premature EOF" on the follower and an unrecoverable resync loop (issue #4729).
    final AtomicLong lastProgressMs = new AtomicLong(System.currentTimeMillis());

    // Poll a few times within the timeout window so a stall is detected (and the semaphore slot freed)
    // promptly once progress halts, without spinning. Floored at 1s for very small configured timeouts.
    final long pollIntervalMs = Math.max(1_000L, writeTimeoutMs / 4);
    final ScheduledFuture<?> watchdog = scheduleStallWatchdog(watchdogExecutor, completed, lastProgressMs,
        writeTimeoutMs, pollIntervalMs, databaseName, () -> {
          try {
            exchange.getConnection().close();
          } catch (final Exception ignored) {
          }
        });

    try (final OutputStream rawOut = exchange.getOutputStream();
        final OutputStream out = new ProgressTrackingOutputStream(rawOut, lastProgressMs);
        final ZipOutputStream zipOut = new ZipOutputStream(out)) {

      // Accumulate one manifest record per file actually streamed (name + size + CRC32), written as the
      // final ZIP entry so the follower can detect a truncated download (issue #4831).
      final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();

      final File configFile = ((LocalDatabase) db.getEmbedded()).getConfigurationFile();
      if (configFile.exists())
        addFileToZip(zipOut, configFile, manifest);

      final File schemaFile = ((LocalSchema) db.getSchema()).getConfigurationFile();
      if (schemaFile.exists())
        addFileToZip(zipOut, schemaFile, manifest);

      final Collection<ComponentFile> files = db.getFileManager().getFiles();
      for (final ComponentFile file : new ArrayList<>(files))
        if (file != null)
          addFileToZip(zipOut, file.getOSFile(), manifest);

      // TimeSeries sealed-store files (.ts.sealed) use raw FileChannel I/O and are NOT registered with
      // the FileManager, so they are absent from getFiles(). Add them explicitly so a snapshot-syncing
      // follower also receives the compacted time-series data instead of only the mutable buckets
      // (issue #4382).
      final File dbDir = new File(db.getDatabasePath());
      final File[] sealedFiles = dbDir.listFiles((d, name) -> name.endsWith(".ts.sealed"));
      if (sealedFiles != null)
        for (final File sealedFile : sealedFiles)
          addFileToZip(zipOut, sealedFile, manifest);

      // Ship the recency marker (issue #5277). last-tx-id.bin is written on a clean close and on WAL
      // rotation, but a follower that receives this database via snapshot and is later force-killed
      // before either happens has no WAL and no marker, so it reports lastTxId=-1 for fully intact
      // data and is needlessly re-installed from a full snapshot at the next cold bootstrap. The
      // LIVE counter is used because the leader's own on-disk copy is stale while the database is
      // open; it is read under the same suspendFlushAndExecute window as the data files above.
      final long lastTxId = ((LocalDatabase) db.getEmbedded()).getLastTransactionId();
      if (lastTxId >= 0)
        addBytesToZip(zipOut, TransactionManager.LAST_TX_ID_FILE_NAME, longToBytes(lastTxId), manifest);

      // Final entry: the manifest. Anything truncated upstream drops this entry, so the follower's
      // "manifest present?" check turns a silently-short archive into a loud, retryable failure.
      final ZipEntry manifestEntry = new ZipEntry(SnapshotManager.MANIFEST_ENTRY_NAME);
      zipOut.putNextEntry(manifestEntry);
      zipOut.write(SnapshotManager.buildManifest(manifest).getBytes(StandardCharsets.UTF_8));
      zipOut.closeEntry();

      zipOut.finish();
      HALog.log(this, HALog.BASIC, "Database snapshot for '%s' sent successfully", databaseName);

    } catch (final Exception e) {
      // A follower that restarts (or whose stall watchdog closed the connection) drops the socket
      // mid-transfer, surfacing here as a broken pipe / connection reset. That is benign and expected:
      // the follower will retry. Log it quietly and do not rethrow (rethrowing would also surface a
      // second SEVERE from the enclosing suspendFlushAndExecute). Genuine errors stay SEVERE and rethrow.
      if (isClientDisconnect(e))
        LogManager.instance().log(this, Level.FINE,
            "Snapshot transfer for '%s' aborted: the follower closed the connection (%s)", databaseName, e.getMessage());
      else {
        LogManager.instance().log(this, Level.SEVERE, "Error serving snapshot for '%s'", e, databaseName);
        throw new RuntimeException(e);
      }
    } finally {
      completed.set(true);
      watchdog.cancel(false);
    }
  }

  /**
   * Returns {@code true} when the throwable chain indicates the follower closed the connection
   * mid-transfer (broken pipe, connection reset, or a closed channel) - a benign, expected event during
   * a follower restart or after the stall watchdog force-closes the connection - rather than a genuine
   * server-side snapshot error. Package-private and static so the classification is unit-testable.
   */
  static boolean isClientDisconnect(final Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof ClosedChannelException)
        return true;
      if (c instanceof IOException) {
        final String m = c.getMessage();
        if (m != null) {
          final String lower = m.toLowerCase(Locale.ROOT);
          if (lower.contains("broken pipe") || lower.contains("connection reset")
              || lower.contains("connection closed") || lower.contains("connection abort"))
            return true;
        }
      }
    }
    return false;
  }

  /**
   * Streams a single file into the ZIP and, on success, appends its {@link SnapshotManager.ManifestEntry}
   * (name + uncompressed size + CRC32) to {@code manifest}. The CRC and size are computed inline from the
   * exact bytes streamed (via {@link CheckedInputStream}), so they describe what the follower actually
   * receives rather than a separate re-read of the file. Skipped files (absent or symlink) contribute no
   * manifest entry, matching what is sent.
   */
  private void addFileToZip(final ZipOutputStream zipOut, final File inputFile,
      final List<SnapshotManager.ManifestEntry> manifest) throws Exception {
    if (!inputFile.exists())
      return;
    final Path filePath = inputFile.toPath();
    if (Files.isSymbolicLink(filePath)) {
      LogManager.instance().log(this, Level.WARNING, "Skipping symlink in snapshot: %s", filePath);
      return;
    }
    final ZipEntry entry = new ZipEntry(inputFile.getName());
    zipOut.putNextEntry(entry);
    final CRC32 crc = new CRC32();
    final long size;
    try (final FileInputStream fis = new FileInputStream(inputFile);
        final CheckedInputStream cis = new CheckedInputStream(fis, crc)) {
      size = cis.transferTo(zipOut);
    }
    zipOut.closeEntry();
    manifest.add(new SnapshotManager.ManifestEntry(inputFile.getName(), size, crc.getValue()));
  }

  /**
   * Writes an in-memory payload as a ZIP entry and appends its manifest record. Used for entries
   * synthesized at snapshot time (e.g. the {@code last-tx-id.bin} recency marker, issue #5277) whose
   * on-disk counterpart on the leader is stale while the database is open.
   */
  private void addBytesToZip(final ZipOutputStream zipOut, final String name, final byte[] payload,
      final List<SnapshotManager.ManifestEntry> manifest) throws Exception {
    final ZipEntry entry = new ZipEntry(name);
    zipOut.putNextEntry(entry);
    zipOut.write(payload);
    zipOut.closeEntry();
    final CRC32 crc = new CRC32();
    crc.update(payload);
    manifest.add(new SnapshotManager.ManifestEntry(name, payload.length, crc.getValue()));
  }

  /** Big-endian 8-byte encoding, matching {@code DataOutputStream.writeLong} used by TransactionManager. */
  private static byte[] longToBytes(final long value) {
    return ByteBuffer.allocate(8).putLong(value).array();
  }

  /**
   * Returns {@code true} when a snapshot transfer should be force-closed because no bytes have been
   * written for at least {@code writeTimeoutMs}. This is a stall check (idle time since the last write),
   * not an absolute deadline, so a large but actively-progressing transfer is never killed (issue #4729).
   * Package-private and static so the decision can be unit-tested independently of the HTTP exchange.
   */
  static boolean isSnapshotWriteStalled(final long lastProgressMs, final long nowMs, final long writeTimeoutMs) {
    return nowMs - lastProgressMs >= writeTimeoutMs;
  }

  /**
   * Schedules a periodic stall watchdog that force-closes the connection (via {@code onStall}) only when
   * the transfer has made no progress for {@code writeTimeoutMs}. Returns the {@link ScheduledFuture} the
   * caller cancels once the transfer completes. Package-private so the watchdog wiring is unit-testable
   * with a fake close action.
   */
  static ScheduledFuture<?> scheduleStallWatchdog(final ScheduledExecutorService executor,
      final AtomicBoolean completed, final AtomicLong lastProgressMs, final long writeTimeoutMs,
      final long pollIntervalMs, final String databaseName, final Runnable onStall) {
    return executor.scheduleWithFixedDelay(() -> {
      if (completed.get())
        return;
      final long now = System.currentTimeMillis();
      if (isSnapshotWriteStalled(lastProgressMs.get(), now, writeTimeoutMs)) {
        LogManager.instance().log(SnapshotHttpHandler.class, Level.WARNING,
            "Snapshot write for '%s' stalled for %dms with no progress, closing connection to release semaphore slot",
            databaseName, now - lastProgressMs.get());
        onStall.run();
      }
    }, pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * {@link OutputStream} wrapper that records the wall-clock time of the most recent write into a shared
   * {@link AtomicLong}, letting the snapshot watchdog tell a stalled transfer apart from a slow-but-healthy
   * one. Each successful write to the delegate refreshes {@code lastProgressMs}; the watchdog force-closes
   * the connection only when no progress is made within HA_SNAPSHOT_WRITE_TIMEOUT (issue #4729).
   * Package-private for unit testing.
   */
  static final class ProgressTrackingOutputStream extends FilterOutputStream {
    private final AtomicLong lastProgressMs;

    ProgressTrackingOutputStream(final OutputStream out, final AtomicLong lastProgressMs) {
      super(out);
      this.lastProgressMs = lastProgressMs;
    }

    @Override
    public void write(final int b) throws IOException {
      out.write(b);
      lastProgressMs.set(System.currentTimeMillis());
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      // FilterOutputStream.write(byte[],int,int) relays byte-by-byte; override to write the whole chunk
      // in one call (throughput) and record progress once per chunk instead of once per byte.
      out.write(b, off, len);
      lastProgressMs.set(System.currentTimeMillis());
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }
  }
}
