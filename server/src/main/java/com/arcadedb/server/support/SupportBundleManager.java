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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * The previews: each one is a temporary directory holding the redacted files (logs.zip, diagnostics.json, summary.json,
 * threads.txt) under an unguessable id, so that what the user reviewed is exactly what is sent or downloaded. A preview
 * lives 15 minutes; expired ones are deleted when they expire, on every later call, and on shutdown.
 */
public class SupportBundleManager implements AutoCloseable {
  public static final long TTL_MS      = 15 * 60_000L;
  static final        int  MAX_BUNDLES = 5;

  private static final SecureRandom RANDOM = new SecureRandom();

  private final Map<String, Bundle> bundles = new ConcurrentHashMap<>();
  private final LongSupplier        clock;
  private final long                ttlMs;
  private volatile ScheduledExecutorService cleaner;
  private volatile boolean                  closed;

  /** One preview. */
  public static final class Bundle {
    private final String id;
    private final Path   directory;
    private final long   expiresAt;
    // Volatile: a bundle is published in the map by create() before the preview fills it in
    private volatile Path       logs;
    private volatile Path       diagnostics;
    private volatile Path       summary;
    private volatile Path       threads;
    private volatile JSONObject description;
    private volatile String     githubSummary = "";
    // Serialises building the download zip, so two concurrent downloads of one preview do not write the same file
    private final Object   buildLock = new Object();
    // In-use count and removal flag, both guarded by synchronized(this): see SupportBundleManager#lease
    private          int        leases;
    private          boolean    removed;

    Bundle(final String id, final Path directory, final long expiresAt) {
      this.id = id;
      this.directory = directory;
      this.expiresAt = expiresAt;
    }

    public String getId() {
      return id;
    }

    public Path getDirectory() {
      return directory;
    }

    public long getExpiresAt() {
      return expiresAt;
    }

    public Path getLogs() {
      return logs;
    }

    public void setLogs(final Path logs) {
      this.logs = logs;
    }

    public Path getDiagnostics() {
      return diagnostics;
    }

    public void setDiagnostics(final Path diagnostics) {
      this.diagnostics = diagnostics;
    }

    public Path getSummary() {
      return summary;
    }

    public void setSummary(final Path summary) {
      this.summary = summary;
    }

    public Path getThreads() {
      return threads;
    }

    public void setThreads(final Path threads) {
      this.threads = threads;
    }

    public JSONObject getDescription() {
      return description;
    }

    public void setDescription(final JSONObject description) {
      this.description = description;
    }

    public String getGithubSummary() {
      return githubSummary;
    }

    public void setGithubSummary(final String githubSummary) {
      this.githubSummary = githubSummary;
    }

    public boolean isEmpty() {
      return logs == null && diagnostics == null && summary == null && threads == null;
    }

    public Object getBuildLock() {
      return buildLock;
    }

    synchronized boolean isLeased() {
      return leases > 0;
    }
  }

  public SupportBundleManager() {
    this(System::currentTimeMillis, TTL_MS);
  }

  SupportBundleManager(final LongSupplier clock, final long ttlMs) {
    this.clock = clock;
    this.ttlMs = ttlMs;
  }

  /** A new, empty preview in a private temporary directory. */
  public Bundle create() throws IOException {
    if (closed)
      throw new SupportException("support_stopped", "The support service is stopping: try again when the server is back");
    purgeExpired();

    // Bound the disk used by previews nobody sent: the oldest goes
    // (one that is being sent or downloaded is never the victim; if every one is, the limit is exceeded until they finish)
    while (bundles.size() >= MAX_BUNDLES) {
      final Bundle oldest = bundles.values().stream().filter(b -> !b.isLeased()).min(Comparator.comparingLong(Bundle::getExpiresAt))
          .orElse(null);
      if (oldest == null)
        break;
      removeIfIdle(oldest);
    }

    final byte[] random = new byte[16];
    RANDOM.nextBytes(random);
    final String id = HexFormat.of().formatHex(random);
    // Files.createTempDirectory is readable by the owner only on POSIX
    final Path directory = Files.createTempDirectory("arcadedb-support-");
    final Bundle bundle = new Bundle(id, directory, clock.getAsLong() + ttlMs);
    bundles.put(id, bundle);
    scheduleCleanup();
    return bundle;
  }

  /**
   * @throws SupportException {@code preview_not_found} when the id is unknown or expired
   */
  public Bundle get(final String id) {
    purgeExpired();
    final Bundle bundle = id == null ? null : bundles.get(id);
    if (bundle == null)
      throw new SupportException("preview_not_found",
          "The preview does not exist or has expired (previews last 15 minutes): build the preview again");
    return bundle;
  }

  /**
   * Takes a preview for the duration of a send or a download: until the lease is closed the preview is neither expired
   * nor evicted, so a slow 100 MB upload that outlives the 15 minutes is not deleted under its own feet. An expired
   * preview is deleted as soon as its last lease closes.
   *
   * @throws SupportException {@code preview_not_found} when the id is unknown, expired or already removed
   */
  public Lease lease(final String id) {
    final Bundle bundle = get(id);
    synchronized (bundle) {
      if (bundle.removed)
        throw new SupportException("preview_not_found",
            "The preview does not exist or has expired (previews last 15 minutes): build the preview again");
      bundle.leases++;
    }
    return new Lease(bundle);
  }

  /** A preview held by a send or a download. Close it in a finally. */
  public final class Lease implements AutoCloseable {
    private final Bundle bundle;
    private       boolean closedLease;

    private Lease(final Bundle bundle) {
      this.bundle = bundle;
    }

    public Bundle bundle() {
      return bundle;
    }

    @Override
    public void close() {
      synchronized (bundle) {
        if (closedLease)
          return;
        closedLease = true;
        bundle.leases--;
      }
      purgeExpired();
    }
  }

  public void remove(final String id) {
    final Bundle bundle = bundles.remove(id);
    if (bundle != null)
      dispose(bundle);
  }

  /** Removes the preview unless somebody holds it; true when it is gone. */
  private boolean removeIfIdle(final Bundle bundle) {
    synchronized (bundle) {
      if (bundle.leases > 0)
        return false;
      bundle.removed = true;
    }
    bundles.remove(bundle.id, bundle);
    deleteRecursively(bundle.directory);
    return true;
  }

  private static void dispose(final Bundle bundle) {
    synchronized (bundle) {
      bundle.removed = true;
    }
    deleteRecursively(bundle.directory);
  }

  public int size() {
    return bundles.size();
  }

  /** Deletes the expired previews. */
  public void purgeExpired() {
    final long now = clock.getAsLong();
    for (final Bundle bundle : new ArrayList<>(bundles.values()))
      if (bundle.expiresAt <= now)
        removeIfIdle(bundle);
  }

  private void scheduleCleanup() {
    ScheduledExecutorService executor = cleaner;
    if (executor == null)
      synchronized (this) {
        executor = cleaner;
        if (executor == null) {
          executor = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "ArcadeDB-SupportBundleCleaner");
            t.setDaemon(true);
            return t;
          });
          cleaner = executor;
        }
      }
    try {
      executor.schedule(this::purgeExpired, ttlMs + 1000L, TimeUnit.MILLISECONDS);
    } catch (final RuntimeException e) {
      // stopped: close() removed everything
    }
  }

  /** Deletes every preview: on shutdown. */
  @Override
  public void close() {
    closed = true;
    for (final String id : new ArrayList<>(bundles.keySet()))
      remove(id);
    final ScheduledExecutorService executor = cleaner;
    if (executor != null)
      executor.shutdownNow();
  }

  static void deleteRecursively(final Path directory) {
    if (directory == null || !Files.exists(directory))
      return;
    try {
      Files.walkFileTree(directory, new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
          Files.deleteIfExists(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
          Files.deleteIfExists(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (final IOException e) {
      LogManager.instance().log(SupportBundleManager.class, Level.WARNING, "Cannot delete the support preview '%s'", e, directory);
    }
  }

  /** For tests. */
  List<String> ids() {
    return new ArrayList<>(bundles.keySet());
  }
}
