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

  /** One preview. */
  public static final class Bundle {
    private final String id;
    private final Path   directory;
    private final long   expiresAt;
    private       Path       logs;
    private       Path       diagnostics;
    private       Path       summary;
    private       Path       threads;
    private       JSONObject description;
    private       String     githubSummary = "";

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
  }

  private final Map<String, Bundle> bundles = new ConcurrentHashMap<>();
  private final LongSupplier        clock;
  private final long                ttlMs;
  private volatile ScheduledExecutorService cleaner;
  private volatile boolean                  closed;

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
      throw new IllegalStateException("The support service is stopped");
    purgeExpired();

    // Bound the disk used by previews nobody sent: the oldest goes
    while (bundles.size() >= MAX_BUNDLES) {
      bundles.values().stream().min(Comparator.comparingLong(Bundle::getExpiresAt)).ifPresent(oldest -> remove(oldest.getId()));
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

  public void remove(final String id) {
    final Bundle bundle = bundles.remove(id);
    if (bundle != null)
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
        remove(bundle.id);
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
