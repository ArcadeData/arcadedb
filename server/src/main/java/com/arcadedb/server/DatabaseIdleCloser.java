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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.HttpServer;
import io.micrometer.core.instrument.Metrics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Closes the databases nobody uses (issue #11068), so that a server holding one database per tenant keeps open the files and
 * the heap of the tenants active now, not of every tenant that ever connected since it started.
 * <p>
 * <b>Used.</b> A database is used when its activity count moves ({@link LocalDatabase#getActivityCount()}: a query, a command,
 * a transaction ended, a record read or written, a scan, a count, a write), whichever protocol asked: the count is the engine's,
 * so no client and no protocol has to report anything, and sampling it costs nothing on the request path. A database is IN USE,
 * and never closed however long it has been quiet, while a transaction is open on it - on any thread
 * ({@link LocalDatabase#hasActiveTransactionOnAnyThread()}, which covers the Postgres and Bolt connections and the gRPC
 * transactions), or in an HTTP or WebSocket insert session, which hold theirs between two requests on no thread - while a
 * statement runs on it, while its asynchronous executor has work, or while a backup, a restore, an import or an export holds
 * it. An open connection with no transaction does not keep it open: a connection pool would otherwise keep every tenant open
 * for ever, and the next request of that connection reopens it.
 * <p>
 * <b>Closed.</b> {@link ArcadeDBServer#closeIdleDatabase} closes it for REOPEN and deregisters it, without the mark of a
 * {@code close database}: the database is left exactly as one not opened at startup is, which every protocol opens on its next
 * request, and the listings show as a database of the server. A request that resolved the instance a moment before the close
 * gets the retryable {@code DatabaseIsReopeningException}, never the permanent closed one, and its retry reopens the database.
 * <p>
 * <b>The cap.</b> With {@link GlobalConfiguration#SERVER_MAX_OPEN_DATABASES} set, each check also closes the least recently used
 * databases beyond it that are not in use, whether they reached the idle timeout or not. Both kinds of close share the budget of
 * {@link GlobalConfiguration#SERVER_DATABASE_IDLE_MAX_CLOSES_PER_CHECK}.
 * <p>
 * Never closed: a replicated database (the replication applies the cluster's log to it whether a client uses it or not), and the
 * databases named in {@link GlobalConfiguration#SERVER_DATABASE_IDLE_EXCLUDE}. The settings are read at each check, so a
 * {@code set server setting} takes effect at the next one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseIdleCloser {
  static final String IDLE_CLOSED_METER = "arcadedb.server.databases.idle_closed";
  static final String REOPENED_METER    = "arcadedb.server.databases.reopened";

  private final ArcadeDBServer           server;
  /** What the last check saw of each open database. Read and written only by the check, which runs on one thread at a time. */
  private final Map<String, Sample>      samples      = new ConcurrentHashMap<>();
  /** The databases closed for idleness or by the cap and not opened since, to count their reopens. */
  private final Set<String>              closedByIdle = ConcurrentHashMap.newKeySet();
  private final AtomicLong               idleCloses   = new AtomicLong();
  private final AtomicLong               capCloses    = new AtomicLong();
  private final AtomicLong               reopens      = new AtomicLong();
  private volatile ScheduledExecutorService scheduler;

  /** The activity count and the instance it was read from, and when the database was last seen used. */
  private static final class Sample {
    private final ServerDatabase instance;
    private       long           activity;
    private       long           lastUsedNanos;

    private Sample(final ServerDatabase instance, final long activity, final long now) {
      this.instance = instance;
      this.activity = activity;
      this.lastUsedNanos = now;
    }
  }

  DatabaseIdleCloser(final ArcadeDBServer server) {
    this.server = server;
  }

  /** Starts the periodic check, on a daemon thread of its own. The interval is read again after every check. */
  synchronized void start() {
    if (scheduler != null)
      return;
    scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread thread = new Thread(r, "ArcadeDB-DatabaseIdleCloser-" + server.getServerName());
      thread.setDaemon(true);
      return thread;
    });
    scheduleNext(scheduler);
  }

  synchronized void stop() {
    final ScheduledExecutorService current = scheduler;
    scheduler = null;
    if (current != null) {
      current.shutdownNow();
      try {
        current.awaitTermination(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    samples.clear();
    closedByIdle.clear();
  }

  private void scheduleNext(final ScheduledExecutorService executor) {
    final long interval = Math.max(100L,
        server.getConfiguration().getValueAsLong(GlobalConfiguration.SERVER_DATABASE_IDLE_CHECK_INTERVAL));
    try {
      executor.schedule(() -> {
        try {
          check();
        } catch (final Throwable e) {
          LogManager.instance().log(this, Level.WARNING, "Error on looking for idle databases to close", e);
        } finally {
          if (scheduler == executor)
            scheduleNext(executor);
        }
      }, interval, TimeUnit.MILLISECONDS);
    } catch (final RejectedExecutionException e) {
      // STOPPED MEANWHILE
    }
  }

  /**
   * One check: samples every open database, then closes those idle past the timeout and those beyond the cap, least recently
   * used first, within the budget of one check. Answers how many it closed. Runs on the scheduler's thread; public for an
   * embedder, or a test, that drives a check now instead of waiting for the next one.
   */
  public int check() {
    if (!server.isStarted())
      return 0;

    final ContextConfiguration configuration = server.getConfiguration();
    final long idleTimeoutMs = configuration.getValueAsLong(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT);
    final int maxOpen = configuration.getValueAsInteger(GlobalConfiguration.SERVER_MAX_OPEN_DATABASES);
    if (idleTimeoutMs <= 0 && maxOpen <= 0) {
      samples.clear();
      return 0;
    }

    final int budgetSetting = configuration.getValueAsInteger(GlobalConfiguration.SERVER_DATABASE_IDLE_MAX_CLOSES_PER_CHECK);
    int budget = budgetSetting > 0 ? budgetSetting : Integer.MAX_VALUE;
    final Set<String> excluded = parseExclusions(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_IDLE_EXCLUDE));
    final long now = System.nanoTime();

    // SAMPLE EVERY OPEN DATABASE: ONE THAT MOVED, OR IS IN USE NOW, WAS USED NOW
    final Set<String> open = server.getDatabaseNames();
    samples.keySet().retainAll(open);
    final List<Sample> closable = new ArrayList<>();
    final List<String> closableNames = new ArrayList<>();
    for (final String name : open) {
      final ServerDatabase database = server.getRegisteredDatabase(name);
      if (database == null || !database.isOpen())
        continue;
      final long activity = activityOf(database);
      Sample sample = samples.get(name);
      if (sample == null || sample.instance != database) {
        sample = new Sample(database, activity, now);
        samples.put(name, sample);
      } else if (sample.activity != activity) {
        sample.activity = activity;
        sample.lastUsedNanos = now;
      }
      if (excluded.contains(name) || database.isReplicated())
        continue;
      if (isInUse(name, database)) {
        sample.lastUsedNanos = now;
        continue;
      }
      closable.add(sample);
      closableNames.add(name);
    }

    int closed = 0;

    // IDLE PAST THE TIMEOUT
    if (idleTimeoutMs > 0) {
      final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(idleTimeoutMs);
      for (int i = 0; i < closable.size() && budget > 0; i++) {
        final Sample sample = closable.get(i);
        if (now - sample.lastUsedNanos < timeoutNanos)
          continue;
        if (close(closableNames.get(i), sample, false)) {
          closable.set(i, null);
          ++closed;
          --budget;
        }
      }
    }

    // BEYOND THE CAP: THE LEAST RECENTLY USED FIRST, AMONG THOSE NOT IN USE
    if (maxOpen > 0 && budget > 0) {
      int excess = server.getDatabaseNames().size() - maxOpen;
      if (excess > 0) {
        final List<Integer> order = new ArrayList<>(closable.size());
        for (int i = 0; i < closable.size(); i++)
          if (closable.get(i) != null)
            order.add(i);
        order.sort((a, b) -> Long.compare(closable.get(a).lastUsedNanos, closable.get(b).lastUsedNanos));
        for (int k = 0; k < order.size() && excess > 0 && budget > 0; k++) {
          final int i = order.get(k);
          if (close(closableNames.get(i), closable.get(i), true)) {
            ++closed;
            --budget;
            --excess;
          }
        }
      }
    }
    return closed;
  }

  /** Called by the server when it registers a database it opened: the reopen of one this closed is counted. */
  void onOpened(final String databaseName) {
    if (closedByIdle.remove(databaseName)) {
      reopens.incrementAndGet();
      Metrics.counter(REOPENED_METER).increment();
    }
  }

  public long getIdleCloses() {
    return idleCloses.get();
  }

  public long getCapCloses() {
    return capCloses.get();
  }

  public long getReopens() {
    return reopens.get();
  }

  private boolean close(final String name, final Sample sample, final boolean byCap) {
    final ServerDatabase database = sample.instance;
    // RE-ASKED UNDER THE REGISTRY LOCK, RIGHT BEFORE THE CLOSE: A REQUEST THAT ARRIVED SINCE THE SAMPLE KEEPS IT OPEN
    final boolean closed = server.closeIdleDatabase(name, database,
        () -> activityOf(database) == sample.activity && !isInUse(name, database));
    if (!closed)
      return false;
    samples.remove(name, sample);
    closedByIdle.add(name);
    if (byCap)
      capCloses.incrementAndGet();
    else
      idleCloses.incrementAndGet();
    Metrics.counter(IDLE_CLOSED_METER, "reason", byCap ? "cap" : "idle").increment();
    LogManager.instance().log(this, Level.INFO, "Closed database '%s' %s: the next request reopens it", name,
        byCap ? "to stay within the cap on open databases (" + GlobalConfiguration.SERVER_MAX_OPEN_DATABASES.getKey() + ")" :
            "after it stayed unused for " + GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT.getKey());
    return true;
  }

  private boolean isInUse(final String name, final ServerDatabase database) {
    final DatabaseInternal embedded = database.getEmbedded();
    if (embedded instanceof LocalDatabase local && (local.hasActiveTransactionOnAnyThread() || local.isAsyncProcessing()))
      return true;
    if (server.getRunningQueries().isRunningOn(name))
      return true;
    final HttpServer http = server.getHttpServer();
    return http != null && (http.getSessionManager().hasSessionOnDatabase(name) || http.getInsertSessionManager()
        .hasSessionOnDatabase(name));
  }

  private static long activityOf(final ServerDatabase database) {
    return database.getEmbedded() instanceof LocalDatabase local ? local.getActivityCount() : 0L;
  }

  private static Set<String> parseExclusions(final String value) {
    if (value == null || value.isBlank())
      return Set.of();
    final Set<String> names = new HashSet<>();
    for (final String name : value.split(",")) {
      final String trimmed = name.trim();
      if (!trimmed.isEmpty())
        names.add(trimmed);
    }
    return names;
  }
}
