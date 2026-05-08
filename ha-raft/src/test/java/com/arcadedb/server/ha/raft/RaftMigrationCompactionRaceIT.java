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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduction for the user-reported follow-up to issue #4083: even after the May 5 schema-alignment
 * fix, a migration tool that streams inserts into MANY indexed types still emits "Cannot find indexes
 * [...]" warnings on followers and ends up with fewer records than the leader.
 * <p>
 * The original {@link RaftBulkInsertCompactionRaceIT} only stresses one type with one index. The
 * user's scenario writes into ~14 types in parallel, so multiple compactions can run back-to-back on
 * different indexes while user transactions interleave with all of them. This test exercises that
 * shape: 8 types, each with an LSM index, 8 writer threads round-robin across the types, and a
 * dedicated compactor that walks every type each round.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftMigrationCompactionRaceIT extends BaseRaftHATest {

  private static final int    TYPE_COUNT          = 14;
  private static final int    BUCKETS_PER_TYPE    = 3;
  private static final int    WRITER_THREADS      = 12;
  private static final int    RECORDS_PER_THREAD  = 6_000;
  private static final int    COMMIT_EVERY        = 50;
  private static final int    COMPACTION_ROUNDS   = 24;
  private static final int    COMPACTION_GAP_MS   = 30;
  private static final String TYPE_NAME_PREFIX    = "MigrationType_";
  private static final String INDEX_PROPERTY_NAME = "id";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
  }

  @Override
  protected void populateDatabase() {
    // No default population; the test owns the schema.
  }

  @Test
  void migrationStyleBulkInsertAcrossManyTypesNeverEmitsCannotFindIndexes() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    for (int t = 0; t < TYPE_COUNT; t++) {
      final String typeName = TYPE_NAME_PREFIX + t;
      final VertexType type = leaderDb.getSchema().buildVertexType()
          .withName(typeName).withTotalBuckets(BUCKETS_PER_TYPE).create();
      type.createProperty(INDEX_PROPERTY_NAME, Long.class);
      type.createProperty("payload", String.class);
      leaderDb.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, typeName, INDEX_PROPERTY_NAME);
    }

    final WarningCollector warnings = WarningCollector.attach();

    final AtomicLong globalIdCounter = new AtomicLong(0);
    final AtomicBoolean stopCompactor = new AtomicBoolean(false);
    final CountDownLatch writersReady = new CountDownLatch(WRITER_THREADS);
    final CountDownLatch goSignal = new CountDownLatch(1);

    final ExecutorService pool = Executors.newFixedThreadPool(WRITER_THREADS + 1);
    final List<Future<?>> futures = new ArrayList<>();

    final int batchCount = RECORDS_PER_THREAD / COMMIT_EVERY;
    for (int w = 0; w < WRITER_THREADS; w++) {
      final int writerId = w;
      futures.add(pool.submit(() -> {
        writersReady.countDown();
        try {
          goSignal.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
        for (int b = 0; b < batchCount; b++) {
          // Round-robin across the types so each batch produces writes (and potential compaction
          // pressure) on a different LSM index than its neighbour.
          final String typeName = TYPE_NAME_PREFIX + ((writerId + b) % TYPE_COUNT);
          for (int attempt = 0; attempt < 30; attempt++) {
            try {
              leaderDb.transaction(() -> {
                for (int k = 0; k < COMMIT_EVERY; k++) {
                  final long id = globalIdCounter.getAndIncrement();
                  leaderDb.newVertex(typeName).set(INDEX_PROPERTY_NAME, id, "payload", "p-" + id).save();
                }
              });
              break;
            } catch (final NeedRetryException retry) {
              try {
                Thread.sleep(15);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
              }
            }
          }
        }
        return null;
      }));
    }

    futures.add(pool.submit(() -> {
      try {
        writersReady.await(10, TimeUnit.SECONDS);
        goSignal.countDown();
        Thread.sleep(300);
        for (int round = 0; round < COMPACTION_ROUNDS && !stopCompactor.get(); round++) {
          for (int t = 0; t < TYPE_COUNT; t++) {
            try {
              for (final Index idx : leaderDb.getSchema().getType(TYPE_NAME_PREFIX + t).getAllIndexes(false)) {
                if (idx instanceof TypeIndex typeIdx) {
                  typeIdx.scheduleCompaction();
                  typeIdx.compact();
                }
              }
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.INFO, "TEST: compaction round %d on type %d failed: %s",
                  round, t, e.getMessage());
            }
          }
          Thread.sleep(COMPACTION_GAP_MS);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    }));

    pool.shutdown();
    assertThat(pool.awaitTermination(8, TimeUnit.MINUTES)).isTrue();
    for (final Future<?> f : futures)
      f.get();

    stopCompactor.set(true);

    // Final compaction across all types so the leader and followers settle on the same compacted
    // file set before we count.
    for (int t = 0; t < TYPE_COUNT; t++) {
      for (final Index idx : leaderDb.getSchema().getType(TYPE_NAME_PREFIX + t).getAllIndexes(false)) {
        if (idx instanceof TypeIndex typeIdx) {
          typeIdx.scheduleCompaction();
          typeIdx.compact();
        }
      }
    }

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    long expectedRecordsTotal = 0;
    final long[] expectedRecordsPerType = new long[TYPE_COUNT];
    final long[] expectedIndexEntriesPerType = new long[TYPE_COUNT];
    for (int t = 0; t < TYPE_COUNT; t++) {
      final String typeName = TYPE_NAME_PREFIX + t;
      expectedRecordsPerType[t] = ((DatabaseInternal) leaderDb).countType(typeName, true);
      expectedIndexEntriesPerType[t] = sumIndexEntriesForType(leaderIndex, typeName);
      expectedRecordsTotal += expectedRecordsPerType[t];
    }

    LogManager.instance().log(this, Level.INFO,
        "TEST: leader has %d total records across %d types", expectedRecordsTotal, TYPE_COUNT);

    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      for (int t = 0; t < TYPE_COUNT; t++) {
        final String typeName = TYPE_NAME_PREFIX + t;
        assertThat(serverDb.countType(typeName, true))
            .as("server %d record count for type %s", i, typeName).isEqualTo(expectedRecordsPerType[t]);

        assertThat(sumIndexEntriesForType(i, typeName))
            .as("server %d aggregated index entry count for type '%s'", i, typeName)
            .isEqualTo(expectedIndexEntriesPerType[t]);
      }
    }

    assertThat(warnings.getMissingIndexWarnings())
        .as("no follower should report 'Cannot find indexes' across the migration window")
        .isEmpty();

    warnings.detach();
  }

  private long sumIndexEntriesForType(final int serverIndex, final String typeName) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    long total = 0;
    for (final Index idx : db.getSchema().getType(typeName).getAllIndexes(false))
      total += idx.countEntries();
    return total;
  }

  /**
   * Captures every WARNING-level log record whose message names the "Cannot find indexes" pattern -
   * the symptom mdre reports in #4083. We attach to the root logger so warnings emitted by either
   * the leader's or any follower's {@code LocalSchema} surface here.
   */
  private static final class WarningCollector extends Handler {
    private final List<String> missingIndexWarnings = new ArrayList<>();
    private final Logger       root                 = Logger.getLogger("");
    private final Level        previousLevel        = root.getLevel();

    static WarningCollector attach() {
      final WarningCollector h = new WarningCollector();
      h.setLevel(Level.WARNING);
      h.root.addHandler(h);
      if (h.root.getLevel() == null || h.root.getLevel().intValue() > Level.WARNING.intValue())
        h.root.setLevel(Level.WARNING);
      return h;
    }

    void detach() {
      root.removeHandler(this);
      if (previousLevel != null)
        root.setLevel(previousLevel);
    }

    synchronized List<String> getMissingIndexWarnings() {
      return new ArrayList<>(missingIndexWarnings);
    }

    @Override
    public synchronized void publish(final LogRecord record) {
      if (record == null)
        return;
      final String msg = record.getMessage();
      if (msg == null)
        return;
      if (msg.contains("Cannot find indexes") || msg.contains("Cannot find index"))
        missingIndexWarnings.add(record.getLoggerName() + ": " + msg);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }
}
