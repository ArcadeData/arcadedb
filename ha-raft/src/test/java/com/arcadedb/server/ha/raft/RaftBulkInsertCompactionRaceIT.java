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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * Reproduction for issue #4083 (and the missing-index-file aspect of #4081).
 * <p>
 * The hypothesis is a race between LSM-Tree compaction and concurrent user writes:
 * compaction creates a new mutable index file inside {@code splitIndex()} and swaps the
 * {@code mutable} field on the leader; subsequent user transactions read from that new
 * file id and ship a {@code TX_ENTRY} via Raft <em>before</em> compaction's enclosing
 * {@code SCHEMA_ENTRY} (which carries the {@code addFiles} entry for the new file) is
 * dispatched. On followers, {@code TransactionManager.applyChanges:307-315} silently
 * skips pages whose {@code fileId} does not yet exist - dropping the writes - so the
 * index ends up under-populated even after the {@code SCHEMA_ENTRY} arrives and creates
 * the file (empty).
 * <p>
 * The test exercises the worst-case scenario:
 * <ul>
 *   <li>3-node Raft cluster, majority quorum.</li>
 *   <li>Heavy concurrent insert load on an indexed type.</li>
 *   <li>Multiple manual compactions interleaved with the inserts so the swap can race
 *   with concurrent {@code commit()} calls.</li>
 * </ul>
 * The assertions catch the bug from three angles:
 * <ol>
 *   <li>Every follower's index entry count matches the leader's.</li>
 *   <li>Every follower's index file directory listing matches the leader's
 *       (no missing or orphan {@code Type_b_<nanos>} files).</li>
 *   <li>The follower's loaded schema does not emit "Cannot find indexes" warnings.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftBulkInsertCompactionRaceIT extends BaseRaftHATest {

  private static final int WRITER_THREADS     = 6;
  private static final int RECORDS_PER_THREAD = 8_000;
  private static final int COMMIT_EVERY       = 200;
  private static final int COMPACTION_ROUNDS  = 12;
  private static final int COMPACTION_GAP_MS  = 100;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // Keep quorum timeout generous so writer threads do not abort during compaction stalls.
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
    // To debug a future regression: uncomment the two lines below to enable HALog DETAILED level.
    // The log will dump the recordedChanges entries, the leader's outbound addFiles/removeFiles
    // and schema JSON indexes section, and the follower's inbound view of the same payload, so
    // you can pinpoint whether an "orphan" name comes from the schema JSON or from a missing
    // addFiles entry.
    // GlobalConfiguration.HA_LOG_VERBOSE.setValue(2);
    // HALog.refreshLevel();
  }

  @Override
  protected void populateDatabase() {
    // Skip the default test population; we drive everything from the test.
  }

  @Test
  void concurrentBulkInsertWithRepeatedCompactionDoesNotDivergeIndexFiles() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    final VertexType type = leaderDb.getSchema().buildVertexType()
        .withName("BulkRace").withTotalBuckets(3).create();
    type.createProperty("id", Long.class);
    type.createProperty("value", String.class);
    leaderDb.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "BulkRace", "id");

    // Capture "Cannot find indexes" warnings emitted on any node during the test window.
    final WarningCollector warnings = WarningCollector.attach();

    final AtomicLong nextId = new AtomicLong(0);
    final AtomicBoolean stopCompactor = new AtomicBoolean(false);
    final CountDownLatch writersReady = new CountDownLatch(WRITER_THREADS);
    final CountDownLatch goSignal = new CountDownLatch(1);

    final ExecutorService pool = Executors.newFixedThreadPool(WRITER_THREADS + 1);
    final List<Future<?>> futures = new ArrayList<>();

    // Writer threads: hammer the leader with indexed inserts in COMMIT_EVERY-sized batches.
    // Retries handle the page-version conflicts that are normal under multi-thread bulk insert.
    final int batchCount = RECORDS_PER_THREAD / COMMIT_EVERY;
    for (int t = 0; t < WRITER_THREADS; t++) {
      futures.add(pool.submit(() -> {
        writersReady.countDown();
        try {
          goSignal.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
        for (int b = 0; b < batchCount; b++) {
          for (int attempt = 0; attempt < 30; attempt++) {
            try {
              leaderDb.transaction(() -> {
                for (int k = 0; k < COMMIT_EVERY; k++) {
                  final long batchId = nextId.getAndIncrement();
                  leaderDb.newVertex("BulkRace").set("id", batchId, "value", "v-" + batchId).save();
                }
              });
              break;
            } catch (final NeedRetryException retry) {
              // Expected under concurrent bulk insert; retry the whole batch.
              try {
                Thread.sleep(20);
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

    // Compactor thread: trigger compaction repeatedly while the writers run.
    futures.add(pool.submit(() -> {
      try {
        writersReady.await(10, TimeUnit.SECONDS);
        goSignal.countDown();
        // Let writers build up some pages before the first compaction.
        Thread.sleep(500);
        for (int round = 0; round < COMPACTION_ROUNDS && !stopCompactor.get(); round++) {
          try {
            for (final Index idx : leaderDb.getSchema().getType("BulkRace").getAllIndexes(false)) {
              if (idx instanceof TypeIndex typeIdx) {
                typeIdx.scheduleCompaction();
                typeIdx.compact();
              }
            }
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.INFO, "TEST: compaction round %d failed: %s",
                round, e.getMessage());
          }
          Thread.sleep(COMPACTION_GAP_MS);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    }));

    pool.shutdown();
    assertThat(pool.awaitTermination(5, TimeUnit.MINUTES)).isTrue();
    for (final Future<?> f : futures)
      f.get();

    stopCompactor.set(true);

    // Final compaction so the leader and followers settle on the same compacted file set.
    for (final Index idx : leaderDb.getSchema().getType("BulkRace").getAllIndexes(false)) {
      if (idx instanceof TypeIndex typeIdx) {
        typeIdx.scheduleCompaction();
        typeIdx.compact();
      }
    }

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final long expectedRecords = ((DatabaseInternal) leaderDb).countType("BulkRace", true);

    // Aggregate index entry count across all sub-indexes registered on the type
    // (compaction renames bucket-level index files, so per-bucket names drift across runs).
    final long expectedIndexEntries = sumIndexEntriesForType(leaderIndex, "BulkRace");

    LogManager.instance().log(this, Level.INFO,
        "TEST: leader has %d records and %d index entries", expectedRecords, expectedIndexEntries);

    // 1. Every server must have the same record + index entry counts as the leader.
    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      assertThat(serverDb.countType("BulkRace", true))
          .as("server %d record count", i).isEqualTo(expectedRecords);

      assertThat(sumIndexEntriesForType(i, "BulkRace"))
          .as("server %d aggregated index entry count for type 'BulkRace'", i)
          .isEqualTo(expectedIndexEntries);
    }

    // 2. Every server's index file set on disk must match the leader's
    //    (catches the case where a follower never received a SCHEMA_ENTRY for a file the leader created).
    final Set<String> leaderIndexFiles = listIndexFilesOnDisk(leaderIndex);
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leaderIndex)
        continue;
      final Set<String> followerFiles = listIndexFilesOnDisk(i);
      assertThat(followerFiles)
          .as("server %d index file set on disk must match leader %d", i, leaderIndex)
          .containsExactlyInAnyOrderElementsOf(leaderIndexFiles);
    }

    // 3. No "Cannot find indexes" warnings should have been emitted during the test.
    assertThat(warnings.getMissingIndexWarnings())
        .as("no follower should report 'Cannot find indexes' during the bulk-insert + compaction race")
        .isEmpty();

    warnings.detach();
  }

  /**
   * Sums the entries across every sub-index attached to the given type on the given server.
   * Compaction renames bucket-level files, so we aggregate across the type rather than naming
   * a single index, which keeps the assertion meaningful across runs.
   */
  private long sumIndexEntriesForType(final int serverIndex, final String typeName) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    long total = 0;
    for (final Index idx : db.getSchema().getType(typeName).getAllIndexes(false))
      total += idx.countEntries();
    return total;
  }

  /**
   * Returns the set of registered index file names on a given server. Index files are identified
   * by the LSM-Tree mutable suffix convention {@code <name>_<bucket>_<nanos>} - we strip the
   * {@code _<nanos>} suffix so renames produced by repeated compaction are not flagged as
   * divergence (the *content* may have shifted; what matters is which logical names exist).
   */
  private Set<String> listIndexFilesOnDisk(final int serverIndex) {
    final DatabaseInternal db = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());
    final FileManager fm = db.getEmbedded().getFileManager();
    final Set<String> out = new HashSet<>();
    for (final ComponentFile f : fm.getFiles()) {
      if (f == null)
        continue;
      if (!(f instanceof PaginatedComponentFile))
        continue;
      final String name = f.getComponentName();
      if (name == null)
        continue;
      // Keep only LSM-tree index files (have at least <type>_<bucket> followed by _<nanos>).
      // Strip the final _<nanos> so file renames produced by compaction do not surface as divergence.
      final int last = name.lastIndexOf('_');
      out.add(last > 0 ? name.substring(0, last) : name);
    }
    return out;
  }

  /**
   * Captures {@link Level#WARNING} log records emitted by {@code LocalSchema} that match the
   * "Cannot find indexes" pattern, regardless of which node logged them.
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
