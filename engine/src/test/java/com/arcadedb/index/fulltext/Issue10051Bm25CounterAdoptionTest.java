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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10051 (#10018): a {@code REBUILD INDEX ... WITH statsOnly = true} run on the leader reached a follower only on a full
 * schema reload or a restart, because a follower's wrapper survives an incremental schema load and keeps its own counters. The
 * counters are now persisted with the epoch of the last explicit recompute, and a surviving wrapper adopts the persisted counters
 * only when their epoch is newer than its own: the counters it keeps current from the replicated transactions are never
 * overwritten by a save that is older than they are.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051Bm25CounterAdoptionTest extends TestHelper {
  private static final int DOCS = 12;

  private void createAndFill() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
    reopenDatabase();
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName("Doc[content]"));
  }

  private LocalSchema schema() {
    return database.getSchema().getEmbedded();
  }

  private Path schemaFile() {
    return new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toPath();
  }

  /** What a leader's save looks like once it has recomputed the counters of the index: new values and a newer epoch. */
  private void leaderSavesCounters(final long totalDocs, final long sumDocLength, final long epoch) throws IOException {
    final JSONObject schema = new JSONObject(Files.readString(schemaFile()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    for (final String name : indexes.keySet()) {
      final JSONObject index = indexes.getJSONObject(name);
      index.put("ft_totalDocs", totalDocs);
      index.put("ft_sumDocLength", sumDocLength);
      index.put("ft_countersValid", true);
      index.put("ft_countersEpoch", epoch);
    }
    Files.writeString(schemaFile(), schema.toString());
  }

  private void followerReloadsTheSchema() throws IOException {
    schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());
  }

  @Test
  void aRecomputeOnTheLeaderReachesAFollowerThatKeepsItsWrapper() throws IOException {
    createAndFill();
    final FullTextIndexMetadata before = metadata();
    // The follower's own counters, kept current by the replicated transactions, have drifted from the leader's recompute
    before.applyCorpusDelta(5, 20);

    leaderSavesCounters(DOCS, DOCS * 3L, 1L);
    followerReloadsTheSchema();

    assertThat(metadata()).as("the wrapper survived the reload").isSameAs(before);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    assertThat(metadata().getCountersEpoch()).isEqualTo(1L);
  }

  /** A save older than the live counters carries the epoch they already have: it must not roll them back. */
  @Test
  void aSaveOfTheSameEpochNeverOverwritesTheLiveCounters() throws IOException {
    createAndFill();
    metadata().applyCorpusDelta(5, 20);

    leaderSavesCounters(DOCS, DOCS * 3L, 0L);
    followerReloadsTheSchema();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 5L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 20);
  }

  @Test
  void anOlderEpochIsIgnored() throws IOException {
    createAndFill();
    leaderSavesCounters(DOCS, DOCS * 3L, 3L);
    followerReloadsTheSchema();
    assertThat(metadata().getCountersEpoch()).isEqualTo(3L);

    metadata().applyCorpusDelta(2, 6);
    leaderSavesCounters(1L, 1L, 2L);
    followerReloadsTheSchema();

    assertThat(metadata().getCountersEpoch()).isEqualTo(3L);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 2L);
  }

  /**
   * The adoption is not atomic with a commit applying its delta on another thread, so it arms the once-per-session drift check
   * again even when it already ran: that check is only a live count against the adopted counters, and it finds nothing to repair
   * when they agree. It must not be the unconditional rescan, or every follower scans the type after every rebuild.
   */
  @Test
  void adoptedCountersRearmTheCheapDriftCheckWhichRepairsNothing() throws IOException {
    createAndFill();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(metadata().isStaleCheckDone()).isTrue();

    leaderSavesCounters(DOCS, DOCS * 3L, 1L);
    followerReloadsTheSchema();

    assertThat(metadata().isStaleCheckDone()).as("armed again by the adoption").isFalse();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(metadata().isStaleCheckDone()).isTrue();
    assertThat(metadata().getScans()).as("the counters agree with the live data: nothing to rescan").isZero();
  }

  /**
   * A commit applying its delta and an adoption of a persisted pair exclude each other: every delta here moves the pair by (1, 3),
   * so a pair that was never half overwritten keeps the difference length - 3 * docs of the pair that was adopted.
   */
  @Test
  void anAdoptionRacingCommitsNeverLeavesAHalfOverwrittenPair() throws Exception {
    final FullTextIndexMetadata m = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    final int threads = 4;
    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      for (int round = 1; round <= 300; round++) {
        m.setCounters(1000L, 3000L);
        final long epoch = round;
        final JSONObject definition = new JSONObject().put("ft_countersValid", true).put("ft_totalDocs", 50L)
            .put("ft_sumDocLength", 150L + 7L).put("ft_countersEpoch", epoch);
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++)
          futures.add(executor.submit(() -> {
            start.await();
            for (int i = 0; i < 50; i++)
              m.applyCorpusDelta(1, 3);
            return null;
          }));
        futures.add(executor.submit(() -> {
          start.await();
          m.adoptPersistedCounters(definition);
          return null;
        }));
        start.countDown();
        for (final Future<?> future : futures)
          future.get(30, TimeUnit.SECONDS);

        assertThat(m.getCountersEpoch()).isEqualTo(epoch);
        assertThat(m.getSumDocLength() - 3 * m.getTotalDocs()).as("round %d: the pair was adopted whole", round).isEqualTo(7L);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * A save serializing the counters while a schema load adopts a newer pair must write the old epoch with the old pair or the new
   * epoch with the new pair, never a mix: a mix would persist an epoch that claims counters this node does not hold.
   */
  @Test
  void aSaveRacingAnAdoptionNeverRecordsTheNewEpochNextToTheOldPair() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      for (int round = 0; round < 300; round++) {
        final FullTextIndexMetadata m = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
        m.setCounters(1000L, 3000L);
        final JSONObject definition = new JSONObject().put("ft_countersValid", true).put("ft_totalDocs", 50L)
            .put("ft_sumDocLength", 157L).put("ft_countersEpoch", 1L);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger adopted = new AtomicInteger();
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 3; t++)
          futures.add(executor.submit(() -> {
            start.await();
            do {
              final JSONObject saved = m.writeToJSON(new JSONObject());
              final long epoch = saved.getLong("ft_countersEpoch");
              final long docs = saved.getLong("ft_totalDocs");
              final long length = saved.getLong("ft_sumDocLength");
              if (epoch == 0L)
                assertThat(new long[] { docs, length }).containsExactly(1000L, 3000L);
              else
                assertThat(new long[] { docs, length }).as("epoch %d", epoch).containsExactly(50L, 157L);
            } while (adopted.get() == 0);
            return null;
          }));
        futures.add(executor.submit(() -> {
          start.await();
          m.adoptPersistedCounters(definition);
          adopted.incrementAndGet();
          return null;
        }));
        start.countDown();
        for (final Future<?> future : futures)
          future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /** The bucket indexes of one logical index all carry the same definition: exactly one reader of it adopts. */
  @Test
  void concurrentAdoptersOfTheSameDefinitionHaveOneWinner() throws Exception {
    createAndFill();
    final JSONObject definition = new JSONObject().put("ft_countersValid", true).put("ft_totalDocs", 5L)
        .put("ft_sumDocLength", 15L).put("ft_countersEpoch", 4L);
    final int threads = 8;
    final AtomicInteger winners = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++)
        futures.add(executor.submit(() -> {
          start.await();
          if (metadata().adoptPersistedCounters(definition))
            winners.incrementAndGet();
          return null;
        }));
      start.countDown();
      for (final Future<?> future : futures)
        future.get(30, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertThat(winners.get()).isEqualTo(1);
    assertThat(metadata().getCountersEpoch()).isEqualTo(4L);
    assertThat(metadata().getTotalDocs()).isEqualTo(5L);
  }

  /**
   * A delta that removes more than the counters hold is clamped at zero, and the node that clamped must not stay silently
   * different from one that did not: it arms the unconditional rescan. A delta that fits arms nothing.
   */
  @Test
  void aClampThatFiresArmsTheRescanAndOneThatDoesNotLeavesTheCheckAlone() {
    final FullTextIndexMetadata m = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    m.setCounters(10L, 30L);
    assertThat(m.isStaleCheckDone()).isTrue();

    m.applyCorpusDelta(-4, -12);
    assertThat(m.getTotalDocs()).isEqualTo(6L);
    assertThat(m.getSumDocLength()).isEqualTo(18L);
    assertThat(m.isStaleCheckDone()).as("no clamp fired").isTrue();
    assertThat(m.consumeRescanRequired()).isFalse();

    m.applyCorpusDelta(-50, -12);
    assertThat(m.getTotalDocs()).isZero();
    assertThat(m.getSumDocLength()).isEqualTo(6L);
    assertThat(m.isStaleCheckDone()).as("the clamp re-armed the check").isFalse();
    assertThat(m.consumeRescanRequired()).isTrue();
  }

  /** A node that skipped a delta arms the drift check, which then repairs it. */
  @Test
  void aSkippedDeltaArmsTheDriftCheck() {
    createAndFill();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(metadata().isStaleCheckDone()).isTrue();

    metadata().requireRescan();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();

    assertThat(metadata().getScans()).as("the check rescans even though the counts agree").isEqualTo(1L);
  }

  @Test
  void anInvalidSaveIsNotAdopted() throws IOException {
    createAndFill();
    metadata().applyCorpusDelta(5, 20);

    final JSONObject schema = new JSONObject(Files.readString(schemaFile()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    for (final String name : indexes.keySet()) {
      indexes.getJSONObject(name).put("ft_countersEpoch", 9L);
      indexes.getJSONObject(name).put("ft_countersValid", false);
    }
    Files.writeString(schemaFile(), schema.toString());
    followerReloadsTheSchema();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 5L);
    assertThat(metadata().getCountersEpoch()).isZero();
  }

  /** The explicit recompute is what advances the epoch, and it is persisted with the counters. */
  @Test
  void aStatsOnlyRebuildAdvancesAndPersistsTheEpoch() throws IOException {
    createAndFill();
    metadata().applyCorpusDelta(5, 20);

    database.command("sql", "REBUILD INDEX `Doc[content]` WITH statsOnly = true");

    assertThat(metadata().getCountersEpoch()).isEqualTo(1L);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    final JSONObject schema = new JSONObject(Files.readString(schemaFile()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    assertThat(indexes.keySet()).isNotEmpty().allSatisfy(name -> {
      assertThat(indexes.getJSONObject(name).getLong("ft_countersEpoch")).isEqualTo(1L);
      assertThat(indexes.getJSONObject(name).getLong("ft_totalDocs")).isEqualTo(DOCS);
      assertThat(indexes.getJSONObject(name).getLong("ft_sumDocLength")).isEqualTo(DOCS * 3L);
    });
  }

  /** The rebuild of the statistics run inside a transaction the caller opened: the schema save waits for its end, not the change. */
  @Test
  void aStatsOnlyRebuildInsideAnOpenTransactionAdvancesAndPersistsTheEpoch() throws IOException {
    createAndFill();
    metadata().applyCorpusDelta(5, 20);

    database.transaction(() -> database.command("sql", "REBUILD INDEX `Doc[content]` WITH statsOnly = true"));

    assertThat(metadata().getCountersEpoch()).isEqualTo(1L);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    final JSONObject schema = new JSONObject(Files.readString(schemaFile()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    assertThat(indexes.keySet()).isNotEmpty().allSatisfy(name -> {
      assertThat(indexes.getJSONObject(name).getLong("ft_countersEpoch")).isEqualTo(1L);
      assertThat(indexes.getJSONObject(name).getLong("ft_totalDocs")).isEqualTo(DOCS);
    });
  }

  /** A repair made by a query repairs this node's copy only: nobody else is meant to adopt it. */
  @Test
  void aRepairMadeByASearchDoesNotAdvanceTheEpoch() {
    createAndFill();
    metadata().applyCorpusDelta(5, 20);

    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getCountersEpoch()).isZero();
  }
}
