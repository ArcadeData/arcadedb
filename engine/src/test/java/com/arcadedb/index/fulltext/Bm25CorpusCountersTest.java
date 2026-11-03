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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * BM25 corpus counters of a FULL_TEXT index (document count and sum of the document lengths): their adoption by a follower, their
 * repair by the first search, their update at commit, and the drift checks that arm a rescan (issues #10051, #10184, #10397, #10606,
 * #10735). Each section below keeps the description of the issue it covers.
 */
class Bm25CorpusCountersTest extends TestHelper {
  private static final int    DOCS             = 12;
  private static final int    SMALL_DOCS       = 10;
  private static final int    BUCKETS          = 3;
  private static final String INDEX            = "Doc[content]";
  private static final String KEYWORD_ANALYZER = "org.apache.lucene.analysis.core.KeywordAnalyzer";

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(INDEX));
  }

  private FullTextIndexMetadata metadata(final String indexName) {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(indexName));
  }

  private Map<String, Object> describe() {
    return LSMTreeFullTextIndex.describeCorpusCounters(database.getSchema().getIndexByName(INDEX));
  }

  private void search() {
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('" + INDEX + "', 'alpha') = true").close();
  }

  private Path schemaFile() {
    return new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toPath();
  }

  /** Edits the definition of the index while the database is closed, as an operator editing {@code schema.json} would. */
  private void editIndexDefinition(final Consumer<JSONObject> edit) throws IOException {
    database.close();
    final JSONObject schema = new JSONObject(Files.readString(schemaFile()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    for (final String name : indexes.keySet())
      edit.accept(indexes.getJSONObject(name));
    Files.writeString(schemaFile(), schema.toString());
    reopenDatabase();
  }

  // ===== Issue #10051 (#10018, adoption) =====
  // Issue #10051 (#10018): a {@code REBUILD INDEX ... WITH statsOnly = true} run on the leader reached a follower only on a full
  // schema reload or a restart, because a follower's wrapper survives an incremental schema load and keeps its own counters. The
  // counters are now persisted with the epoch of the last explicit recompute, and a surviving wrapper adopts the persisted counters
  // only when their epoch is newer than its own: the counters it keeps current from the replicated transactions are never
  // overwritten by a save that is older than they are.

  private void createFillAndReopen() {
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

  private LocalSchema schema() {
    return database.getSchema().getEmbedded();
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
  @DisplayName("#10051 a recompute on the leader reaches afollower that keeps its wrapper")
  void aRecomputeOnTheLeaderReachesAFollowerThatKeepsItsWrapper() throws IOException {
    createFillAndReopen();
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
  @DisplayName("#10051 a save of the same epoch never overwrites the live counters")
  void aSaveOfTheSameEpochNeverOverwritesTheLiveCounters() throws IOException {
    createFillAndReopen();
    metadata().applyCorpusDelta(5, 20);

    leaderSavesCounters(DOCS, DOCS * 3L, 0L);
    followerReloadsTheSchema();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 5L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 20);
  }

  @Test
  @DisplayName("#10051 an older epoch is ignored")
  void anOlderEpochIsIgnored() throws IOException {
    createFillAndReopen();
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
  @DisplayName("#10051 adopted counters rearm the cheap drift check which repairs nothing")
  void adoptedCountersRearmTheCheapDriftCheckWhichRepairsNothing() throws IOException {
    createFillAndReopen();
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
  @DisplayName("#10051 an adoption racing commits never leaves ahalf overwritten pair")
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
  @DisplayName("#10051 a save racing an adoption never records the new epoch next to the old pair")
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
  @DisplayName("#10051 concurrent adopters of the same definition have one winner")
  void concurrentAdoptersOfTheSameDefinitionHaveOneWinner() throws Exception {
    createFillAndReopen();
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
  @DisplayName("#10051 a clamp that fires arms the rescan and one that does not leaves the check alone")
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
  @DisplayName("#10051 a skipped delta arms the drift check")
  void aSkippedDeltaArmsTheDriftCheck() {
    createFillAndReopen();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(metadata().isStaleCheckDone()).isTrue();

    metadata().requireRescan();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();

    assertThat(metadata().getScans()).as("the check rescans even though the counts agree").isEqualTo(1L);
  }

  @Test
  @DisplayName("#10051 an invalid save is not adopted")
  void anInvalidSaveIsNotAdopted() throws IOException {
    createFillAndReopen();
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
  @DisplayName("#10051 a stats only rebuild advances and persists the epoch")
  void aStatsOnlyRebuildAdvancesAndPersistsTheEpoch() throws IOException {
    createFillAndReopen();
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
  @DisplayName("#10051 a stats only rebuild inside an open transaction advances and persists the epoch")
  void aStatsOnlyRebuildInsideAnOpenTransactionAdvancesAndPersistsTheEpoch() throws IOException {
    createFillAndReopen();
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
  @DisplayName("#10051 a repair made by asearch does not advance the epoch")
  void aRepairMadeByASearchDoesNotAdvanceTheEpoch() {
    createFillAndReopen();
    metadata().applyCorpusDelta(5, 20);

    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getCountersEpoch()).isZero();
  }

  // ===== Issue #10051 (#10019, repair) =====
  // Issue #10051 (#10019): the repair of drifted BM25 corpus counters done by the first search used to be lost on a restart, so
  // the same whole-type scan ran again on the first search of every session. Its warning named only the type, and the counters
  // could not be inspected without triggering that repair.

  private void createFillAndDrift() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.questions STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      database.command("sql", "CREATE INDEX ON Doc (questions) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma', questions = 'why'");
    });
    // The drift a crash leaves behind: counters written to the schema that do not match the data
    metadata("Doc[content]").applyCorpusDelta(7, 40);
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();
    reopenDatabase();
  }

  @Test
  @DisplayName("#10051 a repair made by asearch survives arestart")
  void aRepairMadeByASearchSurvivesARestart() {
    createFillAndDrift();
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS + 7L);

    search();

    assertThat(metadata("Doc[content]").getScans()).isEqualTo(1L);
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata("Doc[content]").getSumDocLength()).isEqualTo(DOCS * 3L);

    reopenDatabase();
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);

    search();

    // Paid once: the second session found the counters exact and scanned nothing
    assertThat(metadata("Doc[content]").getScans()).isZero();
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata("Doc[content]").getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  /** What the repair of one index found and fixed is no business of its siblings on the same type. */
  @Test
  @DisplayName("#10051 a repair leaves the other full text index of the type alone")
  void aRepairLeavesTheOtherFullTextIndexOfTheTypeAlone() {
    createFillAndDrift();

    search();

    assertThat(metadata("Doc[questions]").getScans()).isZero();
    assertThat(metadata("Doc[questions]").getTotalDocs()).isEqualTo(DOCS);
  }

  @Test
  @DisplayName("#10051 the warning names the logical index and the exact repair")
  void theWarningNamesTheLogicalIndexAndTheExactRepair() {
    final String message = LSMTreeFullTextIndex.corpusDriftMessage("Doc[questions]", "Doc", 19L, 12L, false);

    assertThat(message).contains("'Doc[questions]'").contains("type 'Doc'").contains("persisted=19").contains("live=12");
    assertThat(message).contains("REBUILD INDEX `Doc[questions]` WITH statsOnly = true");
  }

  @Test
  @DisplayName("#10051 the warning says when the counts agree and only an inexact scan is being redone")
  void theWarningSaysWhenTheCountsAgreeAndOnlyAnInexactScanIsBeingRedone() {
    final String message = LSMTreeFullTextIndex.corpusDriftMessage("Doc[content]", "Doc", 12L, 12L, true);

    assertThat(message).contains("raced concurrent writers").doesNotContain("diverged");
    assertThat(message).contains("REBUILD INDEX `Doc[content]` WITH statsOnly = true");
  }

  /** Looking at the counters must not repair them: the drift is what the operator came to see. */
  @Test
  @DisplayName("#10051 schema index shows the counters without repairing them")
  void schemaIndexShowsTheCountersWithoutRepairingThem() {
    createFillAndDrift();

    final ResultSet resultSet = database.query("sql", "SELECT FROM schema:index:`Doc[content]`");
    assertThat(resultSet.hasNext()).isTrue();
    final Result detail = resultSet.next();
    resultSet.close();

    final Map<String, Object> bm25 = detail.getProperty("bm25");
    assertThat(bm25).isNotNull();
    assertThat(bm25.get("totalDocs")).isEqualTo(DOCS + 7L);
    assertThat(bm25.get("sumDocLength")).isEqualTo(DOCS * 3L + 40L);
    assertThat((double) bm25.get("avgDocLength")).isEqualTo((DOCS * 3.0 + 40) / (DOCS + 7));
    assertThat(bm25.get("countersValid")).isEqualTo(true);
    assertThat(bm25.get("staleCheckDone")).isEqualTo(false);
    assertThat(bm25.get("scans")).isEqualTo(0L);

    assertThat(metadata("Doc[content]").getScans()).isZero();
    assertThat(metadata("Doc[content]").isStaleCheckDone()).isFalse();
  }

  @Test
  @DisplayName("#10051 schema index of an index that is not full text has no counters")
  void schemaIndexOfAnIndexThatIsNotFullTextHasNoCounters() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Plain");
      database.command("sql", "CREATE PROPERTY Plain.name STRING");
      database.command("sql", "CREATE INDEX ON Plain (name) NOTUNIQUE");
    });

    final ResultSet resultSet = database.query("sql", "SELECT FROM schema:index:`Plain[name]`");
    final Result detail = resultSet.next();
    resultSet.close();

    assertThat(detail.hasProperty("bm25")).isFalse();
  }

  @Test
  @DisplayName("#10051 the counters the type index reports are the ones its bucket indexes share")
  void theCountersTheTypeIndexReportsAreTheOnesItsBucketIndexesShare() {
    createFillAndDrift();

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    final FullTextIndexMetadata viaType = LSMTreeFullTextIndex.metadataOf(typeIndex);
    assertThat(viaType).isNotNull();
    assertThat(typeIndex.getIndexesOnBuckets()).allSatisfy(
        bucketIndex -> assertThat(LSMTreeFullTextIndex.metadataOf(bucketIndex)).isSameAs(viaType));
  }

  /**
   * A crash between commits and the close: the schema was last written before the inserts, the WAL replay restores the pages but
   * carries no corpus deltas, so the counters read from the schema lag the data. The first search finds the live count disagrees
   * and repairs them.
   */
  @Test
  @DisplayName("#10051 after acrash the first search repairs the counters the log does not carry")
  void afterACrashTheFirstSearchRepairsTheCountersTheLogDoesNotCarry() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.questions STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);

    ((DatabaseInternal) database).kill();
    database.close(); // unregisters the killed instance, as the ACID crash tests do
    database = factory.open();

    assertThat(database.countType("Doc", false)).as("the pages were restored").isEqualTo(DOCS);
    assertThat(metadata("Doc[content]").getTotalDocs()).as("the counters come from the last schema write").isLessThan(DOCS);

    search();

    assertThat(metadata("Doc[content]").getScans()).isEqualTo(1L);
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata("Doc[content]").getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  /** A database opened read-only repairs its counters in memory and never writes its schema. */
  @Test
  @DisplayName("#10051 a repair on aread only database writes nothing")
  void aRepairOnAReadOnlyDatabaseWritesNothing() throws IOException {
    createFillAndDrift();
    final Path schemaFile = new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toPath();
    final String before = Files.readString(schemaFile);

    reopenDatabaseInReadOnlyMode();
    search();
    assertThat(metadata("Doc[content]").getScans()).isEqualTo(1L);
    assertThat(metadata("Doc[content]").getTotalDocs()).isEqualTo(DOCS);
    database.close();

    assertThat(Files.readString(schemaFile)).isEqualTo(before);
  }

  // ===== Issue #10051 (#10018, after commit) =====
  // Issue #10051 (#10018): the BM25 corpus counters of a FULL_TEXT index were bumped at index time, before the transaction
  // committed, and nothing undid them. A rolled-back insert (a {@code ConcurrentModificationException} retry under write
  // contention is the everyday case) left {@code totalDocs} / {@code sumDocLength} inflated, which the next search then
  // noticed through the stale check and repaired with a whole-type scan on the query thread. The counters now move only
  // when the transaction commits.

  private void createAndFill() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
  }

  /** The one metadata every bucket sub-index of the logical index shares. */
  private FullTextIndexMetadata sharedMetadata() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    final Index first = typeIndex.getIndexesOnBuckets()[0];
    return ((LSMTreeFullTextIndex) first).getFullTextMetadata();
  }

  @Test
  @DisplayName("#10051 committed inserts move the counters")
  void committedInsertsMoveTheCounters() {
    createAndFill();
    assertThat(sharedMetadata().isCommitApplying()).as("nothing is left marked as being applied").isFalse();

    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  @Test
  @DisplayName("#10051 rolled back inserts leave the counters untouched")
  void rolledBackInsertsLeaveTheCountersUntouched() {
    createAndFill();

    database.begin();
    for (int i = 0; i < 5; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.rollback();

    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    assertThat(database.countType("Doc", false)).isEqualTo(DOCS);
  }

  @Test
  @DisplayName("#10051 rolled back deletes leave the counters untouched")
  void rolledBackDeletesLeaveTheCountersUntouched() {
    createAndFill();

    database.begin();
    database.command("sql", "DELETE FROM Doc LIMIT 4");
    database.rollback();

    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  @Test
  @DisplayName("#10051 the counters do not move before the commit")
  void theCountersDoNotMoveBeforeTheCommit() {
    createAndFill();

    database.begin();
    database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta epsilon'");
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    database.commit();

    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS + 1L);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L + 5);
  }

  @Test
  @DisplayName("#10051 a committed delete moves the counters once the transaction commits")
  void aCommittedDeleteMovesTheCountersOnceTheTransactionCommits() {
    createAndFill();

    database.begin();
    database.command("sql", "DELETE FROM Doc LIMIT 2");
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    database.commit();

    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS - 2L);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo((DOCS - 2L) * 3L);
  }

  /** An update is a remove and an add inside one transaction: only their net effect may reach the counters, and only on commit. */
  @Test
  @DisplayName("#10051 an update nets out at commit")
  void anUpdateNetsOutAtCommit() {
    createAndFill();

    database.begin();
    database.command("sql", "UPDATE Doc SET content = 'alpha beta gamma delta epsilon zeta' LIMIT 1");
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    database.commit();

    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L + 3);
  }

  /** The stale check compares the type-wide live count with the counter: after a rollback they must agree, so nothing rescans. */
  @Test
  @DisplayName("#10051 a rolled back insert leaves nothing for the stale check to repair")
  void aRolledBackInsertLeavesNothingForTheStaleCheckToRepair() {
    createAndFill();

    database.begin();
    for (int i = 0; i < 4; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.rollback();

    final FullTextIndexMetadata m = sharedMetadata();
    assertThat(m.getTotalDocs()).isEqualTo(database.countType("Doc", false));
    final long mutationsBefore = m.getMutations();

    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();

    assertThat(m.getTotalDocs()).isEqualTo(DOCS);
    assertThat(m.getSumDocLength()).isEqualTo(DOCS * 3L);
    assertThat(m.getMutations()).isEqualTo(mutationsBefore);
  }

  /**
   * A search that has to rebuild cold counters inside a transaction that already holds uncommitted inserts scans them too
   * (a transaction reads its own writes): the commit must not add them a second time.
   */
  @Test
  @DisplayName("#10051 a cold counters scan inside the writing transaction does not double count")
  void aColdCountersScanInsideTheWritingTransactionDoesNotDoubleCount() {
    createAndFill();
    sharedMetadata().setCountersValid(false);

    database.begin();
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    database.commit();

    final long live = database.countType("Doc", false);
    assertThat(live).isEqualTo(DOCS + 3L);
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(live);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L + 3 * 4L);
  }

  /**
   * The pending effect of the writing transaction is negative when it removed committed documents (the scan no longer sees
   * them), and mixed when it also inserted: what is subtracted from the scan must leave exactly the committed data, so the
   * commit lands the counters on the final state.
   */
  @Test
  @DisplayName("#10051 a cold counters scan inside atransaction that deleted and inserted counts the committed data once")
  void aColdCountersScanInsideATransactionThatDeletedAndInsertedCountsTheCommittedDataOnce() {
    createAndFill();
    sharedMetadata().setCountersValid(false);

    database.begin();
    database.command("sql", "DELETE FROM Doc LIMIT 2");
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(sharedMetadata().getTotalDocs()).as("the scan described the committed data: %d documents", DOCS).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    database.commit();

    assertThat(database.countType("Doc", false)).isEqualTo(DOCS + 1L);
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS + 1L);
    // 2 documents of 3 tokens left, 3 of 4 tokens came in
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L - 2 * 3L + 3 * 4L);
  }

  /** A scan inside a transaction that only deleted: the pending effect is negative and is added back. */
  @Test
  @DisplayName("#10051 a cold counters scan inside atransaction that only deleted counts the committed data once")
  void aColdCountersScanInsideATransactionThatOnlyDeletedCountsTheCommittedDataOnce() {
    createAndFill();
    sharedMetadata().setCountersValid(false);

    database.begin();
    database.command("sql", "DELETE FROM Doc LIMIT 4");
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    database.commit();

    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS - 4L);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo((DOCS - 4L) * 3L);
  }

  /**
   * A transaction whose WAL ships inside a schema entry carries no corpus deltas: it marks the counters of the indexes it wrote
   * as recomputed instead, and the schema entry's counters then reach the followers.
   */
  @Test
  @DisplayName("#10051 advancing the epochs marks exactly the indexes the transaction wrote")
  void advancingTheEpochsMarksExactlyTheIndexesTheTransactionWrote() {
    createAndFill();
    assertThat(sharedMetadata().getCountersEpoch()).isZero();

    database.begin();
    database.command("sql", "INSERT INTO Doc SET content = 'alpha beta'");
    final FullTextCorpusDeltas deltas = FullTextCorpusDeltas.peek(((DatabaseInternal) database).getTransaction());
    assertThat(deltas).isNotNull();
    deltas.advanceEpochs();
    database.commit();

    assertThat(sharedMetadata().getCountersEpoch()).isEqualTo(1L);
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS + 1L);
  }

  /**
   * A scan that overlaps a commit between making its pages visible and applying its delta reads documents the counters do not
   * hold yet, and would count them twice once the delta lands: it must not call its result exact, or the drift check is spent and
   * the error stays until the next explicit recompute. The commit marks itself, and the scan arms the check.
   */
  @Test
  @DisplayName("#10051 a scan overlapping acommit that is applying its delta is not called exact")
  void aScanOverlappingACommitThatIsApplyingItsDeltaIsNotCalledExact() {
    createAndFill();
    reopenDatabase();
    final FullTextIndexMetadata m = sharedMetadata();
    m.applyCorpusDelta(5, 20); // a drift, so the first search scans

    // The marker is held for the whole search here, so every scan the search runs (one per bucket index) overlaps it; a real
    // window lasts microseconds and only the first scan can meet it
    m.commitApplyStarted();
    try {
      database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    } finally {
      m.commitApplyFinished();
    }
    final long scansWhileApplying = m.getScans();
    assertThat(scansWhileApplying).isGreaterThanOrEqualTo(1L);
    assertThat(m.isStaleCheckDone()).as("the result was not exact: the check stays armed").isFalse();

    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(m.getScans()).as("with no commit in flight it rescans, even though the counts agree")
        .isGreaterThan(scansWhileApplying);
    assertThat(m.isStaleCheckDone()).isTrue();

    final long settled = m.getScans();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(m.getScans()).as("then it settles").isEqualTo(settled);
    assertThat(m.getTotalDocs()).isEqualTo(DOCS);
  }

  /** The same scan, but the transaction is then rolled back: the scan result must describe the committed data only. */
  @Test
  @DisplayName("#10051 a cold counters scan inside atransaction that rolls back describes the committed data")
  void aColdCountersScanInsideATransactionThatRollsBackDescribesTheCommittedData() {
    createAndFill();
    sharedMetadata().setCountersValid(false);

    database.begin();
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    database.rollback();

    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  /**
   * A commit moves the counters but does not dirty the schema (that would rewrite schema.json after every commit), so the close
   * has to persist what the commits since the last schema save did: a restart then finds them exact and scans nothing.
   */
  @Test
  @DisplayName("#10051 the counters committed since the last schema save are persisted at close")
  void theCountersCommittedSinceTheLastSchemaSaveArePersistedAtClose() {
    createAndFill();
    assertThat(sharedMetadata().isCountersDirty()).as("dirty before close").isTrue();

    reopenDatabase();

    assertThat(sharedMetadata().isCountersValid()).isTrue();
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    assertThat(sharedMetadata().getScans()).isZero();
  }

  /**
   * Serializing the counters for something that is not a write of the schema file (an export, a replication payload) must not
   * make the close believe they were saved.
   */
  @Test
  @DisplayName("#10051 serializing the schema without writing it does not lose the counters at close")
  void serializingTheSchemaWithoutWritingItDoesNotLoseTheCountersAtClose() {
    createAndFill();
    assertThat(sharedMetadata().isCountersDirty()).isTrue();

    database.getSchema().getEmbedded().toJSON();
    database.getSchema().getEmbedded().toJSON();

    assertThat(sharedMetadata().isCountersDirty()).as("an export saved nothing").isTrue();
    reopenDatabase();
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getScans()).isZero();
  }

  /** The generation a write records as saved is the one read BEFORE it serialized: a change made while the file was written stays unsaved. */
  @Test
  @DisplayName("#10051 a change made while the schema was being written stays unsaved")
  void aChangeMadeWhileTheSchemaWasBeingWrittenStaysUnsaved() {
    createAndFill();
    final FullTextIndexMetadata m = sharedMetadata();

    final long readBeforeSerializing = m.getCountersGeneration();
    m.applyCorpusDelta(1, 3); // a commit lands while the file is being written
    m.markCountersSaved(readBeforeSerializing);

    assertThat(m.isCountersDirty()).as("the commit made during the write is not in the file").isTrue();

    m.markCountersSaved(m.getCountersGeneration());
    assertThat(m.isCountersDirty()).isFalse();
  }

  @Test
  @DisplayName("#10051 a completed schema write marks the counters saved unless they moved meanwhile")
  void aCompletedSchemaWriteMarksTheCountersSavedUnlessTheyMovedMeanwhile() {
    createAndFill();
    assertThat(sharedMetadata().isCountersDirty()).isTrue();

    database.getSchema().getEmbedded().saveConfiguration();
    assertThat(sharedMetadata().isCountersDirty()).isFalse();

    sharedMetadata().applyCorpusDelta(1, 3);
    assertThat(sharedMetadata().isCountersDirty()).isTrue();
  }

  /**
   * The retry of a transaction that a ConcurrentModificationException refused runs its body again on the same context: what the
   * refused attempt accumulated must be gone, or the counters move twice.
   */
  @Test
  @DisplayName("#10051 a transaction retried after aconcurrent modification counts only the attempt that commits")
  void aTransactionRetriedAfterAConcurrentModificationCountsOnlyTheAttemptThatCommits() {
    createAndFill();
    final RID rid = database.iterateType("Doc", false).next().getIdentity();
    final AtomicInteger attempts = new AtomicInteger();

    database.transaction(() -> {
      final boolean firstAttempt = attempts.incrementAndGet() == 1;
      database.lookupByRID(rid, true).asDocument().modify().set("content", "alpha beta gamma delta").save();
      database.newDocument("Doc").set("content", "alpha beta").save();
      if (firstAttempt) {
        // another thread commits a change to the same record before this attempt does
        final Thread other = new Thread(() -> database.transaction(
            () -> database.lookupByRID(rid, true).asDocument().modify().set("content", "alpha").save()));
        other.start();
        try {
          other.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, false, 3);

    assertThat(attempts.get()).as("the first attempt was refused").isEqualTo(2);
    // 11 untouched documents of 3 tokens, the one that was rewritten to 4, and the one the committed attempt inserted (2)
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS + 1L);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo((DOCS - 1) * 3L + 4 + 2);
  }

  /**
   * A commit that the cluster-wide check refuses with a ConcurrentModificationException counts nothing, and the transaction that
   * won counts exactly its own effect: the commit-time index replay never goes through the counting code.
   */
  @Test
  @DisplayName("#10051 a commit refused with aconcurrent modification never counts its documents")
  void aCommitRefusedWithAConcurrentModificationNeverCountsItsDocuments() throws Exception {
    createAndFill();
    final long lengthBefore = sharedMetadata().getSumDocLength();

    database.begin();
    final MutableDocument doc = database.iterateType("Doc", false).next().asDocument().modify();
    doc.set("content", "alpha beta gamma delta epsilon zeta eta theta").save();
    final RID rid = doc.getIdentity();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread other = new Thread(() -> {
      try {
        database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("content", "alpha").save());
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    other.start();
    other.join(30_000L);
    assertThat(other.isAlive()).as("the concurrent transaction finished").isFalse();
    assertThat(failure.get()).isNull();

    assertThatThrownBy(database::commit).isInstanceOf(ConcurrentModificationException.class);
    if (database.isTransactionActive())
      database.rollback();

    // The winner shortened one document from 3 tokens to 1; the refused transaction (8 tokens) is nowhere in the counters
    assertThat(sharedMetadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(sharedMetadata().getSumDocLength()).isEqualTo(lengthBefore - 2);
  }

  // ===== Issue #10184 =====
  // Issue #10184 (#10130, #10131, #10133): the BM25 corpus counters drifted in ways the drift check could not see.
  // <ul>
  //   <li>#10131: a document whose only indexed property is null was counted with length 0 by the put and uncounted with length 1
  //   by the remove, so every insert+delete of such a document took one token off {@code sumDocLength} for good;</li>
  //   <li>#10130: the drift check compared the polymorphic counters with the type's own record count, so a full-text index on a
  //   type with subtypes rescanned the whole type and warned on the first search of every session;</li>
  //   <li>#10133: after a crash the persisted pair is the one of the last schema save, and the drift check only compares the
  //   document count, so a recovered workload that left the count unchanged kept the old {@code sumDocLength} forever.</li>
  // </ul>

  private void search(final String type, final String indexName) {
    database.query("sql", "SELECT FROM " + type + " WHERE SEARCH_INDEX('" + indexName + "', 'alpha') = true").close();
  }

  @Test
  @DisplayName("#10184 inserting and deleting anull valued document leaves the counters where they were")
  void insertingAndDeletingANullValuedDocumentLeavesTheCountersWhereTheyWere() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma', tag = 't'");
    });
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(5L, 15L);

    for (int i = 0; i < 4; i++) {
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET tag = 'nulltest'"));
      database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE tag = 'nulltest'"));
    }

    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(5L, 15L);
  }

  @Test
  @DisplayName("#10184 updating the indexed property from and to null keeps the counters exact")
  void updatingTheIndexedPropertyFromAndToNullKeepsTheCountersExact() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma', tag = 'a'"));
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET tag = 'b'"));
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(2L, 3L);

    database.transaction(() -> database.command("sql", "UPDATE Doc SET content = 'alpha beta' WHERE tag = 'b'"));
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(2L, 5L);

    database.transaction(() -> database.command("sql", "UPDATE Doc REMOVE content WHERE tag = 'a'"));
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(2L, 2L);

    database.transaction(() -> database.command("sql", "DELETE FROM Doc"));
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(0L, 0L);
  }

  @Test
  @DisplayName("#10184 a multi property index with anull property balances put and remove")
  void aMultiPropertyIndexWithANullPropertyBalancesPutAndRemove() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.body STRING");
      database.command("sql", "CREATE INDEX ON Doc (title, body) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET title = 'alpha beta', body = 'gamma'"));
    assertThat(metadata("Doc[title,body]").getCountersPair()).containsExactly(1L, 3L);

    for (int i = 0; i < 3; i++) {
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET title = 'delta'"));
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET body = 'epsilon zeta'"));
      database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE title = 'delta' OR body = 'epsilon zeta'"));
    }

    assertThat(metadata("Doc[title,body]").getCountersPair()).containsExactly(1L, 3L);
  }

  @Test
  @DisplayName("#10184 an index on atype with subtypes finds nothing to repair on afresh session")
  void anIndexOnATypeWithSubtypesFindsNothingToRepairOnAFreshSession() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Parent BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Parent.content STRING");
      database.command("sql", "CREATE INDEX ON Parent (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      database.command("sql", "CREATE DOCUMENT TYPE Child EXTENDS Parent BUCKETS 2");
    });
    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        database.command("sql", "INSERT INTO Parent SET content = 'alpha beta gamma'");
      for (int i = 0; i < 6; i++)
        database.command("sql", "INSERT INTO Child SET content = 'alpha beta gamma'");
    });

    for (int session = 0; session < 2; session++) {
      reopenDatabase();
      search("Parent", "Parent[content]");

      assertThat(metadata("Parent[content]").isStaleCheckDone()).isTrue();
      // The counters were exact: the drift check found nothing to repair and scanned nothing
      assertThat(metadata("Parent[content]").getScans()).isZero();
      assertThat(metadata("Parent[content]").getCountersPair()).containsExactly(10L, 30L);
    }
  }

  @Test
  @DisplayName("#10184 a real drift on atype with subtypes is still repaired")
  void aRealDriftOnATypeWithSubtypesIsStillRepaired() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Parent BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Parent.content STRING");
      database.command("sql", "CREATE INDEX ON Parent (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      database.command("sql", "CREATE DOCUMENT TYPE Child EXTENDS Parent BUCKETS 2");
    });
    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        database.command("sql", "INSERT INTO Parent SET content = 'alpha beta gamma'");
      for (int i = 0; i < 6; i++)
        database.command("sql", "INSERT INTO Child SET content = 'alpha beta gamma'");
    });
    metadata("Parent[content]").applyCorpusDelta(3, 20);
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();

    reopenDatabase();
    search("Parent", "Parent[content]");

    assertThat(metadata("Parent[content]").getScans()).isEqualTo(1L);
    assertThat(metadata("Parent[content]").getCountersPair()).containsExactly(10L, 30L);
  }

  /** The invariant behind all of the above: whatever the workload, the incremental counters are what a fresh rescan finds. */
  @Test
  @DisplayName("#10184 after amixed workload the counters are what arescan finds")
  void afterAMixedWorkloadTheCountersAreWhatARescanFinds() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Parent BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Parent.content STRING");
      database.command("sql", "CREATE PROPERTY Parent.tag STRING");
      database.command("sql", "CREATE INDEX ON Parent (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      database.command("sql", "CREATE DOCUMENT TYPE Child EXTENDS Parent BUCKETS 2");
    });
    database.transaction(() -> {
      for (int i = 0; i < 6; i++) {
        database.command("sql", "INSERT INTO Parent SET content = 'alpha beta gamma', tag = 'p" + i + "'");
        database.command("sql", "INSERT INTO Child SET content = 'alpha delta', tag = 'c" + i + "'");
        database.command("sql", "INSERT INTO Parent SET tag = 'pn" + i + "'");
        database.command("sql", "INSERT INTO Child SET tag = 'cn" + i + "'");
      }
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE Parent SET content = 'one two three four' WHERE tag = 'pn1'");
      database.command("sql", "UPDATE Child SET content = 'five' WHERE tag = 'cn2'");
      database.command("sql", "UPDATE Parent REMOVE content WHERE tag = 'p3'");
      database.command("sql", "UPDATE Child REMOVE content WHERE tag = 'c4'");
      database.command("sql", "DELETE FROM Parent WHERE tag IN ['p0', 'pn0', 'c0', 'cn0', 'pn5', 'cn5']");
    });

    final FullTextIndexMetadata metadata = metadata("Parent[content]");
    final long[] incremental = metadata.getCountersPair();
    assertThat(incremental[0]).isEqualTo(database.countType("Parent", true));

    ((LSMTreeFullTextIndex) ((TypeIndex) database.getSchema().getIndexByName("Parent[content]")).getIndexesOnBuckets()[0]).recomputeBM25Counters();

    assertThat(metadata.getCountersPair()).containsExactly(incremental[0], incremental[1]);
  }

  @Test
  @DisplayName("#10184 after acrash the first search repairs alength drift the document count cannot show")
  void afterACrashTheFirstSearchRepairsALengthDriftTheDocumentCountCannotShow() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'aa bb cc', tag = 'old'");
    });
    // A clean close persists {10, 30}
    reopenDatabase();
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(10L, 30L);

    // Same number of documents, longer ones: only sumDocLength moves
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'dd ee ff gg hh ii jj kk ll mm', tag = 'new'");
      database.command("sql", "DELETE FROM Doc WHERE tag = 'old' LIMIT 5");
    });
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(10L, 65L);

    // Crash: the schema still holds the pair of the last save, the WAL holds the transaction
    ((DatabaseInternal) database).kill();
    database.close();
    database = factory.open();

    search("Doc", "Doc[content]");

    assertThat(database.countType("Doc", true)).isEqualTo(10L);
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(10L, 65L);
    assertThat(metadata("Doc[content]").getScans()).isEqualTo(1L);
  }

  // ===== Issue #10397 =====
  // Issue #10397: the drift check of the BM25 corpus counters compared the document count only, so it could not see a corpus
  // whose record count was right and whose total length was wrong. The sum of the lengths cannot be compared with the live data
  // without analyzing it, so the check compares what the sum depends on: the counters are persisted with the analyzer (and the
  // rule that counts tokens) they were measured with, and a different one arms the rescan.

  private void createFillAndCheckPair() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < SMALL_DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, SMALL_DOCS * 3L);
  }

  @Test
  @DisplayName("#10397 an analyzer changed between sessions rescans even when the count is unchanged")
  void anAnalyzerChangedBetweenSessionsRescansEvenWhenTheCountIsUnchanged() throws IOException {
    createFillAndCheckPair();

    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));
    // THE RECORD COUNT STILL AGREES WITH THE PERSISTED ONE: ONLY THE LENGTHS ARE WRONG NOW
    assertThat(metadata().getTotalDocs()).isEqualTo(SMALL_DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(SMALL_DOCS * 3L);

    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    // A KeywordAnalyzer MAKES ONE TOKEN OF A WHOLE VALUE
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, (long) SMALL_DOCS);
  }

  @Test
  @DisplayName("#10397 an unchanged analyzer scans nothing")
  void anUnchangedAnalyzerScansNothing() {
    createFillAndCheckPair();

    for (int session = 0; session < 3; session++) {
      reopenDatabase();
      search();

      assertThat(metadata().getScans()).as("session %d", session).isZero();
      assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, SMALL_DOCS * 3L);
    }
  }

  @Test
  @DisplayName("#10397 counters persisted before the signature existed are trusted and adopted")
  void countersPersistedBeforeTheSignatureExistedAreTrustedAndAdopted() throws IOException {
    createFillAndCheckPair();
    database.getSchema().getEmbedded().saveConfiguration();

    editIndexDefinition(index -> index.remove("ft_lengthSignature"));
    search();

    assertThat(metadata().getScans()).isZero();
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, SMALL_DOCS * 3L);
    // ADOPTED: THE NEXT SAVE RECORDS WHAT THEY ARE MEASURED WITH FROM NOW ON
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();
    assertThat(indexDefinition().has("ft_lengthSignature")).isTrue();
  }

  /**
   * A schema save between the change and the first search must not record the new analyzer next to the old counters: after a
   * restart the check would then find them consistent, and nothing would ever repair them.
   */
  @Test
  @DisplayName("#10397 a schema save before the first search does not forget the change")
  void aSchemaSaveBeforeTheFirstSearchDoesNotForgetTheChange() throws IOException {
    createFillAndCheckPair();

    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();
    reopenDatabase();
    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, (long) SMALL_DOCS);
  }

  @Test
  @DisplayName("#10397 a rescan records the analyzer it measured with")
  void aRescanRecordsTheAnalyzerItMeasuredWith() throws IOException {
    createFillAndCheckPair();
    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));
    search();
    assertThat(metadata().getScans()).isEqualTo(1L);

    reopenDatabase();
    search();

    assertThat(metadata().getScans()).as("the repair is paid once").isZero();
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, (long) SMALL_DOCS);
  }

  @Test
  @DisplayName("#10397 an explicit recompute records the current analyzer")
  void anExplicitRecomputeRecordsTheCurrentAnalyzer() throws IOException {
    createFillAndCheckPair();
    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));

    database.command("sql", "REBUILD INDEX `" + INDEX + "` WITH statsOnly = true").close();
    reopenDatabase();
    search();

    assertThat(metadata().getScans()).isZero();
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, (long) SMALL_DOCS);
  }

  @Test
  @DisplayName("#10397 the description of the counters says whether they are measured with the current analyzer")
  void theDescriptionOfTheCountersSaysWhetherTheyAreMeasuredWithTheCurrentAnalyzer() throws IOException {
    createFillAndCheckPair();
    assertThat(describe().get("measuredWithCurrentAnalyzer")).isEqualTo(true);

    editIndexDefinition(index -> index.put("index_analyzer", KEYWORD_ANALYZER));
    assertThat(describe().get("measuredWithCurrentAnalyzer")).isEqualTo(false);

    search();
    assertThat(describe().get("measuredWithCurrentAnalyzer")).isEqualTo(true);
  }

  /**
   * Pins what the rule that counts the tokens of a document gives for a fixture, and that the signature the counters are saved
   * with is the one derived from the analyzer and the rule themselves ({@link FullTextLengthSignature}, issue #10606). There is no
   * version to bump when the rule changes: the signature follows, and the counters persisted by the old rule are rescanned.
   */
  @Test
  @DisplayName("#10397 the rule that counts the tokens is pinned to its derived signature")
  void theRuleThatCountsTheTokensIsPinnedToItsDerivedSignature() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
      database.command("sql", "INSERT INTO Doc SET content = 'one'");
      database.command("sql", "INSERT INTO Doc SET content = 'the quick brown fox', tag = 'x'");
      database.command("sql", "INSERT INTO Doc SET tag = 'no content'");
    });

    assertThat(metadata().getCountersPair()).as("documents and tokens: a null value is a document of no token").containsExactly(4L, 8L);
    try (final StandardAnalyzer analyzer = new StandardAnalyzer()) {
      assertThat(metadata().getLengthSignature()).as("derived from what the analyzer and the rule do, not from a number")
          .isEqualTo(FullTextIndexMetadata.DEFAULT_ANALYZER + "#" + FullTextLengthSignature.of(analyzer));
    }
  }

  private JSONObject indexDefinition() throws IOException {
    final JSONObject schema = new JSONObject(Files.readString(schemaFile()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    return indexes.getJSONObject(indexes.keySet().iterator().next());
  }

  // ===== Issue #10606 =====
  // Issue #10606: the persisted BM25 pair (document count, sum of the document lengths) was guarded by the count and by a
  // signature, {@code analyzer class name + '#' + LENGTH_MEASURE_VERSION}, standing in for the sum, which nothing compares. Three
  // routes broke the inference and left every score wrong with the count right and nothing arming a rescan:
  // <ol>
  //   <li>the version was a constant somebody had to remember to bump;</li>
  //   <li>the class NAME stood for the analyzer, so an analyzer whose tokenization changed under the same name (a Lucene upgrade) was
  //   invisible;</li>
  //   <li>the sum was clamped at zero with the count and the signature intact, so an arithmetic error became a permanently wrong
  //   average length.</li>
  // </ol>
  // The signature is now derived from what it describes (the lengths the real analyzer and the real rule produce over a fixed
  // corpus, plus the Lucene version), a clamp is counted and arms a rescan, and the one regime in which the sum can be read without a
  // scan (an empty corpus holds no token) is checked.

  /**
   * An analyzer whose behaviour is switched from outside while its class name stays the same: what a library upgrade does to
   * {@code StandardAnalyzer} between two runs of the same application.
   */
  public static final class MovingAnalyzer extends Analyzer {
    static volatile boolean wholeValueIsOneToken = false;

    public MovingAnalyzer() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      return new TokenStreamComponents(wholeValueIsOneToken ? new KeywordTokenizer() : new WhitespaceTokenizer());
    }
  }

  @AfterEach
  void restoreTheMovingAnalyzer() {
    MovingAnalyzer.wholeValueIsOneToken = false;
  }

  // ---- route 2: the analyzer is told by what it does, not by its name

  @Test
  @DisplayName("#10606 an analyzer that changes under the same name changes the signature")
  void anAnalyzerThatChangesUnderTheSameNameChangesTheSignature() {
    // ONE INSTANCE PER STATE: A LUCENE ANALYZER KEEPS THE COMPONENTS IT BUILT FIRST, AS AN APPLICATION RESTARTED ON ANOTHER LIBRARY DOES
    final String before;
    try (final MovingAnalyzer analyzer = new MovingAnalyzer()) {
      before = FullTextLengthSignature.of(analyzer);
    }

    MovingAnalyzer.wholeValueIsOneToken = true;
    try (final MovingAnalyzer analyzer = new MovingAnalyzer()) {
      assertThat(FullTextLengthSignature.of(analyzer)).isNotEqualTo(before);
    }
  }

  @Test
  @DisplayName("#10606 the signature of an unchanged analyzer is stable")
  void theSignatureOfAnUnchangedAnalyzerIsStable() {
    try (final StandardAnalyzer first = new StandardAnalyzer(); final StandardAnalyzer second = new StandardAnalyzer()) {
      assertThat(FullTextLengthSignature.of(first)).isEqualTo(FullTextLengthSignature.of(second)).isEqualTo(FullTextLengthSignature.of(first));
    }
  }

  @Test
  @DisplayName("#10606 a lucene upgrade is part of the signature")
  void aLuceneUpgradeIsPartOfTheSignature() {
    try (final StandardAnalyzer analyzer = new StandardAnalyzer()) {
      assertThat(FullTextLengthSignature.of(analyzer)).contains(Version.LATEST.toString());
    }
  }

  // ---- route 1: the length rule is told by what it counts, not by a number somebody bumps

  @Test
  @DisplayName("#10606 a change of the length rule changes the signature")
  void aChangeOfTheLengthRuleChangesTheSignature() {
    try (final StandardAnalyzer analyzer = new StandardAnalyzer()) {
      final String current = FullTextLengthSignature.of(analyzer);

      // a rule that counts the null token of a null value as a token: the numbers of a four-document fixture need not move
      final String countsNulls = FullTextLengthSignature.of(analyzer, List::size);
      // a rule that counts one token less per value
      final String countsOneLess = FullTextLengthSignature.of(analyzer, tokens -> Math.max(0, tokens.size() - 1));

      assertThat(countsNulls).isNotEqualTo(current);
      assertThat(countsOneLess).isNotEqualTo(current).isNotEqualTo(countsNulls);
    }
  }

  @Test
  @DisplayName("#10606 the metadata binds the rule of its index into the signature")
  void theMetadataBindsTheRuleOfItsIndexIntoTheSignature() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    final String unbound = metadata.getLengthSignature();

    metadata.bindLengthRule("lucene-x:abc");

    assertThat(metadata.getLengthSignature()).isNotEqualTo(unbound).isEqualTo(metadata.getIndexAnalyzerClass() + "#lucene-x:abc");
  }

  // ---- route 3: a clamp is not a floor

  @Test
  @DisplayName("#10606 a removal below zero is counted and arms the rescan")
  void aRemovalBelowZeroIsCountedAndArmsTheRescan() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(1, 3);
    assertThat(metadata.getClamps()).isZero();

    metadata.removeDocument(3L);
    assertThat(metadata.getClamps()).as("an exact removal clamps nothing").isZero();
    assertThat(metadata.consumeRescanRequired()).isFalse();

    metadata.removeDocument(3L);

    assertThat(metadata.getClamps()).isPositive();
    assertThat(metadata.consumeRescanRequired()).isTrue();
    assertThat(metadata.getCountersPair()).containsExactly(0L, 0L);
  }

  @Test
  @DisplayName("#10606 a sum driven below zero with the count intact is counted")
  void aSumDrivenBelowZeroWithTheCountIntactIsCounted() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 2);

    // one document of 9 tokens leaves while the sum holds 2: the count is fine, the sum cannot be
    metadata.removeDocument(9L);

    assertThat(metadata.getClamps()).isPositive();
    assertThat(metadata.consumeRescanRequired()).isTrue();
  }

  @Test
  @DisplayName("#10606 a commit delta below zero is counted")
  void aCommitDeltaBelowZeroIsCounted() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 10);

    metadata.applyCorpusDelta(-1, -30);

    assertThat(metadata.getClamps()).isPositive();
    assertThat(metadata.consumeRescanRequired()).isTrue();
  }

  @Test
  @DisplayName("#10606 a scan published over counters that drove it below zero is not exact")
  void aScanPublishedOverCountersThatDroveItBelowZeroIsNotExact() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 10);
    final long mutationsAtStart = metadata.getMutations();

    // the scan found an empty corpus, and the counters sit at the values it started from, so the delta takes them below zero
    // exactly where the scan result is zero: the clamp makes the published pair EQUAL to the scan result by accident
    final boolean exact = metadata.publishScannedCounters(0, 0, 10, 40, mutationsAtStart);

    assertThat(metadata.getClamps()).isPositive();
    assertThat(exact).isFalse();
    assertThat(metadata.consumeRescanRequired()).isTrue();
  }

  @Test
  @DisplayName("#10606 the counters describe their clamps")
  void theCountersDescribeTheirClamps() {
    createAndFillSmall();
    metadata().removeDocument(1L);
    for (int i = 0; i < SMALL_DOCS + 1; i++)
      metadata().removeDocument(1L);

    assertThat(LSMTreeFullTextIndex.describeCorpusCounters(database.getSchema().getIndexByName(INDEX)).get("clamps")).isNotEqualTo(0L);
  }

  // ---- the pair: the sum is read where it can be

  @Test
  @DisplayName("#10606 an empty corpus holds no tokens whatever the sum says")
  void anEmptyCorpusHoldsNoTokensWhateverTheSumSays() throws IOException {
    createAndFillSmall();
    database.transaction(() -> database.command("sql", "DELETE FROM Doc"));
    assertThat(metadata().getCountersPair()).containsExactly(0L, 0L);

    // the persisted pair is torn the way a crash or a clamp tears it: the count is right (no documents), the sum is not
    editIndexDefinition(index -> index.put("ft_sumDocLength", 5L));
    assertThat(metadata().getCountersPair()).containsExactly(0L, 5L);

    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly(0L, 0L);
  }

  @Test
  @DisplayName("#10606 an analyzer behaviour changed under the same class name rescans even when the count is unchanged")
  void anAnalyzerBehaviourChangedUnderTheSameClassNameRescansEvenWhenTheCountIsUnchanged() {
    createAndFillSmall(MovingAnalyzer.class.getName());
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, SMALL_DOCS * 3L);

    // THE SAME CLASS NAME, ANOTHER TOKENIZATION: THE NEXT SESSION MEASURES A WHOLE VALUE AS ONE TOKEN
    database.close();
    MovingAnalyzer.wholeValueIsOneToken = true;
    reopenDatabase();
    assertThat(metadata().getSumDocLength()).as("the persisted pair is the old one").isEqualTo(SMALL_DOCS * 3L);

    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, (long) SMALL_DOCS);
  }

  @Test
  @DisplayName("#10606 an unchanged analyzer scans nothing in the next sessions")
  void anUnchangedAnalyzerScansNothingInTheNextSessions() {
    createAndFillSmall(MovingAnalyzer.class.getName());

    for (int session = 0; session < 3; session++) {
      reopenDatabase();
      search();

      assertThat(metadata().getScans()).as("session %d", session).isZero();
      assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, SMALL_DOCS * 3L);
    }
  }

  private void createAndFillSmall() {
    createAndFillSmall(null);
  }

  private void createAndFillSmall(final String analyzerClass) {
    final String metadata = analyzerClass == null ? "{\"similarity\": \"BM25\"}" : "{\"similarity\": \"BM25\", \"analyzer\": \"" + analyzerClass + "\"}";
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA " + metadata);
    });
    database.transaction(() -> {
      for (int i = 0; i < SMALL_DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
  }

  // ===== Issue #10735 =====
  // Issue #10735: a clamp is the engine's own proof that the persisted BM25 pair is wrong, and it armed the rescan in memory only.
  // The pair was saved with {@code ft_countersValid: true} and without the armed state, so a restart between the clamp and the next
  // search dropped the proof and kept the damage: the reloaded pair passed every check of the drift check (the count is right, the
  // analyzer did not change, the corpus is not empty) and the average document length stayed wrong for good. The armed state is now
  // part of the persisted unit, it is written whenever it is armed, and it is cleared only by a scan that repaired the pair.

  /** The reporter's steps: a sum driven below zero with the count intact, a restart, a search. */
  @Test
  @DisplayName("#10735 a clamp survives arestart and the first search repairs the pair")
  void aClampSurvivesARestartAndTheFirstSearchRepairsThePair() {
    createAndFillSmall();
    metadata().applyCorpusDelta(0, -1000);
    assertThat(metadata().getCountersPair()).as("the premise: the clamp tore the pair").containsExactly((long) SMALL_DOCS, 0L);

    restart();
    assertThat(metadata().getCountersPair()).as("the torn pair was persisted").containsExactly((long) SMALL_DOCS, 0L);
    assertThat(describe().get("rescanRequired")).as("and so was the proof that it is torn").isEqualTo(true);

    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, SMALL_DOCS * 3L);
    assertThat(metadata().avgDocLength()).isEqualTo(3.0);
    assertThat(describe().get("rescanRequired")).isEqualTo(false);

    // REPAIRED, AND THE REPAIR IS WHAT THE NEXT SESSION FINDS: NO SCAN AGAIN
    restart();
    search();
    assertThat(metadata().getScans()).isZero();
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, SMALL_DOCS * 3L);
  }

  /** The per-document removal path clamps too, lock-free: its proof must reach the disk the same way. */
  @Test
  @DisplayName("#10735 a removal clamp survives arestart")
  void aRemovalClampSurvivesARestart() {
    createAndFillSmall();
    // one document of 1000 tokens leaves a corpus that holds 30: the count stays sane, the sum cannot
    metadata().removeDocument(1000L);
    metadata().addDocument(0L);
    assertThat(metadata().getClamps()).isPositive();

    restart();
    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly((long) SMALL_DOCS, SMALL_DOCS * 3L);
  }

  /**
   * A rescan armed without moving a counter (crash recovery arms every BM25 index, a replicated delta that could not be applied
   * arms its own) used to leave the counters clean, so a close before the first search wrote nothing and the next session never
   * rescanned. Arming is a change of what the persisted unit says.
   */
  @Test
  @DisplayName("#10735 a rescan armed without any counter change survives aclean close")
  void aRescanArmedWithoutAnyCounterChangeSurvivesACleanClose() {
    createAndFillSmall();
    restart();
    assertThat(metadata().isCountersDirty()).as("the premise: nothing left to save").isFalse();

    metadata().requireRescan();
    assertThat(metadata().isCountersDirty()).isTrue();

    restart();
    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
  }

  /**
   * A save that serialized the counters BEFORE the rescan was armed must not be taken for one that recorded it: the generation it
   * marks saved is older than the arming, so the counters stay dirty and the next save (the close) writes the armed state.
   */
  @Test
  @DisplayName("#10735 an arming that lands after aserialization keeps the counters dirty")
  void anArmingThatLandsAfterASerializationKeepsTheCountersDirty() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 15);

    final long generation = metadata.getCountersGeneration();
    final JSONObject saved = metadata.writeToJSON(new JSONObject());
    metadata.applyCorpusDelta(0, -100);
    metadata.markCountersSaved(generation);

    assertThat(saved.has("ft_rescanRequired")).isFalse();
    assertThat(metadata.isCountersDirty()).isTrue();
    assertThat(metadata.writeToJSON(new JSONObject()).getBoolean("ft_rescanRequired", false)).isTrue();
  }

  /** The armed state is read back by a full load, and adopted with the pair by a node that takes a leader's recompute. */
  @Test
  @DisplayName("#10735 the armed state travels with the pair")
  void theArmedStateTravelsWithThePair() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 15);
    assertThat(metadata.writeToJSON(new JSONObject()).has("ft_rescanRequired")).as("a sound pair carries no flag").isFalse();

    metadata.requireRescan();
    final JSONObject definition = metadata.writeToJSON(new JSONObject());
    assertThat(definition.getBoolean("ft_rescanRequired", false)).isTrue();

    final FullTextIndexMetadata restored = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    restored.fromJSON(definition);
    assertThat(restored.isRescanRequired()).isTrue();

    final FullTextIndexMetadata follower = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    follower.setCounters(5, 15);
    assertThat(follower.adoptPersistedCounters(definition.put("ft_countersEpoch", 1L))).isTrue();
    assertThat(follower.isRescanRequired()).as("a pair known to be wrong is adopted with what is known against it").isTrue();
  }

  /** A scan that could not be proven exact leaves the flag armed, and that too reaches the disk. */
  @Test
  @DisplayName("#10735 only an exact scan disarms the flag")
  void onlyAnExactScanDisarmsTheFlag() {
    final FullTextIndexMetadata metadata = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    metadata.setCounters(5, 15);
    metadata.requireRescan();

    assertThat(metadata.publishScannedCounters(5, 15, 5, 15, metadata.getMutations() - 1)).isFalse();
    assertThat(metadata.isRescanRequired()).isTrue();
    assertThat(metadata.writeToJSON(new JSONObject()).getBoolean("ft_rescanRequired", false)).isTrue();

    assertThat(metadata.publishScannedCounters(5, 15, 5, 15, metadata.getMutations())).isTrue();
    assertThat(metadata.isRescanRequired()).isFalse();
    assertThat(metadata.writeToJSON(new JSONObject()).has("ft_rescanRequired")).isFalse();
  }

  /**
   * Until the rescan runs, the torn pair averages 0.0. The scorer normalizes by the neutral 1.0 then, and every path reports and
   * uses that same value: the bucket-level path (and EXPLAIN) used to report the 0.0, the type-wide one floored every average below
   * one token per document to 1.0, real ones included.
   */
  @Test
  @DisplayName("#10735 every path normalizes by the same average")
  void everyPathNormalizesByTheSameAverage() {
    createAndFillSmall();
    metadata().applyCorpusDelta(0, -1000);
    assertThat(metadata().avgDocLength()).as("the premise: the torn pair averages nothing").isZero();

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final LSMTreeFullTextIndex bucketIndex = (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];
    assertThat(bucketIndex.getTypeWideAverageDocumentLength()).isEqualTo(1.0);

    assertThat(BM25Scorer.lengthNormalizer(0.0)).isEqualTo(1.0);
    assertThat(BM25Scorer.lengthNormalizer(Double.NaN)).isEqualTo(1.0);
    assertThat(BM25Scorer.lengthNormalizer(0.5)).as("a real average below one token is a real average").isEqualTo(0.5);
    assertThat(BM25Scorer.termScore(1.0, 1, 1, 0.0, 1.2, 0.75)).isEqualTo(BM25Scorer.termScore(1.0, 1, 1, 1.0, 1.2, 0.75));
  }

  private void restart() {
    database.close();
    reopenDatabase();
  }
}
