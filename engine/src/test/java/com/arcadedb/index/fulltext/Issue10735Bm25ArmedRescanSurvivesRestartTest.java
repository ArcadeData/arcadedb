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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10735: a clamp is the engine's own proof that the persisted BM25 pair is wrong, and it armed the rescan in memory only.
 * The pair was saved with {@code ft_countersValid: true} and without the armed state, so a restart between the clamp and the next
 * search dropped the proof and kept the damage: the reloaded pair passed every check of the drift check (the count is right, the
 * analyzer did not change, the corpus is not empty) and the average document length stayed wrong for good. The armed state is now
 * part of the persisted unit, it is written whenever it is armed, and it is cleared only by a scan that repaired the pair.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10735Bm25ArmedRescanSurvivesRestartTest extends TestHelper {
  private static final int    DOCS  = 10;
  private static final String INDEX = "Doc[content]";

  /** The reporter's steps: a sum driven below zero with the count intact, a restart, a search. */
  @Test
  void aClampSurvivesARestartAndTheFirstSearchRepairsThePair() {
    createAndFill();
    metadata().applyCorpusDelta(0, -1000);
    assertThat(metadata().getCountersPair()).as("the premise: the clamp tore the pair").containsExactly((long) DOCS, 0L);

    restart();
    assertThat(metadata().getCountersPair()).as("the torn pair was persisted").containsExactly((long) DOCS, 0L);
    assertThat(describe().get("rescanRequired")).as("and so was the proof that it is torn").isEqualTo(true);

    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, DOCS * 3L);
    assertThat(metadata().avgDocLength()).isEqualTo(3.0);
    assertThat(describe().get("rescanRequired")).isEqualTo(false);

    // REPAIRED, AND THE REPAIR IS WHAT THE NEXT SESSION FINDS: NO SCAN AGAIN
    restart();
    search();
    assertThat(metadata().getScans()).isZero();
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, DOCS * 3L);
  }

  /** The per-document removal path clamps too, lock-free: its proof must reach the disk the same way. */
  @Test
  void aRemovalClampSurvivesARestart() {
    createAndFill();
    // one document of 1000 tokens leaves a corpus that holds 30: the count stays sane, the sum cannot
    metadata().removeDocument(1000L);
    metadata().addDocument(0L);
    assertThat(metadata().getClamps()).isPositive();

    restart();
    search();

    assertThat(metadata().getScans()).isEqualTo(1L);
    assertThat(metadata().getCountersPair()).containsExactly((long) DOCS, DOCS * 3L);
  }

  /**
   * A rescan armed without moving a counter (crash recovery arms every BM25 index, a replicated delta that could not be applied
   * arms its own) used to leave the counters clean, so a close before the first search wrote nothing and the next session never
   * rescanned. Arming is a change of what the persisted unit says.
   */
  @Test
  void aRescanArmedWithoutAnyCounterChangeSurvivesACleanClose() {
    createAndFill();
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
  void everyPathNormalizesByTheSameAverage() {
    createAndFill();
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

  private void createAndFill() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
  }

  private void restart() {
    database.close();
    reopenDatabase();
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(INDEX));
  }

  private Map<String, Object> describe() {
    return LSMTreeFullTextIndex.describeCorpusCounters(database.getSchema().getIndexByName(INDEX));
  }

  private void search() {
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('" + INDEX + "', 'alpha') = true").close();
  }
}
