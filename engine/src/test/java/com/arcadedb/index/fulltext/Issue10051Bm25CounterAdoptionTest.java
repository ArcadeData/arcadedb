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
import java.util.Set;

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

  /** The adopted counters are the leader's exact result: the follower owes no drift check this session. */
  @Test
  void adoptedCountersConsumeTheDriftCheck() throws IOException {
    createAndFill();
    assertThat(metadata().isStaleCheckDone()).isFalse();

    leaderSavesCounters(DOCS, DOCS * 3L, 1L);
    followerReloadsTheSchema();

    assertThat(metadata().isStaleCheckDone()).isTrue();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(metadata().getScans()).isZero();
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
