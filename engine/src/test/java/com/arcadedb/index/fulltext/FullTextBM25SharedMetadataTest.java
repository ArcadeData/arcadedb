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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The BM25 corpus counters of a FULL_TEXT index are TYPE-wide, and are meant to live in ONE {@link FullTextIndexMetadata}
 * shared by every bucket sub-index of the logical index. The creation path shares it; the schema load path used to build a
 * separate copy per bucket sub-index, so after any reopen (and after every HA full schema reload on a follower):
 * <ul>
 *   <li>each copy only counted the inserts routed to its own bucket, so the counters drifted from the live data on
 *   ordinary ingestion, with no rollback involved;</li>
 *   <li>the once-per-session stale check ran once PER BUCKET, so one logical search did N full type scans;</li>
 *   <li>{@code REBUILD INDEX ... WITH statsOnly = true} repaired only the first bucket's copy.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextBM25SharedMetadataTest extends TestHelper {
  private static final int BUCKETS = 4;
  private static final int DOCS    = 20;

  private void createAndFill() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    insert(DOCS);
  }

  /** Every document is 3 tokens long, so the expected total length is always 3 x the document count. */
  private void insert(final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
  }

  private List<FullTextIndexMetadata> bucketMetadata() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    final List<FullTextIndexMetadata> result = new ArrayList<>();
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      result.add(((LSMTreeFullTextIndex) bucketIndex).getFullTextMetadata());
    assertThat(result).hasSize(BUCKETS);
    return result;
  }

  private void saveSchema() {
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();
  }

  @Test
  void bucketIndexesShareOneMetadataAfterReopen() {
    createAndFill();
    assertThat(bucketMetadata()).allSatisfy(m -> assertThat(m).isSameAs(bucketMetadata().getFirst()));

    reopenDatabase();

    final List<FullTextIndexMetadata> metadata = bucketMetadata();
    assertThat(metadata).allSatisfy(m -> assertThat(m).isSameAs(metadata.getFirst()));
  }

  @Test
  void countersFollowInsertsIntoEveryBucketAfterReopen() {
    createAndFill();
    saveSchema();
    reopenDatabase();

    insert(DOCS);

    // Read BEFORE any search: a search runs the stale check, which would repair a drifted counter and hide the bug.
    for (final FullTextIndexMetadata m : bucketMetadata()) {
      assertThat(m.getTotalDocs()).isEqualTo(2L * DOCS);
      assertThat(m.getSumDocLength()).isEqualTo(2L * DOCS * 3);
    }
  }

  @Test
  void countersPersistedAfterReopenMatchTheLiveData() {
    createAndFill();
    saveSchema();
    reopenDatabase();
    insert(DOCS);
    saveSchema();
    reopenDatabase();

    for (final FullTextIndexMetadata m : bucketMetadata()) {
      assertThat(m.isCountersValid()).isTrue();
      assertThat(m.getTotalDocs()).isEqualTo(database.countType("Doc", false));
      assertThat(m.getSumDocLength()).isEqualTo(2L * DOCS * 3);
    }
  }

  /** Issue #9337: a writer bumped the counters while the scan ran: the bump must survive the publication of the scan result. */
  @Test
  void scannedCountersKeepIncrementsAppliedDuringTheScan() {
    final FullTextIndexMetadata m = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    m.setCounters(100L, 300L);
    final long docsAtStart = m.getTotalDocs();
    final long lenAtStart = m.getSumDocLength();
    final long mutationsAtStart = m.getMutations();

    m.addDocument(3L); // committed by another thread after the scan passed its bucket
    m.addDocument(3L);
    m.removeDocument(3L);

    final boolean exact = m.publishScannedCounters(100L, 300L, docsAtStart, lenAtStart, mutationsAtStart);

    assertThat(exact).isFalse();
    assertThat(m.getTotalDocs()).isEqualTo(101L);
    assertThat(m.getSumDocLength()).isEqualTo(303L);
    // the result is only approximate, so the session's drift check must still be armed
    assertThat(m.claimStaleCheck()).isTrue();
  }

  /** A remove and an add that cancel out leave the net counters equal, yet the scan may have read the old document. */
  @Test
  void scannedCountersAreNotExactWhenWritesCancelOut() {
    final FullTextIndexMetadata m = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    m.setCounters(10L, 30L);
    final long mutationsAtStart = m.getMutations();

    m.removeDocument(3L);
    m.addDocument(3L);

    assertThat(m.publishScannedCounters(10L, 30L, 10L, 30L, mutationsAtStart)).isFalse();
    assertThat(m.consumeRescanRequired()).isTrue();
    assertThat(m.consumeRescanRequired()).isFalse();
  }

  @Test
  void scannedCountersWithoutConcurrentWritesAreExactAndConsumeTheStaleCheck() {
    final FullTextIndexMetadata m = new FullTextIndexMetadata("Doc", new String[] { "content" }, 0);
    final boolean exact = m.publishScannedCounters(50L, 150L, m.getTotalDocs(), m.getSumDocLength(), m.getMutations());

    assertThat(exact).isTrue();
    assertThat(m.isCountersValid()).isTrue();
    assertThat(m.getTotalDocs()).isEqualTo(50L);
    assertThat(m.getSumDocLength()).isEqualTo(150L);
    assertThat(m.claimStaleCheck()).isFalse();
  }

  @Test
  void statsRecomputeRacingOrdinaryInsertsEndsConsistentWithTheLiveData() throws Exception {
    createAndFill();
    saveSchema();
    insert(2000);

    final int writes = 300;
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        for (int i = 0; i < writes; i++)
          database.transaction(() -> database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'"));
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    writer.start();
    for (int i = 0; i < 5 && writer.isAlive(); i++)
      database.command("sql", "REBUILD INDEX `Doc[content]` WITH statsOnly = true");
    writer.join();
    assertThat(failure.get()).isNull();

    // A search runs the (possibly re-armed) drift check, which must leave the counters exact
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true LIMIT 1").close();

    final long live = database.countType("Doc", false);
    assertThat(live).isEqualTo(DOCS + 2000L + writes);
    for (final FullTextIndexMetadata m : bucketMetadata()) {
      assertThat(m.getTotalDocs()).isEqualTo(live);
      assertThat(m.getSumDocLength()).isEqualTo(live * 3);
    }
  }

  @Test
  void statsOnlyRebuildRepairsEveryBucketIndex() {
    createAndFill();
    saveSchema();
    reopenDatabase();

    bucketMetadata().getLast().setCounters(1L, 1L);
    database.command("sql", "REBUILD INDEX `Doc[content]` WITH statsOnly = true");

    for (final FullTextIndexMetadata m : bucketMetadata()) {
      assertThat(m.getTotalDocs()).isEqualTo(DOCS);
      assertThat(m.getSumDocLength()).isEqualTo(DOCS * 3L);
    }
  }

  /**
   * A schema written by a build that split the metadata carries a different counter per bucket sub-index. Whichever one
   * the load picked would be wrong, so a disagreement must invalidate the shared counters: the first search then rebuilds
   * them from the live data once.
   */
  @Test
  void disagreeingPersistedCountersAreRebuiltOnFirstSearch() throws IOException {
    createAndFill();
    saveSchema();
    database.close();

    final Path schemaFile = new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toPath();
    final JSONObject schema = new JSONObject(Files.readString(schemaFile));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Doc").getJSONObject("indexes");
    final String lastIndex = indexes.keySet().stream().sorted().reduce((a, b) -> b).orElseThrow();
    indexes.getJSONObject(lastIndex).put("ft_totalDocs", 999L);
    Files.writeString(schemaFile, schema.toString());

    database = factory.open();

    final List<FullTextIndexMetadata> metadata = bucketMetadata();
    assertThat(metadata).allSatisfy(m -> assertThat(m).isSameAs(metadata.getFirst()));
    assertThat(metadata.getFirst().isCountersValid()).isFalse();

    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();

    assertThat(metadata.getFirst().isCountersValid()).isTrue();
    assertThat(metadata.getFirst().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata.getFirst().getSumDocLength()).isEqualTo(DOCS * 3L);
  }
}
