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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8057: the issue #3722 shortfall fallback compared the answer against
 * {@code vectorIndex().size()}, the COMMITTED live count, which still holds the rows the calling transaction removed -
 * their tombstones are written only when the commit replays the queued REMOVEs. Since #7378 the search excludes those
 * rows, so a search inside such a transaction asking for (nearly) the whole corpus always came up "short": it logged a
 * WARNING that the graph may need rebuilding and ran a brute-force scan over every ordinal that found nothing new.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8057ShortfallNetsOwnDeletesTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int SEEDED     = 30;

  @BeforeEach
  void freezeTheGraph() {
    // THE REMOVED ROW MUST STAY IN THE COMMITTED GRAPH: A REBUILD WOULD NOT CHANGE WHAT IS PINNED, BUT IT WOULD BUMP
    // THE METRIC THE TEST READS
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void thawTheGraph() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
  }

  @Test
  void aSearchForTheWholeCorpusAfterAnOwnDeleteDoesNotFallBackToBruteForce() {
    seedAndBuildGraph();
    final LSMVectorIndex index = vectorIndex();

    database.begin();
    try {
      database.lookupByRID(ridOf("doc3"), true).delete();

      final long scansBefore = bruteForceScans(index);
      for (int i = 0; i < 3; i++) {
        final List<String> ids = idsOf(index.findNeighborsFromVector(seedVector(0), SEEDED));
        assertThat(ids).hasSize(SEEDED - 1).doesNotContain("doc3");
      }
      assertThat(bruteForceScans(index))
          .as("every row the transaction can see was returned: there is no shortfall to fall back on")
          .isEqualTo(scansBefore);
    } finally {
      database.rollback();
    }
  }

  /** Several deletes, and a k above the live count the transaction sees but below the committed one. */
  @Test
  void severalOwnDeletesAreAllNettedOut() {
    seedAndBuildGraph();
    final LSMVectorIndex index = vectorIndex();

    database.begin();
    try {
      for (final String id : new String[] { "doc1", "doc7", "doc20" })
        database.lookupByRID(ridOf(id), true).delete();

      final long scansBefore = bruteForceScans(index);
      assertThat(idsOf(index.findNeighborsFromVector(seedVector(5), SEEDED - 1)))
          .hasSize(SEEDED - 3)
          .doesNotContain("doc1", "doc7", "doc20");
      assertThat(bruteForceScans(index)).isEqualTo(scansBefore);
    } finally {
      database.rollback();
    }
  }

  /** A rewritten row is superseded on the committed side and contributed again on the pending side: still no shortfall. */
  @Test
  void aReEmbeddedRowIsNotAShortfallEither() {
    seedAndBuildGraph();
    final LSMVectorIndex index = vectorIndex();

    database.begin();
    try {
      final float[] moved = seedVector(29);
      moved[5] = 0.5f;
      database.lookupByRID(ridOf("doc4"), true).asDocument().modify().set("embedding", moved).save();

      final long scansBefore = bruteForceScans(index);
      assertThat(idsOf(index.findNeighborsFromVector(seedVector(0), SEEDED))).hasSize(SEEDED).containsOnlyOnce("doc4");
      assertThat(bruteForceScans(index)).isEqualTo(scansBefore);
    } finally {
      database.rollback();
    }
  }

  private static long bruteForceScans(final LSMVectorIndex index) {
    return ((Number) index.getStats().get("bruteForceScans")).longValue();
  }

  private void seedAndBuildGraph() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      for (int i = 0; i < SEEDED; i++)
        database.newDocument("Doc").set("id", "doc" + i).set("embedding", seedVector(i)).save();
    });

    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
          "dimensions": %d,
          "similarity": "COSINE"
        }""".formatted(DIMENSIONS));

    // ONE SEARCH OUTSIDE ANY TRANSACTION MOVES THE ROWS INTO THE GRAPH: THE SHORTFALL CHECK IS ON THE GRAPH PATH
    final LSMVectorIndex index = vectorIndex();
    index.findNeighborsFromVector(seedVector(0), 1);
    assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) SEEDED);
  }

  private static float[] seedVector(final int i) {
    final float[] v = new float[DIMENSIONS];
    v[0] = 1.0f;
    v[1] = 0.02f * (i + 1);
    v[2] = 0.01f * ((i * 7) % 13);
    v[3] = 0.005f * ((i * 3) % 11);
    v[4] = 0.001f * (i + 1);
    return v;
  }

  private List<String> idsOf(final List<Pair<RID, Float>> results) {
    final List<String> ids = new ArrayList<>(results.size());
    for (final Pair<RID, Float> r : results)
      ids.add(((Document) database.lookupByRID(r.getFirst(), true)).getString("id"));
    return ids;
  }

  private RID ridOf(final String id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }
}
