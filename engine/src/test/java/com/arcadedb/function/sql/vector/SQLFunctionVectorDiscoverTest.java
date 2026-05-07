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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins {@code vector.discover} (Tier 4 follow-up). Six docs in three pairs of clusters:
 * (A, B) on {@code +X}, (C, D) on {@code +Y}, (E, F) on {@code +Z}. Each test sets up a context
 * that should pull from one cluster and push away from another, and verifies the resulting
 * top-K respects the per-pair margin sum.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorDiscoverTest extends TestHelper {

  private final Map<String, RID> ridByName = new HashMap<>();

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Doc.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (name) UNIQUE");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR
          METADATA {
            dimensions: 3,
            similarity: 'COSINE',
            idPropertyName: 'name'
          }""");
    });
    database.transaction(() -> {
      ridByName.put("A", insert("A", new float[] { 1.0f, 0.0f, 0.0f }));
      ridByName.put("B", insert("B", new float[] { 0.9f, 0.1f, 0.0f }));
      ridByName.put("C", insert("C", new float[] { 0.0f, 1.0f, 0.0f }));
      ridByName.put("D", insert("D", new float[] { 0.1f, 0.9f, 0.0f }));
      ridByName.put("E", insert("E", new float[] { 0.0f, 0.0f, 1.0f }));
      ridByName.put("F", insert("F", new float[] { 0.0f, 0.1f, 0.9f }));
    });
  }

  /**
   * Single (positive=A, negative=C) pair. The discovery score is
   * {@code cos(c, A) - cos(c, C)} - everything in cluster X scores high (positive on the +X
   * axis, negative on +Y), everything in cluster Y scores low. Top result must be B (closest to
   * A among non-examples).
   */
  @Test
  void pullsFromPositiveClusterAndPushesFromNegativeCluster() {
    // 4 non-example docs (B, D, E, F) total in the corpus; we fetch all of them and verify the
    // ORDER, not the membership: discovery is a re-ranker, so when k saturates the candidate
    // pool the test signal is which items end up at the top vs. the bottom.
    final List<String> result = discover(List.of(pair("A", "C")), 4);
    assertThat(result).doesNotContain("A", "C");
    // B is firmly in the positive cluster; must be rank 0.
    assertThat(result.get(0)).isEqualTo("B");
    // D is firmly in the negative cluster; must rank below E and F (orthogonal). Specifically,
    // D's discovery score is sharply negative (~-0.94), while E and F's are ~0, so D should be
    // last whenever it appears.
    if (result.contains("D"))
      assertThat(result.indexOf("D")).as("D (negative cluster) must rank last").isEqualTo(result.size() - 1);
  }

  /**
   * Two pairs: (A, C) and (B, D). Both pairs steer toward cluster X, away from cluster Y.
   * The discovery score sums both margins, reinforcing the +X bias. With only E/F left in the
   * non-example pool, the top result must be from {E, F} (the orthogonal cluster) - not from
   * Y, which discovery rejected.
   */
  @Test
  void multiplePairsSumPerPairMargins() {
    final List<String> result = discover(List.of(pair("A", "C"), pair("B", "D")), 4);
    assertThat(result).doesNotContain("A", "B", "C", "D");
    // Cluster X has been used as positives (A, B); only E/F (cluster Z) remain. Both should be
    // present in the top-K, in cluster-X-bias-decreasing order: F has 0.0 cos to neg cluster Y,
    // E has 0.1 cos to it. Either order is acceptable; we just assert the result is from {E, F}.
    assertThat(result.get(0)).isIn("E", "F");
  }

  @Test
  void emptyPairsListIsRejected() {
    assertThatThrownBy(() -> database.query("sql",
        "SELECT expand(`vector.discover`('Doc[embedding]', [], 3))"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("at least one");
  }

  @Test
  void degeneratePairWithSameRidIsRejected() {
    assertThatThrownBy(() -> database.query("sql",
        "SELECT expand(`vector.discover`('Doc[embedding]', [[?, ?]], 3))",
        ridByName.get("A"), ridByName.get("A")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("cannot be the same RID");
  }

  // --- helpers ---

  private RID insert(final String name, final float[] embedding) {
    return database.newVertex("Doc").set("name", name).set("embedding", embedding).save().getIdentity();
  }

  private List<RID> pair(final String pos, final String neg) {
    return List.of(ridByName.get(pos), ridByName.get(neg));
  }

  private List<String> discover(final List<List<RID>> pairs, final int k) {
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.discover`('Doc[embedding]', ?, ?))", pairs, k);
    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      names.add(row.getProperty("name"));
    }
    rs.close();
    return names;
  }
}
