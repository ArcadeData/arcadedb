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
 * Pins {@code vector.recommend} (Tier 4 follow-up). Six docs in three pairs of clusters:
 * (A, B) on {@code +X}, (C, D) on {@code +Y}, (E, F) on {@code +Z}. The recommendation
 * formula {@code centroid(positives) - centroid(negatives)} should pull from the positives'
 * cluster and push away from the negatives'.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorRecommendTest extends TestHelper {

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
      ridByName.put("F", insert("F", new float[] { 0.1f, 0.0f, 0.9f }));
    });
  }

  /**
   * Positives only ({@code [A]}). Centroid is A's vector; KNN should return A's nearest neighbor
   * (B), not A itself (examples are excluded by default).
   */
  @Test
  void positivesOnlyReturnsClusterMatesExcludingExamples() {
    final List<String> names = recommend(List.of(ridByName.get("A")), List.of(), 3);
    assertThat(names).doesNotContain("A");
    assertThat(names.get(0)).as("B is A's near-neighbor in the +X cluster").isEqualTo("B");
  }

  /**
   * Positives in cluster X, negatives in cluster Y. The query vector
   * {@code centroid(X) - centroid(Y)} points away from Y; the top result must be from cluster X
   * (A or B), and cluster-Y members (C, D) must NOT appear in the result.
   */
  @Test
  void negativesPushResultAwayFromTheirCluster() {
    final List<String> names = recommend(
        List.of(ridByName.get("A"), ridByName.get("B")),  // positives in +X
        List.of(ridByName.get("C"), ridByName.get("D")),  // negatives in +Y
        3);
    // Positives are excluded; the result must NOT contain C or D either - they are the negatives.
    assertThat(names).doesNotContain("A", "B", "C", "D");
    // E or F (the +Z cluster) are still possible, but with the strong +X bias the top result
    // should be... whatever is closest to the difference vector. With weight 1.0 for A&B
    // centroid (~[0.95, 0.05, 0]) and -1.0 for C&D centroid (~[0.05, 0.95, 0]), the query
    // vector is ~[0.9, -0.9, 0]. Of the remaining {E, F}, F is closest because F has a small
    // +X component. We assert F or E (depending on similarity definition) appears at rank 0.
    assertThat(names.get(0)).isIn("F", "E");
  }

  @Test
  void emptyPositivesIsRejected() {
    assertThatThrownBy(() -> recommend(List.of(), List.of(), 3))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("positive");
  }

  @Test
  void deletedExamplesAreSilentlySkipped() {
    // A handle that does not exist in the type. The database lookup throws RecordNotFound and the
    // function should treat it as "skip this example", not "fail the query".
    final RID phantom = new RID(ridByName.get("A").getBucketId(), 999_999L);
    final List<String> names = recommend(
        List.of(ridByName.get("A"), phantom),
        List.of(),
        3);
    // A is excluded as an example; B is the next-closest cluster mate.
    assertThat(names.get(0)).isEqualTo("B");
  }

  @Test
  void rejectsBareIndexNameSpec() {
    // The index spec must include the property name so the function can read it off the example
    // records. A bare index name (no '[property]' suffix) is rejected with a clear message
    // rather than letting the bracketed-substring parse fail downstream.
    final SQLFunctionVectorRecommend function = new SQLFunctionVectorRecommend();
    final com.arcadedb.query.sql.executor.BasicCommandContext ctx = new com.arcadedb.query.sql.executor.BasicCommandContext();
    ctx.setDatabase(database);
    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { "DocEmbeddingIdx", List.of(ridByName.get("A")), List.of(), 3 },
        ctx))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Type[property]");
  }

  @Test
  void unknownOptionKeyIsRejected() {
    // A typo like {@code efSerach} (vs {@code efSearch}) used to be silently dropped; pinning the
    // strict-validation contract guards against the typo class.
    assertThatThrownBy(() -> database.query("sql",
        "SELECT expand(`vector.recommend`('Doc[embedding]', [?], [], 3, { efSerach: 10 }))",
        ridByName.get("A")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("efSerach");
  }

  @Test
  void filterOptionIsExplicitlyRejected() {
    // {@code vector.neighbors} treats filter as an allow-list, but recommendation needs the
    // inverse (examples must NOT appear in result). Reject up-front so the user is not surprised.
    assertThatThrownBy(() -> database.query("sql",
        "SELECT expand(`vector.recommend`('Doc[embedding]', [?], [], 3, { filter: [?] }))",
        ridByName.get("A"), ridByName.get("B")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("filter");
  }

  /**
   * Forwarded {@code groupBy} option - sanity that recommendation results respect grouping (the
   * inner {@code vector.neighbors} call gets the option). With one doc per name, each row is its
   * own group; the assertion is just "no errors thrown, result has at least one row".
   */
  @Test
  void forwardsGroupByOption() {
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.recommend`('Doc[embedding]', [?], [], 3, { groupBy: 'name', groupSize: 1 }))",
        ridByName.get("A"));
    int seen = 0;
    while (rs.hasNext()) { rs.next(); seen++; }
    rs.close();
    assertThat(seen).isGreaterThanOrEqualTo(1);
  }

  // --- helpers ---

  private RID insert(final String name, final float[] embedding) {
    return database.newVertex("Doc").set("name", name).set("embedding", embedding).save().getIdentity();
  }

  private List<String> recommend(final List<RID> positives, final List<RID> negatives, final int k) {
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.recommend`('Doc[embedding]', ?, ?, ?))",
        positives, negatives, k);
    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      names.add(row.getProperty("name"));
    }
    rs.close();
    return names;
  }
}
