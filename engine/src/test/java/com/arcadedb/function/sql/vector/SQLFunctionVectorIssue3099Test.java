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
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #3099 (review of new vector functions). Covers three correctness bugs:
 * <ol>
 *   <li>vector.sum/avg/min/max leaked accumulated state across independent queries (repeated calls
 *       returned increasing results) because they were registered as shared singleton instances.</li>
 *   <li>vector.hasNaN / vector.hasInf threw a NullPointerException on a NULL collection element
 *       (e.g. from sqrt(-1.0) coerced to NULL).</li>
 *   <li>vector.dimension threw on a NULL argument instead of returning 0.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
class SQLFunctionVectorIssue3099Test extends TestHelper {

  // ========== Bug 1: aggregate state must not leak across queries ==========

  @Test
  void vectorSumDoesNotAccumulateAcrossQueries() {
    // The issue reported that repeated SELECT vector.sum([1.0,2.0,3.0]) returned [1,2,3], then [2,4,6],
    // then [3,6,9]. A fresh per-query instance must always return [1,2,3].
    final String query = "SELECT `vector.sum`([1.0, 2.0, 3.0]) as s";
    for (int i = 0; i < 3; i++) {
      try (final ResultSet results = database.query("sql", query)) {
        assertThat(results.hasNext()).isTrue();
        final float[] s = results.next().getProperty("s");
        assertThat(s).as("iteration %d", i).containsExactly(1.0f, 2.0f, 3.0f);
      }
    }
  }

  @Test
  void vectorAggregatesAcrossRowsAreStableAndCorrect() {
    database.command("sql", "CREATE DOCUMENT TYPE Emb");
    database.command("sql", "CREATE PROPERTY Emb.v ARRAY_OF_FLOATS");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Emb SET v = [1.0, 2.0, 3.0]");
      database.command("sql", "INSERT INTO Emb SET v = [4.0, 5.0, 6.0]");
    });

    // Run the same aggregation twice; the second run must not be polluted by the first one's state.
    for (int i = 0; i < 2; i++) {
      final String query = "SELECT `vector.sum`(v) as s, `vector.avg`(v) as a, `vector.min`(v) as mn, `vector.max`(v) as mx FROM Emb";
      try (final ResultSet results = database.query("sql", query)) {
        assertThat(results.hasNext()).isTrue();
        final var r = results.next();
        assertThat(r.<float[]>getProperty("s")).as("sum iteration %d", i).containsExactly(5.0f, 7.0f, 9.0f);

        final float[] avg = r.getProperty("a");
        assertThat(avg[0]).isCloseTo(2.5f, Offset.offset(0.001f));
        assertThat(avg[1]).isCloseTo(3.5f, Offset.offset(0.001f));
        assertThat(avg[2]).isCloseTo(4.5f, Offset.offset(0.001f));

        assertThat(r.<float[]>getProperty("mn")).as("min iteration %d", i).containsExactly(1.0f, 2.0f, 3.0f);
        assertThat(r.<float[]>getProperty("mx")).as("max iteration %d", i).containsExactly(4.0f, 5.0f, 6.0f);
      }
    }
  }

  @Test
  void vectorSumAliasStillResolvesAfterClassRegistration() {
    // Switching from instance to class registration must keep the backward-compatible camelCase alias working.
    final String query = "SELECT vectorSum([1.0, 2.0, 3.0]) as s";
    try (final ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();
      assertThat(results.next().<float[]>getProperty("s")).containsExactly(1.0f, 2.0f, 3.0f);
    }
  }

  // ========== Bug 2: hasNaN / hasInf must not NPE on a NULL element ==========

  @Test
  void vectorHasNaNDetectsNullElementAsNaN() {
    // sqrt(-1.0) is coerced to NULL inside the collection; hasNaN must treat it as NaN, not crash.
    final String query = "SELECT `vector.hasNaN`([1.0, sqrt(-1.0), 3.0]) as r";
    try (final ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();
      assertThat(results.next().<Boolean>getProperty("r")).isTrue();
    }
  }

  @Test
  void vectorHasNaNFalseForCleanVector() {
    final String query = "SELECT `vector.hasNaN`([1.0, 2.0, 3.0]) as r";
    try (final ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();
      assertThat(results.next().<Boolean>getProperty("r")).isFalse();
    }
  }

  @Test
  void vectorHasInfIgnoresNullElement() {
    // A NULL element maps to NaN, which is not infinite: hasInf must return false (and must not crash).
    final String query = "SELECT `vector.hasInf`([1.0, sqrt(-1.0), 3.0]) as r";
    try (final ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();
      assertThat(results.next().<Boolean>getProperty("r")).isFalse();
    }
  }

  // ========== Bug 3: vector.dimension on NULL returns 0 ==========

  @Test
  void vectorDimensionOfNullReturnsZero() {
    final String query = "SELECT `vector.dimension`(null) as d";
    try (final ResultSet results = database.query("sql", query)) {
      assertThat(results.hasNext()).isTrue();
      assertThat(results.next().<Integer>getProperty("d")).isEqualTo(0);
    }
  }
}
