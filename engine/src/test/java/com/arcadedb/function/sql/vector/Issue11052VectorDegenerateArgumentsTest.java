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
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11052: the vector functions disagreed on a NULL vector and on an empty one. {@code vector.l2Norm} (the alias of
 * {@code vector.magnitude}) failed the whole query on a NULL where {@code vector.l1Norm} and {@code vector.lInfNorm}
 * answered NULL, and those two refused an empty vector whose norm {@code vector.l2Norm} answered as 0. The two-vector
 * functions (similarities, distances, element-wise arithmetic) failed the query on a NULL too, so one row of a type being
 * backfilled took down every other row of the result.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11052VectorDegenerateArgumentsTest extends TestHelper {
  private static final String[] VALUES = { "[1,2]", "2" };

  /**
   * The searches and the reranker: their arguments name an index, a query vector, a count of neighbours or the dimensions of
   * the vector to build, which the caller supplies once for the whole query and no row can lack, so a NULL one is the
   * caller's mistake and is refused. Refused with a message, never a crash.
   */
  private static final Set<String> SEARCH_ARGUMENTS = Set.of("vector.neighbors", "vector.sparseneighbors", "vector.discover",
      "vector.recommend", "vector.rerank", "vector.sparsecreate");

  /**
   * Over every registered {@code vector.*} function: a NULL argument never makes a call fail that would not fail with a value
   * in its place. A call refused for its arity, or for another argument, keeps failing the same way, which is why the
   * comparison is with the same call holding a value rather than with "must answer NULL". No call crashes on a NULL with
   * anything but a database exception.
   */
  @Test
  void aNullArgumentNeverIntroducesAFailure() {
    final List<String> violations = new ArrayList<>();
    for (final String name : new TreeSet<>(DefaultSQLFunctionFactory.getInstance().getBuiltInFunctionNames())) {
      if (!name.startsWith("vector."))
        continue;
      for (int arity = 1; arity <= 4; arity++)
        for (final String[] base : combinations(arity))
          for (int position = 0; position < arity; position++) {
            final String[] withNull = base.clone();
            withNull[position] = "null";
            final String failure = failureOf(name, withNull);
            if (failure == null)
              continue;

            if (!failure.startsWith(ArcadeDBException.class.getPackageName())) {
              violations.add(call(name, withNull) + " -> " + failure);
              continue;
            }
            if (SEARCH_ARGUMENTS.contains(name))
              continue;

            boolean sameFailureWithAValue = false;
            for (final String value : VALUES) {
              final String[] withValue = base.clone();
              withValue[position] = value;
              if (failure.equals(failureOf(name, withValue))) {
                sameFailureWithAValue = true;
                break;
              }
            }
            if (!sameFailureWithAValue)
              violations.add(call(name, withNull) + " -> " + failure);
          }
    }
    assertThat(violations).as("calls a NULL argument made fail").isEmpty();
  }

  @Test
  void theThreeNormsAgreeOnNullAndEmpty() {
    for (final String norm : new String[] { "vector.l1Norm", "vector.l2Norm", "vector.lInfNorm", "vector.magnitude" }) {
      assertThat(scalar("SELECT `" + norm + "`([3,4]) as v")).as(norm).isNotNull();
      assertThat(scalar("SELECT `" + norm + "`(null) as v")).as(norm + "(null)").isNull();
      assertThat(((Number) scalar("SELECT `" + norm + "`([]) as v")).floatValue()).as(norm + "([])").isEqualTo(0.0f);
    }
    assertThat(((Number) scalar("SELECT `vector.l1Norm`([3,4]) as v")).floatValue()).isEqualTo(7.0f);
    assertThat(((Number) scalar("SELECT `vector.l2Norm`([3,4]) as v")).floatValue()).isEqualTo(5.0f);
    assertThat(((Number) scalar("SELECT `vector.lInfNorm`([3,4]) as v")).floatValue()).isEqualTo(4.0f);
  }

  /**
   * An empty vector answers what the function answers on a zero vector wherever that value is defined. Cosine similarity,
   * variance, standard deviation, sparsity and the quantizations are 0/0 on it and keep refusing it.
   */
  @Test
  void anEmptyVectorAnswersWhatAZeroVectorAnswers() {
    assertThat((float[]) scalar("SELECT `vector.normalize`([]) as v")).isEmpty();
    assertThat((float[]) scalar("SELECT `vector.normalize`([0,0]) as v")).containsExactly(0.0f, 0.0f);
    assertThat((float[]) scalar("SELECT `vector.clip`([], 0, 1) as v")).isEmpty();
    assertThat(scalar("SELECT `vector.hasNaN`([]) as v")).isEqualTo(false);
    assertThat(scalar("SELECT `vector.hasInf`([]) as v")).isEqualTo(false);
    assertThat(scalar("SELECT `vector.hasNull`([]) as v")).isEqualTo(false);
    assertThat(scalar("SELECT `vector.isNormalized`([]) as v")).isEqualTo(false);
    assertThat(scalar("SELECT `vector.isNormalized`([0,0]) as v")).isEqualTo(false);
    assertThat(scalar("SELECT `vector.toString`([]) as v")).isEqualTo("[]");
    assertThat(scalar("SELECT `vector.dimension`([]) as v")).isEqualTo(0);
  }

  /** The consequence the issue measured: one row without an embedding no longer takes the other rows down with it. */
  @Test
  void aRowWithoutAnEmbeddingAnswersNullAndKeepsTheOtherRows() {
    database.command("sql", "CREATE DOCUMENT TYPE Emb");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Emb SET id = 1, v = [3,4]");
      database.command("sql", "INSERT INTO Emb SET id = 2");
    });

    for (final String projection : new String[] { "`vector.l2Norm`(v)", "`vector.magnitude`(v)", "`vector.normalize`(v)",
        "`vector.cosineSimilarity`(v, [1,0])", "`vector.dotProduct`(v, [1,0])", "`vector.l2Distance`(v, [1,0])",
        "`vector.l1Distance`(v, [1,0])", "`vector.multiply`(v, [1,1])", "`vector.add`(v, 1)", "`vector.subtract`(v, [1,1])",
        "`vector.scale`(v, 2)" }) {
      try (final ResultSet rs = database.query("sql", "SELECT id, " + projection + " as x FROM Emb ORDER BY id")) {
        final Result first = rs.next();
        assertThat(first.<Integer>getProperty("id")).as(projection).isEqualTo(1);
        assertThat(first.<Object>getProperty("x")).as(projection).isNotNull();
        final Result second = rs.next();
        assertThat(second.<Integer>getProperty("id")).as(projection).isEqualTo(2);
        assertThat(second.<Object>getProperty("x")).as(projection).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  private Object scalar(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().getProperty("v");
    }
  }

  /** The failure message of the call, or null when it answers. */
  private String failureOf(final String name, final String[] args) {
    try (final ResultSet rs = database.query("sql", call(name, args))) {
      rs.next();
      return null;
    } catch (final Exception e) {
      return e.getClass().getName() + ": " + e.getMessage();
    }
  }

  private static String call(final String name, final String[] args) {
    return "SELECT `" + name + "`(" + String.join(", ", args) + ") as v";
  }

  private static List<String[]> combinations(final int arity) {
    final List<String[]> out = new ArrayList<>();
    final int total = (int) Math.pow(VALUES.length, arity);
    for (int i = 0; i < total; i++) {
      final String[] args = new String[arity];
      int n = i;
      for (int p = 0; p < arity; p++) {
        args[p] = VALUES[n % VALUES.length];
        n /= VALUES.length;
      }
      out.add(args);
    }
    return out;
  }
}
