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
package com.arcadedb.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12027: Gremlin orders a negative zero below 0 ({@code 0 > -0.0} is true, as in {@link Double#compare}), while the index
 * of a property reads -0.0 and 0 as the same key. A strict bound at a zero was handed to the index exclusive, so
 * {@code has(k, gt(-0.0d))} through an index on an INTEGER property missed the 0 that the scan returns. The index answer must
 * equal the scan answer.
 */
class Issue12027NegativeZeroBoundIndexTest {
  private static final String[] TYPES = { "INTEGER", "LONG", "SHORT", "DOUBLE", "FLOAT" };

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue12027");
    for (final String t : TYPES)
      for (final boolean indexed : new boolean[] { false, true }) {
        final String type = "V_" + t + (indexed ? "_idx" : "_scan");
        graph.getDatabase().command("sql", "CREATE VERTEX TYPE " + type).close();
        graph.getDatabase().command("sql", "CREATE PROPERTY " + type + ".x " + t).close();
        if (indexed)
          graph.getDatabase().command("sql", "CREATE INDEX ON " + type + " (x) NOTUNIQUE").close();
        for (final Object v : valuesOf(t))
          graph.addVertex(T.label, type, "x", v);
      }
    graph.tx().commit();
  }

  private static Object[] valuesOf(final String type) {
    return switch (type) {
      case "LONG" -> new Object[] { -1L, 0L, 1L };
      case "SHORT" -> new Object[] { (short) -1, (short) 0, (short) 1 };
      case "DOUBLE" -> new Object[] { -1.0, -0.0, 0.0, 1.0 };
      case "FLOAT" -> new Object[] { -1.0f, -0.0f, 0.0f, 1.0f };
      default -> new Object[] { -1, 0, 1 };
    };
  }

  @AfterEach
  void teardown() {
    graph.drop();
  }

  private List<String> run(final String type, final P<?> predicate) {
    final List<String> result = new ArrayList<>();
    for (final Object o : graph.traversal().V().hasLabel(type).has("x", (P) predicate).values("x").toList())
      result.add(String.valueOf(o));
    result.sort(null);
    return result;
  }

  @Test
  void indexAnswersLikeTheScanForZeroBounds() {
    final P<?>[] predicates = { P.gt(-0.0), P.gte(-0.0), P.lt(-0.0), P.lte(-0.0), P.gt(0.0), P.gte(0.0), P.lt(0.0), P.lte(0.0),
        P.gt(-0.0f), P.lt(0.0f), P.gt(0), P.lt(0), P.eq(-0.0), P.eq(0.0) };
    for (final String t : TYPES)
      for (final P<?> p : predicates)
        assertThat(run("V_" + t + "_idx", p)).as(t + " " + p).isEqualTo(run("V_" + t + "_scan", p));
  }

  @Test
  void theNegativeZeroBoundStillUsesTheIndex() {
    assertThat(graph.traversal().V().hasLabel("V_INTEGER_idx").has("x", P.gt(-0.0)).explain().toString()).contains(
        "ArcadeFilterByIndexStep");
    assertThat(run("V_INTEGER_idx", P.gt(-0.0))).containsExactly("0", "1");
  }
}
