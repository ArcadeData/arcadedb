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
 * https://github.com/ArcadeData/arcadedb/issues/9206
 * <p>
 * A fractional bound handed to the index of an integral property was truncated toward zero, so {@code lt(29.5)} dropped the 29
 * and {@code gt(-0.5)} dropped the 0, and the {@code HasStep} after the index cannot bring a dropped vertex back. The index
 * answer must equal the scan answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9206FractionalBoundIndexTest {
  private static final int[]    VALUES = { -1, 0, 1, 28, 29, 30 };
  private static final String[] TYPES  = { "INTEGER", "LONG", "SHORT" };

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9206");
    for (final String t : TYPES)
      for (final boolean indexed : new boolean[] { false, true }) {
        final String type = "V_" + t + (indexed ? "_idx" : "_scan");
        graph.getDatabase().command("sql", "CREATE VERTEX TYPE " + type).close();
        graph.getDatabase().command("sql", "CREATE PROPERTY " + type + ".x " + t).close();
        if (indexed)
          graph.getDatabase().command("sql", "CREATE INDEX ON " + type + " (x) NOTUNIQUE").close();
        for (final int v : VALUES)
          graph.addVertex(T.label, type, "x", t.equals("LONG") ? (Object) (long) v : t.equals("SHORT") ? (Object) (short) v : (Object) v);
      }
    graph.tx().commit();
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
  void indexAnswersLikeTheScanForFractionalBounds() {
    final P<?>[] predicates = { P.lt(29.5), P.lte(29.5), P.gt(28.5), P.gte(28.5), P.lt(0.5), P.gt(-0.5), P.lt(-0.5), P.gt(0.5), P.eq(29.5),
        P.lt(29.5f), P.gt(-0.5f), P.lt(1e19), P.gt(-1e19), P.gt(1e19), P.lt(-1e19), P.lt(Double.POSITIVE_INFINITY),
        P.gt(Double.NEGATIVE_INFINITY) };
    for (final String t : TYPES)
      for (final P<?> p : predicates)
        assertThat(run("V_" + t + "_idx", p)).as(t + " " + p).isEqualTo(run("V_" + t + "_scan", p));
  }

  @Test
  void fractionalBoundsStillUseTheIndex() {
    assertThat(graph.traversal().V().hasLabel("V_INTEGER_idx").has("x", P.lt(29.5)).explain().toString()).contains("ArcadeFilterByIndexStep");
    assertThat(run("V_INTEGER_idx", P.lt(29.5))).containsExactly("-1", "0", "1", "28", "29");
  }
}
