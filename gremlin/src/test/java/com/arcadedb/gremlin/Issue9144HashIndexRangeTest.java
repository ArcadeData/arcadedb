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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/9144
 * <p>
 * A range predicate on a property whose only index is a HASH index threw "does not support ordered iterations": the strategy
 * offered the hash index to a gt/gte/lt/lte container. The range now falls back to the type scan, as it does without an index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9144HashIndexRangeTest {
  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9144");
    for (final String ddl : new String[] { "CREATE VERTEX TYPE P", "CREATE PROPERTY P.plain INTEGER", "CREATE PROPERTY P.h INTEGER",
        "CREATE PROPERTY P.u INTEGER", "CREATE PROPERTY P.l INTEGER", "CREATE INDEX ON P (h) NOTUNIQUE_HASH",
        "CREATE INDEX ON P (u) UNIQUE_HASH", "CREATE INDEX ON P (l) NOTUNIQUE" })
      graph.getDatabase().command("sql", ddl).close();
    for (int i = 0; i < 10; i++)
      graph.addVertex(T.label, "P", "plain", i, "h", i, "u", i, "l", i);
    graph.tx().commit();
  }

  @AfterEach
  void teardown() {
    graph.drop();
  }

  private List<Object> run(final String property, final P<Integer> predicate) {
    return graph.traversal().V().hasLabel("P").has(property, predicate).values("plain").order().toList();
  }

  @Test
  void rangeOnAHashIndexedPropertyAnswersLikeTheUnindexedOne() {
    for (final String property : new String[] { "h", "u" }) {
      assertThat(run(property, P.gt(7))).as(property).containsExactly(8, 9);
      assertThat(run(property, P.gte(7))).as(property).containsExactly(7, 8, 9);
      assertThat(run(property, P.lt(2))).as(property).containsExactly(0, 1);
      assertThat(run(property, P.lte(2))).as(property).containsExactly(0, 1, 2);
    }
  }

  @Test
  void equalityStillUsesTheHashIndex() {
    assertThat(run("h", P.eq(7))).containsExactly(7);
    assertThat(run("u", P.eq(7))).containsExactly(7);
    assertThat(graph.traversal().V().hasLabel("P").has("h", 7).explain().toString()).contains("ArcadeFilterByIndexStep");
  }

  @Test
  void rangeOnAnLsmTreeIndexStillUsesIt() {
    assertThat(run("l", P.gt(7))).containsExactly(8, 9);
    assertThat(graph.traversal().V().hasLabel("P").has("l", P.gt(7)).explain().toString()).contains("ArcadeFilterByIndexStep");
  }
}
