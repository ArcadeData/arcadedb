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
 * https://github.com/ArcadeData/arcadedb/issues/9302
 * <p>
 * A range {@code has()} served from a {@code COLLATE ci} index lost rows the same traversal returns without the index: the
 * index holds lower-cased keys, so its range is not the case sensitive range the predicate asks for. The range now falls back
 * to the type scan, as it does for a hash index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9302FoldedIndexRangeTest {
  private static final String[] NAMES = { "John", "MARY", "anne", "Bob" };

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9302");
    for (final String ddl : new String[] { "CREATE VERTEX TYPE P", "CREATE PROPERTY P.name STRING",
        "CREATE INDEX ON P (name COLLATE ci) NOTUNIQUE", "CREATE VERTEX TYPE Q", "CREATE PROPERTY Q.name STRING" })
      graph.getDatabase().command("sql", ddl).close();
    for (final String name : NAMES) {
      graph.addVertex(T.label, "P", "name", name);
      graph.addVertex(T.label, "Q", "name", name);
    }
    graph.tx().commit();
  }

  @AfterEach
  void teardown() {
    graph.drop();
  }

  private List<Object> run(final String type, final P<String> predicate) {
    return graph.traversal().V().hasLabel(type).has("name", predicate).values("name").order().toList();
  }

  @Test
  void rangeOnAFoldedIndexAnswersLikeTheUnindexedOne() {
    for (final P<String> predicate : List.of(P.gte("C"), P.gt("C"), P.lt("C"), P.lte("C"))) {
      assertThat(run("P", predicate)).as(predicate.toString()).isEqualTo(run("Q", predicate));
    }
    assertThat(run("P", P.gte("C"))).containsExactly("John", "MARY", "anne");
  }

  @Test
  void equalityStillUsesTheFoldedIndex() {
    assertThat(run("P", P.eq("John"))).containsExactly("John");
    assertThat(graph.traversal().V().hasLabel("P").has("name", "John").explain().toString()).contains("ArcadeFilterByIndexStep");
  }
}
