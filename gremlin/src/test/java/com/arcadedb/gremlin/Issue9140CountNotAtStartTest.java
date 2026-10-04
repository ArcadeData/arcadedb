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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/9140
 * <p>
 * A {@code V().hasLabel(X).count()} that is not at the start of the traversal was rewritten into a step that ignores the
 * traversers in front of it. The expected values are TinkerGraph 3.8.2's.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9140CountNotAtStartTest {
  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9140");
    for (final String n : new String[] { "a", "b", "c" })
      graph.addVertex(T.label, "person", "name", n);
    for (final String n : new String[] { "x", "y" })
      graph.addVertex(T.label, "software", "name", n);
    graph.tx().commit();
  }

  @AfterEach
  void teardown() {
    if (graph.tx().isOpen())
      graph.tx().rollback();
    graph.drop();
  }

  @Test
  void countAtTheStartIsStillAnsweredFromTheType() {
    assertThat(graph.traversal().V().hasLabel("person").count().next()).isEqualTo(3L);
  }

  @Test
  void injectedTraversersEachScanTheGraph() {
    assertThat(graph.traversal().inject(1, 2, 3).V().hasLabel("person").count().next()).isEqualTo(9L);
  }

  @Test
  void firstVIsAFilterSecondOneScansPerTraverser() {
    assertThat(graph.traversal().V().hasLabel("person").V().hasLabel("software").count().next()).isEqualTo(6L);
  }

  @Test
  void midTraversalVCountsPerIncomingTraverser() {
    assertThat(graph.traversal().V().has("name", P.neq("zz")).V().hasLabel("software").count().next()).isEqualTo(10L);
    assertThat(graph.traversal().V().has("name", P.neq("zz")).limit(2).V().hasLabel("software").count().next()).isEqualTo(4L);
  }

  @Test
  void anEarlierAddVStillRuns() {
    assertThat(graph.traversal().addV("person").property("name", "d").V().hasLabel("person").count().next()).isEqualTo(4L);
    graph.tx().commit();
    assertThat(graph.traversal().V().hasLabel("person").count().next()).isEqualTo(4L);
  }
}
