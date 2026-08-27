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

import com.arcadedb.database.DatabaseFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A vertex property is single-cardinality here, so {@code (vertex, key)} is its identity, and TinkerPop's
 * {@code ElementHelper} compares vertex properties by id alone. Issue #6823: the id used to be the sum of three hash
 * codes, so distinct properties on the same vertex collided and were deduplicated away.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeVertexPropertyIdTest {

  private static final String DB_PATH = "./target/databases/test-vertex-property-id";

  private ArcadeGraph graph;

  @BeforeEach
  void setUp() {
    try (final DatabaseFactory databaseFactory = new DatabaseFactory(DB_PATH)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }
    graph = ArcadeGraph.open(DB_PATH);
  }

  @AfterEach
  void tearDown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void swappedKeyAndValueDoNotShareAnId() {
    final ArcadeVertex vertex = graph.addVertex(T.label, "Person");
    final VertexProperty<String> ab = vertex.property("a", "b");
    final VertexProperty<String> ba = vertex.property("b", "a");

    assertThat(ab.id()).as("\"a\"->\"b\" and \"b\"->\"a\" are different properties").isNotEqualTo(ba.id());
    assertThat(ab).isNotEqualTo(ba);
    assertThat(new HashSet<>(Set.of(ab, ba))).hasSize(2);
  }

  @Test
  void keysWithTheSameHashCodeDoNotShareAnId() {
    final ArcadeVertex vertex = graph.addVertex(T.label, "Person");
    // the classic "Aa".hashCode() == "BB".hashCode() collision, with an identical value on both
    final VertexProperty<Integer> aa = vertex.property("Aa", 1);
    final VertexProperty<Integer> bb = vertex.property("BB", 1);

    assertThat(aa.id()).isNotEqualTo(bb.id());
    assertThat(aa).isNotEqualTo(bb);
  }

  @Test
  void distinctPropertiesAreNotDeduplicatedAway() {
    final ArcadeVertex vertex = graph.addVertex(T.label, "Person");
    vertex.property("a", "b");
    vertex.property("b", "a");
    graph.tx().commit();

    assertThat(graph.traversal().V(vertex.id()).properties().dedup().count().next())
        .as("both properties must survive dedup()")
        .isEqualTo(2L);
  }

  @Test
  void thePropertyIdIsTheSameOnEveryLookup() {
    final ArcadeVertex vertex = graph.addVertex(T.label, "Person");
    final Object idOnWrite = vertex.property("name", "Jay").id();
    graph.tx().commit();

    assertThat(vertex.<String>property("name").id()).isEqualTo(idOnWrite);
  }

  @Test
  void thePropertiesOfTwoVerticesNeverCollide() {
    final ArcadeVertex first = graph.addVertex(T.label, "Person");
    final ArcadeVertex second = graph.addVertex(T.label, "Person");
    graph.tx().commit();

    assertThat(first.property("name", "same").id()).isNotEqualTo(second.property("name", "same").id());
  }

  @Test
  void anUnsavedVertexDoesNotBreakTheId() {
    graph.tx().begin();
    graph.getDatabase().getSchema().getOrCreateVertexType("Person");
    final ArcadeVertex vertex = new ArcadeVertex(graph, graph.getDatabase().newVertex("Person"));
    assertThat(vertex.id()).as("the fixture must really be an unsaved vertex").isNull();

    final VertexProperty<String> first = new ArcadeVertexProperty<>(vertex, "a", "b");
    final VertexProperty<String> second = new ArcadeVertexProperty<>(vertex, "b", "a");

    assertThat(first.id()).as("an unsaved vertex must not make id() blow up").isNotNull();
    assertThat(first.id()).isNotEqualTo(second.id());
    graph.tx().rollback();
  }
}
