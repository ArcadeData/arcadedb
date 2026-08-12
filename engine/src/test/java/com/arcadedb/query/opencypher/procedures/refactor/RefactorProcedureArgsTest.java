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
package com.arcadedb.query.opencypher.procedures.refactor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the {@code (nodes, config)} argument parsing shared by the {@code refactor.*}
 * procedures. Exercised directly (not through a Cypher CALL) since these are pure functions over
 * already-evaluated procedure arguments.
 */
class RefactorProcedureArgsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-refactor-procedure-args");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void extractVerticesRejectsNonListArgument() {
    assertThatThrownBy(() -> RefactorProcedureArgs.extractVertices("test.proc", "not-a-list"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nodes must be a list");
  }

  @Test
  void extractVerticesRejectsNullArgument() {
    assertThatThrownBy(() -> RefactorProcedureArgs.extractVertices("test.proc", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nodes must be a list");
  }

  @Test
  void extractVerticesRejectsListWithNonVertexElement() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    database.commit();

    assertThatThrownBy(() -> RefactorProcedureArgs.extractVertices("test.proc", List.of(a, "not-a-vertex")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("every element of nodes must be a node");
  }

  @Test
  void extractVerticesDeduplicatesByIdentityKeepingFirstOccurrenceOrder() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    database.commit();

    final List<Vertex> result = RefactorProcedureArgs.extractVertices("test.proc", List.of(a, b, b, a));

    assertThat(result).hasSize(2);
    assertThat(result.get(0).getIdentity()).isEqualTo(a.getIdentity());
    assertThat(result.get(1).getIdentity()).isEqualTo(b.getIdentity());
  }

  @Test
  void extractConfigReturnsEmptyMapForNull() {
    assertThat(RefactorProcedureArgs.extractConfig("test.proc", null)).isEqualTo(Collections.emptyMap());
  }

  @Test
  void extractConfigReturnsTheGivenMap() {
    final Map<String, Object> config = Map.of("key", "value");
    assertThat(RefactorProcedureArgs.extractConfig("test.proc", config)).isEqualTo(config);
  }

  @Test
  void extractConfigRejectsNonMapArgument() {
    assertThatThrownBy(() -> RefactorProcedureArgs.extractConfig("test.proc", "not-a-map"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("config must be a map");
  }
}
