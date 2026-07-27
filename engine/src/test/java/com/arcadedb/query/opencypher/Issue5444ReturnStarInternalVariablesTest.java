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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5444: {@code RETURN *} projects the variables the query wrote. The executor also binds
 * anonymous pattern elements to generated variables ({@code   __anon0}, {@code   src0}, ...), and
 * those were passed straight through to the caller, who never named them and cannot reference them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5444ReturnStarInternalVariablesTest {
  private static final String DATABASE_PATH = "./target/databases/issue-5444-return-star";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Node").set("id", "hub").save();
      for (final String id : List.of("a", "b")) {
        final MutableVertex leaf = database.newVertex("Node").set("id", id).save();
        leaf.newEdge("LINK", hub, true, (Object[]) null).save();
      }
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void returnStarHidesTheAnonymousNodeOfAFixedLengthPattern() {
    assertThat(projectedVariables("MATCH (:Node)-[:LINK]->(hub:Node) WHERE hub.id = 'hub' RETURN *"))
        .hasSize(2)
        .allSatisfy(names -> assertThat(names).noneMatch(InternalVariables::isInternal).contains("hub"));
  }

  @Test
  void returnStarHidesTheAnonymousNodeOfAVariableLengthPattern() {
    assertThat(projectedVariables("MATCH (:Node)-[:LINK*1..2]->(hub:Node) WHERE hub.id = 'hub' RETURN *"))
        .hasSize(2)
        .allSatisfy(names -> assertThat(names).noneMatch(InternalVariables::isInternal).contains("hub"));
  }

  @Test
  void returnStarHidesTheAnonymousRelationshipOfAPattern() {
    assertThat(projectedVariables("MATCH (n:Node)-[:LINK]->(hub:Node) WHERE hub.id = 'hub' RETURN *"))
        .containsExactly(List.of("hub", "n"), List.of("hub", "n"));
  }

  @Test
  void returnDistinctStarIgnoresTheHiddenVariables() {
    // the two anonymous sources differ, so counting them would keep both rows
    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH (:Node)-[:LINK]->(hub:Node) WHERE hub.id = 'hub' RETURN DISTINCT *")) {
      final List<String> ids = new ArrayList<>();
      while (resultSet.hasNext())
        ids.add(resultSet.next().<Vertex>getProperty("hub").getString("id"));
      assertThat(ids).containsExactly("hub");
    }
  }

  @Test
  void returnStarStillProjectsANamedSingleVariable() {
    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH (hub:Node) WHERE hub.id = 'hub' RETURN *")) {
      assertThat(resultSet.hasNext()).isTrue();
      assertThat(resultSet.next().<Vertex>getProperty("hub").getString("id")).isEqualTo("hub");
    }
  }

  private List<List<String>> projectedVariables(final String query) {
    final List<List<String>> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next().getPropertyNames().stream().sorted().toList());
    }
    return rows;
  }
}
