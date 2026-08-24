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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6541: same class of bug as #5444 ({@link Issue5444ReturnStarInternalVariablesTest}), but for
 * {@code WITH *} instead of {@code RETURN *}. The executor binds anonymous pattern elements to
 * generated variables ({@code   __anon0}, ...) that a query never named; {@code WithStep}'s
 * {@code WITH *} branch forwards them unfiltered, and its {@code DISTINCT} key-building loop counted
 * them too, so two rows that only differ by such a hidden binding wrongly stayed distinct.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6541WithDistinctStarInternalVariablesTest {
  private static final String DATABASE_PATH = "./target/databases/issue-6541-with-distinct-star";

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

  /**
   * The anonymous source node differs between the two matched paths, but only {@code hub} is ever
   * returned: {@code WITH DISTINCT *} must collapse the two rows to one, the same way
   * {@code RETURN DISTINCT *} already does for #5444.
   */
  @Test
  void withDistinctStarIgnoresTheHiddenVariables() {
    final List<String> ids = new ArrayList<>();
    try (ResultSet resultSet = database.query("opencypher",
        "MATCH (:Node)-[:LINK]->(hub:Node) WHERE hub.id = 'hub' WITH DISTINCT * RETURN hub.id AS id")) {
      while (resultSet.hasNext())
        ids.add(resultSet.next().getProperty("id"));
    }
    assertThat(ids).containsExactly("hub");
  }

  /**
   * {@code WITH *} still forwards every in-scope variable downstream (only the DISTINCT key ignores
   * the internal ones), so a plain, non-distinct {@code WITH *} keeps behaving exactly as before.
   */
  @Test
  void withStarStillForwardsNamedVariables() {
    try (ResultSet resultSet = database.query("opencypher",
        "MATCH (hub:Node) WHERE hub.id = 'hub' WITH * RETURN hub.id AS id")) {
      assertThat(resultSet.hasNext()).isTrue();
      assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("hub");
    }
  }
}
