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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #11688: a relationship removed by the cascade of a DETACH DELETE of one of its
 * endpoints must be treated as deleted for the rest of the query, exactly as a relationship removed by an explicit
 * {@code DELETE r}: reading one of its properties raises {@code DeletedEntityAccess} and rolls the statement back.
 */
class CypherDetachDeleteCascadeIssue11688Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-detach-delete-cascade-11688").create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:Node {id:1,v:1,s:'a',b:true,l:[1,2]})
        CREATE (b:Node {id:2,v:2,s:'b',b:false,l:[]})
        CREATE (c:Node {id:3,v:3,s:'c',b:true,l:[3]})
        CREATE (a)-[:R {k:1}]->(b)
        CREATE (b)-[:R {k:2}]->(c)"""));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void directDeleteOfTheRelationshipThenPropertyReadIsRejected() {
    assertThatThrownBy(() -> database.transaction(() -> drain("MATCH ()-[r:R]->() DELETE r RETURN r.k AS rk")))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("DeletedEntityAccess");

    assertCounts(3, 2);
  }

  @Test
  void detachDeleteOfTheTargetThenPropertyReadOfTheCascadedRelationshipIsRejected() {
    assertThatThrownBy(() -> database.transaction(
        () -> drain("MATCH (a:Node {id:2})-[r:R]->(b:Node {id:3}) DETACH DELETE b RETURN r.k AS rk")))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("DeletedEntityAccess");

    // the statement is rolled back with the failed read, as the direct DELETE r path is
    assertCounts(3, 2);
  }

  @Test
  void detachDeleteOfTheSourceThenPropertyReadOfTheCascadedRelationshipIsRejected() {
    assertThatThrownBy(() -> database.transaction(
        () -> drain("MATCH (a:Node {id:2})-[r:R]->(b:Node {id:3}) DETACH DELETE a RETURN r.k AS rk")))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("DeletedEntityAccess");

    assertCounts(3, 2);
  }

  @Test
  void detachDeleteThenReturnOfTheCascadedRelationshipMatchesTheDirectDeletePath() {
    final List<Object> direct = new ArrayList<>();
    database.begin();
    try (final ResultSet rs = database.command("opencypher", "MATCH (:Node {id:2})-[r:R]->(:Node {id:3}) DELETE r RETURN r")) {
      while (rs.hasNext())
        direct.add(rs.next().getProperty("r"));
    } finally {
      database.rollback();
    }
    assertCounts(3, 2);

    final List<Object> cascaded = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a:Node {id:2})-[r:R]->(b:Node {id:3}) DETACH DELETE b RETURN r")) {
        while (rs.hasNext())
          cascaded.add(rs.next().getProperty("r"));
      }
    });

    assertThat(cascaded).hasSize(1);
    assertThat(direct).hasSize(1);
    assertThat(String.valueOf(cascaded.getFirst())).isEqualTo(String.valueOf(direct.getFirst()));
    // node c and the relationship b->c are gone, a->b survives
    assertCounts(2, 1);
    assertThat(edgeKeys()).containsExactly(1L);
  }

  @Test
  void typeOfTheCascadedRelationshipIsStillReadable() {
    final List<Object> types = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a:Node {id:2})-[r:R]->(b:Node {id:3}) DETACH DELETE b RETURN type(r) AS t")) {
        while (rs.hasNext())
          types.add(rs.next().getProperty("t"));
      }
    });
    assertThat(types).containsExactly("R");
    assertCounts(2, 1);
  }

  @Test
  void cascadeOverAHubWithIncomingAndOutgoingRelationshipsMarksEveryBoundRelationship() {
    database.transaction(() -> database.command("opencypher", """
        MATCH (b:Node {id:2})
        CREATE (x:Node {id:10})-[:R {k:10}]->(b)
        CREATE (y:Node {id:11})-[:R {k:11}]->(b)
        CREATE (b)-[:R {k:12}]->(:Node {id:12})
        CREATE (b)-[:R {k:13}]->(:Node {id:13})"""));
    assertCounts(7, 6);

    // every row binds a different relationship of the hub, in either direction: all of them went with it
    assertThatThrownBy(() -> database.transaction(
        () -> drain("MATCH (h:Node {id:2})-[r:R]-(o) DETACH DELETE h RETURN r.k AS rk")))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("DeletedEntityAccess");
    assertCounts(7, 6);

    // the cascade removed exactly the hub's six relationships, and a relationship not touching it is still readable
    database.transaction(() -> database.command("opencypher", "MATCH (a:Node {id:1}), (c:Node {id:3}) CREATE (a)-[:R {k:99}]->(c)"));
    final List<Object> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (h:Node {id:2})-[r:R]-(o)
          WITH h, collect(r) AS rels
          MATCH (:Node {id:1})-[keep:R {k:99}]->(:Node {id:3})
          DETACH DELETE h
          RETURN keep.k AS kk, size(rels) AS n""")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(((Number) row.getProperty("kk")).longValue());
          rows.add(((Number) row.getProperty("n")).longValue());
        }
      }
    });
    assertThat(rows).containsExactly(99L, 6L);
    assertCounts(6, 1);
    assertThat(edgeKeys()).containsExactly(99L);
  }

  @Test
  void relationshipNotTouchedByTheCascadeStaysReadable() {
    final List<Object> keys = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a:Node {id:1})-[r:R]->(b:Node {id:2}), (c:Node {id:3}) DETACH DELETE c RETURN r.k AS rk")) {
        while (rs.hasNext())
          keys.add(((Number) rs.next().getProperty("rk")).longValue());
      }
    });
    assertThat(keys).containsExactly(1L);
    assertCounts(2, 1);
  }

  private void drain(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private List<Object> edgeKeys() {
    final List<Object> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:R]->() RETURN r.k AS k ORDER BY k")) {
      while (rs.hasNext())
        keys.add(((Number) rs.next().getProperty("k")).longValue());
    }
    return keys;
  }

  private void assertCounts(final long nodes, final long edges) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n) RETURN count(n) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).as("node count").isEqualTo(nodes);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).as("relationship count").isEqualTo(edges);
    }
  }
}
