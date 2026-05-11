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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4185.
 * <p>
 * Nodes deleted earlier in the same query (via {@code FOREACH ... DELETE}) must not be
 * visible to a later {@code MATCH} in the same query. The later stage must see the graph
 * state produced by the deletion stage, not the pre-deletion snapshot.
 */
class Issue4185ForeachDeleteVisibilityTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-4185-foreach-delete-visibility").create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("REL");
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void laterMatchDoesNotSeeNodeDeletedInForeach() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    final ResultSet rs = database.command("opencypher",
        """
            MATCH p = ()-[*]->()
            WITH relationships(p) AS rels
            FOREACH (r IN rels | DELETE endNode(r) DELETE r)
            WITH 1 AS dummy
            MATCH (n:Node)
            RETURN n.name AS name
            ORDER BY name""");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      names.add(r.getProperty("name"));
    }
    assertThat(names).containsExactly("A");
  }

  /**
   * DETACH DELETE inside FOREACH still routes through the deferred batch and removes both
   * the vertex and its connected edges in one iteration.
   */
  @Test
  void detachDeleteInsideForeachClearsBothEndsAndEdge() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    database.command("opencypher",
        """
            MATCH p = ()-[*]->()
            WITH relationships(p) AS rels
            FOREACH (r IN rels | DETACH DELETE endNode(r))
            RETURN 1 AS x""");

    final ResultSet verify = database.query("opencypher", "MATCH (n:Node) RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (verify.hasNext()) {
      names.add(verify.next().getProperty("name"));
    }
    assertThat(names).containsExactly("A");
  }

  /**
   * Nested FOREACH with DELETE inside the inner loop. Confirms the deferred-batch
   * save/restore around DEFERRED_DELETE_BATCH_VAR isolates iterations correctly.
   */
  @Test
  void nestedForeachDeleteWorks() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (:Node {name:'X'}), (:Node {name:'Y'}), (:Node {name:'Z'})");
    });

    database.command("opencypher",
        """
            MATCH (n:Node)
            WITH collect(n) AS allNodes
            FOREACH (group IN [allNodes] |
              FOREACH (node IN group | DELETE node)
            )
            RETURN 1 AS x""");

    final ResultSet verify = database.query("opencypher", "MATCH (n:Node) RETURN count(n) AS total");
    assertThat(verify.hasNext()).isTrue();
    assertThat(verify.next().<Number>getProperty("total").longValue()).isZero();
  }

  /**
   * Mixed SET and DELETE inside a single FOREACH iteration. SET on the survivor must run
   * before the iteration's deletes flush, so the surviving node carries the updated property.
   */
  @Test
  void mixedSetAndDeleteInsideForeach() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    database.command("opencypher",
        """
            MATCH (a:Node {name:'A'})-[r:REL]->(b:Node {name:'B'})
            WITH a, r, b
            FOREACH (x IN [1] |
              SET a.tagged = true
              DELETE r
              DELETE b
            )
            RETURN 1 AS x""");

    final ResultSet verify = database.query("opencypher",
        "MATCH (n:Node) RETURN n.name AS name, n.tagged AS tagged ORDER BY name");
    final List<String> rows = new ArrayList<>();
    while (verify.hasNext()) {
      final Result r = verify.next();
      rows.add(r.<String>getProperty("name") + ":" + r.<Boolean>getProperty("tagged"));
    }
    assertThat(rows).containsExactly("A:true");
  }

  /**
   * FOREACH+DELETE inside an already-active outer transaction must reuse that transaction:
   * the outer transaction owns commit/rollback, the FOREACH neither begins nor commits.
   */
  @Test
  void foreachDeleteInsideOuterTransaction() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    database.transaction(() -> {
      database.command("opencypher",
          """
              MATCH p = ()-[*]->()
              WITH relationships(p) AS rels
              FOREACH (r IN rels | DELETE endNode(r) DELETE r)""");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Node) RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (verify.hasNext()) {
      names.add(verify.next().getProperty("name"));
    }
    assertThat(names).containsExactly("A");
  }

  /**
   * Control: when there is no later MATCH, deletion must still work.
   */
  @Test
  void foreachDeleteAloneStillWorks() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Node {name:'A'})-[:REL]->(b:Node {name:'B'})");
    });

    database.command("opencypher",
        """
            MATCH p = ()-[*]->()
            WITH relationships(p) AS rels
            FOREACH (r IN rels | DELETE endNode(r) DELETE r)
            RETURN 1 AS x""");

    final ResultSet verify = database.query("opencypher", "MATCH (n:Node) RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (verify.hasNext()) {
      names.add(verify.next().getProperty("name"));
    }
    assertThat(names).containsExactly("A");
  }
}
