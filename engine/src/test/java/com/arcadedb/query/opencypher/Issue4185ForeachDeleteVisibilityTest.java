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
