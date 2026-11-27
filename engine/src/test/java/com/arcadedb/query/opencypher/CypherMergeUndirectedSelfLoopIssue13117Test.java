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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #13117: an undirected MERGE walks the outgoing and then the incoming edges of a vertex, and
 * a self-loop is stored in both lists, so a loop that already existed was answered twice. A loop is one relationship, answered
 * once, as MATCH and Neo4j answer it.
 */
class CypherMergeUndirectedSelfLoopIssue13117Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final String path = "./target/databases/cypher-merge-undirected-selfloop-13117";
    // A run aborted before its tearDown leaves the directory behind
    FileUtils.deleteRecursively(new File(path));
    database = new DatabaseFactory(path).create();
    database.getSchema().createVertexType("E");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> database.command("opencypher", "CREATE (a:E {k:'x'})-[:R]->(a)"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  private long rows(final String query) {
    final long[] count = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext()) {
          rs.next();
          count[0]++;
        }
      }
    });
    return count[0];
  }

  private long relationships() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:R]->() RETURN count(r) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void matchAnswersTheLoopOnce() {
    assertThat(rows("MATCH (a:E {k:'x'}) MATCH (b:E {k:'x'}) MATCH (a)-[r:R]-(b) RETURN r")).isEqualTo(1);
  }

  @Test
  void bothEndsBoundAnswersTheLoopOnce() {
    assertThat(rows("MATCH (a:E {k:'x'}) MATCH (b:E {k:'x'}) MERGE (a)-[r:R]-(b) RETURN r")).isEqualTo(1);
    assertThat(rows("MATCH (a:E {k:'x'}) MERGE (a)-[r:R]-(a) RETURN r")).isEqualTo(1);
    assertThat(relationships()).isEqualTo(1);
  }

  // The bound end is on the right, so MERGE walks leftwards from it (walkLeft)
  @Test
  void rightEndBoundAnswersTheLoopOnce() {
    assertThat(rows("MATCH (b:E {k:'x'}) MERGE (a:E {k:'x'})-[r:R]-(b) RETURN r")).isEqualTo(1);
    assertThat(relationships()).isEqualTo(1);
  }

  // The bound end is on the left, so MERGE walks rightwards from it (walkRight)
  @Test
  void leftEndBoundAnswersTheLoopOnce() {
    assertThat(rows("MATCH (a:E {k:'x'}) MERGE (a)-[r:R]-(b:E {k:'x'}) RETURN r")).isEqualTo(1);
    assertThat(relationships()).isEqualTo(1);
  }

  @Test
  void nothingBoundAnswersTheLoopOnce() {
    assertThat(rows("MERGE (a:E {k:'x'})-[r:R]-(b:E {k:'x'}) RETURN r")).isEqualTo(1);
    assertThat(relationships()).isEqualTo(1);
  }

  @Test
  void indexedEndAnswersTheLoopOnce() {
    database.command("sql", "CREATE PROPERTY E.k STRING");
    database.command("sql", "CREATE INDEX ON E (k) UNIQUE");
    assertThat(rows("MATCH (a:E {k:'x'}) MERGE (a)-[r:R]-(b:E {k:'x'}) RETURN r")).isEqualTo(1);
    assertThat(rows("MERGE (a:E {k:'x'}) MERGE (b:E {k:'x'}) MERGE (a)-[r:R]-(b) RETURN r")).isEqualTo(1);
    assertThat(relationships()).isEqualTo(1);
  }

  @Test
  void lightweightLoopIsAnsweredOnce() {
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    database.transaction(() -> database.command("opencypher", "CREATE (a:E {k:'y'})-[:L]->(a)"));
    assertThat(rows("MATCH (a:E {k:'y'}) MERGE (a)-[r:L]-(a) RETURN r")).isEqualTo(1);
    assertThat(rows("MERGE (a:E {k:'y'})-[r:L]-(b:E {k:'y'}) RETURN r")).isEqualTo(1);
  }

  @Test
  void onMatchActionRunsOncePerLoop() {
    rows("MATCH (a:E {k:'x'}) MERGE (a)-[r:R]-(a) ON MATCH SET r.c = coalesce(r.c, 0) + 1 RETURN r");
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:R]->() RETURN r.c AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1);
    }
  }

  @Test
  void replayedBatchMatchesTheLoopOncePerRow() {
    // The first row finds the loop that the setup created, the second matches it again: one row each
    assertThat(rows("UNWIND [{a:'x', b:'x'}, {a:'x', b:'x'}] AS row MERGE (a:E {k:row.a}) MERGE (b:E {k:row.b}) MERGE (a)-[r:R]-(b) RETURN r"))
        .isEqualTo(2);
    assertThat(relationships()).isEqualTo(1);
  }

  @Test
  void directedAndDistinctEndpointsAreUntouched() {
    assertThat(rows("MATCH (a:E {k:'x'}) MERGE (a)-[r:R]->(a) RETURN r")).isEqualTo(1);
    assertThat(rows("MATCH (a:E {k:'x'}) MERGE (a)<-[r:R]-(a) RETURN r")).isEqualTo(1);
    database.transaction(() -> database.command("opencypher", "CREATE (:E {k:'p'})-[:R]->(:E {k:'q'})"));
    // An ordinary relationship between two vertices is still one path from either side
    assertThat(rows("MATCH (p:E {k:'p'}), (q:E {k:'q'}) MERGE (p)-[r:R]-(q) RETURN r")).isEqualTo(1);
    assertThat(rows("MATCH (p:E {k:'p'}), (q:E {k:'q'}) MERGE (q)-[r:R]-(p) RETURN r")).isEqualTo(1);
    assertThat(relationships()).isEqualTo(2);
  }
}
