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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8585: {@code arcadedb.queryMaxHeapElementsAllowedPerOp} bounded the in-heap operations of SQL only. An
 * OpenCypher query buffering a whole label in a Cartesian product - or sorting, de-duplicating, grouping or collecting
 * without bound - ran the server out of memory instead of failing on its own. Every one of them must now fail with a
 * {@link CommandExecutionException} naming the setting once it holds more elements than allowed, and run as before
 * below it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherHeapElementsLimitIssue8585Test extends TestHelper {
  private static final int ROWS  = 30;
  private static final long LIMIT = 10;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("N");
      database.getSchema().createVertexType("M");
      database.getSchema().createVertexType("Small");
      database.getSchema().createEdgeType("E");
      for (int i = 0; i < ROWS; i++) {
        final MutableVertex n = database.newVertex("N").set("id", i).set("grp", i).save();
        final MutableVertex m = database.newVertex("M").set("id", i).save();
        n.newEdge("E", m).save();
      }
      for (int i = 0; i < 4; i++)
        database.newVertex("Small").set("id", i).save();
    });
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, LIMIT);
  }

  @Test
  void cartesianProductBufferIsBounded() {
    assertExceedsLimit("MATCH (a:N), (b:M) RETURN a.id AS x, b.id AS y");
    // Two MATCH clauses are the same product
    assertExceedsLimit("MATCH (a:N) MATCH (b:M) RETURN a.id AS x, b.id AS y");
    // A count of the product builds no row since issue #9596: it is the product of the two counts
    assertThat(singleLong("MATCH (a:N), (b:M) RETURN count(*) AS c")).isEqualTo((long) ROWS * ROWS);
  }

  @Test
  void sqlMatchCartesianProductIsBounded() {
    // The SQL MATCH buffers the rows of a disconnected pattern it replays the same way
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "MATCH {type: N, as: a}, {type: M, as: b} RETURN count(*) AS c")) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  @Test
  void hashJoinBuildSideIsBounded() {
    assertExceedsLimit("MATCH (a:N), (b:M) WHERE a.id = b.id RETURN count(*) AS c");
  }

  @Test
  void orderByIsBounded() {
    assertExceedsLimit("MATCH (n:N) RETURN n.id AS id ORDER BY id DESC");
    // A top-K heap larger than the limit is bounded too...
    assertExceedsLimit("MATCH (n:N) RETURN n.id AS id ORDER BY id DESC LIMIT 20");
    // ...while one within it never holds more than K rows
    assertThat(count("MATCH (n:N) RETURN n.id AS id ORDER BY id DESC LIMIT 5")).isEqualTo(5);
  }

  @Test
  void distinctIsBounded() {
    assertExceedsLimit("MATCH (n:N) RETURN DISTINCT n.id AS id");
    assertExceedsLimit("MATCH (n:N) WITH DISTINCT n.id AS id RETURN id");
    assertExceedsLimit("MATCH (n:N) RETURN n.id AS id UNION MATCH (m:M) RETURN m.id AS id");
    // UNION ALL keeps no set
    assertThat(count("MATCH (n:N) RETURN n.id AS id UNION ALL MATCH (m:M) RETURN m.id AS id")).isEqualTo(2L * ROWS);
  }

  @Test
  void groupingIsBounded() {
    assertExceedsLimit("MATCH (n:N) RETURN n.grp AS grp, count(*) AS c");
    assertExceedsLimit("MATCH (n:N)-[:E]->(m) RETURN n.id AS id, count(m) AS c");
  }

  @Test
  void collectIsBounded() {
    assertExceedsLimit("MATCH (n:N) RETURN collect(n.id) AS ids");
    assertExceedsLimit("MATCH (n:N) RETURN collect(DISTINCT n.id) AS ids");
  }

  @Test
  void eagerMaterializationIsBounded() {
    // Deleting over a disconnected pattern reads the whole product first: 4 x 4 rows, although each buffer holds 4
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (a:Small), (b:Small) DETACH DELETE a, b").close()))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    assertThat(count("MATCH (s:Small) RETURN s")).as("the failed delete was rolled back").isEqualTo(4);
  }

  @Test
  void everythingRunsWithinTheLimit() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10_000L);

    assertThat(singleLong("MATCH (a:N), (b:M) RETURN count(*) AS c")).isEqualTo((long) ROWS * ROWS);
    assertThat(singleLong("MATCH (a:N), (b:M) WHERE a.id = b.id RETURN count(*) AS c")).isEqualTo(ROWS);
    try (final ResultSet rs = database.query("sql", "MATCH {type: N, as: a}, {type: M, as: b} RETURN count(*) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo((long) ROWS * ROWS);
    }
    assertThat(count("MATCH (n:N) RETURN n.id AS id ORDER BY id DESC")).isEqualTo(ROWS);
    assertThat(count("MATCH (n:N) RETURN DISTINCT n.id AS id")).isEqualTo(ROWS);
    assertThat(count("MATCH (n:N) RETURN n.grp AS grp, count(*) AS c")).isEqualTo(ROWS);
    assertThat(count("MATCH (n:N) RETURN n.id AS id UNION MATCH (m:M) RETURN m.id AS id")).isEqualTo(ROWS);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:N) RETURN collect(n.id) AS ids")) {
      assertThat(rs.next().<List<?>>getProperty("ids")).hasSize(ROWS);
    }
  }

  @Test
  void aNonPositiveLimitMeansNoLimit() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);
    assertThat(singleLong("MATCH (a:N), (b:M) RETURN count(*) AS c")).isEqualTo((long) ROWS * ROWS);
    assertThat(count("MATCH (n:N) RETURN DISTINCT n.id AS id")).isEqualTo(ROWS);
  }

  private void assertExceedsLimit(final String query) {
    assertThatThrownBy(() -> count(query))
        .as(query)
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey())
        .hasMessageContaining("(" + LIMIT + ")");
  }

  private long count(final String query) {
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private long singleLong(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
