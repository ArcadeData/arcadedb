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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/12565
 * <p>
 * A node held by an earlier row (buffered by an aggregation, held by {@code collect()}) must observe a write a later row of
 * the same clause makes to it, as Neo4j and Memgraph do: a node is a reference, not a snapshot.
 * <p>
 * The rows a pipeline breaker (an aggregation, a sort) hands out read the records as every row before it left them. A row that
 * streams from a write to the next clause still reads what it held when it was produced, as a pipelined plan reads it: that is
 * {@link CypherSubqueryRowBindingIssue6362Test}, and {@link #aStreamingRowStillReadsItsOwnTime()} here.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12565HeldNodeObservesLaterWriteTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Node {id: 1, v: 1}), (:Node {id: 2, v: 2})").close());
  }

  /** The issue's first repro: row 1 holds node 2, row 2 writes it, the held node reads -4. */
  @Test
  void singleHeldNodeSeesTheLaterRowsWrite() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WITH n ORDER BY n.id
          WITH collect(n) AS ns
          UNWIND ns AS m
          WITH m, last(ns) AS h
          SET m.v = -2 * m.v
          WITH head(collect(h)) AS first
          RETURN first.v AS v""")) {
        assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(-4);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /** An expression around the aggregate reads the records collect() gathered as the writes of every row left them. */
  @Test
  void anExpressionAroundTheAggregateSeesTheLaterWrite() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WITH n ORDER BY n.id
          WITH collect(n) AS ns
          UNWIND ns AS m
          WITH m, last(ns) AS h
          SET m.v = -2 * m.v
          WITH head(collect(h)).v AS v
          RETURN v""")) {
        assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(-4);
      }
    });
  }

  /** A sort is a pipeline breaker too: the row it hands out first reads the write the second row made. */
  @Test
  void aSortedRowSeesTheLaterWrite() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WITH n ORDER BY n.id
          WITH collect(n) AS ns
          UNWIND ns AS m
          WITH m, last(ns) AS h
          SET m.v = -2 * m.v
          WITH m, h ORDER BY m.id
          RETURN m.id AS id, h.v AS v""")) {
        Result row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(1);
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(-4);
        row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(2);
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(-4);
      }
    });
  }

  /**
   * Without a breaker after the write, the first row is read before the second row's write in a pipelined plan: it still
   * answers the value it held, although the write steps run the two rows in one batch (issue #6362).
   */
  @Test
  void aStreamingRowStillReadsItsOwnTime() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WITH n ORDER BY n.id
          WITH collect(n) AS ns
          UNWIND ns AS m
          WITH m, last(ns) AS h
          SET m.v = -2 * m.v
          RETURN m.id AS id, h.v AS v""")) {
        Result row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(1);
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(2);
        row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(2);
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(-4);
      }
    });
  }

  /** The issue's second repro: a list of per-row copies, every element reads the written value. */
  @Test
  void heldListOfCopiesSeesEveryWrite() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WITH n ORDER BY n.id
          WITH collect(n) AS ns
          UNWIND ns AS m
          WITH m, [head(ns), last(ns)] AS held
          SET m.v = -2 * m.v
          WITH head(collect(held)) AS first
          RETURN [x IN first WHERE x.v <> -2 * x.id | x.id] AS stale""")) {
        assertThat(rs.next().<List<?>>getProperty("stale")).isEmpty();
      }
    });
  }

  /** Many rows: the held copies are evicted from the refresher's bounded index, and still read the written state. */
  @Test
  void heldCopiesBeyondTheRefresherBoundSeeEveryWrite() {
    database.transaction(() -> database.command("opencypher", "UNWIND range(3, 600) AS i CREATE (:Node {id: i, v: i})").close());
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WITH n ORDER BY n.id
          WITH collect(n) AS ns
          UNWIND ns AS m
          WITH m, [last(ns), m, head(ns)] AS held
          SET m.v = -2 * m.v
          WITH collect(held) AS all
          UNWIND all AS h
          UNWIND h AS x
          WITH x WHERE x.v <> -2 * x.id
          RETURN count(x) AS stale""")) {
        assertThat(rs.next().<Number>getProperty("stale").intValue()).isZero();
      }
    });
  }

  /** keys(), properties(), dynamic access and the node returned whole read the written state too. */
  @Test
  void otherReadsOfAHeldNodeSeeTheLaterWrite() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WITH n ORDER BY n.id
          WITH collect(n) AS ns
          UNWIND ns AS m
          WITH m, last(ns) AS h
          SET m.v = -2 * m.v, m.extra = m.id
          WITH head(collect(h)) AS first
          RETURN first['v'] AS dyn, properties(first) AS props, keys(first) AS ks, first AS node, first.extra AS extra""")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("dyn").intValue()).isEqualTo(-4);
        assertThat(row.<Map<String, Object>>getProperty("props")).containsEntry("v", -4L).containsEntry("extra", 2);
        assertThat(row.<List<String>>getProperty("ks")).contains("extra");
        assertThat(row.<Number>getProperty("extra").intValue()).isEqualTo(2);
        final Object node = row.getProperty("node");
        assertThat(node).isInstanceOf(Document.class);
        assertThat(((Document) node).getInteger("v")).isEqualTo(-4);
      }
    });
  }

  /** A held node answered alone (as the row's element), inside a list and through a map projection reads the write. */
  @Test
  void heldNodeAnsweredWholeSeesTheLaterWrite() {
    final String prefix = """
        MATCH (n:Node) WITH n ORDER BY n.id
        WITH collect(n) AS ns
        UNWIND ns AS m
        WITH m, last(ns) AS h
        SET m.v = -2 * m.v
        WITH head(collect(h)) AS first, collect(h) AS all
        """;
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", prefix + "RETURN first")) {
        final Result row = rs.next();
        assertThat(row.isElement()).isTrue();
        assertThat(row.toElement().getInteger("v")).isEqualTo(-4);
        assertThat(row.<Document>getProperty("first").getInteger("v")).isEqualTo(-4);
      }
      try (final ResultSet rs = database.command("opencypher", prefix + "RETURN all, first {.v} AS projected")) {
        final Result row = rs.next();
        final List<?> all = row.getProperty("all");
        assertThat(all).hasSize(2);
        // the second statement doubles and negates again: node 2 holds -2 * -4
        for (final Object held : all)
          assertThat(((Document) held).getInteger("v")).isEqualTo(8);
        assertThat(row.<Map<String, Object>>getProperty("projected")).containsEntry("v", 8L);
      }
    });
  }

  /** A read-only query never replaces a binding with anything (#6362/#11534): the plain read is unchanged. */
  @Test
  void readOnlyQueryUnchanged() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (n:Node) RETURN n.v AS v ORDER BY n.id")) {
        assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(1);
        assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(2);
      }
    });
  }
}
