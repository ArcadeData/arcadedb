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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.opencypher.executor.HeldCollectionRefresher;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/11754
 * <p>
 * A {@code REMOVE} whose target is an expression ({@code REMOVE (head(ns)).s}) used to be split on its source text, so
 * the "variable" was {@code (head(ns))} and the clause silently did nothing. And a write through any alias other than a
 * top-level variable left the copies a list or a map of the row holds at their pre-write state, so the same statement
 * read the old value back through them ({@code collect()}, a {@code CALL} subquery's outer list).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue11754SetRemoveThroughExpressionTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (:Node {id: 1, v: 1, s: 'a'})
        CREATE (:Node {id: 2, v: 2, s: 'b'})""").close());
  }

  /** The reporter's query: the RETURN reads v=10, s=null, and so does the committed state. */
  @Test
  void setAndRemoveThroughHeadOfCollect() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 1})
          WITH collect(n) AS ns
          SET (head(ns)).v = 10
          REMOVE (head(ns)).s
          RETURN head(ns).id AS id, head(ns).v AS v, head(ns).s AS s""")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(1);
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(10);
        assertThat(row.<Object>getProperty("s")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
    });

    assertThat(committed(1)).containsEntry("v", 10).containsEntry("s", null);
    assertThat(committed(2)).containsEntry("v", 2).containsEntry("s", "b");
  }

  /** REMOVE through a CASE target, the shape SET has accepted since #5227: the property goes, and only on the named node. */
  @Test
  void removeThroughCaseTarget() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node)
          REMOVE (CASE WHEN n.id = 1 THEN n END).s
          RETURN n.id AS id, n.s AS s ORDER BY id""")) {
        assertThat(rs.next().<Object>getProperty("s")).isNull();
        assertThat(rs.next().<String>getProperty("s")).isEqualTo("b");
      }
    });

    assertThat(committed(1)).containsEntry("s", null);
    assertThat(committed(2)).containsEntry("s", "b");
  }

  /** A parenthesised plain variable is that variable, so REMOVE (n).s writes through the row's own binding. */
  @Test
  void removeThroughParenthesisedVariable() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 1})
          REMOVE (n).s
          RETURN n.s AS s""")) {
        assertThat(rs.next().<Object>getProperty("s")).isNull();
      }
    });

    assertThat(committed(1)).containsEntry("s", null);
  }

  /**
   * A label removal ahead of an expression target in the same clause replaces the vertex the target named in phase 1: the
   * property is removed from the replacement, not from the deleted original.
   */
  @Test
  void removeLabelAndPropertyThroughExpressionInOneClause() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Node:Tmp {id: 3, s: 'c'})").close());

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 3})
          WITH n, collect(n) AS ns
          REMOVE n:Tmp, (head(ns)).s
          RETURN labels(n) AS labels, n.s AS s, head(ns).s AS hs""")) {
        final Result row = rs.next();
        assertThat(row.<List<String>>getProperty("labels")).containsExactly("Node");
        assertThat(row.<Object>getProperty("s")).isNull();
        assertThat(row.<Object>getProperty("hs")).isNull();
      }
    });

    try (final ResultSet rs = database.query("opencypher", "MATCH (n {id: 3}) RETURN labels(n) AS labels, n.s AS s")) {
      final Result row = rs.next();
      assertThat(row.<List<String>>getProperty("labels")).containsExactly("Node");
      assertThat(row.<Object>getProperty("s")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** A null target is a no-op, like SET (CASE ... END).p = x on a null. */
  @Test
  void removeThroughNullTargetIsANoOp() {
    database.transaction(() -> database.command("opencypher", """
        MATCH (n:Node {id: 1})
        REMOVE (CASE WHEN n.id = 2 THEN n END).s""").close());

    assertThat(committed(1)).containsEntry("s", "a");
  }

  /** A target that is neither a node nor a relationship is refused, as SET refuses it (#11693). */
  @Test
  void removeThroughMapTargetIsRefused() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", """
        WITH [{s: 1}] AS ms
        REMOVE (head(ms)).s""").close())).isInstanceOf(TypeMismatchException.class);
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", """
        MATCH (n:Node {id: 1})
        REMOVE (n {.s}).s""").close())).isInstanceOf(CommandSemanticException.class);

    assertThat(committed(1)).containsEntry("s", "a");
  }

  /** Two properties removed through the same expression target, and a dynamic key on an expression target. */
  @Test
  void removeSeveralPropertiesThroughOneTarget() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node)
          WITH collect(n) AS ns
          REMOVE (head(ns)).s, (head(ns)).v, (last(ns))[$key]
          RETURN [x IN ns | x.s] AS ss, [x IN ns | x.v] AS vs""", Map.of("key", "s"))) {
        final Result row = rs.next();
        assertThat(row.<List<Object>>getProperty("ss")).containsOnlyNulls();
        assertThat(row.<List<Object>>getProperty("vs")).containsOnlyOnce((Object) null);
      }
    });

    assertThat(committed(1)).containsEntry("s", null).containsEntry("v", null);
    assertThat(committed(2)).containsEntry("s", null).containsEntry("v", 2);
  }

  /**
   * A list long enough to be indexed rather than walked on every row ({@link HeldCollectionRefresher}): every row of the
   * UNWIND brings the same list, and every write shows through it, the first row's included, as it does in Neo4j, where the
   * list holds the nodes themselves.
   */
  @Test
  void largeSharedListFollowsEveryWrite() {
    final int count = 3 * HeldCollectionRefresher.MIN_INDEXED_SIZE;
    database.transaction(() -> database.command("opencypher", "UNWIND range(10, $last) AS i CREATE (:Node {id: i, v: i})",
        Map.of("last", 9 + count)).close());

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WHERE n.id >= 10
          WITH collect(n) AS ns
          UNWIND ns AS m
          SET m.v = -2 * m.v
          WITH head(collect(ns)) AS first, last(collect(ns)) AS last, count(*) AS rows
          RETURN rows, [x IN first WHERE x.v <> -2 * x.id | x.id] AS staleFirst,
                 [x IN last WHERE x.v <> -2 * x.id | x.id] AS staleLast""")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("rows").intValue()).isEqualTo(count);
        assertThat(row.<List<Object>>getProperty("staleFirst")).isEmpty();
        assertThat(row.<List<Object>>getProperty("staleLast")).isEmpty();
      }
    });

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Node) WHERE n.id >= 10 AND n.v <> -2 * n.id RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isZero();
    }
  }

  /** The indexed path of a map, and of REMOVE: a map shared by every row follows each removal made through UNWIND. */
  @Test
  void largeSharedMapFollowsEveryRemoval() {
    final int count = 2 * HeldCollectionRefresher.MIN_INDEXED_SIZE;
    database.transaction(() -> database.command("opencypher", "UNWIND range(10, $last) AS i CREATE (:Node {id: i, s: 'x'})",
        Map.of("last", 9 + count)).close());

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node) WHERE n.id >= 10
          WITH collect(n) AS ns
          WITH ns, apoc.map.fromPairs([x IN ns | [toString(x.id), x]]) AS byId
          UNWIND ns AS m
          REMOVE m.s
          WITH head(collect(byId)) AS byId, count(*) AS rows
          RETURN rows, [k IN keys(byId) WHERE byId[k].s IS NOT NULL | k] AS stale""")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("rows").intValue()).isEqualTo(count);
        assertThat(row.<List<Object>>getProperty("stale")).isEmpty();
      }
    });
  }

  /** The removal is counted, once, and only when the property existed. */
  @Test
  void removeThroughExpressionCountsThePropertyOnce() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 1})
          WITH collect(n) AS ns
          REMOVE (head(ns)).s, (head(ns)).missing""")) {
        while (rs.hasNext())
          rs.next();
        assertThat(rs.getStatistics().orElseThrow().getPropertiesSet()).isEqualTo(1);
      }
    });

    assertThat(committed(1)).containsEntry("s", null).containsEntry("v", 1);
  }

  /** The follow-up's first case: a plain SET of the row-bound node, read back through a list collected before it. */
  @Test
  void plainSetIsVisibleThroughAnEarlierList() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 1})
          WITH n, [n] AS ns, {node: n} AS holder
          SET n.v = 11
          RETURN head(ns).v AS v, holder.node.v AS hv""")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(11);
        assertThat(row.<Number>getProperty("hv").intValue()).isEqualTo(11);
      }
    });
  }

  /** A plain REMOVE of the row-bound node, read back through a list the row holds. */
  @Test
  void plainRemoveIsVisibleThroughAnEarlierList() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 1})
          WITH n, [n] AS ns
          REMOVE n.s
          RETURN head(ns).s AS s""")) {
        assertThat(rs.next().<Object>getProperty("s")).isNull();
      }
    });
  }

  /** SET n += {map} and SET n = {map} refresh the list too. */
  @Test
  void mapSetsAreVisibleThroughAnEarlierList() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 1})
          WITH n, [n] AS ns
          SET n += {v: 21}
          RETURN head(ns).v AS v""")) {
        assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(21);
      }
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 2})
          WITH n, [n] AS ns
          SET n = {id: 2, v: 22}
          RETURN head(ns).v AS v, head(ns).s AS s""")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(22);
        assertThat(row.<Object>getProperty("s")).isNull();
      }
    });
  }

  /** Every element of a collected list is written through UNWIND; the list read on the last row holds every write. */
  @Test
  void setThroughUnwoundElementIsVisibleThroughTheList() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node)
          WITH collect(n) AS ns
          UNWIND ns AS m
          SET m.v = m.v + 10
          WITH ns, m ORDER BY m.id DESC LIMIT 1
          RETURN [x IN ns | x.v] AS vs""")) {
        final List<Number> vs = rs.next().getProperty("vs");
        final List<Integer> ints = new ArrayList<>();
        for (final Number n : vs)
          ints.add(n.intValue());
        assertThat(ints).containsExactlyInAnyOrder(11, 12);
      }
    });
  }

  /** The follow-up's second case: a write in a CALL { } subquery, read back through a list of the outer row. */
  @Test
  void callSubqueryWriteIsVisibleThroughTheOuterList() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node {id: 1})
          WITH n, collect(n) AS ns
          CALL (n) { SET n.v = 31 }
          RETURN head(ns).v AS v, n.v AS nv""")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("nv").intValue()).isEqualTo(31);
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(31);
      }
    });
  }

  /** A CALL subquery that writes through the outer list itself, then read back through it. */
  @Test
  void callSubqueryWriteThroughTheListIsVisibleThroughIt() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Node)
          WITH collect(n) AS ns
          CALL (ns) { UNWIND ns AS m SET m.v = m.v * 100 }
          RETURN [x IN ns | x.v] AS vs""")) {
        final List<Number> vs = rs.next().getProperty("vs");
        final List<Integer> ints = new ArrayList<>();
        for (final Number n : vs)
          ints.add(n.intValue());
        assertThat(ints).containsExactlyInAnyOrder(100, 200);
      }
    });
  }

  private Map<String, Object> committed(final int id) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Node {id: $id}) RETURN n.v AS v, n.s AS s", Map.of("id", id))) {
      final Result row = rs.next();
      final Map<String, Object> map = new HashMap<>();
      final Object v = row.getProperty("v");
      map.put("v", v instanceof Number num ? num.intValue() : v);
      map.put("s", row.getProperty("s"));
      return map;
    }
  }
}
