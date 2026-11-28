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
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SET over rows that fan out onto the same node, with a label write, a second alias and lists holding the node. Every expected row and statistic is what Neo4j 5.26 returns for the same data and statement. A row is rendered
 * {@code k=v, k=v} with the keys sorted, integers without a fraction.
 */
class CypherSetFanoutAliasOracleTest extends TestHelper {
  private QueryStatistics lastStatistics;

  /** Runs one statement in its own transaction and renders its rows. */
  private List<String> run(final String statement) {
    return run(statement, Map.of());
  }

  private List<String> run(final String statement, final Map<String, Object> params) {
    final List<String> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement, params)) {
        while (rs.hasNext())
          rows.add(render(rs.next()));
        lastStatistics = rs.getStatistics().orElse(null);
      }
    });
    return rows;
  }

  private void assertStats(final int propertiesSet, final int nodesCreated, final int relationshipsCreated, final int labelsAdded) {
    assertThat(lastStatistics).isNotNull();
    assertThat(lastStatistics.getPropertiesSet()).as("properties set").isEqualTo(propertiesSet);
    assertThat(lastStatistics.getNodesCreated()).as("nodes created").isEqualTo(nodesCreated);
    assertThat(lastStatistics.getRelationshipsCreated()).as("relationships created").isEqualTo(relationshipsCreated);
    assertThat(lastStatistics.getLabelsAdded()).as("labels added").isEqualTo(labelsAdded);
  }

  private static Map<String, Object> map(final Object... keysAndValues) {
    final Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2)
      m.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return m;
  }

  private static List<Object> list(final Object... values) {
    return new ArrayList<>(Arrays.asList(values));
  }

  private static String render(final Result row) {
    final Map<String, Object> sorted = new TreeMap<>();
    for (final String name : row.getPropertyNames())
      sorted.put(name, row.getProperty(name));
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<String, Object> e : sorted.entrySet()) {
      if (sb.length() > 0)
        sb.append(", ");
      sb.append(e.getKey()).append('=').append(value(e.getValue()));
    }
    return sb.toString();
  }

  private static String value(final Object o) {
    if (o == null)
      return "null";
    if (o instanceof Number n)
      return n.doubleValue() == Math.rint(n.doubleValue()) && !(o instanceof Double || o instanceof Float) ? String.valueOf(n.longValue())
          : String.valueOf(n.doubleValue());
    if (o instanceof Map<?, ?> m) {
      final StringBuilder sb = new StringBuilder("{");
      for (final Map.Entry<?, ?> e : new TreeMap<>(m).entrySet())
        sb.append(sb.length() > 1 ? ", " : "").append(e.getKey()).append(": ").append(value(e.getValue()));
      return sb.append('}').toString();
    }
    if (o instanceof Collection<?> c) {
      final List<String> parts = new ArrayList<>();
      for (final Object x : c)
        parts.add(value(x));
      return parts.toString();
    }
    return String.valueOf(o);
  }

  @Test
  void aPropertyWrittenBeforeALabelInOneClauseFollowsTheNodeAcrossFannedOutRows() {
    run("CREATE (:A {id:1}), (:A {id:2})");
    assertThat(run("MATCH (n:A) UNWIND [1,2,3] AS i SET n.x = i, n:C RETURN n.id AS id, i, labels(n) AS l, n.x AS x ORDER BY id, i")).containsExactly("i=1, id=1, l=[A, C], x=3", "i=2, id=1, l=[A, C], x=3", "i=3, id=1, l=[A, C], x=3", "i=1, id=2, l=[A, C], x=3", "i=2, id=2, l=[A, C], x=3", "i=3, id=2, l=[A, C], x=3");
    assertStats(6, 0, 0, 2);
    assertThat(run("MATCH (n:C) RETURN n.id AS id, labels(n) AS l, n.x AS x ORDER BY id")).containsExactly("id=1, l=[A, C], x=3", "id=2, l=[A, C], x=3");
  }

  @Test
  void aPropertyWrittenAfterALabelInOneClauseFollowsTheNodeAcrossFannedOutRows() {
    run("CREATE (:A {id:1}), (:A {id:2})");
    assertThat(run("MATCH (n:A) UNWIND [1,2,3] AS i SET n:B, n.x = i RETURN n.id AS id, i, labels(n) AS l, n.x AS x ORDER BY id, i")).containsExactly("i=1, id=1, l=[A, B], x=3", "i=2, id=1, l=[A, B], x=3", "i=3, id=1, l=[A, B], x=3", "i=1, id=2, l=[A, B], x=3", "i=2, id=2, l=[A, B], x=3", "i=3, id=2, l=[A, B], x=3");
    assertStats(6, 0, 0, 2);
    assertThat(run("MATCH (n:B) RETURN n.id AS id, labels(n) AS l, n.x AS x ORDER BY id")).containsExactly("id=1, l=[A, B], x=3", "id=2, l=[A, B], x=3");
  }

  @Test
  void aMapMergeAndAReplaceMapWithALabelFollowTheNodeAcrossFannedOutRows() {
    run("CREATE (:A {id:1}), (:A {id:2})");
    assertThat(run("MATCH (n:A) UNWIND [1,2] AS i SET n += {x: i}, n:D RETURN n.id AS id, i, labels(n) AS l, n.x AS x ORDER BY id, i")).containsExactly("i=1, id=1, l=[A, D], x=2", "i=2, id=1, l=[A, D], x=2", "i=1, id=2, l=[A, D], x=2", "i=2, id=2, l=[A, D], x=2");
    assertStats(4, 0, 0, 2);
    assertThat(run("MATCH (n:D) UNWIND [1,2] AS i SET n = {id: n.id, x: i}, n:E RETURN n.id AS id, i, labels(n) AS l, n.x AS x ORDER BY id, i")).containsExactly("i=1, id=1, l=[A, D, E], x=2", "i=2, id=1, l=[A, D, E], x=2", "i=1, id=2, l=[A, D, E], x=2", "i=2, id=2, l=[A, D, E], x=2");
    assertStats(8, 0, 0, 2);
    assertThat(run("MATCH (n:E) RETURN n.id AS id, labels(n) AS l, n.x AS x ORDER BY id")).containsExactly("id=1, l=[A, D, E], x=2", "id=2, l=[A, D, E], x=2");
  }

  @Test
  void aDynamicLabelAndAPropertyFollowTheNodeAcrossFannedOutRows() {
    run("CREATE (:A {id:1}), (:A {id:2})");
    assertThat(run("MATCH (n:A) UNWIND ['D','E'] AS l SET n:$(l), n.y = l RETURN n.id AS id, l, labels(n) AS ls, n.y AS y ORDER BY id, l")).containsExactly("id=1, l=D, ls=[A, D, E], y=E", "id=1, l=E, ls=[A, D, E], y=E", "id=2, l=D, ls=[A, D, E], y=E", "id=2, l=E, ls=[A, D, E], y=E");
    assertStats(4, 0, 0, 4);
    assertThat(run("MATCH (n:E) RETURN n.id AS id, labels(n) AS ls, n.y AS y ORDER BY id")).containsExactly("id=1, ls=[A, D, E], y=E", "id=2, ls=[A, D, E], y=E");
  }

  @Test
  void aSecondAliasAndAListOfTheNodesFollowALabelAndAPropertyWrittenOnUnwoundRows() {
    run("CREATE (:A {id:1}), (:A {id:2})");
    assertThat(run("MATCH (n:A) WITH n, n AS m UNWIND [1,2] AS i SET n.y = i, n:F RETURN n.id AS id, i, labels(m) AS lm, m.y AS y ORDER BY id, i")).containsExactly("i=1, id=1, lm=[A, F], y=2", "i=2, id=1, lm=[A, F], y=2", "i=1, id=2, lm=[A, F], y=2", "i=2, id=2, lm=[A, F], y=2");
    assertStats(4, 0, 0, 2);
    assertThat(run("MATCH (n:F) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND ns AS n SET n.z = 1, n:G RETURN n.id AS id, labels(n) AS l, size(ns) AS c, [x IN ns | x.z] AS zs, [x IN ns | labels(x)] AS ls ORDER BY id")).containsExactly("c=2, id=1, l=[A, F, G], ls=[[A, F, G], [A, F, G]], zs=[1, 1]", "c=2, id=2, l=[A, F, G], ls=[[A, F, G], [A, F, G]], zs=[1, 1]");
    assertStats(2, 0, 0, 2);
    assertThat(run("MATCH (n:G) WITH n, n AS m SET n:I, m.v = 5 RETURN n.id AS id, n.v AS nv, m.v AS mv, labels(m) AS lm ORDER BY id")).containsExactly("id=1, lm=[A, F, G, I], mv=5, nv=5", "id=2, lm=[A, F, G, I], mv=5, nv=5");
    assertStats(2, 0, 0, 2);
  }

  @Test
  void anExpressionTargetNamedThroughAHeldListReadsEveryEarlierRowWrite() {
    run("CREATE (:T {id:1}), (:T {id:2})");
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND [1,2] AS i SET (head(ns)).e = coalesce((head(ns)).e, 0) + 1 RETURN i, [x IN ns | x.e] AS es ORDER BY i")).containsExactly("es=[2, null], i=1", "es=[2, null], i=2");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:T {id:1}) WITH collect(n) AS ns UNWIND [1,2,3] AS i SET (ns[0]).c = coalesce((ns[0]).c, 0) + 1 RETURN i, ns[0].c AS c ORDER BY i")).containsExactly("c=3, i=1", "c=3, i=2", "c=3, i=3");
    assertStats(3, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND [1,2,3] AS i SET (ns[0]).d = coalesce((ns[0]).d, 0) + i, (ns[1]).d = coalesce((ns[1]).d, 0) + i RETURN i, [x IN ns | x.d] AS ds ORDER BY i")).containsExactly("ds=[6, 6], i=1", "ds=[6, 6], i=2", "ds=[6, 6], i=3");
    assertStats(6, 0, 0, 0);
  }

  @Test
  void aListOfNodesCollectedBeforeAMergeActionOrASetSeesTheWrite() {
    run("CREATE (:T {id:1}), (:T {id:2})");
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns MERGE (m:T {id:1}) ON MATCH SET m.z = 5 RETURN [x IN ns | x.z] AS zs")).containsExactly("zs=[5, null]");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns MERGE (m:T {id:1}) ON MATCH SET m:W RETURN [x IN ns | labels(x)] AS ls")).containsExactly("ls=[[T, W], [T]]");
    assertStats(0, 0, 0, 1);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns MATCH (m:T {id:1}) SET m.z2 = 6 RETURN [x IN ns | x.z2] AS zs")).containsExactly("zs=[6, null]");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns MATCH (m:T {id:1}) SET m.z3 = 6 WITH ns, m SET m.y = size([x IN ns WHERE x.z3 = 6]) RETURN m.y AS y")).containsExactly("y=1");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns MATCH (m:T {id:2}) SET m:X SET m.q = size([x IN ns WHERE 'X' IN labels(x)]) RETURN m.q AS q, [x IN ns | labels(x)] AS ls")).containsExactly("ls=[[T, W], [T, X]], q=1");
    assertStats(1, 0, 0, 1);
  }

  @Test
  void aMergeActionOverFannedOutRowsReadsAndRedirectsTheNode() {
    run("CREATE (:T {id:1}), (:T {id:2})");
    assertThat(run("UNWIND [1,2,3] AS i MERGE (n:T {id: 1}) ON MATCH SET n.m = coalesce(n.m, 0) + 1 ON CREATE SET n.m = 100 RETURN i, n.m AS m ORDER BY i")).containsExactly("i=1, m=3", "i=2, m=3", "i=3, m=3");
    assertStats(3, 0, 0, 0);
    assertThat(run("UNWIND [1,2,3] AS i MERGE (n:T {id: 9}) ON CREATE SET n.m = 100 ON MATCH SET n.m = n.m + 1, n:U RETURN i, n.m AS m, labels(n) AS l ORDER BY i")).containsExactly("i=1, l=[T, U], m=102", "i=2, l=[T, U], m=102", "i=3, l=[T, U], m=102");
    assertStats(4, 1, 0, 2);
    assertThat(run("MATCH (n:U) UNWIND [1,2] AS i MERGE (m:T {id: 9}) ON MATCH SET m:V, m.k = i RETURN i, labels(n) AS ln, labels(m) AS lm, m.k AS k ORDER BY i")).containsExactly("i=1, k=2, lm=[T, U, V], ln=[T, U, V]", "i=2, k=2, lm=[T, U, V], ln=[T, U, V]");
    assertStats(2, 0, 0, 1);
    assertThat(run("MATCH (d:T {id:1}) MERGE (c:T {id:1}) ON MATCH SET c.q = coalesce(d.q, 0) + 1 RETURN c.q AS cq, d.q AS dq")).containsExactly("cq=1, dq=1");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (d:T {id:1}) MERGE (c:T {id:1}) ON MATCH SET c.q = coalesce(d.q, 0) + 1 RETURN c.q AS cq, d.q AS dq")).containsExactly("cq=2, dq=2");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aListHeldByEveryRowSeesEveryFormOfSetWrittenByEarlierUnwoundRows() {
    run("CREATE (:T {id:1}), (:T {id:2}), (:T {id:3})");
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND ns AS m SET m.r = size([x IN ns WHERE x.r IS NOT NULL]) RETURN m.id AS id, m.r AS r ORDER BY id")).containsExactly("id=1, r=0", "id=2, r=1", "id=3, r=2");
    assertStats(3, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND ns AS m SET m += {s: size([x IN ns WHERE x.s IS NOT NULL])} RETURN m.id AS id, m.s AS s ORDER BY id")).containsExactly("id=1, s=0", "id=2, s=1", "id=3, s=2");
    assertStats(3, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND ns AS m SET m = {id: m.id, u: size([x IN ns WHERE x.u IS NOT NULL])} RETURN m.id AS id, m.u AS u ORDER BY id")).containsExactly("id=1, u=0", "id=2, u=1", "id=3, u=2");
    assertStats(12, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND ns AS m MERGE (p:T {id: m.id}) ON MATCH SET p += {v: size([x IN ns WHERE x.v IS NOT NULL])} RETURN p.id AS id, p.v AS v ORDER BY id")).containsExactly("id=1, v=0", "id=2, v=1", "id=3, v=2");
    assertStats(3, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND ns AS m MERGE (p:T {id: m.id}) ON MATCH SET p = {id: p.id, w: size([x IN ns WHERE x.w IS NOT NULL])} RETURN p.id AS id, p.w AS w ORDER BY id")).containsExactly("id=1, w=0", "id=2, w=1", "id=3, w=2");
    assertStats(12, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND ns AS m MERGE (p:T {id: m.id}) ON MATCH SET p.z = size([x IN ns WHERE x.z IS NOT NULL]) RETURN p.id AS id, p.z AS z ORDER BY id")).containsExactly("id=1, z=0", "id=2, z=1", "id=3, z=2");
    assertStats(3, 0, 0, 0);
  }
}
