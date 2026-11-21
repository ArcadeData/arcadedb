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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #12832: in one SET clause, an item read the record from before the clause when an earlier
 * item of the clause was a map item ({@code n += {...}}, {@code n = {...}}), a label item, or a property item on another
 * variable, so {@code SET n += {b: n.a + 1}, n += {c: n.b}} left {@code c} unset and {@code SET m.a = 5, n.l = m.a} read
 * the old {@code m.a}. Those items now see the effect of every earlier item.
 * <p>
 * A run of consecutive property items on the same variable stays one simultaneous assignment (issue #5190, covered by
 * {@code CypherSetSnapshotIssue5190Test}): {@code SET a.x = a.y, a.y = a.x} still swaps.
 * <p>
 * A labelled {@code MATCH} runs on the optimizer path, an unlabelled one on the legacy step chain.
 */
class CypherSetItemOrderIssue12832Test extends TestHelper {

  @Test
  void mapItemSeesTheEarlierMapItem() {
    assertThat(properties("CREATE (n:ZF {a: 1}) SET n += {b: n.a + 1}, n += {c: n.b} RETURN properties(n) AS p"))
        .isEqualTo(Map.of("a", 1L, "b", 2L, "c", 2L));
  }

  @Test
  void propertyItemSeesTheEarlierMapItem() {
    assertThat(properties("CREATE (n:ZF {a: 1}) SET n += {b: n.a + 1}, n.c = n.b RETURN properties(n) AS p"))
        .isEqualTo(Map.of("a", 1L, "b", 2L, "c", 2L));
  }

  @Test
  void mapItemSeesTheEarlierPropertyItem() {
    assertThat(properties("CREATE (n:ZF {a: 1}) SET n.b = n.a + 1, n += {c: n.b} RETURN properties(n) AS p"))
        .isEqualTo(Map.of("a", 1L, "b", 2L, "c", 2L));
  }

  @Test
  void mergeMapSeesTheEarlierReplaceMap() {
    assertThat(properties("CREATE (n:ZF {a: 1}) SET n = {b: n.a + 1}, n += {c: n.b} RETURN properties(n) AS p"))
        .isEqualTo(Map.of("b", 2L, "c", 2L));
  }

  @Test
  void mapItemsOnTheOptimizerPath() {
    createZf();
    try (final ResultSet rs = database.command("opencypher", "MATCH (n:ZF {a: 1}) SET n += {x: 5}, n += {y: n.x} RETURN n.x AS x, n.y AS y")) {
      final Result row = rs.next();
      assertThat(longOf(row, "x")).isEqualTo(5L);
      assertThat(longOf(row, "y")).isEqualTo(5L);
    }
    assertStored("MATCH (n:ZF) RETURN n.y AS v", 5L);
  }

  @Test
  void mapItemsOnTheLegacyPath() {
    createZf();
    try (final ResultSet rs = database.command("opencypher", "MATCH (n) WHERE n.a = 1 SET n += {x: 5}, n += {y: n.x} RETURN n.x AS x, n.y AS y")) {
      final Result row = rs.next();
      assertThat(longOf(row, "x")).isEqualTo(5L);
      assertThat(longOf(row, "y")).isEqualTo(5L);
    }
    assertStored("MATCH (n:ZF) RETURN n.y AS v", 5L);
  }

  @Test
  void mergeOnCreateSetSeesTheEarlierItem() {
    assertThat(properties("MERGE (n:ZF {k: 200}) ON CREATE SET n += {b: n.k + 1}, n += {c: n.b} RETURN properties(n) AS p"))
        .isEqualTo(Map.of("k", 200L, "b", 201L, "c", 201L));
  }

  @Test
  void mergeOnMatchSetSeesTheEarlierItem() {
    createZf();
    assertThat(properties("MERGE (n:ZF {a: 1}) ON MATCH SET n += {x: 7}, n.y = n.x RETURN properties(n) AS p"))
        .isEqualTo(Map.of("a", 1L, "x", 7L, "y", 7L));
  }

  @Test
  void foreachSetSeesTheEarlierItem() {
    createZf();
    database.command("opencypher", "MATCH (n:ZF {a: 1}) FOREACH (i IN [3] | SET n += {x: i}, n += {y: n.x})").close();
    assertStored("MATCH (n:ZF) RETURN n.y AS v", 3L);
  }

  @Test
  void propertyItemSeesAnotherVariableWrittenEarlier() {
    try (final ResultSet rs = database.command("opencypher",
        "CREATE (n:ZW {a: 1}), (m:ZW {a: 10}) SET m.a = 5, n.l = m.a RETURN n.l AS l, m.a AS a")) {
      final Result row = rs.next();
      assertThat(longOf(row, "l")).isEqualTo(5L);
      assertThat(longOf(row, "a")).isEqualTo(5L);
    }
  }

  @Test
  void propertyItemSeesAnotherMatchedVariableWrittenEarlier() {
    database.command("opencypher", "CREATE (:ZW {a: 1}), (:ZW {a: 2})").close();
    database.command("opencypher", "MATCH (n:ZW {a: 1}), (m:ZW {a: 2}) SET m.z = 5, n.z = m.z").close();
    assertStored("MATCH (n:ZW {a: 1}) RETURN n.z AS v", 5L);
  }

  @Test
  void readOfAnotherVariableBeforeItsWriteSeesTheOldValue() {
    try (final ResultSet rs = database.command("opencypher",
        "CREATE (n:ZW {a: 1}), (m:ZW {a: 10}) SET n.l = m.a, m.a = 5 RETURN n.l AS l, m.a AS a")) {
      final Result row = rs.next();
      assertThat(longOf(row, "l")).isEqualTo(10L);
      assertThat(longOf(row, "a")).isEqualTo(5L);
    }
  }

  @Test
  void itemAfterALabelItemSeesTheEarlierItems() {
    assertThat(properties("CREATE (n:ZW {a: 1}) SET n.b = 5, n:ZX, n.c = n.b RETURN properties(n) AS p"))
        .isEqualTo(Map.of("a", 1L, "b", 5L, "c", 5L));
  }

  @Test
  void itemAfterALabelItemSeesTheEarlierItemsOnAMatchedNode() {
    database.command("opencypher", "CREATE (:ZW {a: 1})").close();
    assertThat(properties("MATCH (n:ZW {a: 1}) SET n.b = 5, n:ZX, n.c = n.b RETURN properties(n) AS p"))
        .isEqualTo(Map.of("a", 1L, "b", 5L, "c", 5L));
    assertStored("MATCH (n:ZX) RETURN n.c AS v", 5L);
  }

  @Test
  void runOfSameVariablePropertyItemsStaysSimultaneous() {
    database.command("opencypher", "CREATE (:ZS {x: 1, y: 2})").close();
    database.command("opencypher", "MATCH (a:ZS) SET a.x = a.y, a.y = a.x").close();
    assertStored("MATCH (a:ZS) RETURN a.x AS v", 2L);
    assertStored("MATCH (a:ZS) RETURN a.y AS v", 1L);
  }

  @Test
  void foldedCreateSetKeepsEveryItem() {
    // Values that read nothing the CREATE makes are folded into the first write of the node (issue #11925)
    assertThat(properties("CREATE (n:ZG) SET n = {a: 1, b: 1}, n += {b: 2, c: 3}, n.d = 4 RETURN properties(n) AS p"))
        .isEqualTo(Map.of("a", 1L, "b", 2L, "c", 3L, "d", 4L));
    assertStored("MATCH (n:ZG) RETURN n.b AS v", 2L);
  }

  @Test
  void foldedMergeOnCreateSetKeepsEveryItem() {
    assertThat(properties("MERGE (n:ZH {k: 1}) ON CREATE SET n = {k: 1, a: 1}, n += {a: 2, b: 3} RETURN properties(n) AS p"))
        .isEqualTo(Map.of("k", 1L, "a", 2L, "b", 3L));
  }

  private void createZf() {
    database.command("opencypher", "CREATE (:ZF {a: 1})").close();
  }

  private Map<String, Object> properties(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      final Map<String, Object> p = rs.next().getProperty("p");
      assertThat(rs.hasNext()).isFalse();
      return normalized(p);
    }
  }

  private void assertStored(final String cypher, final long expected) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(longOf(rs.next(), "v")).isEqualTo(expected);
    }
  }

  private static Map<String, Object> normalized(final Map<String, Object> p) {
    final Map<String, Object> out = new HashMap<>();
    for (final Map.Entry<String, Object> e : p.entrySet())
      out.put(e.getKey(), e.getValue() instanceof Number n ? n.longValue() : e.getValue());
    return out;
  }

  private static Long longOf(final Result row, final String property) {
    final Object v = row.getProperty(property);
    return v == null ? null : ((Number) v).longValue();
  }
}
