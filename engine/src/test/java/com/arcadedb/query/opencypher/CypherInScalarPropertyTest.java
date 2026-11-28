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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13563: {@code x IN n.p} compares a scalar stored value with x as a one-element list, as Neo4j does (every count below is
 * what Neo4j 5.26 answers); a right side whose type is known (a literal, a parameter) is still refused (#10541).
 */
class CypherInScalarPropertyTest extends TestHelper {
  @Test
  void aScalarPropertyOnTheRightIsAOneElementList() {
    database.getSchema().createVertexType("In");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:In {v: 'Accepted'}), (:In {v: 'Other'}), (:In {w: 1}), (:In {v: ['Accepted', 'x']})").close());

    assertThat(count("MATCH (n:In) WHERE $p IN n.v RETURN count(n) AS c", Map.of("p", "Accepted"))).isEqualTo(2L);
    assertThat(count("MATCH (n:In) WHERE 'Accepted' IN n.v RETURN count(n) AS c", Map.of())).isEqualTo(2L);
    assertThat(count("MATCH (n:In) WITH n WHERE $p IN n.v RETURN count(n) AS c", Map.of("p", "Accepted"))).isEqualTo(2L);
    assertThat(count("MATCH (n:In) WHERE NOT 'Other' IN n.v RETURN count(n) AS c", Map.of())).isEqualTo(2L);
    assertThat(count("MATCH (n:In) RETURN sum(CASE WHEN $p IN n.v THEN 1 ELSE 0 END) AS c", Map.of("p", "Accepted"))).isEqualTo(2L);
    // the dynamic spelling of the same read
    assertThat(count("MATCH (n:In) WHERE 'Accepted' IN n['v'] RETURN count(n) AS c", Map.of())).isEqualTo(2L);
  }

  @Test
  void aMissingPropertyIsNullNotAnError() {
    database.getSchema().createVertexType("In");
    database.transaction(() -> database.command("opencypher", "CREATE (:In {w: 1})").close());

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:In) RETURN 'a' IN n.v AS r")) {
      final Result row = rs.next();
      assertThat(row.hasProperty("r")).isTrue();
      assertThat((Object) row.getProperty("r")).isNull();
    }
  }

  @Test
  void aNumberAMapAndAStoredArrayAreReadAsTheyAre() {
    database.getSchema().createVertexType("In");
    database.transaction(() -> {
      database.newVertex("In").set("id", 1).set("v", 5L).save();
      database.newVertex("In").set("id", 2).set("v", new long[] { 4L, 5L }).save();
      database.newVertex("In").set("id", 3).set("v", Map.of("k", 5L)).save();
      database.newVertex("In").set("id", 4).set("v", 6L).save();
    });

    // a number is a one-element list; a stored primitive array is a list of its elements; a map is one value, which 5 is not
    assertThat(ids("MATCH (n:In) WHERE 5 IN n.v RETURN n.id AS id ORDER BY id", Map.of())).containsExactly(1, 2);
    assertThat(ids("MATCH (n:In) WHERE $m IN n.v RETURN n.id AS id ORDER BY id", Map.of("m", Map.of("k", 5L)))).containsExactly(3);
  }

  @Test
  void aChainedReadOfAStoredMapIsAOneElementList() {
    database.getSchema().createVertexType("In");
    database.transaction(() -> {
      database.newVertex("In").set("id", 1).set("m", Map.of("k", "a")).save();
      database.newVertex("In").set("id", 2).set("m", Map.of("k", List.of("a", "b"))).save();
      database.newVertex("In").set("id", 3).set("m", Map.of("k", "b")).save();
    });

    assertThat(ids("MATCH (n:In) WHERE 'a' IN n.m.k RETURN n.id AS id ORDER BY id", Map.of())).containsExactly(1, 2);
    assertThat(ids("MATCH (n:In) WITH n, n.m AS m WHERE 'a' IN m.k RETURN n.id AS id ORDER BY id", Map.of())).containsExactly(1, 2);
  }

  /**
   * A part of a parameter on the right side ({@code $m.k}, {@code $ids[0]}) is a read whose type is only known at run time, and an
   * index seek answers it from {@code InExpression.listOperand} without evaluating the IN: both paths must answer alike, or the
   * index changes the answer, which #10541 was about.
   */
  @Test
  void aPartOfAParameterIsAnsweredAlikeWithAndWithoutTheIndex() {
    database.getSchema().createVertexType("Idx").createProperty("k", "STRING");
    database.getSchema().createVertexType("Plain").createProperty("k", "STRING");
    database.command("sql", "CREATE INDEX ON Idx (k) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Idx {k: 'a'}), (:Idx {k: 'b'}), (:Plain {k: 'a'}), (:Plain {k: 'b'})").close());

    final Map<String, Object> scalar = Map.of("m", Map.of("k", "a"), "ids", List.of("a", List.of("b")));
    final Map<String, Object> list = Map.of("m", Map.of("k", List.of("a", "b")), "ids", List.of(List.of("a", "b")));
    for (final String rightSide : new String[] { "$m.k", "$ids[0]" }) {
      final String indexed = "MATCH (b:Idx) WHERE b.k IN " + rightSide + " RETURN b.k AS k ORDER BY k";
      assertThat(plan(indexed, scalar)).as(rightSide).contains("NodeIndexSeek");

      assertThat(keys(indexed, scalar)).as(rightSide).containsExactly("a");
      assertThat(keys(indexed.replace(":Idx", ":Plain"), scalar)).as(rightSide).containsExactly("a");
      assertThat(keys(indexed, list)).as(rightSide).containsExactly("a", "b");
      assertThat(keys(indexed.replace(":Idx", ":Plain"), list)).as(rightSide).containsExactly("a", "b");
    }
  }

  @Test
  void aLiteralOrAParameterThatIsNoListIsStillRefused() {
    assertThatThrownBy(() -> count("RETURN 'a' IN 'a' AS c", Map.of())).hasMessageContaining("IN requires a list");
    assertThatThrownBy(() -> count("RETURN 'a' IN $x AS c", Map.of("x", "a"))).hasMessageContaining("IN requires a list");
  }

  @Test
  void aParameterThatIsNoListIsRefusedWithAndWithoutTheIndex() {
    database.getSchema().createVertexType("Idx").createProperty("k", "STRING");
    database.getSchema().createVertexType("Plain").createProperty("k", "STRING");
    database.command("sql", "CREATE INDEX ON Idx (k) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:Idx {k: 'a'}), (:Plain {k: 'a'})").close());

    for (final String label : new String[] { "Idx", "Plain" })
      assertThatThrownBy(() -> keys("MATCH (b:" + label + ") WHERE b.k IN $x RETURN b.k AS k", Map.of("x", "a"))).as(label)
          .hasStackTraceContaining("IN requires a list");
  }

  @Test
  void aBinaryPropertyIsStillRefused() {
    // a BINARY value is one opaque value that is no list (issue #8098): it is neither split into bytes nor compared as one element
    database.getSchema().createVertexType("In");
    database.transaction(() -> database.newVertex("In").set("v", new byte[] { 1, 2 }).save());

    assertThatThrownBy(() -> count("MATCH (n:In) WHERE 1 IN n.v RETURN count(n) AS c", Map.of())).hasStackTraceContaining(
        "IN requires a list");
  }

  private long count(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private List<Integer> ids(final String query, final Map<String, Object> parameters) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      while (rs.hasNext())
        ids.add(((Number) rs.next().getProperty("id")).intValue());
    }
    return ids;
  }

  private List<String> keys(final String query, final Map<String, Object> parameters) {
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      while (rs.hasNext())
        keys.add(rs.next().getProperty("k"));
    }
    return keys;
  }

  private String plan(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, new HashMap<>(parameters))) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
