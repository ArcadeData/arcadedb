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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZonedDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9693 (#9616 + #9617): MERGE must match exactly the records the identical MATCH pattern
 * finds. Two value kinds disagreed: a stored FLOAT (MERGE widened {@code 0.1f} by its bits and missed it, duplicating the
 * node or dying on a UNIQUE index) and a temporal operand against a declared STRING property (MERGE matched the storage
 * text, MATCH did not, so ON MATCH SET fired on a node no MATCH could find).
 */
class Issue9693MergeAgreesWithMatchTest extends TestHelper {

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Long>getProperty("c");
    }
  }

  private String merge(final String query) {
    final String[] tag = new String[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        final Result row = rs.next();
        tag[0] = row.getProperty("tag");
      }
    });
    return tag[0];
  }

  // ---------------------------------------------------------------------------------------------------------- FLOAT

  private void loadFloat(final String type, final String index) {
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".v FLOAT");
    if (index != null)
      database.command("sql", "CREATE INDEX ON " + type + " (v) " + index);
    database.transaction(() -> database.newVertex(type).set("v", 0.1f).set("tag", "orig").save());
  }

  private void assertFloatMergeAgreesWithMatch(final String type) {
    assertThat(count("MATCH (n:" + type + " {v: 0.1}) RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(merge("MERGE (n:" + type + " {v: 0.1}) ON CREATE SET n.tag = 'CREATED' ON MATCH SET n.tag = 'MATCHED' RETURN n.tag AS tag"))
        .isEqualTo("MATCHED");
    assertThat(count("MATCH (n:" + type + ") RETURN count(n) AS c")).isEqualTo(1L);

    database.transaction(() -> database.command("opencypher", "MERGE (n:" + type + " {v: $v})", Map.of("v", 0.1d)).close());
    assertThat(count("MATCH (n:" + type + ") RETURN count(n) AS c")).as("a DOUBLE parameter").isEqualTo(1L);
  }

  @Test
  void mergeOnAFloatPropertyWithoutAnIndexMatchesTheExistingNode() {
    loadFloat("FlN", null);
    assertFloatMergeAgreesWithMatch("FlN");
  }

  @Test
  void mergeOnAFloatPropertyWithANotUniqueIndexMatchesTheExistingNode() {
    loadFloat("FlI", "NOTUNIQUE");
    assertFloatMergeAgreesWithMatch("FlI");
  }

  @Test
  void mergeOnAUniqueFloatPropertyDoesNotDieOnADuplicatedKey() {
    loadFloat("FlU", "UNIQUE");
    assertFloatMergeAgreesWithMatch("FlU");
  }

  @Test
  void mergeOnAFloatPropertyStillCreatesForADifferentValue() {
    loadFloat("FlD", null);
    assertThat(count("MATCH (n:FlD {v: 0.2}) RETURN count(n) AS c")).isEqualTo(0L);
    assertThat(merge("MERGE (n:FlD {v: 0.2}) ON CREATE SET n.tag = 'CREATED' ON MATCH SET n.tag = 'MATCHED' RETURN n.tag AS tag"))
        .isEqualTo("CREATED");
    assertThat(count("MATCH (n:FlD) RETURN count(n) AS c")).isEqualTo(2L);
  }

  @Test
  void mergeOnAFloatRelationshipPropertyMatchesTheExistingEdge() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE PROPERTY R.w FLOAT");
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})-[:R {w: 0.1}]->(:A {id: 2})").close());
    final String pattern = "(a:A {id: 1})-[r:R {w: 0.1}]->(b:A {id: 2})";
    assertThat(count("MATCH " + pattern + " RETURN count(r) AS c")).isEqualTo(1L);
    database.transaction(() -> database.command("opencypher", "MATCH (a:A {id: 1}), (b:A {id: 2}) MERGE (a)-[r:R {w: 0.1}]->(b)").close());
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void mergeRelationshipProcedureMatchesAStoredFloatEdgeProperty() {
    database.command("sql", "CREATE VERTEX TYPE PA");
    database.command("sql", "CREATE EDGE TYPE PR");
    database.command("sql", "CREATE PROPERTY PR.w FLOAT");
    database.transaction(() -> database.command("opencypher", "CREATE (:PA {id: 1})-[:PR {w: 0.1, n: 7}]->(:PA {id: 2})").close());
    assertThat(count("MATCH (:PA {id: 1})-[r:PR {w: 0.1, n: 7}]->(:PA {id: 2}) RETURN count(r) AS c")).isEqualTo(1L);
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:PA {id: 1}), (b:PA {id: 2}) CALL merge.relationship(a, 'PR', {w: 0.1, n: 7}, {}, b) YIELD rel RETURN rel").close());
    assertThat(count("MATCH ()-[r:PR]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void mergeNodeProcedureMatchesAStoredFloatProperty() {
    loadFloat("FlP", null);
    database.transaction(() -> database.command("opencypher", "CALL merge.node(['FlP'], {v: 0.1}) YIELD node RETURN node").close());
    assertThat(count("MATCH (n:FlP) RETURN count(n) AS c")).isEqualTo(1L);
  }

  // ------------------------------------------------------------------------------------- temporal vs declared STRING

  /**
   * A declared STRING property holds the operand's text, which a temporal never equals in a MATCH (inline or
   * {@code WHERE}): MERGE must not see a node there either, so ON MATCH SET cannot fire on a node no MATCH finds.
   */
  @ParameterizedTest
  @ValueSource(strings = { "datetime('2021-06-15T12:30:00Z')", "localdatetime('2021-06-15T12:30:00')", "date('2021-06-15')",
      "time('12:30:00Z')", "localtime('12:30:00')", "duration('P1DT2H')" })
  void mergeAndMatchAgreeOnATemporalAgainstADeclaredStringProperty(final String operand) {
    for (final String index : new String[] { null, "NOTUNIQUE" }) {
      final String type = index == null ? "SN" : "SI";
      if (!database.getSchema().existsType(type)) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".d STRING");
        if (index != null)
          database.command("sql", "CREATE INDEX ON " + type + " (d) " + index);
      }
      database.transaction(() -> database.command("opencypher", "MATCH (n:" + type + ") DELETE n").close());
      database.transaction(() -> database.command("opencypher", "CREATE (:" + type + " {d: " + operand + ", tag: 'orig'})").close());

      final String described = type + " " + operand;
      // Both MATCH spellings: a temporal never equals stored text. Pinned, so a change to MATCH fails here rather than
      // silently moving MERGE along with it (that decision is #9695)
      assertThat(count("MATCH (p:" + type + " {d: " + operand + "}) RETURN count(p) AS c")).as(described + " inline MATCH")
          .isEqualTo(0L);
      assertThat(count("MATCH (p:" + type + ") WHERE p.d = " + operand + " RETURN count(p) AS c")).as(described + " WHERE MATCH")
          .isEqualTo(0L);

      final String tag = merge("MERGE (p:" + type + " {d: " + operand + "}) ON MATCH SET p.tag = 'MATCHED' ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag");
      assertThat(tag).as(described + " MERGE takes the branch the MATCH implies").isEqualTo("CREATED");
      assertThat(count("MATCH (p:" + type + ") RETURN count(p) AS c")).as(described + " total").isEqualTo(2L);
      assertThat(count("MATCH (p:" + type + " {tag: 'orig'}) RETURN count(p) AS c")).as(described + " original node untouched")
          .isEqualTo(1L);
    }
  }

  @Test
  void aTemporalAgainstADeclaredStringPropertyMatchesInNeitherClause() {
    database.command("sql", "CREATE VERTEX TYPE SM");
    database.command("sql", "CREATE PROPERTY SM.d STRING");
    database.transaction(() -> database.command("opencypher", "CREATE (:SM {d: datetime('2021-06-15T12:30:00Z'), tag: 'orig'})").close());
    assertThat(count("MATCH (p:SM {d: datetime('2021-06-15T12:30:00Z')}) RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(merge("MERGE (p:SM {d: datetime('2021-06-15T12:30:00Z')}) ON MATCH SET p.tag = 'MATCHED' ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag"))
        .isEqualTo("CREATED");
    assertThat(count("MATCH (p:SM {tag: 'MATCHED'}) RETURN count(p) AS c")).isEqualTo(0L);
  }

  @Test
  void aTemporalParameterAgainstADeclaredStringPropertyAgreesToo() {
    database.command("sql", "CREATE VERTEX TYPE SP");
    database.command("sql", "CREATE PROPERTY SP.d STRING");
    database.command("sql", "CREATE INDEX ON SP (d) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:SP {d: datetime('2021-06-15T12:30:00Z')})").close());
    final Map<String, Object> params = Map.of("d", ZonedDateTime.parse("2021-06-15T12:30:00Z"));
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:SP {d: $d}) RETURN count(p) AS c", params)) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
    }
    database.transaction(() -> database.command("opencypher", "MERGE (p:SP {d: $d})", params).close());
    assertThat(count("MATCH (p:SP) RETURN count(p) AS c")).isEqualTo(2L);
  }

  @Test
  void aCompositeIndexWithATemporalOnItsStringKeyAgreesWithMatch() {
    database.command("sql", "CREATE VERTEX TYPE SX");
    database.command("sql", "CREATE PROPERTY SX.k INTEGER");
    database.command("sql", "CREATE PROPERTY SX.d STRING");
    database.command("sql", "CREATE INDEX ON SX (k, d) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:SX {k: 1, d: date('2021-06-15'), tag: 'orig'})").close());
    assertThat(count("MATCH (p:SX {k: 1, d: date('2021-06-15')}) RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(merge("MERGE (p:SX {k: 1, d: date('2021-06-15')}) ON MATCH SET p.tag = 'MATCHED' ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag"))
        .isEqualTo("CREATED");
    // The text operand still finds the node through the same composite index
    assertThat(merge("MERGE (p:SX {k: 1, d: '2021-06-15'}) ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag")).isEqualTo("MATCHED");
    assertThat(count("MATCH (p:SX) RETURN count(p) AS c")).isEqualTo(2L);
  }

  @Test
  void anotherRenderingOfTheInstantMatchesInNeitherClause() {
    database.command("sql", "CREATE VERTEX TYPE SR");
    database.command("sql", "CREATE PROPERTY SR.d STRING");
    database.transaction(() -> database.command("opencypher", "CREATE (:SR {d: '2021-06-15T12:30:00.000Z'})").close());
    assertThat(count("MATCH (p:SR {d: datetime('2021-06-15T12:30:00Z')}) RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(merge("MERGE (p:SR {d: datetime('2021-06-15T12:30:00Z')}) ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag"))
        .isEqualTo("CREATED");
    assertThat(count("MATCH (p:SR) RETURN count(p) AS c")).isEqualTo(2L);
  }
}
