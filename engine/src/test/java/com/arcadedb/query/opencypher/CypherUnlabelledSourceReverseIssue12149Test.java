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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12149: a single hop from an unlabelled, unconstrained first node to a node a UNIQUE index pins used to scan every vertex
 * of every type (once per row after an UNWIND), expand all of them, and filter on the pinned node last. The step-based plan now
 * starts from the pinned node. Each test asserts which node the plan starts from (the first MATCH NODE step, and the rows it read
 * under PROFILE) and the answer, compared with the same pattern written from the pinned end.
 */
class CypherUnlabelledSourceReverseIssue12149Test {
  private static final int ITEMS = 60;

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-unlabelled-source-reverse-12149").create();
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.key INTEGER");
    database.command("sql", "CREATE INDEX ON Doc (key) UNIQUE");
    database.command("sql", "CREATE PROPERTY Doc.grp INTEGER");
    database.command("sql", "CREATE INDEX ON Doc (grp) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Item");
    database.command("sql", "CREATE VERTEX TYPE Other");
    database.command("sql", "CREATE EDGE TYPE OF");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, 9) AS i CREATE (:Doc {key: i, grp: i % 2})");
      database.command("opencypher",
          "UNWIND range(1, $n) AS i MATCH (d:Doc {key: i % 10}) CREATE (:Item {i: i})-[:OF]->(d), (:Other {i: -i})-[:U]->(d)",
          Map.of("n", ITEMS));
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void anInListOnTheUniqueKeyStartsFromTheTarget() {
    final String query = "MATCH (x)-[:OF]->(d:Doc) WHERE d.key IN $keys RETURN x.i AS i";
    final Map<String, Object> params = Map.of("keys", List.of(3, 4));

    assertStartsFrom(query, params, "MATCH NODE (d:Doc)", 2);
    assertThat(ints(query, params)).hasSize(12)
        .containsExactlyInAnyOrderElementsOf(ints("MATCH (d:Doc)<-[:OF]-(x) WHERE d.key IN $keys RETURN x.i AS i", params));
  }

  @Test
  void anInlineKeyStartsFromTheTarget() {
    final String query = "MATCH (x)-[:OF]->(d:Doc {key: 7}) RETURN x.i AS i";

    assertStartsFrom(query, Map.of(), "MATCH NODE (d:Doc)", 1);
    assertThat(ints(query, Map.of())).containsExactlyInAnyOrder(7, 17, 27, 37, 47, 57);
  }

  @Test
  void aKeyFromAnUnwindRowStartsFromTheTargetOncePerRow() {
    final String query = "UNWIND $keys AS k MATCH (x)-[:OF]->(:Doc {key: k}) RETURN x.i AS i";
    final Map<String, Object> params = Map.of("keys", List.of(1, 2, 99));

    final String plan = profile(query, params);
    assertThat(plan.indexOf(":Doc)")).as(plan).isLessThan(plan.indexOf("MATCH RELATIONSHIP"));
    assertThat(lineOf(plan, ":Doc)")).as(plan).contains("[index:").contains("2 rows");
    assertThat(ints(query, params)).hasSize(12)
        .containsExactlyInAnyOrderElementsOf(ints("UNWIND $keys AS k MATCH (:Doc {key: k})<-[:OF]-(x) RETURN x.i AS i", params));

    final String withRow = "WITH 5 AS k MATCH (x)-[r:OF]->(:Doc {key: k}) RETURN x.i AS i";
    assertThat(lineOf(profile(withRow, Map.of()), "MATCH NODE")).contains(":Doc)");
    assertThat(ints(withRow, Map.of())).containsExactlyInAnyOrder(5, 15, 25, 35, 45, 55);
  }

  @Test
  void anIncomingArrowIsWalkedTheOtherWay() {
    // the hop is written against the stored direction: no edge goes from a Doc to an Item
    final String query = "MATCH (x)<-[:OF]-(d:Doc {key: 2}) RETURN x.i AS i";
    assertStartsFrom(query, Map.of(), "MATCH NODE (d:Doc)", 1);
    assertThat(ints(query, Map.of())).isEmpty();
  }

  @Test
  void anythingElseKeepsTheWrittenOrder() {
    // a NOTUNIQUE key, a unidirectional type, an untyped hop, an OPTIONAL MATCH
    final List<String> queries = List.of(
        "MATCH (x)-[:OF]->(d:Doc {grp: 1}) RETURN x.i AS i",
        "MATCH (x)-[:U]->(d:Doc {key: 1}) RETURN x.i AS i",
        "MATCH (x)-->(d:Doc {key: 1}) RETURN x.i AS i",
        "OPTIONAL MATCH (x)-[:OF]->(d:Doc {key: 1}) RETURN x.i AS i");
    for (final String query : queries)
      assertThat(lineOf(profile(query, Map.of()), "MATCH NODE")).as(query).contains("MATCH NODE (x");

    assertThat(ints("MATCH (x)-[:U]->(d:Doc {key: 1}) RETURN x.i AS i", Map.of())).hasSize(6);
    assertThat(ints("MATCH (x)-[:OF]->(d:Doc {grp: 1}) RETURN x.i AS i", Map.of())).hasSize(30);
  }

  private void assertStartsFrom(final String query, final Map<String, Object> params, final String firstNodeStep,
      final int rowsRead) {
    final String plan = profile(query, params);
    assertThat(lineOf(plan, "MATCH NODE")).as(plan).contains(firstNodeStep).contains("[index:").contains(rowsRead + " rows");
    assertThat(plan.indexOf(firstNodeStep)).as(plan).isLessThan(plan.indexOf("MATCH RELATIONSHIP"));
  }

  private static String lineOf(final String plan, final String text) {
    for (final String line : plan.split("\n"))
      if (line.contains(text))
        return line;
    return "";
  }

  private List<Integer> ints(final String query, final Map<String, Object> params) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("i")).intValue());
    }
    return values;
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
