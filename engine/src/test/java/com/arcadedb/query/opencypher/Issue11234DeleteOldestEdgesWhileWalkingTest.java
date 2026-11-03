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
import com.arcadedb.graph.StripedEdgeList;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11234: one {@code MATCH (a)-[e]->(b) ... DELETE e} statement that deletes many of the OLDEST edges of a vertex failed with
 * {@code RecordNotFoundException}: the statement emptied a segment of the edge list, the removal deleted that segment, and a walk
 * of the same statement that was still streaming then stepped onto it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11234DeleteOldestEdgesWhileWalkingTest extends TestHelper {

  private static final String DELETE = """
      UNWIND range($from, $to) AS i
      MATCH (a:Entity {external_id: 'HUB'})-[e:RELATES_TO]->(b:Entity {external_id: 'E' + i})
      DELETE e RETURN b.external_id AS b""";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE PROPERTY Entity.external_id STRING");
    database.command("sql", "CREATE INDEX ON Entity (external_id) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE RELATES_TO");
  }

  @Test
  void deletingTheOldestEdgesInOneStatementDeletesThemAll() {
    buildHub(3000);

    assertThat(deleteRange(0, 99)).isEqualTo(100);
    assertThat(hubDegree()).isEqualTo(2900);
    assertThat(hubDegreeTo("E0")).isZero();
    assertThat(hubDegreeTo("E99")).isZero();
    assertThat(hubDegreeTo("E100")).isEqualTo(1);

    assertThat(deleteRange(100, 1099)).isEqualTo(1000);
    assertThat(hubDegree()).isEqualTo(1900);
    assertThat(checkDatabaseErrors()).isZero();
  }

  @Test
  void aSmallerHubFailsTheSameWay() {
    buildHub(300);

    assertThat(deleteRange(0, 99)).isEqualTo(100);
    assertThat(hubDegree()).isEqualTo(200);
    assertThat(checkDatabaseErrors()).isZero();
  }

  @Test
  void deletingEveryEdgeOldestFirstEmptiesTheList() {
    buildHub(3000);

    assertThat(deleteRange(0, 2999)).isEqualTo(3000);
    assertThat(hubDegree()).isZero();
    assertThat(checkDatabaseErrors()).isZero();

    // THE LIST IS STILL USABLE AFTERWARDS
    database.command("opencypher", "MATCH (a:Entity {external_id: 'HUB'}), (b:Entity {external_id: 'E5'}) CREATE (a)-[:RELATES_TO]->(b)");
    assertThat(hubDegree()).isEqualTo(1);
  }

  @Test
  void aSuperNodeHubFailsTheSameWay() {
    // THE HUB IS PROMOTED TO THE STRIPED LAYOUT HALF WAY: THE OLDEST EDGES STAY ON THE CLASSIC CHAIN, THE NEWEST GO TO THE STRIPES
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 256);
    buildHub(3000);
    assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Entity", 0))).isTrue();

    assertThat(deleteRange(0, 999)).isEqualTo(1000);
    assertThat(hubDegree()).isEqualTo(2000);
    assertThat(deleteRange(1000, 2999)).isEqualTo(2000);
    assertThat(hubDegree()).isZero();
    assertThat(checkDatabaseErrors()).isZero();
  }

  @Test
  void aStatementTakenBackLeavesNoStaleRelinkForTheNextOne() {
    buildHub(3000);

    database.transaction(() -> {
      // THE FIRST STATEMENT EMPTIES AND DELETES THE OLD SEGMENTS, THEN FAILS: ONLY IT IS TAKEN BACK, THE TRANSACTION GOES ON
      assertThatThrownBy(() -> database.command("opencypher", """
          UNWIND range(0, 999) AS i
          MATCH (a:Entity {external_id: 'HUB'})-[e:RELATES_TO]->(b:Entity {external_id: 'E' + i})
          DELETE e WITH count(*) AS n RETURN 1 / (n - n) AS boom""").stream().count()).hasMessageContaining("/ by zero");
      assertThat(hubDegree()).isEqualTo(3000);

      // THE SAME SEGMENTS ARE EMPTIED AND DELETED AGAIN, BY A STATEMENT THAT STREAMS ITS MATCH INTO ITS DELETE
      assertThat(deleteRange(0, 999)).isEqualTo(1000);
      assertThat(hubDegree()).isEqualTo(2000);
      assertThat(deleteRange(1000, 1999)).isEqualTo(1000);
      assertThat(hubDegree()).isEqualTo(1000);
    });
    assertThat(hubDegree()).isEqualTo(1000);
    assertThat(checkDatabaseErrors()).isZero();
  }

  @Test
  void deletingTheNewestEdgesStillWorks() {
    buildHub(3000);

    assertThat(deleteRange(2500, 2999)).isEqualTo(500);
    assertThat(hubDegree()).isEqualTo(2500);
    assertThat(checkDatabaseErrors()).isZero();
  }

  private void buildHub(final int edges) {
    database.command("opencypher", "CREATE (:Entity {external_id: 'HUB'})");
    database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:Entity {external_id: 'E' + i})", Map.of("n", edges));
    for (int from = 0; from < edges; from += 500)
      database.command("opencypher", """
          UNWIND range($from, $to) AS i
          MATCH (a:Entity {external_id: 'HUB'}) MATCH (b:Entity {external_id: 'E' + i})
          CREATE (a)-[:RELATES_TO]->(b)""", Map.of("from", from, "to", Math.min(from + 499, edges - 1)));
    assertThat(hubDegree()).isEqualTo(edges);
  }

  private long deleteRange(final int from, final int to) {
    try (final ResultSet rs = database.command("opencypher", DELETE, Map.of("from", from, "to", to))) {
      return rs.stream().count();
    }
  }

  private long hubDegree() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (:Entity {external_id: 'HUB'})-[e:RELATES_TO]->() RETURN count(e) AS n")) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private long hubDegreeTo(final String target) {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:Entity {external_id: 'HUB'})-[e:RELATES_TO]->(:Entity {external_id: $t}) RETURN count(e) AS n", Map.of("t", target))) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private long checkDatabaseErrors() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      return ((Number) rs.next().getProperty("totalErrors")).longValue();
    }
  }
}
