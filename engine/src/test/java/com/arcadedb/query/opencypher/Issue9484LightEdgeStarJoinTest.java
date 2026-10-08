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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9484: a two-arm star {@code count(*)} answered 0 when the first edge type held only light edges, because the
 * out-of-view fallback of the degree product built each vertex's degree from the records of the edge type, and a light
 * edge has no record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9484LightEdgeStarJoinTest extends TestHelper {
  private static final String TWO_MATCH  = "MATCH (s:S)-[:E1]->(t:T) MATCH (s)-[:E2]->(u:U) RETURN count(*) AS n";
  private static final String COMMA      = "MATCH (s:S)-[:E1]->(t:T), (s)-[:E2]->(u:U) RETURN count(*) AS n";
  private static final String CHAIN      = "MATCH (t:T)<-[:E1]-(s:S)-[:E2]->(u:U) RETURN count(*) AS n";
  private static final String WITH       = "MATCH (s:S)-[:E1]->(t:T) WITH s, t MATCH (s)-[:E2]->(u:U) RETURN count(*) AS n";
  private static final String SECOND_ARM = "MATCH (s:S)-[:E2]->(u:U) MATCH (s)-[:E1]->(t:T) RETURN count(*) AS n";
  private static final String REVERSED   = "MATCH (t:T)<-[:E1]-(s:S) MATCH (u:U)<-[:E2]-(s) RETURN count(*) AS n";

  private enum Kind {REGULAR, LIGHT}

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE S");
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE VERTEX TYPE U");
  }

  @Test
  void regularEdges() {
    build("", Kind.REGULAR);
    assertEveryFormCountsThree();
  }

  @Test
  void declaredLightweightType() {
    build(" LIGHTWEIGHT", Kind.LIGHT);
    assertEveryFormCountsThree();
  }

  @Test
  void lightEdgesInUndeclaredType() {
    build("", Kind.LIGHT);
    assertEveryFormCountsThree();
  }

  @Test
  void lightEdgesAfterReopen() {
    build(" LIGHTWEIGHT", Kind.LIGHT);
    reopenDatabase();
    assertEveryFormCountsThree();
  }

  @Test
  void lightEdgesMixedWithRecordEdgesOfTheSameType() {
    database.command("sql", "CREATE EDGE TYPE E1");
    database.command("sql", "CREATE EDGE TYPE E2");
    database.transaction(() -> {
      for (int i = 0; i < 4; i++) {
        final MutableVertex s = database.newVertex("S").set("id", i).save();
        final MutableVertex t = database.newVertex("T").set("id", i).save();
        final MutableVertex u = database.newVertex("U").set("id", i).save();
        if (i % 2 == 0)
          s.newEdge("E1", t);
        else
          s.newLightEdge("E1", t);
        s.newEdge("E2", u);
      }
    });
    assertThat(count(TWO_MATCH)).isEqualTo(4L);
    assertThat(count(CHAIN)).isEqualTo(4L);
  }

  @Test
  void aVertexWithSeveralLightEdgesMultipliesTheArms() {
    database.command("sql", "CREATE EDGE TYPE E1 LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE E2 LIGHTWEIGHT");
    database.transaction(() -> {
      final MutableVertex s = database.newVertex("S").save();
      for (int i = 0; i < 3; i++)
        s.newLightEdge("E1", database.newVertex("T").save());
      for (int i = 0; i < 2; i++)
        s.newLightEdge("E2", database.newVertex("U").save());
      // a second S vertex with only one arm contributes nothing
      database.newVertex("S").save().newLightEdge("E1", database.newVertex("T").save());
    });
    assertThat(count(TWO_MATCH)).isEqualTo(6L);
    assertThat(count(CHAIN)).isEqualTo(6L);
    assertThat(count(WITH)).isEqualTo(6L);
  }

  @Test
  void aMultiHopArmOverLightEdges() {
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE EDGE TYPE E1 LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE E3 LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE E2");
    database.transaction(() -> {
      final MutableVertex s = database.newVertex("S").save();
      // 2 paths s -> t -> w on the first arm (one t with 2 w, one t with 1 w) and 2 edges on the second arm
      final MutableVertex t1 = database.newVertex("T").save();
      final MutableVertex t2 = database.newVertex("T").save();
      s.newLightEdge("E1", t1);
      s.newLightEdge("E1", t2);
      t1.newLightEdge("E3", database.newVertex("W").save());
      t1.newLightEdge("E3", database.newVertex("W").save());
      t2.newLightEdge("E3", database.newVertex("W").save());
      s.newEdge("E2", database.newVertex("U").save());
      s.newEdge("E2", database.newVertex("U").save());
    });
    assertThat(count("MATCH (s:S)-[:E1]->(t:T)-[:E3]->(w:W), (s)-[:E2]->(u:U) RETURN count(*) AS n")).isEqualTo(6L);
    assertThat(count("MATCH (s:S)-[:E1]->(t:T)-[:E3]->(w:W) WITH s, t, w MATCH (s)-[:E2]->(u:U) RETURN count(*) AS n")).isEqualTo(6L);
  }

  private void build(final String e1Modifier, final Kind kind) {
    database.command("sql", "CREATE EDGE TYPE E1" + e1Modifier);
    database.command("sql", "CREATE EDGE TYPE E2");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++) {
        final MutableVertex s = database.newVertex("S").set("id", i).save();
        final MutableVertex t = database.newVertex("T").set("id", i).save();
        final MutableVertex u = database.newVertex("U").set("id", i).save();
        if (kind == Kind.REGULAR)
          s.newEdge("E1", t);
        else
          s.newLightEdge("E1", t);
        s.newEdge("E2", u);
      }
    });
  }

  private void assertEveryFormCountsThree() {
    for (final String query : new String[] { TWO_MATCH, COMMA, CHAIN, WITH, SECOND_ARM, REVERSED })
      assertThat(count(query)).as(query).isEqualTo(3L);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
