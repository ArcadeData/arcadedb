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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13621: a fixed-length pattern of more than one hop whose first node is unbound and whose last node an earlier clause bound
 * (LDBC SNB Interactive IC5: {@code OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)} after a {@code WITH forum,
 * ...}) used to start from a scan of every vertex for each input row and expand all of them. The step-based plan now walks the
 * pattern back to front from the bound node. Each test checks the plan's starting node and the rows, compared with the same
 * pattern written from the bound end.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherMultiHopBoundTargetReverseIssue13621Test {
  private static final String PATH    = "./target/databases/cypher-multihop-bound-target-reverse-13621";
  private static final int    PERSONS = 20;
  private static final int    FORUMS  = 5;
  private static final int    MEMBERS = 3;
  private static final int    POSTS   = 60;

  private static final String IC5 = """
      MATCH (forum:Forum)-[:HAS_MEMBER]->(friend:Person)
      WITH forum, collect(friend) AS friends
      OPTIONAL MATCH %s
      WHERE friend IN friends
      WITH forum, count(post) AS postCount
      RETURN forum.id AS id, postCount ORDER BY id""";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Forum");
    database.command("sql", "CREATE VERTEX TYPE Post");
    database.command("sql", "CREATE EDGE TYPE HAS_MEMBER");
    database.command("sql", "CREATE EDGE TYPE CONTAINER_OF");
    database.command("sql", "CREATE EDGE TYPE HAS_CREATOR");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, $p - 1) AS i CREATE (:Person {id: i})", Map.of("p", PERSONS));
      database.command("opencypher", "UNWIND range(0, $f - 1) AS i CREATE (:Forum {id: i})", Map.of("f", FORUMS));
      database.command("opencypher", "UNWIND range(0, $f - 1) AS i UNWIND range(0, $m - 1) AS j "
          + "MATCH (f:Forum {id: i}), (p:Person {id: (i * 3 + j) % $p}) CREATE (f)-[:HAS_MEMBER]->(p)",
          Map.of("f", FORUMS, "m", MEMBERS, "p", PERSONS));
      // the last forum holds no post: its OPTIONAL MATCH finds nothing
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i MATCH (f:Forum {id: i % ($f - 1)}), (p:Person {id: (i * 7) % $p}) "
          + "CREATE (f)-[:CONTAINER_OF]->(:Post {id: i})-[:HAS_CREATOR]->(p)", Map.of("n", POSTS, "f", FORUMS, "p", PERSONS));
      database.command("opencypher", "UNWIND range(0, $p - 2) AS i MATCH (a:Person {id: i}), (b:Person {id: i + 1}) CREATE (a)-[:KNOWS]->(b)",
          Map.of("p", PERSONS));
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
  void theIc5PatternStartsFromTheBoundForum() {
    final String query = IC5.formatted("(friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)");

    final String plan = profile(query);
    assertThat(plan).as(plan).doesNotContain("MATCH NODE (friend").contains("OPTIONAL MATCH");
    assertThat(optionalSection(plan)).as(plan).contains("MATCH NODE (forum");

    final List<String> rows = rows(query, "id", "postCount");
    assertThat(rows).isEqualTo(expectedPostCounts());
    assertThat(rows).isEqualTo(rows(IC5.formatted("(forum)-[:CONTAINER_OF]->(post)-[:HAS_CREATOR]->(friend)"), "id", "postCount"));
    // the forum without posts keeps its row, with a count of zero
    assertThat(rows).contains((FORUMS - 1) + ":0");
  }

  @Test
  void anOptionalMatchFindingNothingBindsEveryNewVariableToNull() {
    final String query = """
        MATCH (forum:Forum {id: $id})
        OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
        RETURN forum.id AS id, friend IS NULL AS noFriend, post IS NULL AS noPost""";
    try (final ResultSet rs = database.query("opencypher", query, Map.of("id", FORUMS - 1))) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("id")).intValue()).isEqualTo(FORUMS - 1);
      assertThat(row.<Boolean>getProperty("noFriend")).isTrue();
      assertThat(row.<Boolean>getProperty("noPost")).isTrue();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void undirectedAndMixedHopsReadTheSameRowsAsTheBoundEnd() {
    assertThat(rows(IC5.formatted("(friend)-[:HAS_CREATOR]-(post)-[:CONTAINER_OF]-(forum)"), "id", "postCount"))
        .isEqualTo(expectedPostCounts());
    assertThat(rows(IC5.formatted("(friend)-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)"), "id", "postCount"))
        .isEqualTo(expectedPostCounts());
    // a direction that does not match any edge still finds nothing
    assertThat(rows(IC5.formatted("(friend)-[:HAS_CREATOR]->(post)<-[:CONTAINER_OF]-(forum)"), "id", "postCount"))
        .allMatch(row -> row.endsWith(":0")).hasSize(FORUMS);
  }

  @Test
  void relationshipVariablesKeepTheirEndpoints() {
    final String query = """
        MATCH (forum:Forum {id: 0})
        MATCH (friend)<-[c:HAS_CREATOR]-(post)<-[o:CONTAINER_OF]-(forum)
        RETURN post.id AS id, startNode(c) = post AND endNode(c) = friend AS creator, startNode(o) = forum AND endNode(o) = post AS container
        ORDER BY id""";
    assertThat(profile(query)).doesNotContain("MATCH NODE (friend");
    final List<String> rows = rows(query, "id", "creator", "container");
    assertThat(rows).hasSize(POSTS / (FORUMS - 1)).allMatch(row -> row.endsWith(":true:true"));
  }

  @Test
  void aNamedPathKeepsTheWrittenOrder() {
    final String query = """
        MATCH (forum:Forum {id: 0})
        MATCH p = (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
        RETURN post.id AS id, head(nodes(p)) = friend AND last(nodes(p)) = forum AND nodes(p)[1] = post AS ordered
        ORDER BY id""";
    assertThat(profile(query)).contains("MATCH NODE (friend");
    final List<String> rows = rows(query, "id", "ordered");
    assertThat(rows).hasSize(POSTS / (FORUMS - 1)).allMatch(row -> row.endsWith(":true"));
  }

  @Test
  void relationshipUniquenessHoldsAcrossTheReversedHops() {
    // a chain 0 -> 1 -> ... -> 19: two hops from person 0 reach person 2 only, the walk back over the same KNOWS edge is not a match
    final String query = """
        MATCH (p:Person {id: 0})
        WITH p
        MATCH (a)-[:KNOWS]-(b)-[:KNOWS]-(p)
        RETURN a.id AS a, b.id AS b""";
    assertThat(profile(query)).doesNotContain("MATCH NODE (a");
    assertThat(rows(query, "a", "b")).containsExactly("2:1");

    final String middle = """
        MATCH (p:Person {id: 5})
        WITH p
        MATCH (a)-[:KNOWS]-(b)-[:KNOWS]-(p)
        RETURN a.id AS a, b.id AS b ORDER BY a""";
    assertThat(rows(middle, "a", "b")).containsExactly("3:4", "7:6");
  }

  @Test
  void aSecondPatternJoinsTheReversedOne() {
    final String query = """
        MATCH (p:Person {id: 3})
        WITH p
        MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(p), (x)-[:KNOWS]->(a)
        RETURN a.id AS a, b.id AS b, x.id AS x""";
    assertThat(rows(query, "a", "b", "x")).containsExactly("1:2:0");

    final String written = """
        MATCH (p:Person {id: 3})
        WITH p
        MATCH (p)<-[:KNOWS]-(b)<-[:KNOWS]-(a), (x)-[:KNOWS]->(a)
        RETURN a.id AS a, b.id AS b, x.id AS x""";
    assertThat(rows(query, "a", "b", "x")).isEqualTo(rows(written, "a", "b", "x"));
  }

  private List<String> expectedPostCounts() {
    final List<String> expected = new ArrayList<>();
    for (int f = 0; f < FORUMS; f++) {
      int count = 0;
      for (int i = 0; i < POSTS; i++) {
        if (i % (FORUMS - 1) != f)
          continue;
        final int creator = (i * 7) % PERSONS;
        for (int j = 0; j < MEMBERS; j++)
          if ((f * 3 + j) % PERSONS == creator) {
            count++;
            break;
          }
      }
      expected.add(f + ":" + count);
    }
    return expected;
  }

  private static String optionalSection(final String plan) {
    final int start = plan.indexOf("OPTIONAL MATCH");
    return start < 0 ? "" : plan.substring(start);
  }

  private List<String> rows(final String query, final String... columns) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder value = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
          if (i > 0)
            value.append(':');
          value.append(String.valueOf((Object) row.getProperty(columns[i])));
        }
        rows.add(value.toString());
      }
    }
    return rows;
  }

  private String profile(final String query) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
