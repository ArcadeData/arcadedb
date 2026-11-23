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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13267: a hop from a labelled node to a node an index pins, after an UNWIND or a WITH, scanned the whole label of the first
 * node for every row and filtered on the pinned node last, where the same pattern written from the pinned end is a seek. The
 * step-based plan now starts from the pinned node when its seek finds fewer vertices than the label of the first node has. The
 * answer is the same either way; each test asserts which node the plan starts from (the first MATCH NODE step) and the rows.
 */
class CypherLabelledSourceReverseIssue13267Test {
  private static final String PATH     = "./target/databases/cypher-labelled-source-reverse-13267";
  private static final int    ENTITIES = 120;
  private static final int    EPISODES = 30;
  /** An id twice, one no entity has, a null, the first and the last entity */
  private static final List<Object> IDS = Arrays.asList("ent3", "ent3", "missing", null, "ent0", "ent119");

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE PROPERTY Entity.uuid STRING");
    database.command("sql", "CREATE INDEX ON Entity (uuid) NOTUNIQUE");
    database.command("sql", "CREATE PROPERTY Entity.grp INTEGER");
    database.command("sql", "CREATE INDEX ON Entity (grp) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.key STRING");
    database.command("sql", "CREATE INDEX ON Doc (key) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Episodic");
    database.command("sql", "CREATE PROPERTY Episodic.uuid STRING");
    database.command("sql", "CREATE INDEX ON Episodic (uuid) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Other");
    database.command("sql", "CREATE VERTEX TYPE Tiny");
    database.command("sql", "CREATE VERTEX TYPE Lonely");
    database.command("sql", "CREATE EDGE TYPE MENTIONS");
    database.command("sql", "CREATE EDGE TYPE ONEWAY UNIDIRECTIONAL");
    database.transaction(() -> {
      // 120 entities, uuid unique in fact but declared NOTUNIQUE; grp has only 2 values; every tenth entity is also a Person
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:Entity {uuid: 'ent' + i, grp: i % 2, name: 'n' + i})", Map.of("n", ENTITIES));
      database.command("opencypher", "MATCH (e:Entity) WHERE e.grp = 0 AND toInteger(substring(e.uuid, 3)) % 10 = 0 SET e:Person");
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:Episodic {uuid: 'ep' + i})", Map.of("n", EPISODES));
      database.command("opencypher", "UNWIND range(0, 9) AS i CREATE (:Other {uuid: 'ot' + i})");
      database.command("opencypher", "UNWIND range(0, 99) AS i CREATE (:Doc {key: 'doc' + i})");
      database.command("opencypher", "UNWIND range(0, 2) AS i CREATE (:Tiny {uuid: 'ti' + i})");
      database.command("opencypher", "CREATE (:Lonely {uuid: 'lone'})");
      // episode j mentions entities 3j and 3j+1, and 3j again through a second edge; every Other vertex mentions entity 0
      database.command("opencypher",
          "UNWIND range(0, $n - 1) AS j MATCH (p:Episodic {uuid: 'ep' + j}), (a:Entity {uuid: 'ent' + (3 * j)}), (b:Entity {uuid: 'ent' + (3 * j + 1)}) "
              + "CREATE (p)-[:MENTIONS]->(a), (p)-[:MENTIONS]->(b), (p)-[:MENTIONS]->(a)", Map.of("n", EPISODES));
      database.command("opencypher", "MATCH (o:Other), (a:Entity {uuid: 'ent0'}) CREATE (o)-[:MENTIONS]->(a)");
      database.command("opencypher", "MATCH (t:Tiny), (d:Doc {key: 'doc7'}) CREATE (t)-[:MENTIONS]->(d)");
      database.command("opencypher", "MATCH (t:Tiny), (e:Entity) WHERE e.grp = 1 CREATE (t)-[:MENTIONS]->(e)");
      database.command("opencypher", "MATCH (l:Lonely), (d:Doc {key: 'doc7'}) CREATE (l)-[:MENTIONS]->(d)");
      database.command("opencypher", "MATCH (e:Episodic {uuid: 'ep1'}), (a:Entity {uuid: 'ent5'}) CREATE (e)-[:ONEWAY]->(a)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /** What the pattern answers for the ids of {@link #IDS}, worked out from how the graph was built. */
  private static List<String> expectedMentions() {
    final List<String> rows = new ArrayList<>();
    for (final Object id : IDS) {
      if (id == null)
        continue;
      for (int j = 0; j < EPISODES; j++)
        for (final int entity : new int[] { 3 * j, 3 * j + 1, 3 * j })
          if (("ent" + entity).equals(id))
            rows.add(id + "|ep" + j);
    }
    return rows;
  }

  @Test
  void aNotUniqueIndexPinnedByAnUnwindRowStartsFromTheIndexedEnd() {
    final String query = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN id + '|' + e.uuid AS v";
    final Map<String, Object> params = Map.of("ids", IDS);

    assertStartsFrom(query, params, "(n:Entity");
    assertThat(values(query, params)).isNotEmpty().containsExactlyInAnyOrderElementsOf(expectedMentions());
    assertThat(values(query, params)).containsExactlyInAnyOrderElementsOf(
        values("UNWIND $ids AS id MATCH (n:Entity {uuid: id})<-[r:MENTIONS]-(e:Episodic) RETURN id + '|' + e.uuid AS v", params));
  }

  @Test
  void theCountPerEntityOfGraphitisEpisodeMentionsIsTheSameFromEitherEnd() {
    final String shape = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS score, n.uuid AS uuid";
    final Map<String, Object> params = Map.of("ids", IDS);

    assertStartsFrom(shape, params, "(n:Entity");
    final Map<String, Long> counts = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher", shape, params)) {
      while (rs.hasNext()) {
        final var row = rs.next();
        counts.put(row.getProperty("uuid"), ((Number) row.getProperty("score")).longValue());
      }
    }
    // ent3: edges of episode 1 twice (3j, 3j+1, 3j: 3 and 3 again), once per row of the id twice; ent0: episode 0 twice; ent119: none
    assertThat(counts).containsEntry("ent3", 4L).containsEntry("ent0", 2L).hasSize(2);
  }

  @Test
  void anEqualityInTheWhereClauseAndAnInListStartFromTheIndexedEnd() {
    final String equality = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity) WHERE n.uuid = id RETURN id + '|' + e.uuid AS v";
    final String inList = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity) WHERE n.uuid IN $ids RETURN n.uuid + '|' + e.uuid AS v";
    final Map<String, Object> params = Map.of("ids", IDS);

    assertStartsFrom(equality, params, "(n:Entity");
    assertThat(values(equality, params)).containsExactlyInAnyOrderElementsOf(expectedMentions());
    assertStartsFrom(inList, params, "(n:Entity");
    // an IN-list is a set of keys: the duplicate id does not repeat the rows of the entity, whose two edges are two rows
    assertThat(values(inList, params)).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent0|ep0", "ent0|ep0");
  }

  @Test
  void aUniqueKeyStartsFromTheTargetToo() {
    final String query = "UNWIND $keys AS k MATCH (t:Tiny)-[r:MENTIONS]->(d:Doc {key: k}) RETURN count(*) AS c";
    // the 3 Tiny vertices are more than the one vertex a key finds
    assertStartsFrom(query, Map.of("keys", List.of("doc7", "nokey")), "(d:Doc");
    assertThat(longs(query, Map.of("keys", List.of("doc7", "nokey")))).containsExactly(3L);
  }

  @Test
  void aLabelWithoutVerticesOrOneThatIsNotTheEndOfTheEdgesKeepsTheRowsRight() {
    // Other vertices mention entity 0 too: the label of the first node still filters what the walk back from the entity finds
    final String other = "UNWIND ['ent0'] AS id MATCH (o:Other)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(other, Map.of())).containsExactly(10L);
    final String episodic = "UNWIND ['ent0'] AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(episodic, Map.of())).containsExactly(2L);
    final String person = "UNWIND ['ent0', 'ent1', 'ent10'] AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Person {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(person, Map.of())).containsExactly(3L);
    final String absent = "UNWIND ['ent0'] AS id MATCH (g:Ghost)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(absent, Map.of())).containsExactly(0L);
    // both labels of the first node
    final String both = "UNWIND ['ent0'] AS id MATCH (e:Episodic:Other)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(both, Map.of())).containsExactly(0L);
  }

  @Test
  void aKeyThatManyVerticesShareKeepsTheWrittenOrder() {
    // grp has 2 values over 120 entities, 60 each: the seek finds more than the 3 Tiny vertices have
    final String query = "UNWIND [0, 1] AS g MATCH (t:Tiny)-[r:MENTIONS]->(n:Entity {grp: g}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of(), "(t:Tiny");
    assertThat(longs(query, Map.of())).containsExactly(180L);
  }

  @Test
  void aSingleVertexLabelKeepsTheWrittenOrder() {
    final String query = "UNWIND ['doc7'] AS k MATCH (l:Lonely)-[r:MENTIONS]->(d:Doc {key: k}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of(), "(l:Lonely");
    assertThat(longs(query, Map.of())).containsExactly(1L);
  }

  @Test
  void anythingElseKeepsTheWrittenOrder() {
    final List<String> queries = List.of(
        // a first node a property pins, or an equality of the WHERE clause
        "UNWIND $ids AS id MATCH (e:Episodic {uuid: 'ep1'})-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c",
        "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) WHERE e.uuid = 'ep1' RETURN count(*) AS c",
        // an edge type declared unidirectional, an untyped hop, a path variable, a variable-length hop
        "UNWIND $ids AS id MATCH (e:Episodic)-[r:ONEWAY]->(n:Entity {uuid: id}) RETURN count(*) AS c",
        "UNWIND $ids AS id MATCH (e:Episodic)-->(n:Entity {uuid: id}) RETURN count(*) AS c",
        "UNWIND $ids AS id MATCH p = (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c",
        "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS*1..2]->(n:Entity {uuid: id}) RETURN count(*) AS c");
    for (final String query : queries) {
      final String first = lineOf(profile(query, Map.of("ids", IDS)), "MATCH NODE");
      assertThat(first).as(query).doesNotContain("(n:Entity").doesNotContain("(n)");
    }
    assertThat(lineOf(profile("UNWIND $ids AS id MATCH (n:Entity {uuid: id})<-[r:MENTIONS]-(e:Episodic) RETURN count(*) AS c", Map.of("ids", IDS)),
        "MATCH NODE")).contains("(n:Entity");

    // an OPTIONAL MATCH binds its pattern in the order written
    final String optional = "UNWIND $ids AS id OPTIONAL MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(lineOf(profile(optional, Map.of("ids", IDS)), "MATCH NODE")).contains("(e:Episodic");
  }

  /** A user denied a vertex type reads the same rows from either end: the type's vertices are left out or the statement is refused. */
  @Nested
  class RestrictedUser {
    private static final String ACL_PATH = "./target/databases/cypher-labelled-source-reverse-13267-acl";

    private DatabaseFactory factory;
    private Database        aclDatabase;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(ACL_PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();
      aclDatabase = factory.create();
      aclDatabase.command("sql", "CREATE VERTEX TYPE Entity");
      aclDatabase.command("sql", "CREATE PROPERTY Entity.uuid STRING");
      aclDatabase.command("sql", "CREATE INDEX ON Entity (uuid) NOTUNIQUE");
      aclDatabase.command("sql", "CREATE VERTEX TYPE Episodic");
      aclDatabase.command("sql", "CREATE VERTEX TYPE Secret");
      aclDatabase.command("sql", "CREATE EDGE TYPE MENTIONS");
      aclDatabase.transaction(() -> {
        aclDatabase.command("opencypher", "UNWIND range(1, 5) AS i CREATE (:Entity {uuid: 'e' + i})");
        aclDatabase.command("opencypher", "MATCH (n:Entity {uuid: 'e1'}) UNWIND range(1, 5) AS i "
            + "CREATE (:Episodic {i: i})-[:MENTIONS]->(n), (:Secret {i: 100 + i})-[:MENTIONS]->(n)");
      });
    }

    @AfterEach
    void tearDown() {
      DatabaseContext.INSTANCE.getContext(aclDatabase.getDatabasePath()).setCurrentUser(null);
      aclDatabase.drop();
      factory.close();
    }

    @Test
    void aDeniedLabelOfTheFirstNodeIsNeverReadWhicheverEndTheStatementStartsFrom() {
      final String written = "UNWIND ['e1'] AS id MATCH (s:Secret)-[:MENTIONS]->(n:Entity {uuid: id}) RETURN s.i AS i";
      final String reversed = "UNWIND ['e1'] AS id MATCH (n:Entity {uuid: id})<-[:MENTIONS]-(s:Secret) RETURN s.i AS i";
      assertThat(outcomeOf(written)).isEqualTo("[101, 102, 103, 104, 105]");

      bindUserDenying("Secret");
      final String outcome = outcomeOf(written);
      assertThat(outcome).as("no vertex of the denied type is read").isIn("[]", "SecurityException");
      assertThat(outcomeOf(reversed)).isEqualTo(outcome);
    }

    private void bindUserDenying(final String typeName) {
      final Set<Integer> deniedBuckets = new HashSet<>(aclDatabase.getSchema().getType(typeName).getBucketIds(false));
      DatabaseContext.INSTANCE.getContext(aclDatabase.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "restricted";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return !deniedBuckets.contains(fileId);
        }

        @Override
        public boolean requestAccessOnType(final String name, final ACCESS access) {
          return !typeName.equals(name);
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }

    /** The sorted rows of the statement, or the kind of the exception that ended it. */
    private String outcomeOf(final String query) {
      try {
        final List<Integer> values = new ArrayList<>();
        try (final ResultSet rs = aclDatabase.query("opencypher", query)) {
          while (rs.hasNext())
            values.add(((Number) rs.next().getProperty("i")).intValue());
        }
        values.sort(null);
        return String.valueOf(values);
      } catch (final RuntimeException e) {
        return e.getClass().getSimpleName();
      }
    }
  }

  private void assertStartsFrom(final String query, final Map<String, Object> params, final String firstNode) {
    final String plan = profile(query, params);
    assertThat(plan).as("the test is about the step-based plan").doesNotContain("Cost-Based");
    assertThat(lineOf(plan, "MATCH NODE")).as(plan).contains(firstNode);
    assertThat(plan.indexOf(firstNode)).as(plan).isLessThan(plan.indexOf("MATCH RELATIONSHIP"));
  }

  private static String lineOf(final String plan, final String text) {
    for (final String line : plan.split("\n"))
      if (line.contains(text))
        return line;
    return "";
  }

  private List<String> values(final String query, final Map<String, Object> params) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        values.add(String.valueOf(rs.next().<Object>getProperty("v")));
    }
    return values;
  }

  private List<Long> longs(final String query, final Map<String, Object> params) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("c")).longValue());
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
