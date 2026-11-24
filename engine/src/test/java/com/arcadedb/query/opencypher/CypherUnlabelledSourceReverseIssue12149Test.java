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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12149: a single hop from an unlabelled, unconstrained first node to a node a UNIQUE index pins used to scan every vertex
 * of every type (once per row after an UNWIND), expand all of them, and filter on the pinned node last. The step-based plan now
 * starts from the pinned node. Each test asserts which node the plan starts from (the first MATCH NODE step, and the rows it read
 * under PROFILE) and the answer, compared with the same pattern written from the pinned end.
 * <p>
 * The cost-based optimizer plans a plain MATCH of such a pattern from the labelled node by itself (issue #11904), so every query
 * here follows a {@code WITH 1 AS one}: a MATCH after another clause stays on the step-based plan, which is the one under test.
 */
class CypherUnlabelledSourceReverseIssue12149Test {
  private static final String PATH  = "./target/databases/cypher-unlabelled-source-reverse-12149";
  private static final String STEPS = "WITH 1 AS one ";
  private static final int    ITEMS = 60;

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.key INTEGER");
    database.command("sql", "CREATE INDEX ON Doc (key) UNIQUE");
    database.command("sql", "CREATE PROPERTY Doc.grp INTEGER");
    database.command("sql", "CREATE INDEX ON Doc (grp) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Item");
    database.command("sql", "CREATE VERTEX TYPE Other");
    database.command("sql", "CREATE VERTEX TYPE Pair");
    database.command("sql", "CREATE PROPERTY Pair.a INTEGER");
    database.command("sql", "CREATE PROPERTY Pair.b INTEGER");
    database.command("sql", "CREATE INDEX ON Pair (a, b) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE OF");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    database.command("sql", "CREATE EDGE TYPE P");
    database.command("sql", "CREATE EDGE TYPE C EXTENDS P UNIDIRECTIONAL");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, 9) AS i CREATE (:Doc {key: i, grp: i % 2})");
      database.command("opencypher",
          "UNWIND range(1, $n) AS i MATCH (d:Doc {key: i % 10}) CREATE (:Item {i: i})-[:OF]->(d), (:Other {i: -i})-[:U]->(d)",
          Map.of("n", ITEMS));
      database.command("opencypher", "UNWIND range(0, 3) AS p CREATE (pr:Pair {a: p % 2, b: p}) CREATE (:Item {i: 1000 + p})-[:OF]->(pr)");
      // one hop type with a stored incoming side, one of its subtypes without
      database.command("opencypher", "MATCH (d:Doc {key: 8}) CREATE (:Item {i: 2008})-[:P]->(d), (:Other {i: 3008})-[:C]->(d)");
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
    assertThat(ints(STEPS + query, params)).hasSize(12)
        .containsExactlyInAnyOrderElementsOf(ints(STEPS + "MATCH (d:Doc)<-[:OF]-(x) WHERE d.key IN $keys RETURN x.i AS i", params));
  }

  @Test
  void anEqualityInTheWhereClauseStartsFromTheTarget() {
    final String query = "MATCH (x)-[:OF]->(d:Doc) WHERE d.key = $key RETURN x.i AS i";
    final Map<String, Object> params = Map.of("key", 5);

    assertStartsFrom(query, params, "MATCH NODE (d:Doc)", 1);
    assertThat(ints(STEPS + query, params)).containsExactlyInAnyOrder(5, 15, 25, 35, 45, 55);
  }

  @Test
  void anInlineKeyStartsFromTheTarget() {
    final String query = "MATCH (x)-[:OF]->(d:Doc {key: 7}) RETURN x.i AS i";

    assertStartsFrom(query, Map.of(), "MATCH NODE (d:Doc)", 1);
    assertThat(ints(STEPS + query, Map.of())).containsExactlyInAnyOrder(7, 17, 27, 37, 47, 57);
  }

  @Test
  void aKeyFromAnUnwindRowStartsFromTheTargetOncePerRow() {
    final String query = "UNWIND $keys AS k MATCH (x)-[:OF]->(:Doc {key: k}) RETURN x.i AS i";
    final Map<String, Object> params = Map.of("keys", List.of(1, 2, 99));

    final String plan = profile(query, params);
    assertThat(plan).doesNotContain("Cost-Based");
    assertThat(plan.indexOf(":Doc)")).as(plan).isLessThan(plan.indexOf("MATCH RELATIONSHIP"));
    assertThat(lineOf(plan, ":Doc)")).as(plan).contains("[index:").matches(".*, 2 rows\\)$");
    assertThat(ints(query, params)).hasSize(12)
        .containsExactlyInAnyOrderElementsOf(ints("UNWIND $keys AS k MATCH (:Doc {key: k})<-[:OF]-(x) RETURN x.i AS i", params));

    final String withRow = "WITH 5 AS k MATCH (x)-[r:OF]->(:Doc {key: k}) RETURN x.i AS i";
    assertThat(lineOf(profile(withRow, Map.of()), "MATCH NODE")).contains(":Doc)");
    assertThat(ints(withRow, Map.of())).containsExactlyInAnyOrder(5, 15, 25, 35, 45, 55);
  }

  @Test
  void aKeyFromAnEarlierClauseInTheWhereClauseStartsFromTheTarget() {
    // the issue lists the WHERE form with a value an earlier clause bound as well as the inline map
    final Map<String, Object> params = Map.of("keys", List.of(1, 2, 99));
    for (final String where : List.of("d.key = k", "k = d.key", "d.key IN [k]", "d.key IN [k, k + 100]")) {
      final String query = "UNWIND $keys AS k MATCH (x)-[:OF]->(d:Doc) WHERE " + where + " RETURN x.i AS i";

      final String plan = profile(query, params);
      assertThat(plan).as(where).doesNotContain("Cost-Based");
      assertThat(lineOf(plan, "MATCH NODE")).as(plan).contains("(d:Doc)").contains("[index:").matches(".*, 2 rows\\)$");
      assertThat(ints(query, params)).as(where).hasSize(12)
          .containsExactlyInAnyOrderElementsOf(ints("UNWIND $keys AS k MATCH (:Doc {key: k})<-[:OF]-(x) RETURN x.i AS i", params));
    }
  }

  @Test
  void aKeyThatIsNullOrOfAnotherKindAnswersAsTheWrittenOrderDoes() {
    // the seek of the pinned node must not turn a key that equals nothing into a key that equals something, nor the reverse
    final String reversed = STEPS + "MATCH (x)-[:OF]->(:Doc {key: $k}) RETURN x.i AS i";
    // the same statement kept in the order written by an equality on the first node
    final String written = STEPS + "MATCH (x)-[:OF]->(:Doc {key: $k}) WHERE x.i = x.i RETURN x.i AS i";

    final List<Object> keys = new ArrayList<>();
    keys.add(null);
    keys.add("2");
    keys.add("two");
    keys.add(2.5);
    keys.add(2.0);
    keys.add(List.of(2));
    keys.add(2);
    for (final Object key : keys) {
      final Map<String, Object> params = new HashMap<>();
      params.put("k", key);
      assertThat(lineOf(profile(reversed, params), "MATCH NODE")).as("key " + key).contains(":Doc)").doesNotContain("MATCH NODE (x");
      assertThat(lineOf(profile(written, params), "MATCH NODE")).as("key " + key).contains("MATCH NODE (x");
      assertThat(ints(reversed, params)).as("key " + key).containsExactlyInAnyOrderElementsOf(ints(written, params));
    }

    assertThat(ints(reversed, Map.of("k", 2))).containsExactlyInAnyOrder(2, 12, 22, 32, 42, 52);
    for (final Object key : new Object[] { "two", 2.5, List.of(2) })
      assertThat(ints(reversed, Map.of("k", key))).as("key " + key).isEmpty();
  }

  @Test
  void aCompositeUniqueKeyIsPinnedOnEveryColumnOrNotAtAll() {
    final String whole = "MATCH (x)-[:OF]->(p:Pair {a: 1, b: 3}) RETURN x.i AS i";
    assertStartsFrom(whole, Map.of(), "MATCH NODE (p:Pair)", 1);
    assertThat(ints(STEPS + whole, Map.of())).containsExactly(1003);

    final String wholeInWhere = "MATCH (x)-[:OF]->(p:Pair) WHERE p.a = 1 AND p.b = 3 RETURN x.i AS i";
    assertStartsFrom(wholeInWhere, Map.of(), "MATCH NODE (p:Pair)", 1);
    assertThat(ints(STEPS + wholeInWhere, Map.of())).containsExactly(1003);

    // one column of two does not pin a vertex: the written order stays
    final String partly = "MATCH (x)-[:OF]->(p:Pair {a: 1}) RETURN x.i AS i";
    assertKeepsTheWrittenOrder(partly, Map.of());
    assertThat(ints(STEPS + partly, Map.of())).containsExactlyInAnyOrder(1001, 1003);
  }

  @Test
  void anIncomingArrowIsWalkedTheOtherWay() {
    // the hop is written against the stored direction: no edge goes from a Doc to an Item
    final String query = "MATCH (x)<-[:OF]-(d:Doc {key: 2}) RETURN x.i AS i";
    assertStartsFrom(query, Map.of(), "MATCH NODE (d:Doc)", 1);
    assertThat(ints(STEPS + query, Map.of())).isEmpty();
  }

  @Test
  void aSubtypeWithoutAnIncomingSideIsStillRead() {
    // P stores both sides, its subtype C only the outgoing one: the hop over P reads the edges of both
    final String query = "MATCH (x)-[:P]->(d:Doc {key: 8}) RETURN x.i AS i";
    assertStartsFrom(query, Map.of(), "MATCH NODE (d:Doc)", 1);
    assertThat(ints(STEPS + query, Map.of())).containsExactlyInAnyOrder(2008, 3008);
    assertThat(ints(STEPS + "MATCH (d:Doc {key: 8})<-[:P]-(x) RETURN x.i AS i", Map.of())).containsExactlyInAnyOrder(2008, 3008);
  }

  @Test
  void aPredicateOnTheFirstNodeIsStillApplied() {
    final String query = "MATCH (x)-[:OF]->(d:Doc {key: 7}) WHERE x.i > 20 RETURN x.i AS i";
    assertStartsFrom(query, Map.of(), "MATCH NODE (d:Doc)", 1);
    assertThat(ints(STEPS + query, Map.of())).containsExactlyInAnyOrder(27, 37, 47, 57);

    final String named = "MATCH (x)-[r:OF]->(d:Doc {key: 7}) RETURN x.i AS i, type(r) AS t, startNode(r) = x AS fromX, endNode(r) = d AS toD";
    assertStartsFrom(named, Map.of(), "MATCH NODE (d:Doc)", 1);
    try (final ResultSet rs = database.query("opencypher", STEPS + named)) {
      int rows = 0;
      while (rs.hasNext()) {
        final var row = rs.next();
        assertThat(row.<String>getProperty("t")).isEqualTo("OF");
        assertThat(row.<Boolean>getProperty("fromX")).isTrue();
        assertThat(row.<Boolean>getProperty("toD")).isTrue();
        ++rows;
      }
      assertThat(rows).isEqualTo(6);
    }
  }

  @Test
  void theInternalNameOfAnAnonymousTargetIsNotProjected() {
    // the reversed plan names the anonymous target to start from it: RETURN * must not show that name, as it does not in the written order
    final String reversed = STEPS + "MATCH (x)-[:OF]->(:Doc {key: 7}) RETURN *";
    final String written = STEPS + "MATCH (x)-[:OF]->(:Doc {key: 7}) WHERE x.i = x.i RETURN *";
    assertThat(lineOf(profile(reversed, Map.of()), "MATCH NODE")).doesNotContain("MATCH NODE (x");

    final Set<String> columns = new HashSet<>();
    try (final ResultSet rs = database.query("opencypher", reversed)) {
      int rows = 0;
      while (rs.hasNext()) {
        columns.addAll(rs.next().getPropertyNames());
        ++rows;
      }
      assertThat(rows).isEqualTo(6);
    }
    final Set<String> writtenColumns = new HashSet<>();
    try (final ResultSet rs = database.query("opencypher", written)) {
      while (rs.hasNext())
        writtenColumns.addAll(rs.next().getPropertyNames());
    }
    assertThat(columns).isEqualTo(writtenColumns).contains("x");
    assertThat(columns).noneMatch(name -> name.startsWith(" "));
  }

  @Test
  void severalPatternsOfOneMatchAreEachStartedFromTheirTarget() {
    final String query = "MATCH (x)-[:OF]->(d:Doc {key: 7}), (y)-[:OF]->(e:Doc {key: 8}) RETURN x.i AS i, y.i AS j";
    final String plan = profile(STEPS + query, Map.of());
    assertThat(plan).doesNotContain("Cost-Based").doesNotContain("MATCH NODE (x").doesNotContain("MATCH NODE (y");
    assertThat(ints(STEPS + query, Map.of())).hasSize(36);

    // the second pattern ends on a node the first bound: the hop is walked from it, as before this change
    final String shared = "MATCH (x)-[:OF]->(d:Doc {key: 7}), (y)-[:OF]->(d) RETURN x.i AS i, y.i AS j";
    assertThat(ints(STEPS + shared, Map.of())).as("two distinct edges into one vertex: 6 * 5 ordered pairs").hasSize(30);
  }

  @Test
  void anythingElseKeepsTheWrittenOrder() {
    // a NOTUNIQUE key, a unidirectional type, an untyped hop, an extra label, a path variable, a variable-length hop, the first
    // node written again as the last, a first node a WHERE equality pins
    final List<String> queries = List.of(
        "MATCH (x)-[:OF]->(d:Doc {grp: 1}) RETURN x.i AS i",
        "MATCH (x)-[:U]->(d:Doc {key: 1}) RETURN x.i AS i",
        "MATCH (x)-->(d:Doc {key: 1}) RETURN x.i AS i",
        "MATCH (x)-[:OF]->(d:Doc:Item {key: 1}) RETURN x.i AS i",
        "MATCH p = (x)-[:OF]->(d:Doc {key: 1}) RETURN x.i AS i",
        "MATCH (x)-[:OF*1..2]->(d:Doc {key: 1}) RETURN x.i AS i",
        "MATCH (x)-[:OF]->(x:Doc {key: 1}) RETURN x.i AS i",
        "MATCH (x)-[:OF]->(d:Doc {key: 7}) WHERE x.i = 7 RETURN x.i AS i",
        "MATCH (x)-[:OF]->(d:Doc) WHERE d.key = 7 OR x.i = 7 RETURN x.i AS i");
    for (final String query : queries)
      assertKeepsTheWrittenOrder(query, Map.of());

    // an OPTIONAL MATCH binds its pattern in the order written, whichever plan answers it
    final String optional = "OPTIONAL MATCH (x)-[:OF]->(d:Doc {key: 1}) RETURN x.i AS i";
    assertThat(lineOf(profile(optional, Map.of()), "MATCH NODE")).contains("MATCH NODE (x");

    assertThat(ints(STEPS + "MATCH (x)-[:U]->(d:Doc {key: 1}) RETURN x.i AS i", Map.of())).hasSize(6);
    assertThat(ints(STEPS + "MATCH (x)-[:OF]->(d:Doc {grp: 1}) RETURN x.i AS i", Map.of())).hasSize(30);
    assertThat(ints(STEPS + "MATCH (x)-[:OF]->(x:Doc {key: 1}) RETURN x.i AS i", Map.of())).isEmpty();
    assertThat(ints(STEPS + "MATCH (x)-[:OF]->(d:Doc {key: 7}) WHERE x.i = 7 RETURN x.i AS i", Map.of())).containsExactly(7);
  }

  @Test
  void aWriteAfterTheReversedMatchSeesEveryRowOnce() {
    // the walk reads the incoming edges of the node the statement then writes to: the rows are the ones the MATCH found
    database.transaction(() -> database.command("opencypher",
        STEPS + "MATCH (x)-[:OF]->(d:Doc {key: 7}) CREATE (:Item {i: -7})-[:OF]->(d)"));
    assertThat(ints(STEPS + "MATCH (x)-[:OF]->(d:Doc {key: 7}) WHERE x.i = -7 RETURN x.i AS i", Map.of())).hasSize(6);
    assertThat(ints("MATCH (d:Doc {key: 7})<-[:OF]-(x) RETURN x.i AS i", Map.of())).hasSize(12);

    database.transaction(() -> database.command("opencypher", STEPS + "MATCH (x)-[:OF]->(d:Doc {key: 7}) DETACH DELETE x"));
    assertThat(ints("MATCH (d:Doc {key: 7})<-[:OF]-(x) RETURN x.i AS i", Map.of())).isEmpty();
    assertThat(ints("MATCH (d:Doc {key: 6})<-[:OF]-(x) RETURN x.i AS i", Map.of())).hasSize(6);
  }

  private void assertStartsFrom(final String query, final Map<String, Object> params, final String firstNodeStep,
      final int rowsRead) {
    final String plan = profile(STEPS + query, params);
    assertThat(plan).as("the test is about the step-based plan").doesNotContain("Cost-Based");
    assertThat(lineOf(plan, "MATCH NODE")).as(plan).contains(firstNodeStep).contains("[index:").matches(".*, " + rowsRead + " rows\\)$");
    assertThat(plan.indexOf(firstNodeStep)).as(plan).isLessThan(plan.indexOf("MATCH RELATIONSHIP"));
  }

  private void assertKeepsTheWrittenOrder(final String query, final Map<String, Object> params) {
    final String plan = profile(STEPS + query, params);
    assertThat(plan).as("the test is about the step-based plan").doesNotContain("Cost-Based");
    assertThat(lineOf(plan, "MATCH NODE")).as(query + "\n" + plan).contains("MATCH NODE (x");
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

  /**
   * A user denied a vertex type reads the same rows through the walk from the pinned node as through the written order, which
   * scans the vertex types it may read: the vertices of the denied type are left out, the statement is not failed.
   */
  @Nested
  class RestrictedUser {
    private static final String ACL_PATH = "./target/databases/cypher-unlabelled-source-reverse-12149-acl";

    private DatabaseFactory factory;
    private Database        aclDatabase;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(ACL_PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();
      aclDatabase = factory.create();
      aclDatabase.command("sql", "CREATE VERTEX TYPE Doc");
      aclDatabase.command("sql", "CREATE PROPERTY Doc.key INTEGER");
      aclDatabase.command("sql", "CREATE INDEX ON Doc (key) UNIQUE");
      aclDatabase.command("sql", "CREATE VERTEX TYPE Item");
      aclDatabase.command("sql", "CREATE VERTEX TYPE Secret");
      aclDatabase.command("sql", "CREATE EDGE TYPE OF");
      aclDatabase.transaction(() -> aclDatabase.command("opencypher",
          "CREATE (d:Doc {key: 1}) CREATE (:Item {i: 1})-[:OF]->(d), (:Item {i: 2})-[:OF]->(d), (:Secret {i: 99})-[:OF]->(d)"));
    }

    @AfterEach
    void tearDown() {
      DatabaseContext.INSTANCE.getContext(aclDatabase.getDatabasePath()).setCurrentUser(null);
      aclDatabase.drop();
      factory.close();
    }

    @Test
    void aDeniedTypeOfTheFirstNodeIsNeverReadWhicheverPlanWalksToIt() {
      final String query = STEPS + "MATCH (x)-[:OF]->(d:Doc {key: 1}) RETURN x.i AS i";
      assertThat(aclInts(query)).containsExactlyInAnyOrder(1, 2, 99);

      bindUserDenying("Secret");
      final String outcome = outcomeOf(query);
      assertThat(outcome).as("a vertex of the denied type is never read").doesNotContain("99");
      // the cost-based optimizer starts from the pinned node as well, and so does a user who writes the pattern from that end
      assertThat(outcomeOf("MATCH (x)-[:OF]->(d:Doc {key: 1}) RETURN x.i AS i")).isEqualTo(outcome);
      assertThat(outcomeOf(STEPS + "MATCH (d:Doc {key: 1})<-[:OF]-(x) RETURN x.i AS i")).isEqualTo(outcome);

      // a statement that reads no property of the first node ends the same way in the three plans as well
      final String count = outcomeOf(STEPS + "MATCH (x)-[:OF]->(d:Doc {key: 1}) RETURN count(x) AS i");
      assertThat(outcomeOf("MATCH (x)-[:OF]->(d:Doc {key: 1}) RETURN count(x) AS i")).isEqualTo(count);
      assertThat(outcomeOf(STEPS + "MATCH (d:Doc {key: 1})<-[:OF]-(x) RETURN count(x) AS i")).isEqualTo(count);
    }

    @Test
    void aDeniedTypeOfTheTargetIsNeverRead() {
      bindUserDenying("Doc");
      final String outcome = outcomeOf(STEPS + "MATCH (x)-[:OF]->(d:Doc {key: 1}) RETURN x.i AS i");
      assertThat(outcome).as("no row of a denied target").isIn("[]", "SecurityException");
      assertThat(outcomeOf("MATCH (x)-[:OF]->(d:Doc {key: 1}) RETURN x.i AS i")).isEqualTo(outcome);
      assertThat(outcomeOf(STEPS + "MATCH (d:Doc {key: 1})<-[:OF]-(x) RETURN x.i AS i")).isEqualTo(outcome);
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

    private List<Integer> aclInts(final String query) {
      final List<Integer> values = new ArrayList<>();
      try (final ResultSet rs = aclDatabase.query("opencypher", query)) {
        while (rs.hasNext())
          values.add(((Number) rs.next().getProperty("i")).intValue());
      }
      return values;
    }

    /** The rows of the statement, or the kind of the exception that ended it. */
    private String outcomeOf(final String query) {
      try {
        return String.valueOf(aclInts(query));
      } catch (final RuntimeException e) {
        return e.getClass().getSimpleName();
      }
    }
  }
}
