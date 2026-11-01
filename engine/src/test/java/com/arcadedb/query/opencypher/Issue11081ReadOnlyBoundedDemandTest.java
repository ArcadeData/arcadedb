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
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #11081, #11082 and #11083: three read-only shapes that did work their result could not depend on.
 * <ul>
 * <li>#11081: a {@code LIMIT 0} drained its whole input, which only a write behind it needs;</li>
 * <li>#11082: a {@code CALL { }} body, and a UNION inside it, collected every row before the first one was handed on, so
 * a {@code LIMIT} after the CALL still ran every branch to the end;</li>
 * <li>#11083: an {@code OPTIONAL MATCH} ran its expansion once per input row even when consecutive rows held the same
 * values for everything it reads, which after a fan-out of the same vertex repeated an identical walk of its whole
 * adjacency for every row.</li>
 * </ul>
 * The work is counted, never timed: {@link #PROBE} is a function that counts its calls, placed in a predicate of the part
 * that should not run, and the OPTIONAL MATCH reports its chain runs in the profile.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11081ReadOnlyBoundedDemandTest {
  private static final String DB_PATH = "./target/databases/issue-11081";
  private static final String PROBE   = "issue11081.probe";
  private static final int    HIGH    = 2_000;

  private static final AtomicLong PROBES = new AtomicLong();

  private Database database;

  @BeforeEach
  void setUp() {
    CypherFunctionRegistry.registerOrReplace(new StatelessFunction() {
      @Override
      public String getName() {
        return PROBE;
      }

      @Override
      public Object execute(final Object[] args, final CommandContext context) {
        PROBES.incrementAndGet();
        return true;
      }
    });

    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // The graph of the reports: one root, 2 low leaves, 3 witness leaves and many high leaves
    for (final String type : List.of("GspRoot", "GspLow", "GspHigh", "GspWitness")) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".uid LONG");
      database.command("sql", "CREATE INDEX ON " + type + " (uid) NOTUNIQUE");
    }
    for (final String type : List.of("GSP_LOW", "GSP_HIGH", "GSP_WITNESS"))
      database.command("sql", "CREATE EDGE TYPE " + type);

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:GspRoot {uid:0})");
      database.command("opencypher",
          "UNWIND range(1,2) AS i MATCH (r:GspRoot {uid:0}) CREATE (r)-[:GSP_LOW]->(:GspLow {uid:i,keep:true})");
      database.command("opencypher",
          "UNWIND range(3,5) AS i MATCH (r:GspRoot {uid:0}) CREATE (r)-[:GSP_WITNESS]->(:GspWitness {uid:i,keep:true})");
      database.command("opencypher", "UNWIND range(6,$last) AS i MATCH (r:GspRoot {uid:0}) "
          + "CREATE (r)-[:GSP_HIGH]->(:GspHigh {uid:i,keep:true})", Map.of("last", 5 + HIGH));
    });
    PROBES.set(0);
  }

  @AfterEach
  void tearDown() {
    CypherFunctionRegistry.unregister(PROBE);
    if (database != null)
      database.drop();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // #11081: a read-only LIMIT 0 reads nothing
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void existsWithLimitZeroDoesNotRunItsBody() {
    // The shape of the report
    final String query = "RETURN EXISTS { MATCH (r:GspRoot)-[:GSP_HIGH]->(n:GspHigh) WHERE n.keep = true AND " + PROBE
        + "(n) RETURN 1 AS item LIMIT 0 } AS answer";
    assertThat(single(query, "answer")).isEqualTo(false);
    assertThat(PROBES.get()).isZero();

    // Without the LIMIT 0 the body runs, and answers true
    assertThat(single(query.replace(" LIMIT 0", ""), "answer")).isEqualTo(true);
    assertThat(PROBES.get()).isPositive();
  }

  @Test
  void statementLevelLimitZeroDoesNotRunWhatIsBehindIt() {
    assertThat(rows("MATCH (n:GspHigh) WHERE " + PROBE + "(n) RETURN n.uid AS uid LIMIT 0")).isEmpty();
    assertThat(rows("MATCH (n:GspHigh) WHERE " + PROBE + "(n) WITH n ORDER BY n.uid LIMIT 0 RETURN n.uid AS uid")).isEmpty();
    assertThat(rows("MATCH (n:GspHigh) WHERE " + PROBE + "(n) WITH count(n) AS c LIMIT 0 RETURN c")).isEmpty();
    assertThat(PROBES.get()).isZero();
  }

  @Test
  void writeStatementLimitZeroStillRunsItsWrites() {
    database.transaction(() -> assertThat(commandRows("MATCH (n:GspLow) SET n.touched = true RETURN n LIMIT 0")).isEmpty());
    assertThat(single("MATCH (n:GspLow) WHERE n.touched = true RETURN count(n) AS c", "c")).isEqualTo(2L);

    // The write is inside a CALL body: the statement can still write, so the LIMIT 0 still drains
    database.transaction(
        () -> assertThat(commandRows("UNWIND range(1, 3) AS i CALL (i) { CREATE (:GspLow {uid: 100 + i}) } RETURN i LIMIT 0")).isEmpty());
    assertThat(single("MATCH (n:GspLow) WHERE n.uid > 100 RETURN count(n) AS c", "c")).isEqualTo(3L);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // #11082: a read-only CALL body streams, so a LIMIT after it stops the body
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void callUnionAllStopsAtTheLimit() {
    // The shape of the report: the witness branch alone satisfies LIMIT 3
    final String query = """
        CALL () {
          MATCH (r:GspRoot)-[:GSP_WITNESS]->(w:GspWitness) WHERE w.keep = true RETURN 1 AS item
          UNION ALL
          MATCH (r:GspRoot)-[:GSP_HIGH]->(n:GspHigh) WHERE n.keep = true AND %s(n) RETURN 1 AS item
        }
        RETURN item AS answer LIMIT 3""".formatted(PROBE);
    assertThat(column(query, "answer")).containsExactly(1L, 1L, 1L);
    // The body is asked for the three rows the LIMIT wants: the second branch never starts
    assertThat(PROBES.get()).isZero();

    // Every row is still there when the consumer asks for all of them
    PROBES.set(0);
    assertThat(column(query.replace(" LIMIT 3", ""), "answer")).hasSize(3 + HIGH);
    assertThat(PROBES.get()).isEqualTo(HIGH);
  }

  @Test
  void callBodyStopsAtTheLimit() {
    // The probe is in the body's RETURN, so it counts the rows the body produced: the two the LIMIT wants, of 2,000
    final String query = "CALL () { MATCH (n:GspHigh) RETURN n.uid AS uid, " + PROBE + "(n) AS p } RETURN uid LIMIT 2";
    assertThat(column(query, "uid")).hasSize(2);
    assertThat(PROBES.get()).isEqualTo(2);

    // A body run per outer row: each one stops where the consumer does
    PROBES.set(0);
    assertThat(column("UNWIND range(1, 5) AS k CALL (k) { MATCH (n:GspHigh) RETURN n.uid AS uid, " + PROBE
        + "(n) AS p } RETURN k, uid LIMIT 3", "uid")).hasSize(3);
    assertThat(PROBES.get()).isEqualTo(3);
  }

  @Test
  void streamedCorrelatedAndOptionalCallsAnswerAsBefore() {
    // Correlated: one body run per outer row, each streamed
    assertThat(rows("UNWIND [0, 1] AS k CALL (k) { MATCH (l:GspLow) WHERE l.uid > k RETURN l.uid AS u } "
        + "RETURN k, u ORDER BY k, u")).containsExactly("0,1", "0,2", "1,2");
    // An OPTIONAL CALL whose body has no row keeps the outer row
    assertThat(rows("UNWIND [0, 99] AS k OPTIONAL CALL (k) { MATCH (l:GspLow) WHERE l.uid > k RETURN l.uid AS u } "
        + "RETURN k, u ORDER BY k, u")).containsExactly("0,1", "0,2", "99,null");
    // A plain CALL whose body has no row drops it
    assertThat(rows("UNWIND [0, 99] AS k CALL (k) { MATCH (l:GspLow) WHERE l.uid > k RETURN l.uid AS u } "
        + "RETURN k, u ORDER BY k, u")).containsExactly("0,1", "0,2");
    // A nested read-only CALL, partially consumed by an inner LIMIT
    assertThat(rows("UNWIND [1, 2] AS k CALL (k) { CALL () { MATCH (w:GspWitness) RETURN w.uid AS u } RETURN u ORDER BY u "
        + "LIMIT 2 } RETURN k, u ORDER BY k, u")).containsExactly("1,3", "1,4", "2,3", "2,4");
  }

  @Test
  void callUnionDeduplicatesLikeAStatementLevelUnion() {
    // A UNION inside a CALL body de-duplicated on the text of the values, where a statement-level UNION compares them:
    // 1 and 1.0 were two rows there and one here
    final List<String> statementLevel = rows("RETURN 1 AS x UNION RETURN 1.0 AS x UNION RETURN 1 AS x");
    final List<String> inCall = rows("CALL () { RETURN 1 AS x UNION RETURN 1.0 AS x UNION RETURN 1 AS x } RETURN x");
    assertThat(inCall).hasSameSizeAs(statementLevel);

    assertThat(rows("CALL () { MATCH (w:GspWitness) RETURN w.uid AS u UNION MATCH (w:GspWitness) RETURN w.uid AS u } "
        + "RETURN u ORDER BY u")).containsExactly("3", "4", "5");
    assertThat(rows("CALL () { MATCH (w:GspWitness) RETURN w.uid AS u UNION ALL MATCH (w:GspWitness) RETURN w.uid AS u } "
        + "RETURN u ORDER BY u")).containsExactly("3", "3", "4", "4", "5", "5");
  }

  @Test
  void writingCallUnionRunsEveryBranch() {
    database.transaction(() -> assertThat(commandRows("""
        CALL () {
          CREATE (a:GspLow {uid: 200}) RETURN a.uid AS u
          UNION ALL
          CREATE (b:GspLow {uid: 201}) RETURN b.uid AS u
        }
        RETURN u LIMIT 1""")).hasSize(1));
    assertThat(single("MATCH (n:GspLow) WHERE n.uid >= 200 RETURN count(n) AS c", "c")).isEqualTo(2L);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // #11083: an OPTIONAL MATCH remembers the matches of the last input row
  // ---------------------------------------------------------------------------------------------------------------

  private static final String LOW_THEN_HIGH = "MATCH (r:GspRoot {uid:0}) OPTIONAL MATCH (r)-[:GSP_LOW]->(l:GspLow) "
      + "OPTIONAL MATCH (r)-[:GSP_HIGH]->(h:GspHigh) RETURN count(h) AS answer";
  private static final String HIGH_THEN_LOW = "MATCH (r:GspRoot {uid:0}) OPTIONAL MATCH (r)-[:GSP_HIGH]->(h:GspHigh) "
      + "OPTIONAL MATCH (r)-[:GSP_LOW]->(l:GspLow) RETURN count(h) AS answer";

  @Test
  void swappedOptionalMatchesRunTheLowBranchOnce() {
    assertThat(single(LOW_THEN_HIGH, "answer")).isEqualTo(2L * HIGH);
    assertThat(single(HIGH_THEN_LOW, "answer")).isEqualTo(2L * HIGH);

    // The low branch follows the high fan-out: every one of its input rows holds the same root
    final String profile = profile(HIGH_THEN_LOW);
    assertThat(counter(profile, "reused per l, r]", "match runs")).isEqualTo(1);
    assertThat(counter(profile, "reused per l, r]", "reused")).isEqualTo(HIGH - 1);
  }

  @Test
  void rememberedMatchesKeepEveryRowOfEachInput() {
    final String query = "MATCH (r:GspRoot) OPTIONAL MATCH (r)-[:GSP_HIGH]->(h:GspHigh) WHERE h.uid <= 8 "
        + "OPTIONAL MATCH p = (r)-[e:GSP_LOW]->(l:GspLow) RETURN h.uid AS h, l.uid AS l, type(e) AS t, length(p) AS len "
        + "ORDER BY h, l";
    assertThat(rows(query)).containsExactly("6,1,GSP_LOW,1", "6,2,GSP_LOW,1", "7,1,GSP_LOW,1", "7,2,GSP_LOW,1", "8,1,GSP_LOW,1",
        "8,2,GSP_LOW,1");
    assertThat(counter(profile(query), "reused per e, l, p, r]", "match runs")).isEqualTo(1);
    assertThat(rows(query)).isEqualTo(rows(notRemembered(query)));
  }

  @Test
  void theKeyHoldsEveryNameTheClauseReads() {
    // Same root on every row, but the WHERE reads k too: each k has its own match
    final String where = "MATCH (r:GspRoot) UNWIND [3, 3, 4, 9, 5] AS k OPTIONAL MATCH (r)-[:GSP_WITNESS]->(w:GspWitness) "
        + "WHERE w.uid = k RETURN k, w.uid AS w";
    assertThat(rows(where)).containsExactly("3,3", "3,3", "4,4", "9,null", "5,5");
    assertThat(counter(profile(where), "reused per k, r, w]", "match runs")).isEqualTo(4);

    // The same through an inline property map
    final String inline = "MATCH (r:GspRoot) UNWIND [3, 3, 4, 9, 5] AS k "
        + "OPTIONAL MATCH (r)-[:GSP_WITNESS]->(w:GspWitness {uid: k}) RETURN k, w.uid AS w";
    assertThat(rows(inline)).containsExactly("3,3", "3,3", "4,4", "9,null", "5,5");
  }

  @Test
  void recordingPausesWhileNoKeyRepeatsAndResumesWhenOneDoes() {
    // 100 keys that never repeat, then one repeated three times
    final String query = "MATCH (r:GspRoot) UNWIND range(1, 100) + [4, 4, 4] AS k "
        + "OPTIONAL MATCH (r)-[:GSP_WITNESS]->(w:GspWitness) WHERE w.uid = k RETURN k, w.uid AS w";
    final List<String> rows = rows(query);
    assertThat(rows).hasSize(103);
    assertThat(rows.subList(100, 103)).containsExactly("4,4", "4,4", "4,4");
    assertThat(rows).isEqualTo(rows(notRemembered(query)));

    // Past the first 64 runs nothing is recorded; the first repeat turns recording back on, the second one is answered
    final String profile = profile(query);
    assertThat(counter(profile, "reused per k, r, w]", "match runs")).isEqualTo(102);
    assertThat(counter(profile, "reused per k, r, w]", "reused")).isEqualTo(1);
  }

  @Test
  void anEmptyAnswerIsRememberedAsWell() {
    final String query = "UNWIND range(1, 4) AS i MATCH (r:GspRoot) OPTIONAL MATCH (r)<-[:GSP_LOW]-(x) RETURN i, x ORDER BY i";
    assertThat(rows(query)).containsExactly("1,null", "2,null", "3,null", "4,null");
    final String profile = profile(query);
    assertThat(counter(profile, "reused per r, x]", "match runs")).isEqualTo(1);
    assertThat(counter(profile, "reused per r, x]", "reused")).isEqualTo(3);
  }

  @Test
  void anUncorrelatedClauseRunsOnce() {
    final String query = "UNWIND range(1, 5) AS i OPTIONAL MATCH (w:GspWitness) RETURN i, w.uid AS w ORDER BY i, w";
    assertThat(rows(query)).hasSize(15);
    assertThat(counter(profile(query), "reused per w]", "match runs")).isEqualTo(1);
  }

  @Test
  void aNullOrAbsentKeyIsComparedAsSuch() {
    // a is bound to null by the first OPTIONAL MATCH: the second one matches nothing for it
    final String query = "UNWIND [1, 2] AS i OPTIONAL MATCH (a:GspRoot {uid: 99}) OPTIONAL MATCH (a)-[:GSP_LOW]->(l) "
        + "RETURN i, a, l ORDER BY i";
    assertThat(rows(query)).containsExactly("1,null,null", "2,null,null");
  }

  @Test
  void relationshipUniquenessIsPerClause() {
    // e2 may be the same edge as e1: uniqueness holds within a MATCH clause, not across clauses
    final String query = "MATCH (r:GspRoot)-[e1:GSP_LOW]->(l1) OPTIONAL MATCH (r)-[e2:GSP_LOW]->(l2) "
        + "RETURN l1.uid AS a, l2.uid AS b ORDER BY a, b";
    assertThat(rows(query)).containsExactly("1,1", "1,2", "2,1", "2,2");
    assertThat(rows(query)).isEqualTo(rows(notRemembered(query)));
  }

  @Test
  void notRememberedWhenTheAnswerCanVaryOrTheStatementWrites() {
    assertThat(explain(HIGH_THEN_LOW)).contains("matches reused per");
    // A function that can answer differently for the same arguments
    assertThat(explain(notRemembered(HIGH_THEN_LOW))).contains("reused per h, r]").doesNotContain("reused per l, r]");
    // A subquery expression, whose reads are not modelled as reads of the row
    assertThat(explain("MATCH (r:GspRoot) OPTIONAL MATCH (r)-[:GSP_LOW]->(l) WHERE EXISTS { (l)<--(r) } RETURN l"))
        .doesNotContain("matches reused per");
    // A statement that writes
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "EXPLAIN MATCH (r:GspRoot) OPTIONAL MATCH (r)-[:GSP_LOW]->(l) "
          + "SET l.seen = true RETURN l")) {
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("matches reused per");
      }
    });
  }

  /** The same query with a predicate that turns the memo off and keeps every row: the reference answer. */
  private static String notRemembered(final String query) {
    final int at = query.lastIndexOf("OPTIONAL MATCH");
    final int returnAt = query.indexOf(" RETURN ", at);
    final String clause = query.substring(at, returnAt);
    final String guarded = clause.contains(" WHERE ") ? clause + " AND rand() < 2" : clause + " WHERE rand() < 2";
    return query.substring(0, at) + guarded + query.substring(returnAt);
  }

  // ---------------------------------------------------------------------------------------------------------------

  private Object single(final String query, final String column) {
    final List<Object> values = column(query, column);
    assertThat(values).hasSize(1);
    return values.getFirst();
  }

  private List<Object> column(final String query, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }

  /** Every row as its values joined by commas, in the order of the RETURN items. */
  private List<String> rows(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rows(rs);
    }
  }

  private List<String> commandRows(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      return rows(rs);
    }
  }

  private static List<String> rows(final ResultSet rs) {
    final List<String> rows = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      final List<String> values = new ArrayList<>();
      for (final String name : row.getPropertyNames())
        values.add(String.valueOf(row.<Object>getProperty(name)));
      rows.add(String.join(",", values));
    }
    return Collections.unmodifiableList(rows);
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private String profile(final String query) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  /** A counter the OPTIONAL MATCH step whose header contains {@code step} reports in the profile. */
  private static long counter(final String profile, final String step, final String name) {
    for (final String line : profile.split("\n"))
      if (line.contains(step)) {
        final Matcher matcher = Pattern.compile(name + ": (\\d+)").matcher(line);
        assertThat(matcher.find()).as(line).isTrue();
        return Long.parseLong(matcher.group(1));
      }
    throw new AssertionError("No step " + step + " in\n" + profile);
  }
}
