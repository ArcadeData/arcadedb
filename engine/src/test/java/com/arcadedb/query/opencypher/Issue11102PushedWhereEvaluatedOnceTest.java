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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11102: on the traditional execution path a WHERE conjunct pushed into the {@code MatchNodeStep} that binds its
 * variable was evaluated a second time by the clause's {@code FilterPropertiesStep}, once per row that passed it. The
 * filter step now applies only the residual the node steps did not take, and is left out when nothing is left.
 * <p>
 * The work is counted, never timed: {@link #PROBE} counts its calls and answers true, so every call is a row the
 * predicate was asked about. A seeded {@code CALL () { }} body is planned on the traditional path, which is where the
 * double evaluation lived.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11102PushedWhereEvaluatedOnceTest {
  private static final String DB_PATH = "./target/databases/issue-11102";
  private static final String PROBE   = "issue11102.probe";
  private static final int    NODES   = 50;

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

    database.command("sql", "CREATE VERTEX TYPE PwNode");
    database.command("sql", "CREATE VERTEX TYPE PwOther");
    database.command("sql", "CREATE EDGE TYPE PW_LINK");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(1, $n) AS i CREATE (:PwNode {uid: i, grp: i % 5})", Map.of("n", NODES));
      database.command("opencypher", "UNWIND range(0, 4) AS i CREATE (:PwOther {grp: i})");
      database.command("opencypher", "MATCH (a:PwNode), (b:PwNode) WHERE b.uid = a.uid + 1 CREATE (a)-[:PW_LINK]->(b)");
    });
    PROBES.set(0);
  }

  @AfterEach
  void tearDown() {
    CypherFunctionRegistry.unregister(PROBE);
    if (database != null)
      database.drop();
  }

  @Test
  void theShapeOfTheReportCallsThePredicateOncePerRow() {
    // The report: 4 calls for 2 rows, 2 from the pushed filter and 2 from the clause filter
    assertThat(column("CALL () { MATCH (n:PwNode) WHERE " + PROBE + "(n) RETURN n.uid AS uid } RETURN uid LIMIT 2", "uid")).hasSize(2);
    assertThat(PROBES.get()).isEqualTo(2);

    PROBES.set(0);
    assertThat(column("CALL () { MATCH (n:PwNode) WHERE " + PROBE + "(n) RETURN n.uid AS uid } RETURN uid", "uid")).hasSize(NODES);
    assertThat(PROBES.get()).isEqualTo(NODES);
  }

  @Test
  void aPushedConjunctNextToAResidualOneIsEvaluatedOnce() {
    // probe(a) is pushed into the node step of a; a.grp = b.grp needs b, bound by the second pattern part, and stays
    final List<Object> rows = column("CALL () { MATCH (a:PwNode), (b:PwOther) WHERE " + PROBE
        + "(a) AND a.grp = b.grp RETURN a.uid AS uid } RETURN uid", "uid");
    assertThat(rows).hasSize(NODES);
    // a is scanned once and each a is asked once, whatever b it is later joined with
    assertThat(PROBES.get()).isEqualTo(NODES);
  }

  @Test
  void aConjunctOnTheFarEndOfAHopIsStillApplied() {
    // probe(a) is pushed into the source step; b.uid > 40 reads the hop's target, which no node step binds, and stays
    final List<Object> rows = column("CALL () { MATCH (a:PwNode)-[:PW_LINK]->(b:PwNode) WHERE " + PROBE
        + "(a) AND b.uid > 40 RETURN b.uid AS uid } RETURN uid ORDER BY uid", "uid");
    assertThat(rows).containsExactly(41L, 42L, 43L, 44L, 45L, 46L, 47L, 48L, 49L, 50L);
    // every source with an outgoing link is asked once (the last node has none, and is still asked once)
    assertThat(PROBES.get()).isEqualTo(NODES);
  }

  @Test
  void anOptionalMatchStillFiltersInsideItsChain() {
    final List<String> rows = rows("CALL () { MATCH (o:PwOther) OPTIONAL MATCH (n:PwNode) WHERE " + PROBE
        + "(n) AND n.grp = o.grp AND n.uid < 10 RETURN o.grp AS g, count(n) AS c } RETURN g, c ORDER BY g");
    assertThat(rows).containsExactly("0,1", "1,2", "2,2", "3,2", "4,2");
  }

  @Test
  void aShortestPathPredicateIsDecidedOnceThePathExists() {
    // length(p) reads the path, which the endpoints' node steps run before: it must stay with the clause's filter
    final String body = "MATCH p = shortestPath((a:PwNode {uid: 1})-[:PW_LINK*]->(b:PwNode {uid: 5})) WHERE length(p) > 2 "
        + "RETURN length(p) AS len";
    assertThat(column("CALL () { " + body + " } RETURN len", "len")).containsExactly(4L);
    assertThat(column(body, "len")).containsExactly(4L);
  }

  @Test
  void theAnswersMatchTheOptimizerPath() {
    // The same statements outside a CALL body take the optimizer path: both answers must agree
    final String[] bodies = {
        "MATCH (n:PwNode) WHERE n.uid > 45 RETURN n.uid AS uid",
        "MATCH (a:PwNode), (b:PwOther) WHERE a.uid < 8 AND a.grp = b.grp RETURN a.uid AS uid",
        "MATCH (a:PwNode)-[:PW_LINK]->(b:PwNode) WHERE a.uid > 44 AND b.uid < 49 RETURN b.uid AS uid",
        "MATCH (a:PwNode) WHERE a.uid = 3 OR a.uid = 7 RETURN a.uid AS uid",
        "MATCH (a:PwNode) WHERE NOT (a.grp = 1) AND a.uid <= 6 RETURN a.uid AS uid" };
    for (final String body : bodies) {
      final List<String> optimizer = sorted(rows(body));
      final List<String> traditional = sorted(rows("CALL () { " + body + " } RETURN uid"));
      assertThat(traditional).as(body).isNotEmpty().isEqualTo(optimizer);
    }
  }

  private static List<String> sorted(final List<String> rows) {
    final List<String> copy = new ArrayList<>(rows);
    copy.sort(null);
    return copy;
  }

  private List<Object> column(final String query, final String name) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(name));
    }
    return values;
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String name : row.getPropertyNames()) {
          if (!sb.isEmpty())
            sb.append(',');
          sb.append((Object) row.getProperty(name));
        }
        rows.add(sb.toString());
      }
    }
    return rows;
  }
}
