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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9652: the Cypher queries answered from an index - a count with an equality predicate, {@code min}, {@code max},
 * {@code ORDER BY ... LIMIT 1} - cost 6 to 14 percent more per call after #9601, with the same answers. A plan is built for
 * every execution, and every execution asked every count push-down whether it applied: #9601 added a fold of the
 * statement's pass-through {@code WITH} clauses and a split into disconnected parts in front of them, and taught the
 * anti-join and star detectors to parse the {@code WHERE} before checking the pattern's shape. None of them takes a
 * single node, so all of that was paid only to be declined.
 * <p>
 * The fold is now derived once per statement and kept on it, a {@code MATCH} with no relationship skips the edge-walking
 * detectors, the split counts its units before allocating, and the shape checks come before the {@code WHERE} is read.
 * What is tested here is that the work is not redone and that every push-down the reordered checks guard is still taken,
 * with the answer the row pipeline gives.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9652CountPushDownDeclineCostTest extends TestHelper {
  private static final int VERTICES = 2_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG");
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
    final Random random = new Random(9652);
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.newVertex("V").set("a", (long) random.nextInt(500)).save();
      final MutableVertex[] bs = new MutableVertex[7];
      for (int i = 0; i < bs.length; i++)
        bs[i] = database.newVertex("B").set("id", i).save();
      final MutableVertex[] as = new MutableVertex[11];
      for (int i = 0; i < as.length; i++) {
        as[i] = database.newVertex("A").set("id", i).save();
        for (int k = 0; k < i % 4; k++)
          as[i].newEdge("K", bs[(i + k) % bs.length]);
      }
      for (int i = 0; i < as.length; i++)
        for (int k = 1; k <= 2; k++)
          as[i].newEdge("L", as[(i * 3 + k) % as.length]);
    });
  }

  @Test
  void aStatementWithNoWithIsReadAsItselfAndNotFoldedAgain() {
    final String query = "MATCH (v:V) WHERE v.a = 77 RETURN count(*) AS c";
    final long expected = sql("SELECT count(@rid) AS c FROM V WHERE a = 77");
    assertThat(cypher(query)).isEqualTo(expected);
    assertThat(cypher(query)).isEqualTo(expected);

    final SimpleCypherStatement statement = cachedStatement(query);
    assertThat(statement.getCountPushDownForm()).isSameAs(statement);

    assertThat(cypher(query)).isEqualTo(expected);
    assertThat(cachedStatement(query)).isSameAs(statement);
    assertThat(statement.getCountPushDownForm()).isSameAs(statement);
  }

  @Test
  void aFoldedStatementIsDerivedOnceAndStillPushedDown() {
    final String query = "MATCH (v:V) WITH v RETURN count(v) AS c";
    assertThat(cypher(query)).isEqualTo(VERTICES);
    assertThat(cypher(query)).isEqualTo(VERTICES);

    final SimpleCypherStatement statement = cachedStatement(query);
    final CypherStatement folded = statement.getCountPushDownForm();
    assertThat(folded).isNotNull().isNotSameAs(statement);
    assertThat(folded.getWithClauses()).isEmpty();

    // a second execution reads the same fold rather than folding again
    assertThat(cypher(query)).isEqualTo(VERTICES);
    assertThat(statement.getCountPushDownForm()).isSameAs(folded);

    assertThat(profile(query)).contains("TYPE COUNT OPTIMIZATION (V)");
    assertThat(profile("MATCH (v:V) WITH v RETURN max(v.a) AS c")).contains("MAX FROM INDEX V[a]");
    assertThat(cypher("MATCH (v:V) WITH v RETURN max(v.a) AS c")).isEqualTo(sql("SELECT max(a) AS c FROM V"));
  }

  @Test
  void aRefusedFoldIsRememberedAsTheStatementItself() {
    final String query = "MATCH (v:V) WITH v WHERE v.a > 10 RETURN count(*) AS c";
    final long expected = sql("SELECT count(@rid) AS c FROM V WHERE a > 10");
    assertThat(cypher(query)).isEqualTo(expected);
    assertThat(cypher(query)).isEqualTo(expected);
    final SimpleCypherStatement statement = cachedStatement(query);
    assertThat(statement.getCountPushDownForm()).isSameAs(statement);

    // the remembered refusal sends the next execution straight to the pipeline, with the same answer
    assertThat(cypher(query)).isEqualTo(expected);
    assertThat(statement.getCountPushDownForm()).isSameAs(statement);
  }

  @Test
  void theIndexAnsweredQueriesOfTheIssueAnswerAsBefore() {
    assertThat(cypher("MATCH (v:V) WHERE v.a = 77 RETURN count(*) AS c")).isEqualTo(
        cypher("MATCH (v:V) WHERE v.a = 77 RETURN sum(1) AS c")).isEqualTo(sql("SELECT count(@rid) AS c FROM V WHERE a = 77"));
    assertThat(cypher("MATCH (v:V) RETURN max(v.a) AS c")).isEqualTo(sql("SELECT max(a) AS c FROM V"));
    assertThat(cypher("MATCH (v:V) WHERE v.a > 250 RETURN min(v.a) AS c")).isEqualTo(
        sql("SELECT min(a) AS c FROM V WHERE a > 250"));
    assertThat(cypher("MATCH (v:V) WHERE v.a > 250 RETURN v.a AS c ORDER BY v.a ASC LIMIT 1")).isEqualTo(
        sql("SELECT a AS c FROM V WHERE a > 250 ORDER BY a ASC LIMIT 1"));
    assertThat(profile("MATCH (v:V) RETURN max(v.a) AS c")).contains("MAX FROM INDEX V[a]");
    assertThat(profile("MATCH (v:V) WHERE v.a > 250 RETURN min(v.a) AS c")).contains("MIN FROM INDEX V[a]");
  }

  @Test
  void patternsWithNoRelationshipKeepTheirPushDownsAndAnswers() {
    // the product of two single nodes is asked before the relationship-free shortcut
    assertThat(profile("MATCH (a:A), (b:B) RETURN count(*) AS c")).contains("COUNT CARTESIAN PRODUCT");
    assertThat(cypher("MATCH (a:A), (b:B) RETURN count(*) AS c")).isEqualTo(11L * 7);
    assertThat(cypher("MATCH (a:A), (b:B) WHERE a.id > 4 AND b.id < 3 RETURN count(*) AS c")).isEqualTo(6L * 3);

    // one node written twice is one variable: no star arm, no product, the pipeline counts it
    assertThat(cypher("MATCH (a:A), (a) RETURN count(*) AS c")).isEqualTo(
        cypher("MATCH (a:A), (a) RETURN sum(1) AS c")).isEqualTo(11L);
    assertThat(cypher("MATCH (a:A) WHERE a.id >= 3 RETURN count(*) AS c")).isEqualTo(8L);
    assertThat(cypher("MATCH (a:A {id: 3}) RETURN count(a) AS c")).isEqualTo(1L);
  }

  @Test
  void patternsWithRelationshipsStillReachTheEdgeWalkingDetectors() {
    // a star around the anchor written alone as well as in its arm
    final String star = "MATCH (a:A), (a)-[:K]->(b:B) RETURN count(*) AS c";
    assertThat(profile(star)).contains("COUNT STAR JOIN");
    assertThat(cypher(star)).isEqualTo(cypher("MATCH (a:A), (a)-[:K]->(b:B) RETURN sum(1) AS c"));

    // the relationship sits in a later, optional clause: the check reads every clause, so the star still answers it
    final String optional = "MATCH (a:A) OPTIONAL MATCH (a)-[:K]->(b:B) RETURN count(*) AS c";
    assertThat(profile(optional)).contains("COUNT STAR JOIN");
    assertThat(cypher(optional)).isEqualTo(cypher("MATCH (a:A) OPTIONAL MATCH (a)-[:K]->(b:B) RETURN sum(1) AS c"));

    final String chain = "MATCH (a:A)-[:K]->(b:B) WHERE a.id > 2 RETURN count(*) AS c";
    assertThat(profile(chain)).contains("COUNT CHAIN PATHS");
    assertThat(cypher(chain)).isEqualTo(cypher("MATCH (a:A)-[:K]->(b:B) WHERE a.id > 2 RETURN sum(1) AS c"));

    // an anti-join chain still has its WHERE read once its shape qualifies
    final String antiJoin = "MATCH (a1:A)-[:L]-(a2:A)-[:L]-(a3:A)-[:K]->(b:B) WHERE NOT (a1)-[:L]-(a3) AND a1 <> a3 "
        + "RETURN count(*) AS c";
    assertThat(profile(antiJoin)).contains("COUNT ANTI-JOIN CHAIN");
    assertThat(cypher(antiJoin)).isEqualTo(cypher(antiJoin.replace("count(*)", "sum(1)")));
  }

  private long cypher(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long sql(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private String profile(final String query) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  /**
   * The statement the engine executes for the query from now on: the cached one of its shape, its literals extracted
   * (issue #8307). The first text of a shape runs as written, so the query has to have run twice before it is the one used.
   */
  private SimpleCypherStatement cachedStatement(final String query) {
    return (SimpleCypherStatement) ((DatabaseInternal) database).getCypherStatementCache().getParameterized(query).statement()
        .statement();
  }
}
