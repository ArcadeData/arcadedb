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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issue #7165: a {@code MATCH} written straight after {@code CALL ... YIELD}
 * returned no rows at all, and inserting a {@code WITH} that changed nothing else made the same query
 * return the expected ones.
 * <p>
 * Cypher scopes relationship uniqueness to a single MATCH clause: within one clause every relationship
 * pattern binds a distinct edge, and an edge some earlier clause bound is not a relationship of this
 * clause's pattern. The planner used to decide that by exclusion - every edge the incoming row carried
 * counted against the pattern unless its variable name was on a list of names bound earlier - and only
 * {@code MATCH}, {@code WITH}, {@code UNWIND} and {@code LOAD CSV} ever put a name on that list. So a
 * {@code CALL ... YIELD relationship AS rel} handed the following MATCH an edge under a name the list did
 * not carry, the MATCH read it as one of its own relationships, and the uniqueness rule rejected the only
 * edge that could have matched. Silently: no error, just an empty result.
 * <p>
 * {@code CALL { }} subqueries, {@code CREATE} and {@code MERGE} lost rows the same way, so the tests below
 * cover them too. The scope is now stated positively - the clause's own variables - which is a property of
 * the clause rather than of everything that came before it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherClauseScopedRelationshipUniquenessIssue7165Test extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE P IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE LINK IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY LINK.uuid IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY LINK.fact IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY LINK.srcUuid IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON LINK (fact) FULL_TEXT");
    });

    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {uuid:'a'})-[:LINK {uuid:'r1', fact:'alpha beta', srcUuid:'a'}]->(b:P {uuid:'b'})"));
  }

  /**
   * The reporter's query verbatim. The {@code WITH} variant is the control: the two forms differ by nothing
   * but that clause, so they have to answer the same.
   */
  @Test
  void matchAfterCallYieldMatchesTheYieldedRelationship() {
    final String withoutWith = """
        CALL db.index.fulltext.queryRelationships('LINK[fact]', 'alpha')
        YIELD relationship AS rel, score
        MATCH (n)-[e:LINK]->(m) WHERE e.uuid = rel.uuid
        RETURN e.uuid AS uuid""";
    final String withWith = """
        CALL db.index.fulltext.queryRelationships('LINK[fact]', 'alpha')
        YIELD relationship AS rel, score
        WITH rel, score
        MATCH (n)-[e:LINK]->(m) WHERE e.uuid = rel.uuid
        RETURN e.uuid AS uuid""";

    assertThat(queryColumn(withoutWith, "uuid")).containsExactly("r1");
    assertThat(queryColumn(withoutWith, "uuid"))
        .as("an interposed WITH changes nothing else about the query, so it cannot change the rows")
        .isEqualTo(queryColumn(withWith, "uuid"));
  }

  /**
   * The predicate is not what failed: the MATCH dropped every row even when it never mentioned the yielded
   * relationship, because the uniqueness rule rejected the edge before any WHERE ran.
   */
  @Test
  void matchAfterCallYieldIsUnaffectedByAnUnrelatedYieldedRelationship() {
    final List<Object> uuids = queryColumn("""
        CALL db.index.fulltext.queryRelationships('LINK[fact]', 'alpha')
        YIELD relationship AS rel, score
        MATCH (n)-[e:LINK]->(m)
        RETURN e.uuid AS uuid""", "uuid");

    assertThat(uuids).containsExactly("r1");
  }

  /**
   * {@code YIELD *} and a bare {@code CALL} name no aliases, so the planner reads the procedure's declared
   * output fields instead. Both shapes have to chain into a following MATCH the same way an explicit YIELD does.
   */
  @Test
  void yieldStarAndABareCallChainIntoAFollowingMatchToo() {
    assertThat(queryColumn("""
        CALL db.index.fulltext.queryRelationships('LINK[fact]', 'alpha')
        YIELD *
        MATCH (n)-[e:LINK]->(m)
        RETURN e.uuid AS uuid""", "uuid")).containsExactly("r1");

    assertThat(queryColumn("CALL db.labels() MATCH (n)-[e:LINK]->(m) RETURN e.uuid AS uuid", "uuid"))
        .containsExactly("r1");
  }

  /**
   * A procedure name the Cypher procedure registry does not know - here an ArcadeDB SQL function reached
   * through {@code CALL} - cannot reintroduce the bug. Stating the uniqueness scope as the MATCH clause's own
   * variables is what makes that true: the rule no longer consults what earlier clauses bound, so a name the
   * planner cannot resolve to declared output fields is simply not one of this clause's relationships.
   */
  @Test
  void aProcedureTheRegistryDoesNotKnowCannotReintroduceTheBug() {
    final List<Object> bare = new ArrayList<>();
    final List<Object> yielded = new ArrayList<>();
    database.transaction(() -> {
      bare.addAll(commandColumn("CALL math.abs(-3) MATCH (n)-[e:LINK]->(m) RETURN e.uuid AS uuid", "uuid"));
      yielded.addAll(commandColumn(
          "CALL math.abs(-3) YIELD value AS v MATCH (n)-[e:LINK]->(m) RETURN e.uuid AS uuid", "uuid"));
    });

    assertThat(bare).containsExactly("r1");
    assertThat(yielded).containsExactly("r1");
  }

  /**
   * The other half of registering a CALL's outputs in the planner's scope: a WHERE that reads a yielded name
   * is now pushed into the following MATCH's scan instead of being evaluated on every row it produces. Asserted
   * on the plan, because the rows alone are the same either way - which is exactly why a regression here would
   * otherwise go unnoticed.
   */
  @Test
  void aPredicateReadingAYieldedVariableIsPushedIntoTheMatchScan() {
    final String query = """
        CALL db.index.fulltext.queryRelationships('LINK[fact]', 'alpha')
        YIELD relationship AS rel, score
        MATCH (n:P) WHERE n.uuid = rel.srcUuid
        RETURN n.uuid AS uuid""";

    assertThat(queryColumn(query, "uuid")).containsExactly("a");

    final String plan;
    try (final ResultSet result = database.query("opencypher", "EXPLAIN " + query)) {
      plan = result.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }

    assertThat(plan)
        .as("the scan itself must carry the predicate, not just the FILTER behind it")
        .contains("MATCH NODE (n:P) [filter: n.uuid = rel.srcUuid]");
  }

  /** What a {@code CALL { }} subquery returns reaches the following MATCH's scan the same way a YIELD does. */
  @Test
  void aPredicateReadingASubqueryOutputIsPushedIntoTheMatchScanToo() {
    final String query = """
        CALL { MATCH (z)-[q:LINK]->(w) RETURN q.srcUuid AS src }
        MATCH (n:P) WHERE n.uuid = src
        RETURN n.uuid AS uuid""";

    assertThat(queryColumn(query, "uuid")).containsExactly("a");

    try (final ResultSet result = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(result.getExecutionPlan().orElseThrow().prettyPrint(0, 2))
          .contains("MATCH NODE (n:P) [filter: n.uuid = src]");
    }
  }

  /** A yielded scalar was never the problem, and must keep working: the control for the two tests above. */
  @Test
  void matchAfterCallYieldingAScalarStillChainsOnTheCallRows() {
    final List<Object> uuids = queryColumn(
        "CALL db.labels() YIELD label MATCH (n)-[e:LINK]->(m) RETURN e.uuid AS uuid", "uuid");

    assertThat(uuids).containsExactly("r1");
  }

  /**
   * A quantified path pattern does its own relationship-isomorphism bookkeeping in {@code QuantifiedPathStep},
   * off the same scope, so it lost rows to a preceding CALL exactly as a plain relationship pattern did.
   */
  @Test
  void aQuantifiedPathPatternAfterCallYieldMatchesTheYieldedRelationship() {
    final String afterCall = """
        CALL db.index.fulltext.queryRelationships('LINK[fact]', 'alpha')
        YIELD relationship AS rel
        MATCH (a:P)((x:P)-[:LINK]->(y:P)){1,2}(b:P)
        RETURN b.uuid AS uuid""";

    assertThat(queryColumn(afterCall, "uuid"))
        .as("the same rows the pattern produces on its own")
        .isEqualTo(queryColumn("MATCH (a:P)((x:P)-[:LINK]->(y:P)){1,2}(b:P) RETURN b.uuid AS uuid", "uuid"));
    assertThat(queryColumn(afterCall, "uuid")).containsExactly("b");
  }

  /** {@code CALL { }} exports its relationship the same way an in-query CALL yields one. */
  @Test
  void matchAfterASubqueryExportingARelationshipMatchesThatRelationship() {
    final List<Object> uuids = queryColumn("""
        CALL { MATCH (a)-[q:LINK]->(b) RETURN q }
        MATCH (n)-[e:LINK]->(m)
        RETURN e.uuid AS uuid""", "uuid");

    assertThat(uuids).containsExactly("r1");
  }

  /**
   * A relationship a {@code CREATE} just bound belongs to the CREATE, not to the MATCH that follows, so the
   * MATCH must still be able to match it.
   */
  @Test
  void matchAfterCreateMatchesTheCreatedRelationship() {
    final List<Object> uuids = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet result = database.command("opencypher", """
          CREATE (a:P {uuid:'c'})-[r:LINK {uuid:'r2'}]->(b:P {uuid:'d'})
          MATCH (n)-[e:LINK]->(m)
          RETURN e.uuid AS uuid""")) {
        while (result.hasNext())
          uuids.add(result.next().getProperty("uuid"));
      }
    });

    assertThat(uuids).containsExactlyInAnyOrder("r1", "r2");
  }

  /** Same for a relationship {@code MERGE} bound, whether it created it or found it. */
  @Test
  void matchAfterMergeMatchesTheMergedRelationship() {
    final List<Object> uuids = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet result = database.command("opencypher", """
          MERGE (a:P {uuid:'a'})-[r:LINK {uuid:'r1'}]->(b:P {uuid:'b'})
          MATCH (n)-[e:LINK]->(m)
          RETURN e.uuid AS uuid""")) {
        while (result.hasNext())
          uuids.add(result.next().getProperty("uuid"));
      }
    });

    assertThat(uuids)
        .as("MERGE found the existing edge, and the following MATCH is a clause of its own")
        .containsExactly("r1");
  }

  /**
   * A relationship variable an earlier clause bound is still one of this clause's relationship patterns when
   * the clause names it again, so the clause's other patterns must be distinct from it. The clause's freshly
   * bound variables alone cannot say that - they deliberately omit an already-bound name, since OPTIONAL MATCH
   * nulls what it lists and must not null a carried binding - so the scope also carries every relationship
   * variable the clause writes.
   * <p>
   * Without it {@code su} could rebind the very edge {@code ru} carries, and the answer depended on the order
   * the patterns happened to be written in. Both spellings of the shape are checked: the {@code CALL ... YIELD}
   * one this issue is about, and the {@code WITH} one, which behaved the same way before the fix.
   */
  @Test
  void aRelationshipVariableReusedFromAnEarlierClauseStillExcludesItsEdge() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (c:P {uuid:'c'})-[:LINK {uuid:'r2', fact:'alpha gamma', srcUuid:'c'}]->(d:P {uuid:'d'})"));

    final String afterCall = """
        CALL db.index.fulltext.queryRelationships('LINK[fact]', 'alpha')
        YIELD relationship AS r
        MATCH (a)-[r:LINK]->(b), (c)-[s:LINK]->(d)
        RETURN r.uuid + '/' + s.uuid AS pair""";
    final String afterWith = """
        MATCH (x)-[r:LINK]->(y)
        WITH r
        MATCH (a)-[r:LINK]->(b), (c)-[s:LINK]->(d)
        RETURN r.uuid + '/' + s.uuid AS pair""";

    assertThat(queryColumn(afterCall, "pair"))
        .as("s must never bind the edge r already carries")
        .containsExactlyInAnyOrder("r1/r2", "r2/r1");
    assertThat(queryColumn(afterWith, "pair")).containsExactlyInAnyOrder("r1/r2", "r2/r1");
  }

  /**
   * The rule the fix must not weaken: two relationship patterns of the SAME clause still bind distinct
   * edges, so a two-hop pattern cannot walk the single edge out and back again.
   */
  @Test
  void twoHopsOfTheSameClauseStillCannotShareOneRelationship() {
    final List<Object> uuids = queryColumn(
        "MATCH (n)-[e1:LINK]-(m)-[e2:LINK]-(o) RETURN e1.uuid AS uuid", "uuid");

    assertThat(uuids).as("only one LINK edge exists, so no pair of distinct relationships can match").isEmpty();
  }

  /** The same rule across a comma, where the two patterns are separate parts of one clause. */
  @Test
  void twoPatternPartsOfTheSameClauseStillCannotShareOneRelationship() {
    final List<Object> uuids = queryColumn(
        "MATCH (n)-[e1:LINK]->(m), (o)-[e2:LINK]->(p) RETURN e1.uuid AS uuid", "uuid");

    assertThat(uuids).isEmpty();
  }

  /** And a variable-length hop must still refuse an edge another hop of its own path already bound. */
  @Test
  void variableLengthHopStillCannotReuseARelationshipOfItsOwnPath() {
    final List<Object> uuids = queryColumn(
        "MATCH (n)-[e:LINK]->(m)-[rest:LINK*1..3]->(o) RETURN e.uuid AS uuid", "uuid");

    assertThat(uuids).isEmpty();
  }

  /** {@code command} rather than {@code query}: an unresolved procedure name reads as possibly-writing. */
  private List<Object> commandColumn(final String cypher, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet result = database.command("opencypher", cypher)) {
      while (result.hasNext())
        values.add(result.next().getProperty(column));
    }
    return values;
  }

  private List<Object> queryColumn(final String cypher, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet result = database.query("opencypher", cypher)) {
      while (result.hasNext()) {
        final Result row = result.next();
        values.add(row.getProperty(column));
      }
    }
    return values;
  }
}
