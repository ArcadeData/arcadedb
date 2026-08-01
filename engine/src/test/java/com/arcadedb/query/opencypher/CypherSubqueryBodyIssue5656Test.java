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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5656: the body of an {@code EXISTS { }}, {@code COUNT { }} or {@code COLLECT { }}
 * subquery was held as a string, handed to {@code database.query("opencypher", ...)} as a standalone statement once
 * per outer row, and any failure was absorbed into the expression's neutral value.
 *
 * <p>Three consequences of that one decision, one section each:
 * <ol>
 *   <li>ten of the twelve validation phases stopped at the body, so a mistake rejected when written one way was
 *   accepted written one level in;</li>
 *   <li>the body was correlated by editing its text, with a keyword table and a bracket-depth counter deciding where
 *   a clause began - the source of issues #4995/#5165, #5464, #5461 and #5541;</li>
 *   <li>a body that <i>failed</i> was answered as a body that <i>did not match</i>, so a
 *   {@code WHERE NOT EXISTS { ... } CREATE ...} guard degraded into an unconditional {@code CREATE}.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSubqueryBodyIssue5656Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher",
        "CREATE (a:P {name: 'a', age: 30})-[:KNOWS {since: 2020}]->(b:P {name: 'b', age: 40})");
    database.command("opencypher", "CREATE (c:P {name: 'c', age: 50})");
  }

  // ===================================================================================================
  // 1. every validation phase reaches inside a body
  // ===================================================================================================

  /**
   * The reproducers from the issue: each is rejected when written as the query and was accepted when written as a
   * subquery body. Asserted as a pair so the point being made is the symmetry, not the individual message.
   */
  @Test
  void nestedAggregationIsRejectedInsideABodyAsOutsideOne() {
    assertRejectedBothWays("MATCH (n:P) RETURN count(count(n)) AS r",
        "MATCH (n:P) RETURN COUNT { MATCH (m:P) RETURN count(count(m)) } AS r", "NestedAggregation");
  }

  @Test
  void negativeSkipIsRejectedInsideABodyAsOutsideOne() {
    assertRejectedBothWays("MATCH (n:P) RETURN n.name AS r SKIP -1",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) RETURN m.name SKIP -1 } AS r", "NegativeIntegerArgument");
  }

  @Test
  void negativeLimitIsRejectedInsideABodyAsOutsideOne() {
    assertRejectedBothWays("MATCH (n:P) RETURN n.name AS r LIMIT -1",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) RETURN m.name LIMIT -1 } AS r", "NegativeIntegerArgument");
  }

  @Test
  void duplicateColumnNameIsRejectedInsideABodyAsOutsideOne() {
    assertRejectedBothWays("MATCH (n:P) RETURN n.name AS x, n.age AS x",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) RETURN m.name AS x, m.age AS x } AS r", "ColumnNameConflict");
  }

  @Test
  void returnStarWithNothingInScopeIsRejectedInsideABodyAsOutsideOne() {
    assertRejectedBothWays("RETURN *", "MATCH (n:P) RETURN COLLECT { RETURN * } AS r", "NoVariablesInScope");
  }

  /**
   * A repeated relationship variable asks for a relationship that is two different ones at once. The plain
   * {@code MATCH} spelling always said so; the body did not, and the path that answered it could not correlate a
   * relationship variable either, so it reported "no match".
   */
  @Test
  void repeatedRelationshipVariableIsRejectedInsideABodyAsOutsideOne() {
    assertRejectedBothWays("MATCH (a:P)-[r:KNOWS]->()<-[r:KNOWS]-() RETURN a",
        "MATCH (n:P) WHERE EXISTS { MATCH (a:P)-[r:KNOWS]->()<-[r:KNOWS]-() RETURN a } RETURN n",
        "RelationshipUniquenessViolation");
  }

  /** The same phase list, applied to the body of each of the four subquery forms. */
  @Test
  void everySubqueryFormHasItsBodyValidated() {
    for (final String query : List.of(
        "MATCH (n:P) WHERE EXISTS { MATCH (m:P) RETURN count(count(m)) > 0 } RETURN n",
        "MATCH (n:P) RETURN COUNT { MATCH (m:P) RETURN count(count(m)) } AS r",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) RETURN count(count(m)) } AS r",
        "MATCH (n:P) CALL { MATCH (m:P) RETURN count(count(m)) AS c } RETURN c"))
      assertThatThrownBy(() -> explain(query)).as(query)
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("NestedAggregation");
  }

  /** A body nested inside another body is a body too. */
  @Test
  void aBodyNestedInsideAnotherBodyIsValidated() {
    assertThatThrownBy(() -> explain("MATCH (n:P) WHERE EXISTS { MATCH (m:P) "
        + "WHERE COUNT { MATCH (o:P) RETURN o.name SKIP -1 } > 0 RETURN m } RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NegativeIntegerArgument");
  }

  /**
   * {@code validateUnion} returns early, so the two checks that are about the UNION itself - the column names of its
   * branches and the mixing of UNION with UNION ALL - never ran on a UNION written as a subquery body.
   */
  @Test
  void aUnionBodyGetsTheUnionChecks() {
    assertRejectedBothWays("RETURN 1 AS a UNION RETURN 2 AS b",
        "MATCH (n:P) CALL { RETURN 1 AS a UNION RETURN 2 AS b } RETURN n", "DifferentColumnsInUnion");
    assertThatThrownBy(
        () -> explain("MATCH (n:P) CALL { RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a } RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidClauseComposition");
  }

  /** Each branch of a UNION body is a scope of its own and gets the phase list on its own. */
  @Test
  void eachUnionBranchOfABodyIsValidated() {
    assertThatThrownBy(() -> explain("MATCH (n:P) CALL { RETURN 1 AS x "
        + "UNION MATCH (m:P) RETURN count(count(m)) AS x } RETURN x"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NestedAggregation");
  }

  /** Widening what is rejected must not reject what is valid. */
  @Test
  void validBodiesStillParse() {
    for (final String query : List.of(
        "MATCH (n:P) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m:P) WHERE m.age > n.age RETURN m } RETURN n",
        "MATCH (n:P) RETURN COUNT { MATCH (m:P) RETURN count(m) } AS r",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) RETURN m.name ORDER BY m.name SKIP 1 LIMIT 1 } AS r",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) RETURN m.name AS x, m.age AS y } AS r",
        "MATCH (n:P) CALL { MATCH (m:P) RETURN m.name AS x UNION MATCH (o:P) RETURN o.name AS x } RETURN x",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) WITH m RETURN * } AS r"))
      assertThatCode(() -> explain(query)).as(query).doesNotThrowAnyException();
  }

  // ===================================================================================================
  // 2. the body runs from its AST, not from its text
  // ===================================================================================================

  /**
   * What executes is the parsed body seeded with the outer row, so the body is never handed to the query engine as a
   * statement of its own. The statement cache is the witness: it holds the outer query and nothing else.
   * <p>
   * The correlated text used to be a second entry in it. (It was one entry, not one per row: the outer row's values
   * went into parameters, so the rewritten text was the same for every row and both caches hit - the issue's
   * "re-parsed once per outer row" reading of the cost is not what the code did.)
   */
  @Test
  void theBodyIsNeverHandedToTheQueryEngineAsText() {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getCypherStatementCache().clear();

    final String query = "MATCH (n:P) RETURN n.name AS name, COUNT { MATCH (n)-[:KNOWS]->(m) } AS c ORDER BY name";
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }

    assertThat(db.getCypherStatementCache().size()).isEqualTo(1);
    assertThat(db.getCypherStatementCache().contains(query)).isTrue();
  }

  /**
   * Two bodies the text scan could not read, both of them read-only, both of them rejected as updates before.
   * {@code WITH 1 AS set RETURN set} is lexically indistinguishable from {@code SET n.x = 1}, and a keyword inside a
   * comment is not a clause. The parse tree knows which is which; a keyword table cannot.
   */
  @Test
  void aReadOnlyBodyThatOnlyLooksLikeAWriteIsAccepted() {
    assertThat(collect("MATCH (n:P {name:'a'}) RETURN COLLECT { WITH 1 AS set RETURN set } AS r"))
        .containsExactly(1L);
    assertThat(collect("MATCH (n:P {name:'a'}) RETURN COLLECT { MATCH (m:P) /* CREATE (x:Q) */ RETURN m.name "
        + "ORDER BY m.name } AS r")).containsExactly("a", "b", "c");
  }

  /** A body that really writes is still rejected, in all three forms. */
  @Test
  void aWritingBodyIsStillRejected() {
    for (final String query : List.of(
        "MATCH (n:P) WHERE EXISTS { CREATE (x:Q) RETURN x } RETURN n",
        "MATCH (n:P) RETURN COUNT { MATCH (m:P) SET m.age = 1 RETURN m } AS r",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) DETACH DELETE m RETURN 1 } AS r",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) REMOVE m.age RETURN 1 } AS r"))
      assertThatThrownBy(() -> explain(query)).as(query)
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("InvalidClauseComposition");
  }

  /**
   * The correlation itself, in the shapes the text rewriter had to reason about: an existing {@code WHERE} the
   * injected condition had to be ANDed into without capturing an {@code OR} (#4995/#5165), an inline pattern
   * predicate that is not the clause-level {@code WHERE} (#5464), a leading clause that is not {@code MATCH} (#5461),
   * and a scalar outer variable that came in through a synthesized {@code WITH}.
   */
  @Test
  void correlationStillAnswersTheSameWayInEveryBodyShape() {
    assertThat(names("MATCH (n:P) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:P) } RETURN n.name AS name ORDER BY name"))
        .containsExactly("a");
    assertThat(names("MATCH (n:P) WHERE EXISTS { MATCH (n)-[r:KNOWS]->(m:P) WHERE m.age = 40 OR m.age = 99 RETURN m } "
        + "RETURN n.name AS name ORDER BY name")).containsExactly("a");
    assertThat(names("MATCH (n:P) WHERE EXISTS { (n)-[r:KNOWS WHERE r.since = 2020]->(:P) } "
        + "RETURN n.name AS name ORDER BY name")).containsExactly("a");
    assertThat(names("MATCH (n:P) WHERE EXISTS { (n)-[r:KNOWS WHERE r.since = 1999]->(:P) } "
        + "RETURN n.name AS name ORDER BY name")).isEmpty();
    assertThat(names("MATCH (n:P) WHERE COUNT { UNWIND [1, 2] AS y MATCH (n)-[:KNOWS]->(:P) RETURN y } = 2 "
        + "RETURN n.name AS name ORDER BY name")).containsExactly("a");

    // A scalar outer variable: `age` is a number on the row, not a graph entity.
    assertThat(names("MATCH (n:P) WITH n, n.age AS age WHERE COUNT { MATCH (m:P) WHERE m.age > age RETURN m } = 1 "
        + "RETURN n.name AS name ORDER BY name")).containsExactly("b");
  }

  /**
   * The count push-downs answer a {@code RETURN count(*)} from the schema and the CSR arrays, reading the statement's
   * patterns and never the incoming rows. Running a body from a seed row puts an incoming row in front of them for
   * the first time, so a correlated body has to be kept off that path - otherwise a count over one bound vertex is
   * answered with the count over every vertex in the graph. A body that binds nothing from outside keeps it, since
   * there is no name it could have taken from the enclosing query.
   */
  @Test
  void aCorrelatedCountIsNotAnsweredByTheGlobalCountPushDown() {
    database.command("opencypher", "CREATE (q1:Q {k:1})-[:LINKS]->(q2:Q {k:2})-[:LINKS]->(q3:Q {k:3})");

    // Correlated: q1 has one outgoing LINKS, the graph has two.
    assertThat(collect("MATCH (q:Q {k:1}) RETURN COLLECT { MATCH (q)-[:LINKS]->(x:Q) RETURN count(*) } AS r"))
        .containsExactly(1L);
    // Uncorrelated, and written with no outer row at all, so the seed binds nothing: the whole graph is the answer.
    assertThat(collect("RETURN COLLECT { MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) } AS r")).containsExactly(2L);
  }

  /**
   * A body is pulled through the step chain in batches of 100, and what {@code COUNT} and {@code COLLECT} do with it
   * is drive the result set to exhaustion. Nothing caps that at the first batch - {@code hasNext()} re-fetches - but
   * nothing pinned it either, and the body reaching this path at all is what this issue changed. A body wider than
   * one batch is the assertion that a future change to the pull contract cannot silently truncate one.
   */
  @Test
  void aBodyWiderThanOnePullBatchIsNotTruncated() {
    database.command("opencypher", "UNWIND range(1, 250) AS i CREATE (:Big {k: i})");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:P {name: 'a'}) RETURN COUNT { MATCH (b:Big) } AS c, "
            + "COLLECT { MATCH (b:Big) RETURN b.k } AS l")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(250L);
      assertThat((List<Object>) row.getProperty("l")).hasSize(250);
    }
  }

  /**
   * The body used to be handed to {@code database.query()}, which runs the physical planner; it now runs through the
   * step chain with no physical plan, the way a {@code CALL { }} body always has. The two engines have to agree on
   * more than counting rows, so this drives a body through the shapes that lean on the planner: a lookup an index
   * backs, and an {@code ORDER BY ... LIMIT} that has to sort before it truncates rather than after.
   */
  @Test
  void aBodyThatLeansOnThePlannerAnswersTheSameWay() {
    // Declared through SQL so the property has a type to index: a Cypher CREATE alone leaves it schema-less.
    database.command("sql", "CREATE VERTEX TYPE Idx");
    database.command("sql", "CREATE PROPERTY Idx.k INTEGER");
    database.command("sql", "CREATE INDEX ON Idx (k) UNIQUE");
    database.command("opencypher", "UNWIND range(1, 250) AS i CREATE (:Idx {k: i, tag: 'x'})");

    // Index-backed equality lookup inside a body.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:P {name: 'a'}) RETURN COUNT { MATCH (x:Idx {k: 7}) } AS c, "
            + "COLLECT { MATCH (x:Idx) WHERE x.k = 7 RETURN x.tag } AS l")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
      assertThat((List<Object>) row.getProperty("l")).containsExactly("x");
    }

    // ORDER BY ... LIMIT has to sort the whole body and then truncate: the top 3 of 250, not the first 3 found.
    assertThat(collect("MATCH (n:P {name: 'a'}) RETURN COLLECT { MATCH (x:Idx) RETURN x.k ORDER BY x.k DESC LIMIT 3 } "
        + "AS r")).containsExactly(250, 249, 248);
    // SKIP travels with it.
    assertThat(collect("MATCH (n:P {name: 'a'}) RETURN COLLECT { MATCH (x:Idx) RETURN x.k ORDER BY x.k ASC "
        + "SKIP 2 LIMIT 2 } AS r")).containsExactly(3, 4);
    // DISTINCT collapses inside the body, not after it.
    assertThat(collect("MATCH (n:P {name: 'a'}) RETURN COLLECT { MATCH (x:Idx) RETURN DISTINCT x.tag } AS r"))
        .containsExactly("x");
  }

  @Test
  void countAndCollectStillProduceTheSameValues() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:P {name: 'a'}) RETURN COUNT { (n)-[:KNOWS]->(:P) } AS c, "
            + "COLLECT { MATCH (n)-[:KNOWS]->(m:P) RETURN m.name } AS names")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
      assertThat((List<Object>) row.getProperty("names")).containsExactly("b");
    }
  }

  // ===================================================================================================
  // 3. a body that fails is reported, not answered
  // ===================================================================================================

  /**
   * {@code abs(m.name)} passes the parse-time check - the argument is a property, not a literal - and fails on the
   * row. That failure used to be absorbed: {@code EXISTS} answered false, {@code COUNT} zero, {@code COLLECT} empty,
   * with nothing above {@code FINE} to say so. It is now the same error the enclosing query would raise.
   */
  @Test
  void aFailingBodyRaisesInsteadOfAnsweringItsNeutralValue() {
    final String expected = "abs()";
    assertThatThrownBy(() -> names("MATCH (n:P) WHERE abs(n.name) > 0 RETURN n.name AS name"))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining(expected);

    for (final String query : List.of(
        "MATCH (n:P) WHERE EXISTS { MATCH (m:P) WHERE abs(m.name) > 0 RETURN m } RETURN n.name AS name",
        "MATCH (n:P) RETURN COUNT { MATCH (m:P) WHERE abs(m.name) > 0 RETURN m } AS c",
        "MATCH (n:P) RETURN COLLECT { MATCH (m:P) WHERE abs(m.name) > 0 RETURN m.name } AS c"))
      assertThatThrownBy(() -> names(query)).as(query)
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining(expected);
  }

  /**
   * Why it is load-bearing. A de-duplicating guard reads {@code NOT EXISTS { ... }}: absorbing the body's failure
   * into {@code false} makes {@code NOT EXISTS} answer {@code true}, and the guard becomes an unconditional
   * {@code CREATE}. The write must not happen.
   */
  @Test
  void aFailingGuardDoesNotTurnIntoAnUnconditionalWrite() {
    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (n:P {name: 'a'}) WHERE NOT EXISTS { MATCH (m:P) WHERE abs(m.name) > 0 RETURN m } "
            + "CREATE (:Guarded {name: 'x'})"))
        .isInstanceOf(CommandSemanticException.class);

    try (final ResultSet rs = database.query("opencypher", "MATCH (g:Guarded) RETURN count(g) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(0L);
    }
  }

  /** A body over a label no type declares still matches nothing rather than failing: that is not an error. */
  @Test
  void anEmptyBodyIsStillAnEmptyAnswer() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:P {name: 'a'}) RETURN COUNT { MATCH (m:NoSuchLabel) RETURN m } AS c, "
            + "COLLECT { MATCH (m:NoSuchLabel) RETURN m.name } AS l")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(0L);
      assertThat((List<Object>) row.getProperty("l")).isEmpty();
    }
    assertThat(names("MATCH (n:P) WHERE EXISTS { MATCH (n)-[:NOSUCH]->(m) } RETURN n.name AS name")).isEmpty();
  }

  // ===================================================================================================

  private void assertRejectedBothWays(final String plainQuery, final String queryWithBody, final String errorCode) {
    assertThatThrownBy(() -> explain(plainQuery)).as(plainQuery)
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining(errorCode);
    assertThatThrownBy(() -> explain(queryWithBody)).as(queryWithBody)
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining(errorCode);
  }

  /** Parses and plans the query without executing it, so what is asserted happens before any row is touched. */
  private void explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  private List<Object> collect(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("r");
    }
  }
}
