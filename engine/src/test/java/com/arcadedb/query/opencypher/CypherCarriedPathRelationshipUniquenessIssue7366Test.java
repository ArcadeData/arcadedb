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
 * Regression tests for GitHub issue #7366: a named path left in scope silenced the {@code MATCH} that came
 * after it. The reporter's two queries differ by a {@code collect}/{@code UNWIND} round trip that restores
 * exactly the same bindings, and only the one that carried the path variable across the {@code MATCH}
 * returned no rows.
 * <p>
 * Cypher scopes relationship uniqueness to a single MATCH clause: within one clause every relationship pattern
 * binds a distinct edge, and an edge some earlier clause bound is not a relationship of this clause's pattern.
 * A named path is the sharpest way to hit a scope stated by exclusion, because a single variable carries every
 * edge of the pattern that created it: the reporter's {@code MATCH} had to rebind two of {@code p0}'s three
 * edges, so leaving {@code p0} in scope took away the only match there was. The {@code collect}/{@code UNWIND}
 * form answered correctly for no better reason than that its projections dropped {@code p0} on the way through
 * - and, before the fix, that a {@code WITH} was one of the four clause kinds whose output names the old
 * exclusion list happened to carry, which is why the reporter's variant with an intervening {@code WITH} was
 * never affected while the {@code CALL} one was.
 * <p>
 * The fix is the one issue #7165 made - the scope is now the clause's own variables, so nothing a previous
 * clause bound is consulted at all - and these tests pin the named-path half of it, which
 * {@link CypherClauseScopedRelationshipUniquenessIssue7165Test} does not cover: there the carried value is a
 * single edge under its own name, here it is a whole path under one name, reached through the
 * {@code TraversalPath} branch of the same check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCarriedPathRelationshipUniquenessIssue7366Test extends TestHelper {

  /**
   * The graph of the report: a directed path whose {@code :C} vertex has one outgoing edge to {@code n0} and
   * one to the {@code :D} vertex, so the MATCH below has exactly one valid match.
   */
  private static final String CREATE_NAMED_PATH = """
      CREATE p0 = (:A {id: 1}) -[:E]-> (n0:B {klist: [], k7: 'q'}) <-[:R]- (:C {k4: 'f', k8: true}) -[:E2]-> (:D {klist: []})
      """;

  /** Rebinds two of the three edges the named path holds: the {@code :E2} one as {@code r4}, the {@code :R} one anonymously. */
  private static final String MATCH_BACK_OVER_THE_PATH = """
      MATCH (n5:D {klist: []}) <-[r4]- (n4 {k4: 'f', k8: true}) -[]-> (n1)
      RETURN labels(n4)[0] + '/' + labels(n5)[0] + '/' + n1.k7 AS binding
      """;

  /** The reporter's LEFT query: the named path stays in scope across the subquery and the MATCH that follows. */
  @Test
  void aNamedPathInScopeDoesNotBlockTheFollowingMatch() {
    final List<Object> bindings = commandColumn(CREATE_NAMED_PATH + """
        CALL (*) { RETURN n0 AS n1 OFFSET 0 }
        """ + MATCH_BACK_OVER_THE_PATH, "binding");

    assertThat(bindings)
        .as("the :C vertex reaches n1 and the :D vertex by one edge each, so the pattern has one match")
        .containsExactly("C/D/q");
  }

  /**
   * The subquery was never needed to show it: the {@code CREATE} that binds the path can be followed straight
   * by the {@code MATCH}. The smallest shape there is, and the one a regression would reintroduce first.
   */
  @Test
  void aMatchStraightAfterTheCreateThatBoundThePathIsEnoughToShowIt() {
    assertThat(commandColumn(CREATE_NAMED_PATH + MATCH_BACK_OVER_THE_PATH.replace("(n1)", "(n0)")
        .replace("n1.k7", "n0.k7"), "binding")).containsExactly("C/D/q");
  }

  /**
   * The reporter's RIGHT query and their control, which differ from the first query above by nothing that can
   * change a row: the first materializes the single row into a map and restores the same bindings from it, the
   * second replaces the subquery with the identity projection it amounts to. Both answered correctly while the
   * direct form did not, and that discrepancy is the whole of the report - so they are pinned against it rather
   * than against a literal.
   */
  @Test
  void theMaterializedAndProjectedFormsAnswerTheSame() {
    final String direct = CREATE_NAMED_PATH + """
        CALL (*) { RETURN n0 AS n1 OFFSET 0 }
        """ + MATCH_BACK_OVER_THE_PATH;
    final String materialized = CREATE_NAMED_PATH + """
        CALL (*) { RETURN n0 AS n1 OFFSET 0 }
        WITH {n0: n0, n1: n1} AS row
        WITH collect(row) AS rows
        UNWIND rows AS row
        WITH row.n0 AS n0, row.n1 AS n1
        """ + MATCH_BACK_OVER_THE_PATH;
    final String projected = CREATE_NAMED_PATH + """
        WITH n0 AS n1
        """ + MATCH_BACK_OVER_THE_PATH;

    assertThat(commandColumn(direct, "binding")).isEqualTo(commandColumn(materialized, "binding"));
    assertThat(commandColumn(direct, "binding")).isEqualTo(commandColumn(projected, "binding"));
    assertThat(commandColumn(direct, "binding")).containsExactly("C/D/q");
  }

  /**
   * A {@code WITH} that keeps the path answered correctly even before the fix, because the old exclusion list
   * carried what a WITH bound. It is the control the reporter's failing form has to agree with, and it has to
   * keep answering this way now that the rule reaches it by a different route.
   */
  @Test
  void aWithThatKeepsThePathAnswersTheSameWay() {
    assertThat(commandColumn(CREATE_NAMED_PATH + """
        WITH p0, n0 AS n1
        """ + MATCH_BACK_OVER_THE_PATH, "binding")).containsExactly("C/D/q");
  }

  /** A path a previous {@code MATCH} bound is no more one of the next clause's relationships than a created one is. */
  @Test
  void aPathBoundByAnEarlierMatchDoesNotBlockTheFollowingMatchEither() {
    database.transaction(() -> database.command("opencypher", CREATE_NAMED_PATH.replace("p0 = ", "")));

    assertThat(queryColumn("""
        MATCH p1 = (:A)-[:E]->(n0:B)<-[:R]-(:C)-[:E2]->(:D)
        WITH p1, n0 AS n1
        """ + MATCH_BACK_OVER_THE_PATH, "binding")).containsExactly("C/D/q");
  }

  /**
   * The guard against fixing this the other way round. Uniqueness has to keep firing for a path the MATCH
   * itself binds: {@code p} and the hop beside it belong to one clause, so they must take different edges,
   * and with only one edge in the graph there is nothing to return. Split across two clauses the same two
   * patterns are free to take it twice.
   */
  @Test
  void aPathTheSameClauseBindsStillTakesItsEdgesOutOfReach() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})-[:E]->(:B {id: 2})"));

    assertThat(queryColumn("MATCH p = (x:A)-[:E]->(y:B), (x)-[r]->(z) RETURN z.id AS id", "id"))
        .as("r would have to be the edge p already holds")
        .isEmpty();
    assertThat(queryColumn("MATCH p = (x:A)-[:E]->(y:B), (x)-[r*1..1]->(z) RETURN z.id AS id", "id"))
        .as("a variable-length hop reads the same scope")
        .isEmpty();
    assertThat(queryColumn("MATCH p = (x:A)-[:E]->(y:B), (x)((u)-[:E]->(v)){1,1}(z) RETURN z.id AS id", "id"))
        .as("and so does a quantified path pattern")
        .isEmpty();

    assertThat(queryColumn("""
        MATCH p = (x:A)-[:E]->(y:B)
        MATCH (x)-[r]->(z)
        RETURN z.id AS id""", "id"))
        .as("two clauses, so the second is free to bind the edge the first one's path holds")
        .containsExactly(2);
  }

  /** A carried path leaves a variable-length hop free to walk the edges it holds. */
  @Test
  void aCarriedPathDoesNotBlockAVariableLengthHop() {
    assertThat(commandColumn(CREATE_NAMED_PATH + """
        CALL (*) { RETURN n0 AS n1 OFFSET 0 }
        MATCH (n1)<-[r*1..1]-(w)
        RETURN labels(w)[0] AS label""", "label"))
        .as("both edges into n0 are p0's, and both are still reachable")
        .containsExactlyInAnyOrder("A", "C");
  }

  /** Nor a quantified path pattern, which does its own isomorphism bookkeeping off the same scope. */
  @Test
  void aCarriedPathDoesNotBlockAQuantifiedPathPattern() {
    assertThat(commandColumn(CREATE_NAMED_PATH + """
        CALL (*) { RETURN n0 AS n1 OFFSET 0 }
        MATCH (c:C)((u)-[:E2]->(v)){1,1}(d:D)
        RETURN labels(d)[0] AS label""", "label"))
        .as("the group walks p0's :E2 edge")
        .containsExactly("D");
  }

  /** {@code command} rather than {@code query}: the reporter's queries write before they read. */
  private List<Object> commandColumn(final String cypher, final String column) {
    final List<Object> values = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet result = database.command("opencypher", cypher)) {
        while (result.hasNext())
          values.add(result.next().getProperty(column));
      }
    });
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
