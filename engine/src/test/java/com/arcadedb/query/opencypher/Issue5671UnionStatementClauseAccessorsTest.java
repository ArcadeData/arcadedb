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
import com.arcadedb.query.OperationType;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.executor.CypherVariableUsage;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5671: {@code UnionStatement} used to answer ten {@code CypherStatement} clause accessors by
 * silently delegating to its <b>first</b> branch, with nothing to indicate that branches 2..n were
 * dropped. Nine of them - every one except {@code getReturnClause()}, which is safe because
 * {@code validateUnion} enforces that every branch projects the same column names - now fail loudly
 * with {@link UnsupportedOperationException} instead of answering a question a UNION cannot answer.
 * <p>
 * Auditing every call site of those nine accessors surfaced two that were reached with an actual
 * {@code UnionStatement} at runtime, unguarded:
 * <ul>
 *   <li>{@code CypherVariableUsage.subqueryReferencesVariable}, which silently checked only branch 1
 *   of a {@code CALL { ... }} body, so a relationship variable read only in branch 2 was reported as
 *   unreferenced - the exact "silently drop a still-referenced edge" failure mode this class exists
 *   to prevent.</li>
 *   <li>{@code OpenCypherQueryEngine.analyze(...).getOperationTypes()}, which read
 *   {@code getSetClause()}/{@code getRemoveClauses()} directly instead of through an aggregated
 *   predicate the way {@code hasCreate()}/{@code hasMerge()}/{@code hasDelete()} already do.</li>
 * </ul>
 * Both are fixed alongside the accessors themselves and covered below.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5671UnionStatementClauseAccessorsTest {
  private static final Cypher25AntlrParser PARSER = new Cypher25AntlrParser();

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue5671").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ============================================================
  // AST-level: the nine accessors throw, getReturnClause() does not
  // ============================================================

  @Test
  void divergentClauseAccessorsThrowInsteadOfAnsweringForBranchOneOnly() {
    // Branch 1 has a MATCH and no WITH/CREATE/SET/DELETE/MERGE/UNWIND; branch 2 has all of them, plus a
    // different MATCH. Before the fix every accessor below silently answered with branch 1 - here, mostly
    // empty/null - hiding that branch 2 has real clauses of its own.
    final UnionStatement union = (UnionStatement) PARSER.parse(
        """
        MATCH (a:P) RETURN a.x AS c
        UNION
        MATCH (m:P) UNWIND [1,2] AS u WITH m, u CREATE (n:P {x: u}) SET n.y = 1 MERGE (o:P {x: 0}) \
        DELETE n RETURN m.x AS c""");

    assertThatThrownBy(union::getMatchClauses).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(union::getWhereClause).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(union::getCreateClause).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(union::getSetClause).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(union::getDeleteClause).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(union::getMergeClause).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(union::getUnwindClauses).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(union::getWithClauses).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(union::getClausesInOrder).isInstanceOf(UnsupportedOperationException.class);

    // getReturnClause() is the one safe delegate: DifferentColumnsInUnion already guarantees branch 1's
    // column names are the union's column names.
    assertThat(union.getReturnClause()).isNotNull();
    assertThat(union.getReturnClause().getReturnItems()).hasSize(1);
    assertThat(union.getReturnClause().getReturnItems().getFirst().getAlias()).isEqualTo("c");
  }

  @Test
  void hasCreateMergeDeleteSetRemoveAggregateAcrossBranchesRatherThanAnsweringForBranchOneOnly() {
    final UnionStatement onlySecondBranchWrites = (UnionStatement) PARSER.parse(
        """
        MATCH (a:P) RETURN a.x AS c
        UNION
        MATCH (m:P) SET m.y = 1 REMOVE m.z CREATE (n:P {x: 1}) MERGE (o:P {x: 0}) DELETE m RETURN m.x AS c""");

    assertThat(onlySecondBranchWrites.hasCreate()).isTrue();
    assertThat(onlySecondBranchWrites.hasMerge()).isTrue();
    assertThat(onlySecondBranchWrites.hasDelete()).isTrue();
    assertThat(onlySecondBranchWrites.hasSet()).isTrue();
    assertThat(onlySecondBranchWrites.hasRemove()).isTrue();

    final UnionStatement neitherBranchWrites = (UnionStatement) PARSER.parse(
        "MATCH (a:P) RETURN a.x AS c UNION MATCH (m:P) RETURN m.x AS c");

    assertThat(neitherBranchWrites.hasCreate()).isFalse();
    assertThat(neitherBranchWrites.hasMerge()).isFalse();
    assertThat(neitherBranchWrites.hasDelete()).isFalse();
    assertThat(neitherBranchWrites.hasSet()).isFalse();
    assertThat(neitherBranchWrites.hasRemove()).isFalse();
  }

  // ============================================================
  // Regression: CypherVariableUsage no longer drops a reference that only branch 2 of a
  // CALL subquery body makes (found while auditing call sites for this fix)
  // ============================================================

  @Test
  void edgeVariableReferencedOnlyInSecondUnionBranchOfACallSubqueryIsNotReportedAsUnreferenced() {
    // CALL (*) imports every outer variable without requiring an importing WITH, so r is visible in both
    // branches without either branch needing to mention "r" merely to import it - isolating the actual bug:
    // branch 1 never reads r, branch 2 does, in its RETURN.
    final CypherStatement statement = PARSER.parse(
        """
        MATCH (a)-[r:KNOWS]->(b)
        CALL (*) { MATCH (x:P) RETURN x.x AS n UNION MATCH (y:P) RETURN r.since AS n }
        RETURN b""");

    assertThat(CypherVariableUsage.isEdgeVariableReferenced(statement, "r"))
        .as("r is read in branch 2's RETURN (r.since); the check must not stop at branch 1")
        .isTrue();
  }

  @Test
  void edgeVariableReferencedInNeitherUnionBranchOfACallSubqueryIsStillUnreferenced() {
    // Sanity check the other direction: the fix must not become unconditionally conservative.
    final CypherStatement statement = PARSER.parse(
        """
        MATCH (a)-[r:KNOWS]->(b)
        CALL (*) { MATCH (x:P) RETURN x AS n UNION MATCH (y:P) RETURN y AS n }
        RETURN b""");

    assertThat(CypherVariableUsage.isEdgeVariableReferenced(statement, "r")).isFalse();
  }

  // ============================================================
  // Regression: OpenCypherQueryEngine.analyze(...).getOperationTypes() no longer throws for a
  // UNION whose write clause is only in one branch, and classifies it correctly
  // ============================================================

  @Test
  void operationTypesOfAUnionWithSetOnlyInOneBranchIncludesUpdateAndDoesNotThrow() {
    final Set<OperationType> ops = database.getQueryEngine("opencypher")
        .analyze("MATCH (a:P) RETURN a AS n UNION MATCH (m:P) SET m.y = 1 RETURN m AS n")
        .getOperationTypes();

    assertThat(ops).contains(OperationType.UPDATE);
  }

  @Test
  void operationTypesOfAUnionWithRemoveOnlyInOneBranchIncludesUpdateAndDoesNotThrow() {
    final Set<OperationType> ops = database.getQueryEngine("opencypher")
        .analyze("MATCH (a:P) RETURN a AS n UNION MATCH (m:P) REMOVE m.y RETURN m AS n")
        .getOperationTypes();

    assertThat(ops).contains(OperationType.UPDATE);
  }

  // ============================================================
  // Functional: a UNION with genuinely different MATCH shapes per branch, nested inside a CALL
  // subquery, still parses, plans and executes correctly end to end
  // ============================================================

  @Test
  void divergentUnionBranchesInsideACallSubqueryStillExecuteCorrectly() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("REL");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:P {x: 1})");
      database.command("opencypher", "CREATE (b:P {x: 2})");
      database.command("opencypher", "CREATE (c:P {x: 3})");
      database.command("opencypher", "MATCH (a:P {x: 1}), (b:P {x: 2}) CREATE (a)-[:REL]->(b)");
    });

    // Branch 1 matches every P directly; branch 2 matches a P reached by a REL hop. The two branches have
    // different MATCH shapes (different pattern, different variable), which is exactly the case
    // UnionStatement.getMatchClauses() used to silently collapse into "branch 1's shape".
    final ResultSet result = database.query("opencypher",
        """
        CALL {
          MATCH (a:P) RETURN a.x AS c
          UNION
          MATCH (:P)-[:REL]->(m:P) RETURN m.x AS c
        }
        RETURN c ORDER BY c""");

    final Set<Integer> values = new HashSet<>();
    while (result.hasNext()) {
      final Result row = result.next();
      values.add(((Number) row.getProperty("c")).intValue());
    }

    // UNION dedups: branch 1 contributes {1,2,3}, branch 2 contributes {2}, combined distinct is {1,2,3}.
    assertThat(values).containsExactlyInAnyOrder(1, 2, 3);
  }
}
