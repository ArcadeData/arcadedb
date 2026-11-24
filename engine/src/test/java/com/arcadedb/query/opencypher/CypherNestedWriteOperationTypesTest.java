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
import com.arcadedb.query.OperationType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code analyze().getOperationTypes()} of an openCypher statement declares every kind of write the statement can run, not
 * only the ones its top-level clauses spell: a write inside a {@code CALL { ... }} subquery, a {@code FOREACH} body, a branch
 * of a {@code UNION} or a conditional query, or a write procedure call next to another clause is declared too. A permission
 * gate that reads the declared types (the MCP {@code execute_command} tool) refuses what the identity may not do whatever
 * clause the write is nested in.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherNestedWriteOperationTypesTest extends TestHelper {

  private Set<OperationType> operationTypes(final String query) {
    return database.getQueryEngine("opencypher").analyze(query).getOperationTypes();
  }

  @Test
  void aSubqueryWriteIsDeclaredNextToATopLevelCreate() {
    assertThat(operationTypes("CREATE (a:Person {name: 'x'}) WITH a CALL { MATCH (b:Person) DETACH DELETE b } RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
    assertThat(operationTypes("CREATE (a:Person {name: 'x'}) WITH a CALL { MATCH (b:Person) SET b.name = 'y' } RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
    assertThat(operationTypes("CREATE (a:Person {name: 'x'}) WITH a CALL { MATCH (b:Person) REMOVE b.name } RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
    assertThat(operationTypes("CREATE (a:Person {name: 'x'}) WITH a CALL { MERGE (b:Person {name: 'z'}) } RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
  }

  @Test
  void aSubqueryWriteIsDeclaredWhenTheOuterStatementOnlyReads() {
    assertThat(operationTypes("MATCH (a:Person) CALL (a) { CREATE (:Person {name: 'x'}) } RETURN a"))
        .containsExactly(OperationType.CREATE);
    assertThat(operationTypes("MATCH (a:Person) CALL (a) { SET a.name = 'y' } RETURN a"))
        .containsExactly(OperationType.UPDATE);
    assertThat(operationTypes("MATCH (a:Person) CALL (a) { DETACH DELETE a } RETURN 1 AS one"))
        .containsExactly(OperationType.DELETE);
  }

  @Test
  void aWriteTwoSubqueriesDeepIsDeclared() {
    assertThat(operationTypes("CREATE (a:Person) WITH a CALL { CALL { MATCH (b:Person) DELETE b } } RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
  }

  @Test
  void aBatchedSubqueryWriteIsDeclared() {
    assertThat(operationTypes("CREATE (a:Person) WITH a CALL { MATCH (b:Person) DELETE b } IN TRANSACTIONS RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
  }

  @Test
  void aForeachWriteIsDeclaredNextToATopLevelCreate() {
    assertThat(operationTypes("CREATE (a:Person {name: 'x'}) WITH a FOREACH (i IN [1] | SET a.name = 'y') RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
    assertThat(operationTypes("CREATE (a:Person {name: 'x'}) WITH a FOREACH (i IN [a] | DELETE i) RETURN 1 AS one"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
    assertThat(operationTypes("CREATE (a:Person {name: 'x'}) WITH a FOREACH (i IN [a] | REMOVE i.name) RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
    assertThat(operationTypes("CREATE (a:Person {name: 'x'}) WITH a FOREACH (i IN [1] | MERGE (:Person {name: 'z'})) RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
  }

  @Test
  void aNestedForeachWriteIsDeclared() {
    assertThat(operationTypes("CREATE (a:Person) WITH a FOREACH (i IN [1] | FOREACH (j IN [a] | DELETE j)) RETURN 1 AS one"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
  }

  @Test
  void aForeachWriteIsDeclaredWhenTheOuterStatementOnlyReads() {
    assertThat(operationTypes("MATCH (a:Person) FOREACH (i IN [1] | SET a.name = 'y') RETURN a"))
        .containsExactly(OperationType.UPDATE);
    assertThat(operationTypes("MATCH (a:Person) FOREACH (i IN [1] | CREATE (:Person {name: 'x'})) RETURN a"))
        .containsExactly(OperationType.CREATE);
  }

  @Test
  void aSubqueryAndAForeachNestedTogetherAreDeclared() {
    assertThat(operationTypes("CREATE (a:Person) WITH a CALL { MATCH (b:Person) FOREACH (i IN [b] | DELETE i) } RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
  }

  @Test
  void aSecondSetClauseIsDeclared() {
    assertThat(operationTypes("CREATE (a:Person) WITH a SET a.x = 1 WITH a SET a.y = 2 RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
  }

  @Test
  void aNestedWriteInAUnionBranchIsDeclared() {
    assertThat(operationTypes(
        "CREATE (a:Person) RETURN a AS n UNION MATCH (m:Person) CALL (m) { DETACH DELETE m } RETURN m AS n"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
    assertThat(operationTypes(
        "CREATE (a:Person) RETURN a AS n UNION MATCH (m:Person) FOREACH (i IN [1] | SET m.y = 1) RETURN m AS n"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
    assertThat(operationTypes(
        "MATCH (a:Person) RETURN a AS n UNION ALL MATCH (m:Person) CALL (m) { DETACH DELETE m } RETURN m AS n"))
        .containsExactly(OperationType.DELETE);
  }

  @Test
  void aNestedWriteInAConditionalBranchIsDeclared() {
    assertThat(operationTypes("WHEN $mode = 1 THEN CREATE (a:Person) RETURN a AS n "
        + "ELSE MATCH (m:Person) CALL (m) { DETACH DELETE m } RETURN m AS n"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
    assertThat(operationTypes("WHEN $mode = 1 THEN MATCH (m:Person) RETURN m AS n "
        + "WHEN $mode = 2 THEN MATCH (m:Person) FOREACH (i IN [1] | SET m.y = 1) RETURN m AS n"))
        .containsExactly(OperationType.UPDATE);
  }

  @Test
  void aWriteProcedureCallNextToATopLevelCreateDeclaresEveryDataWrite() {
    final Set<OperationType> everyDataWrite = Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
    assertThat(operationTypes("CREATE (a:Person) WITH a CALL merge.node(['Person'], {name: 'X'}, {}) YIELD node RETURN node"))
        .isEqualTo(everyDataWrite);
    assertThat(operationTypes(
        "CREATE (a:Person) WITH a CALL { CALL merge.node(['Person'], {name: 'X'}, {}) YIELD node RETURN node } RETURN a"))
        .isEqualTo(everyDataWrite);
    assertThat(operationTypes("CALL merge.node(['Person'], {name: 'X'}, {}) YIELD node RETURN node")).isEqualTo(everyDataWrite);
  }

  @Test
  void anUnresolvedFunctionCallNextToATopLevelCreateDeclaresEveryDataWrite() {
    final Set<OperationType> everyDataWrite = Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
    assertThat(operationTypes("CREATE (a:Person) RETURN unknownlibrary.run(a) AS r")).isEqualTo(everyDataWrite);
    assertThat(operationTypes("CREATE (a:Person) WITH a CALL { RETURN unknownlibrary.run(1) AS r } RETURN a"))
        .isEqualTo(everyDataWrite);
  }

  @Test
  void aFunctionCalledInAWhenConditionDeclaresEveryDataWrite() {
    final Set<OperationType> everyDataWrite = Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
    final String readOnlyBranches = " THEN MATCH (m:Person) RETURN m AS n ELSE MATCH (m:Person) RETURN m AS n";
    assertThat(operationTypes("WHEN unknownlibrary.run($mode)" + readOnlyBranches)).isEqualTo(everyDataWrite);
    assertThat(operationTypes("WHEN $mode = 1 THEN MATCH (m:Person) RETURN m AS n WHEN unknownlibrary.run($mode) THEN MATCH (m:Person) "
        + "RETURN m AS n")).isEqualTo(everyDataWrite);
    assertThat(operationTypes("WHEN unknownlibrary.run($mode) THEN CREATE (a:Person) RETURN a AS n ELSE MATCH (m:Person) RETURN m AS n"))
        .isEqualTo(everyDataWrite);
    assertThat(database.getQueryEngine("opencypher").analyze("WHEN unknownlibrary.run($mode)" + readOnlyBranches).isIdempotent())
        .isFalse();
  }

  @Test
  void aBuiltInFunctionCalledInAWhenConditionStaysARead() {
    final String query = "WHEN toInteger($mode) = 1 THEN MATCH (m:Person) RETURN m AS n ELSE MATCH (m:Person) RETURN m AS n";
    assertThat(operationTypes(query)).containsExactly(OperationType.READ);
    assertThat(database.getQueryEngine("opencypher").analyze(query).isIdempotent()).isTrue();
  }

  @Test
  void aSchemaProcedureInASubqueryDeclaresTheSchemaChange() {
    assertThat(operationTypes("CALL { CALL db.analyticalView.drop('roads') YIELD name RETURN name } RETURN name"))
        .containsExactly(OperationType.SCHEMA);
    assertThat(operationTypes(
        "CREATE (a:Person) WITH a CALL { CALL db.analyticalView.drop('roads') YIELD name RETURN name } RETURN a"))
        .containsExactlyInAnyOrder(OperationType.SCHEMA, OperationType.CREATE);
  }

  @Test
  void aStatementWithoutWritesStaysAReadAndTheTopLevelClausesKeepTheirTypes() {
    assertThat(operationTypes("MATCH (a:Person) CALL (a) { MATCH (a)-->(b) RETURN b } RETURN b"))
        .containsExactly(OperationType.READ);
    assertThat(operationTypes("MATCH (a:Person) FOREACH (i IN [] | SET a.x = 1) RETURN a")).containsExactly(OperationType.UPDATE);
    assertThat(operationTypes("CREATE (a:Person) RETURN a")).containsExactly(OperationType.CREATE);
    assertThat(operationTypes("MERGE (a:Person {name: 'x'}) RETURN a"))
        .containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
    assertThat(operationTypes("MATCH (a:Person) DELETE a")).containsExactly(OperationType.DELETE);
    assertThat(operationTypes("MATCH (a:Person) SET a.x = 1")).containsExactly(OperationType.UPDATE);
    assertThat(operationTypes("MATCH (a:Person) REMOVE a.x")).containsExactly(OperationType.UPDATE);
  }
}
