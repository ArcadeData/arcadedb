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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9628: {@code isIdempotent()}, {@code isDDL()} and {@code getOperationTypes()} were answered by each statement node
 * from its own class and never from the statements nested in it. A write in a {@code LET}, in a SELECT's {@code LET}, in
 * a projection or in a FROM target was reported read-only ({@code [READ]}) and executed through the read-only
 * {@code query()} door; a DDL inside {@code IF}/{@code FOREACH}/{@code WHILE} was reported as a data write, and a read
 * inside {@code FOREACH}/{@code WHILE} as a write.
 */
class Issue9628NestedStatementClassificationTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Victim");
      database.getSchema().createVertexType("V1");
    });
  }

  private QueryEngine.AnalyzedQuery sql(final String command) {
    return database.getQueryEngine("sql").analyze(command);
  }

  private QueryEngine.AnalyzedQuery script(final String command) {
    return database.getQueryEngine("sqlscript").analyze(command);
  }

  // ---------------------------------------------------------------- nested data writes: classification

  @ParameterizedTest
  @ValueSource(strings = {
      "LET $x = (INSERT INTO Victim SET a = 1)",
      "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2)",
      "SELECT $y FROM Victim LET $y = (INSERT INTO Victim SET a = 3)",
      "SELECT (INSERT INTO Victim SET a = 5) as p FROM Victim",
      "SELECT FROM (INSERT INTO Victim SET a = 6)",
      "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2) limit 20001",
      "PROFILE SELECT FROM (INSERT INTO Victim SET a = 6)",
      "LET $x = (PROFILE INSERT INTO Victim SET a = 7)",
      "SELECT CASE WHEN 1 = 1 THEN (INSERT INTO Victim SET a = 8) END as p FROM Victim",
      "RETURN (INSERT INTO Victim SET a = 9)" })
  void aNestedInsertIsAWrite(final String command) {
    final QueryEngine.AnalyzedQuery analyzed = sql(command);
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.isDDL()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
  }

  @Test
  void aNestedDeleteAndUpdateAreWrites() {
    assertThat(sql("LET $x = (DELETE FROM Victim)").isIdempotent()).isFalse();
    assertThat(sql("LET $x = (DELETE FROM Victim)").getOperationTypes()).contains(OperationType.DELETE);
    assertThat(sql("LET $x = (UPDATE Victim SET a = 9)").isIdempotent()).isFalse();
    assertThat(sql("LET $x = (UPDATE Victim SET a = 9)").getOperationTypes()).contains(OperationType.UPDATE);
    assertThat(sql("SELECT FROM Victim LET $y = (DELETE FROM Victim)").getOperationTypes()).contains(OperationType.DELETE);
  }

  @Test
  void aNestedSchemaChangeIsDdl() {
    final QueryEngine.AnalyzedQuery let = sql("LET $x = (CREATE DOCUMENT TYPE Sneak1)");
    assertThat(let.isIdempotent()).isFalse();
    assertThat(let.isDDL()).isTrue();
    assertThat(let.getOperationTypes()).containsExactly(OperationType.SCHEMA);

    final QueryEngine.AnalyzedQuery select = sql("SELECT FROM Victim LET $z = (CREATE DOCUMENT TYPE Sneak2)");
    assertThat(select.isIdempotent()).isFalse();
    assertThat(select.isDDL()).isTrue();
    assertThat(select.getOperationTypes()).contains(OperationType.SCHEMA);
  }

  /** A write nested in another write adds what it does: an INSERT that runs a CREATE TYPE is also a schema change. */
  @Test
  void aWriteNestedInAWriteContributesItsOperation() {
    final QueryEngine.AnalyzedQuery analyzed = sql("INSERT INTO Victim SET a = (CREATE DOCUMENT TYPE Sneak3)");
    assertThat(analyzed.isDDL()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.SCHEMA);
  }

  // ---------------------------------------------------------------- the read-only door refuses them and writes nothing

  @ParameterizedTest
  @ValueSource(strings = {
      "LET $x = (INSERT INTO Victim SET a = 1)",
      "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2)",
      "SELECT $y FROM Victim LET $y = (INSERT INTO Victim SET a = 3)",
      "SELECT (INSERT INTO Victim SET a = 5) as p FROM Victim",
      "SELECT FROM (INSERT INTO Victim SET a = 6)" })
  void queryRefusesANestedInsert(final String command) {
    database.transaction(() -> database.command("sql", "INSERT INTO Victim SET a = 0"));
    assertThatThrownBy(() -> database.query("sql", command)).isInstanceOf(QueryNotIdempotentException.class);
    assertThat(database.countType("Victim", false)).isEqualTo(1);
  }

  @Test
  void queryRefusesANestedDeleteAndUpdate() {
    database.transaction(() -> database.command("sql", "INSERT INTO Victim SET a = 0"));
    assertThatThrownBy(() -> database.query("sql", "LET $x = (DELETE FROM Victim)")).isInstanceOf(QueryNotIdempotentException.class);
    assertThatThrownBy(() -> database.query("sql", "LET $x = (UPDATE Victim SET a = 9)")).isInstanceOf(QueryNotIdempotentException.class);
    assertThat(database.countType("Victim", false)).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "SELECT a FROM Victim")) {
      assertThat(rs.next().<Integer>getProperty("a")).isZero();
    }
  }

  @Test
  void queryRefusesANestedSchemaChange() {
    assertThatThrownBy(() -> database.query("sql", "LET $x = (CREATE DOCUMENT TYPE Sneak1)")).isInstanceOf(QueryNotIdempotentException.class);
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Victim LET $z = (CREATE DOCUMENT TYPE Sneak2)"))
        .isInstanceOf(QueryNotIdempotentException.class);
    assertThat(database.getSchema().existsType("Sneak1")).isFalse();
    assertThat(database.getSchema().existsType("Sneak2")).isFalse();
  }

  @Test
  void sqlScriptQueryRefusesANestedInsert() {
    assertThatThrownBy(() -> database.query("sqlscript", "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2);"))
        .isInstanceOf(QueryNotIdempotentException.class);
    assertThat(database.countType("Victim", false)).isZero();
  }

  /** command() is the door for writes: the same statements still run there. */
  @Test
  void commandStillRunsANestedWrite() {
    database.transaction(() -> database.command("sql", "LET $x = (INSERT INTO Victim SET a = 1)"));
    assertThat(database.countType("Victim", false)).isEqualTo(1);
  }

  // ---------------------------------------------------------------- nested reads stay reads

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT FROM Victim",
      "LET $x = 1",
      "LET $x = (SELECT FROM Victim)",
      "SELECT FROM Victim LET $y = (SELECT FROM Victim)",
      "SELECT (SELECT count(*) FROM Victim) as p FROM Victim",
      "SELECT FROM (SELECT FROM Victim)",
      "SELECT FROM Victim WHERE a IN (SELECT a FROM Victim)",
      "MATCH {type: Victim, as: v} RETURN v",
      "TRAVERSE out() FROM (SELECT FROM V1)",
      "SELECT FROM Victim LET $x = (EXPLAIN INSERT INTO Victim SET a = 1)",
      "EXPLAIN SELECT FROM (INSERT INTO Victim SET a = 6)" })
  void aNestedReadStaysARead(final String command) {
    final QueryEngine.AnalyzedQuery analyzed = sql(command);
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.isDDL()).isFalse();
    if (!command.startsWith("EXPLAIN"))
      assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
    try (final ResultSet rs = database.query("sql", command)) {
      rs.stream().count();
    }
  }

  /** A write that reads to find what it writes is still classified by its write alone, as it always was. */
  @Test
  void aWriteWithANestedReadKeepsItsOwnOperations() {
    assertThat(sql("INSERT INTO Victim FROM (SELECT FROM Victim)").getOperationTypes()).containsExactly(OperationType.CREATE);
    assertThat(sql("DELETE FROM Victim WHERE a IN (SELECT a FROM Victim)").getOperationTypes()).containsExactly(OperationType.DELETE);
    assertThat(sql("UPDATE Victim SET a = (SELECT count(*) FROM Victim)").getOperationTypes()).containsExactly(OperationType.UPDATE);
  }

  /** A DDL stores the statements it holds (a view's query, a trigger's body) and runs none of them now. */
  @Test
  void aDdlIsClassifiedByItselfNotByTheStatementItDefines() {
    final QueryEngine.AnalyzedQuery view = sql("CREATE MATERIALIZED VIEW SneakView AS SELECT FROM Victim");
    assertThat(view.isDDL()).isTrue();
    assertThat(view.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  /** An expression a DDL evaluates while it runs is executed now, so a write in it is reported with the schema change. */
  @Test
  void aDdlEvaluatingANestedWriteReportsTheWrite() {
    final QueryEngine.AnalyzedQuery analyzed = sql("ALTER TYPE Victim CUSTOM sneak = (INSERT INTO Victim SET a = 1)");
    assertThat(analyzed.isDDL()).isTrue();
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.SCHEMA, OperationType.CREATE);
  }

  // ---------------------------------------------------------------- IF / FOREACH / WHILE follow their body

  @ParameterizedTest
  @ValueSource(strings = {
      "IF (1=1) { CREATE VERTEX TYPE Pwned; }",
      "FOREACH ($i IN [1]) { CREATE VERTEX TYPE Pwned; }",
      "WHILE ($x < 1) { CREATE VERTEX TYPE Pwned; }",
      "IF (1=1) { DROP TYPE Victim; }" })
  void aBlockWithADdlBodyIsDdl(final String command) {
    final QueryEngine.AnalyzedQuery analyzed = script(command);
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.isDDL()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void ifInPlainSqlWithADdlBodyIsDdl() {
    final QueryEngine.AnalyzedQuery analyzed = sql("IF (1=1) { CREATE VERTEX TYPE Pwned; }");
    assertThat(analyzed.isDDL()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void ifAroundBackupKeepsBackupsOperationTypes() {
    final QueryEngine.AnalyzedQuery analyzed = script("IF (1=1) { BACKUP DATABASE; }");
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.READ, OperationType.CREATE);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "FOREACH ($i IN [1]) { SELECT FROM Victim; }",
      "WHILE ($x < 1) { SELECT FROM Victim; }",
      "IF (1=1) { SELECT FROM Victim; }" })
  void aBlockWithAReadOnlyBodyIsARead(final String command) {
    final QueryEngine.AnalyzedQuery analyzed = script(command);
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.isDDL()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "FOREACH ($i IN [1]) { INSERT INTO Victim SET a = $i; }",
      "WHILE ($x < 1) { INSERT INTO Victim SET a = 1; }",
      "IF (1=1) { INSERT INTO Victim SET a = 1; }",
      "IF (1=1) { SELECT FROM (INSERT INTO Victim SET a = 1); }" })
  void aBlockWithAWriteBodyIsAWrite(final String command) {
    final QueryEngine.AnalyzedQuery analyzed = script(command);
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.isDDL()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE).doesNotContain(OperationType.SCHEMA);
  }

  @Test
  void aBlockThatWritesStillExecutesThroughCommand() {
    database.transaction(() -> database.command("sqlscript", "FOREACH ($i IN [1, 2]) { INSERT INTO Victim SET a = $i; }"));
    assertThat(database.countType("Victim", false)).isEqualTo(2);
    database.transaction(() -> database.command("sqlscript", "IF (1=1) { SELECT FROM (INSERT INTO Victim SET a = 3); }"));
    assertThat(database.countType("Victim", false)).isEqualTo(3);
  }

  @Test
  void aReadOnlyForEachRunsThroughQuery() {
    database.transaction(() -> database.command("sql", "INSERT INTO Victim SET a = 1"));
    try (final ResultSet rs = database.query("sqlscript", "FOREACH ($i IN [1]) { SELECT FROM Victim; }")) {
      rs.stream().count();
    }
  }
}
