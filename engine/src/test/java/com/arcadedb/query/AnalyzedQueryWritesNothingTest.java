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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link QueryEngine.AnalyzedQuery#writesNothing()} is what a caller that offers read-only access to text it did not write asks:
 * a statement that is idempotent but declares a write (the backup statement, which writes an archive) is not read-only, in any
 * language that can wrap it, while every plain read still is.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class AnalyzedQueryWritesNothingTest extends TestHelper {

  private boolean writesNothing(final String language, final String text) {
    return database.getQueryEngine(language).analyze(text).writesNothing();
  }

  @Test
  void sqlReadsWriteNothing() {
    assertThat(writesNothing("sql", "SELECT FROM V")).isTrue();
    assertThat(writesNothing("sql", "SELECT count(*) FROM V WHERE name = 'x'")).isTrue();
    assertThat(writesNothing("sql", "EXPLAIN SELECT FROM V")).isTrue();
    assertThat(writesNothing("sql", "MATCH {type: V, as: a} RETURN a")).isTrue();
    assertThat(writesNothing("sql", "TRAVERSE out() FROM #1:0")).isTrue();
  }

  @Test
  void sqlWritesDoNot() {
    assertThat(writesNothing("sql", "INSERT INTO V SET name = 'x'")).isFalse();
    assertThat(writesNothing("sql", "UPDATE V SET name = 'x'")).isFalse();
    assertThat(writesNothing("sql", "DELETE FROM V")).isFalse();
    assertThat(writesNothing("sql", "CREATE VERTEX TYPE Other")).isFalse();
    assertThat(writesNothing("sql", "SELECT FROM V LET $y = (INSERT INTO V SET a = 2)")).isFalse();
  }

  @Test
  void theBackupStatementIsIdempotentButWritesAnArchive() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("BACKUP DATABASE");

    // The reason isIdempotent() is not the question: query() runs it, and it declares the write
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
    assertThat(analyzed.writesNothing()).isFalse();
  }

  @Test
  void theBackupStatementIsNotReadOnlyWhateverItsSpellingOrWrapper() {
    for (final String spelling : new String[] { "backup database", "BaCkUp   DaTaBaSe", "  BACKUP DATABASE  " })
      assertThat(writesNothing("sql", spelling)).as(spelling).isFalse();

    for (final String script : new String[] { "BACKUP DATABASE;", "backup database;", "SELECT FROM V; BACKUP DATABASE;",
        "BACKUP DATABASE; SELECT FROM V;", "IF (1 = 1) { BACKUP DATABASE; }", "LET $x = (SELECT FROM V); BACKUP DATABASE;" })
      assertThat(writesNothing("sqlscript", script)).as(script).isFalse();
  }

  @Test
  void aScriptOfReadsWritesNothing() {
    assertThat(writesNothing("sqlscript", "SELECT FROM V; SELECT count(*) FROM V;")).isTrue();
    assertThat(writesNothing("sqlscript", "LET $x = (SELECT FROM V); RETURN $x;")).isTrue();
    assertThat(writesNothing("sqlscript", "SELECT FROM V; INSERT INTO V SET name = 'x';")).isFalse();
  }

  @Test
  void openCypherReadsWriteNothingAndWritesDoNot() {
    assertThat(writesNothing("opencypher", "MATCH (n:V) RETURN n")).isTrue();
    assertThat(writesNothing("opencypher", "EXPLAIN MATCH (n:V) RETURN n")).isTrue();
    assertThat(writesNothing("opencypher", "CREATE (n:V {name: 'x'}) RETURN n")).isFalse();
    assertThat(writesNothing("opencypher", "MATCH (n:V) DETACH DELETE n")).isFalse();
    assertThat(writesNothing("opencypher", "CREATE CONSTRAINT c FOR (n:V) REQUIRE n.name IS UNIQUE")).isFalse();
  }

  @Test
  void anAdministrativeStatementIsNotReadOnlyEvenWhenItIsIdempotent() {
    // Idempotent (it changes no record, a replica runs it itself) but declared as an administrative operation
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze("SHOW TRANSACTIONS");

    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.ADMIN);
    assertThat(analyzed.writesNothing()).isFalse();
  }
}
