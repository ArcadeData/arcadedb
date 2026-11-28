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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.parser.DeleteStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.UpdateStatement;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12593: which SELECT sources of an UPDATE or a DELETE have their predicates evaluated again on the record a row names. A row
 * that carries the {@code @rid} is evaluated again on every record, contended or not, so a source whose predicates cannot be evaluated
 * again on the record with the same answer must keep its rows as they were read: a built-in that answers differently from one call to
 * the next would drop records the source selected ({@code UPDATE (SELECT @rid FROM R WHERE randomInt(10) = 0)} updated about one row
 * in a hundred instead of one in ten), a {@code $parent} read resolves against the UPDATE's context and not the subquery's, and a row
 * of an index or of the schema is not made of the record it names.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12593SubquerySourceSelectionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sqlscript", "CREATE DOCUMENT TYPE R; CREATE PROPERTY R.k STRING; CREATE INDEX ON R (k) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET k = 'r0', n = 0").close());
  }

  @Test
  void aSelectOfATypeWithAWhereIsEvaluatedAgain() {
    assertThat(ofUpdate("UPDATE (SELECT @rid, claimed FROM R WHERE k = 'r0') SET claimed = 'B' WHERE claimed IS NULL")).isNotNull();
    assertThat(ofUpdate("UPDATE (SELECT FROM R WHERE claimed IS NULL) SET claimed = 'B'")).isNotNull();
    assertThat(ofDelete("DELETE FROM (SELECT @rid, claimed FROM R WHERE k = 'r0') WHERE claimed IS NULL")).isNotNull();
    // deterministic built-ins are evaluated again like any other expression
    assertThat(ofUpdate("UPDATE (SELECT @rid FROM R WHERE k.toUpperCase() = 'R0' AND abs(n) = 0) SET claimed = 'B'")).isNotNull();
  }

  @Test
  void nothingToEvaluateAgainWithoutAWhere() {
    assertThat(ofUpdate("UPDATE (SELECT @rid FROM R) SET n = 1")).isNull();
    assertThat(ofUpdate("UPDATE R SET n = 1 WHERE k = 'r0'")).isNull();
  }

  @Test
  void aBuiltInThatAnswersDifferentlyFromOneCallToTheNextKeepsTheRowsAsRead() {
    assertThat(ofUpdate("UPDATE (SELECT @rid FROM R WHERE randomInt(10) = 0) SET sampled = true")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid, randomInt(10) AS dice FROM R) SET sampled = true WHERE dice = 0")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid, claimed FROM R WHERE k = 'r0') SET sampled = true WHERE randomInt(10) = 0")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid, uuid() AS u FROM R WHERE k = 'r0') SET n = 1 WHERE u IS NOT NULL")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid FROM R WHERE sysdate() > date('2000-01-01', 'yyyy-MM-dd')) SET n = 1")).isNull();
    assertThat(ofDelete("DELETE FROM (SELECT @rid FROM R WHERE randomInt(10) = 0)")).isNull();
  }

  @Test
  void aSelectThatReadsTheEnclosingStatementKeepsTheRowsAsRead() {
    // in the WHERE of the SELECT, not only in its projection
    assertThat(ofUpdate("UPDATE (SELECT @rid FROM R WHERE k = $parent.$current.k) SET n = 1")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid, $parent.x AS x FROM R WHERE k = 'r0') SET n = 1")).isNull();
  }

  @Test
  void aSourceWhoseRowsAreNotMadeOfTheRecordKeepsTheRowsAsRead() {
    assertThat(ofUpdate("UPDATE (SELECT rid AS `@rid` FROM index:`R[k]` WHERE key = 'r0') SET n = 1")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid, count(*) AS c FROM R WHERE k = 'r0' GROUP BY @rid) SET n = 1")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid, count(*) AS c FROM R WHERE k = 'r0') SET n = 1")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT expand(@this) FROM R WHERE k = 'r0') SET n = 1")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid FROM (SELECT FROM R) WHERE k = 'r0') SET n = 1")).isNull();
    assertThat(ofUpdate("UPDATE (SELECT @rid FROM R LET $x = 1 WHERE k = 'r0') SET n = 1")).isNull();
  }

  /** The excluded sources still write: their rows stand as they were read. */
  @Test
  void anExcludedSourceStillWrites() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql",
          "UPDATE (SELECT @rid FROM R WHERE k = 'r0' AND randomInt(1) = 0) SET n = 7")) {
        assertThat(((Number) rs.next().getProperty("count")).longValue()).isEqualTo(1L);
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT n FROM R WHERE k = 'r0'")) {
      assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(7);
    }
  }

  private SubquerySourceSelection ofUpdate(final String sql) {
    final UpdateStatement update = (UpdateStatement) parse(sql);
    return SubquerySourceSelection.of(update.getTarget(), update.getWhereClause(), context());
  }

  private SubquerySourceSelection ofDelete(final String sql) {
    final DeleteStatement delete = (DeleteStatement) parse(sql);
    return SubquerySourceSelection.of(delete.getFromClause(), delete.getWhereClause(), context());
  }

  private Statement parse(final String sql) {
    return ((DatabaseInternal) database).getStatementCache().get(sql);
  }

  private BasicCommandContext context() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }
}
