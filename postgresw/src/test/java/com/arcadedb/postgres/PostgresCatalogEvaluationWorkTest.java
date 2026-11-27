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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The work budget of a catalog statement counts what evaluating its expressions costs, not only the rows they are evaluated
 * against: a condition or a projection that is wide is paid for each time it is evaluated, so a wide one over many rows is refused
 * like a narrow one over too many.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogEvaluationWorkTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogEvaluationWorkTest";
  private static final String USER          = "root";

  /** Elements of a list that holds no value to find: every one of them is read by every evaluation. */
  private static final int WIDE_LIST   = 8_000;
  /** Rows enough that a list of {@link #WIDE_LIST} elements read for each of them is far over what a statement may spend. */
  private static final int MANY_ROWS   = 30_000;
  /** Conjuncts of a condition that holds, so that every one of them is evaluated for each row. */
  private static final int CONJUNCTS   = 2_500;
  /** Columns of the type the one-row-per-entity statements read. */
  private static final int COLUMNS     = 1_000;
  /** A list long enough that reading it for each of {@link #COLUMNS} rows is over what a statement may spend. */
  private static final int LONGER_LIST = 200_000;

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Person");
      database.getSchema().createDocumentType("Note");
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private static String list(final int elements) {
    return "0, ".repeat(elements - 1) + "0";
  }

  private static String conjuncts(final int count) {
    return "g.n > 0 and ".repeat(count - 1) + "g.n > 0";
  }

  private void assertRefused(final String statement) {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve(statement, database, USER)).as(statement.substring(0, Math.min(80, statement.length())))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    stopwatch.assertGaveUpWithin(30_000, "the work of a wide statement over many rows, charged as it is spent");
  }

  // ---------------------------------------------------------------- conditions of a join

  @Test
  void aWideConditionOfAJoinIsChargedForEveryRowItIsEvaluatedFor() {
    assertRefused("select g.n from pg_database d, generate_series(1, " + MANY_ROWS + ") as g(n) where g.n in (" + list(WIDE_LIST) + ")");
  }

  @Test
  void aConditionOfManyConjunctsIsChargedForEveryRowItIsEvaluatedFor() {
    assertRefused("select g.n from pg_database d, generate_series(1, 10000) as g(n) where " + conjuncts(CONJUNCTS));
  }

  @Test
  void aWideConditionOfAnInnerJoinIsChargedForEveryRowItIsEvaluatedFor() {
    assertRefused("select g.n from pg_database d join generate_series(1, " + MANY_ROWS + ") as g(n) on g.n in (" + list(WIDE_LIST) + ")");
  }

  @Test
  void aWideConditionOfAnOuterJoinIsChargedForEveryRowItIsEvaluatedFor() {
    assertRefused("select d.datname, g.n from pg_database d left join generate_series(1, " + MANY_ROWS + ") as g(n) on g.n in ("
        + list(WIDE_LIST) + ")");
  }

  @Test
  void aWideConditionOfASelfJoinOfACatalogRelationIsChargedForEveryRowItIsEvaluatedFor() {
    // the rows of the second entry are the ones the first is paired with, a cross product of a relation with itself
    database.transaction(() -> {
      for (int i = 0; i < 80; i++)
        database.getSchema().createDocumentType("Item" + i);
    });
    assertRefused("select a.relname from pg_class a, pg_class b where a.relname in (" + list(60_000) + ") or b.relname = 'none'");
  }

  // ---------------------------------------------------------------- what is projected

  @Test
  void aWideProjectionIsChargedForEveryRowItIsEvaluatedFor() {
    assertRefused("select g.n in (" + list(WIDE_LIST) + ") as found from pg_database d, generate_series(1, " + MANY_ROWS + ") as g(n)");
  }

  @Test
  void aWideGroupKeyIsChargedForEveryRowItIsEvaluatedFor() {
    assertRefused("select count(*) as total from pg_database d, generate_series(1, " + MANY_ROWS + ") as g(n) group by g.n in ("
        + list(WIDE_LIST) + ")");
  }

  @Test
  void aWideAggregatedExpressionIsChargedForEveryRowItIsEvaluatedFor() {
    assertRefused("select count(g.n in (" + list(WIDE_LIST) + ")) as total from pg_database d, generate_series(1, " + MANY_ROWS
        + ") as g(n)");
    assertRefused("select string_agg('a', case when g.n in (" + list(WIDE_LIST) + ") then ',' else ';' end) as joined from "
        + "pg_database d, generate_series(1, " + MANY_ROWS + ") as g(n)");
  }

  @Test
  void aWideWindowKeyIsChargedForEveryRowItIsEvaluatedFor() {
    assertRefused("select row_number() over (order by g.n in (" + list(WIDE_LIST) + ")) as r from pg_database d, generate_series(1, "
        + MANY_ROWS + ") as g(n)");
  }

  // ---------------------------------------------------------------- one row per entity

  @Test
  void aWideConditionOverTheRowsOfARelationIsChargedForEveryRow() {
    createColumns();
    assertRefused("select attname from pg_attribute where attname in (" + list(LONGER_LIST) + ")");
  }

  @Test
  void aWideProjectionOverTheRowsOfARelationIsChargedForEveryRow() {
    createColumns();
    assertRefused("select attname in (" + list(LONGER_LIST) + ") as found from pg_attribute");
  }

  @Test
  void aWideConditionOfAnOuterJoinOverTheRowsOfARelationIsChargedForEveryRow() {
    createColumns();
    assertRefused("select a.attname, c.relname from pg_attribute a left join pg_class c on a.attname in (" + list(LONGER_LIST) + ")");
  }

  private void createColumns() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Wide");
      for (int i = 0; i < COLUMNS; i++)
        type.createProperty("c" + i, Type.STRING);
    });
  }

  // ---------------------------------------------------------------- what is still answered

  @Test
  void aConditionOfTheUsualSizeIsAnsweredOverManyRows() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select g.n from pg_database d, generate_series(1, 100000) as g(n) "
        + "where g.n in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) and g.n > 0 and d.datname is not null", database, USER);

    assertThat(answer).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).hasSize(10);
  }

  @Test
  void aWideConditionOverFewRowsIsAnswered() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select g.n from pg_database d, generate_series(1, 20) as g(n) "
        + "where g.n in (" + list(WIDE_LIST) + ", 5, 6)", database, USER);

    assertThat(answer).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).hasSize(2);
  }

  @Test
  void theStatementsOfACatalogClientAreAnswered() {
    for (final String statement : List.of(
        "select c.relname, n.nspname from pg_class c join pg_namespace n on c.relnamespace = n.oid where c.relkind in ('r', 'v') "
            + "and n.nspname !~ '^pg_' and c.relname like 'P%' order by c.relname",
        "select c.oid, i.indexrelid from pg_class c left join pg_index i on c.oid = i.indrelid where c.relname ~ '^(Person|Note)$'",
        "select a.attname, a.attnum, case when a.attnotnull then 'NO' else 'YES' end as nullable from pg_attribute a "
            + "where a.attnum > 0 and not a.attisdropped order by a.attnum")) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(statement, database, USER);
      assertThat(answer).as(statement).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    }
  }

  // ---------------------------------------------------------------- the size of an expression

  @Test
  void theSizeOfAnExpressionIsTheNumberOfNodesItIsParsedInto() {
    assertThat(nodesOf("1")).isEqualTo(1);
    assertThat(nodesOf("relname")).isEqualTo(1);
    assertThat(nodesOf("relname = 'x'")).isEqualTo(3);
    assertThat(nodesOf("(relname = 'x')")).isEqualTo(3);
    assertThat(nodesOf("relname = 'x' and relkind in ('r', 'v', 'm')")).isEqualTo(3 + 1 + 1 + 3 + 1);
    assertThat(nodesOf("not relname is null")).isEqualTo(3);
    assertThat(nodesOf("-oid")).isEqualTo(2);
    assertThat(nodesOf("lower(relname) || upper(relkind)")).isEqualTo(5);
    assertThat(nodesOf("case relkind when 'r' then 'TABLE' when 'v' then 'VIEW' else 'OTHER' end")).isEqualTo(7);
    assertThat(nodesOf("indkey[0]")).isEqualTo(3);
    assertThat(nodesOf("oid = any (current_schemas(true))")).isEqualTo(4);
    assertThat(nodesOf("exists (select 1 from pg_class)")).isEqualTo(1);
  }

  @Test
  void aLongTextCountsForItsLengthAsWell() {
    assertThat(nodesOf("'" + "a".repeat(32_000) + "'")).isGreaterThan(1_000);
    assertThat(nodesOf("lower('" + "a".repeat(32_000) + "')")).isGreaterThan(1_000);
    assertThat(nodesOf("'" + "a".repeat(10) + "'")).isEqualTo(1);
  }

  @Test
  void theSizeOfAWideExpressionIsAddedUpAsItIsParsed() {
    assertThat(nodesOf("oid in (" + list(50_000) + ")")).isEqualTo(50_002);
    assertThat(nodesOf("oid in (" + list(50_000) + ") and oid in (" + list(50_000) + ")")).isEqualTo(100_005);
  }

  private static int nodesOf(final String expression) {
    final PostgresCatalogExpression parsed = PostgresCatalogExpression.parse(PostgresCatalogToken.tokenize(expression));
    assertThat(parsed).as("expression did not parse: %s", expression).isNotNull();
    return parsed.nodes();
  }
}
