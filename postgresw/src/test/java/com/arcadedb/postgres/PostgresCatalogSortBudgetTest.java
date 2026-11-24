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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The work the catalog spends sorting the rows of a statement - its ORDER BY, the ORDER BY of a set operation, and the ordering and
 * partitioning of a window function - is charged to the statement's work budget by the rows and the keys it sorts them by, and an
 * ORDER BY item that repeats an earlier one adds no key.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogSortBudgetTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogSortBudgetTest";
  private static final String USER          = "root";
  /** Twelve rows of two columns: {@code g} from 1 to 3, and for each of them {@code r} from 1 to 4. */
  private static final String TWELVE_ROWS   = "from pg_database d, generate_series(1, 3) as a(g), generate_series(1, 4) as b(r)";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- the ORDER BY of a statement

  /** A statement sorting by a few keys is answered in order, and one key breaks the ties of the one before it. */
  @Test
  void aSortByAFewKeysIsAnswered() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(
        "select g as pg_g, r as pg_r " + TWELVE_ROWS + " order by pg_g desc, pg_r", database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.rows).hasSize(12);
    assertThat(answer.rows.get(0).get("pg_g")).isEqualTo(3L);
    assertThat(answer.rows.get(0).get("pg_r")).isEqualTo(1L);
    assertThat(answer.rows.get(3).get("pg_r")).isEqualTo(4L);
    assertThat(answer.rows.get(4).get("pg_g")).isEqualTo(2L);
    assertThat(answer.rows.get(11).get("pg_g")).isEqualTo(1L);
    assertThat(answer.rows.get(11).get("pg_r")).isEqualTo(4L);
  }

  /** Sorting many rows by many keys costs the rows, times the keys, times the depth of the sort: past the budget it is refused. */
  @Test
  void aSortOfManyRowsByManyKeysIsRefused() {
    final int columns = 50;
    final String query = constants(columns, 60_000) + orderBy(columns);

    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  /** The same keys over few rows are within the budget, and so is a single key over the rows that were refused. */
  @Test
  void theSameKeysOverFewRowsOrTheSameRowsByOneKeyAreAnswered() {
    final int columns = 50;

    final PostgresCatalog.Answer fewRows = PostgresCatalog.resolve(constants(columns, 100) + orderBy(columns), database, USER);
    assertThat(fewRows).isNotNull();
    assertThat(fewRows.rows).hasSize(100);

    final PostgresCatalog.Answer oneKey = PostgresCatalog.resolve(constants(columns, 60_000) + " order by pg_c0", database, USER);
    assertThat(oneKey).isNotNull();
    assertThat(oneKey.rows).hasSize(60_000);
  }

  /** A single key over more rows than the budget can sort is refused, as it is for a set operation. */
  @Test
  void aSortOfMoreRowsThanTheBudgetCoversIsRefused() {
    final String query = "select r as pg_r from pg_database d, generate_series(1, 300000) as s(r) order by pg_r desc";

    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  /** An item that repeats an earlier one, by name or by position, never breaks a tie: the sort costs what its distinct keys cost. */
  @Test
  void anItemThatRepeatsAnEarlierOneAddsNoKey() {
    final StringBuilder repeated = new StringBuilder(" order by pg_r desc");
    for (int i = 0; i < 500; i++)
      repeated.append(i % 3 == 0 ? ", pg_r" : i % 3 == 1 ? ", 1 asc" : ", pg_r desc");

    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(
        "select r as pg_r from pg_database d, generate_series(1, 100000) as s(r)" + repeated, database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.rows).hasSize(100_000);
    assertThat(answer.rows.get(0).get("pg_r")).isEqualTo(100_000L);
    assertThat(answer.rows.get(99_999).get("pg_r")).isEqualTo(1L);
  }

  /** A repeated item changes nothing about the order: the first use of a column decides its direction, and the keys after it break ties. */
  @Test
  void aRepeatedItemLeavesTheOrderOfTheDistinctKeys() {
    final PostgresCatalog.Answer once = PostgresCatalog.resolve("select g as pg_g, r as pg_r " + TWELVE_ROWS + " order by pg_g desc, pg_r",
        database, USER);
    final PostgresCatalog.Answer repeated = PostgresCatalog.resolve(
        "select g as pg_g, r as pg_r " + TWELVE_ROWS + " order by pg_g desc, pg_g, pg_r, 1 asc, pg_r desc, 2", database, USER);

    assertThat(once).isNotNull();
    assertThat(repeated).isNotNull();
    assertThat(repeated.rows).isEqualTo(once.rows);
  }

  /** A statement with more ORDER BY items than its projection has columns is charged for finding the column of each. */
  @Test
  void manyItemsOverAWideProjectionAreCharged() {
    final int columns = 3_000;
    final String query = constants(columns, 1) + orderBy(columns);

    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  // ---------------------------------------------------------------- the ORDER BY of a set operation

  /** The keys of the ORDER BY of a set operation are charged, as they are for a statement. */
  @Test
  void theKeysOfTheOrderByOfASetOperationAreCharged() {
    final int columns = 100;
    final String arm = constants(columns, 3_000);

    final PostgresCatalog.Answer unsorted = PostgresCatalog.resolve(arm + " union all " + arm, database, USER);
    assertThat(unsorted).isNotNull();
    assertThat(unsorted.rows).hasSize(6_000);

    // the rows the arms read, and the passes over them, fit the budget: the sort by 100 keys is what takes it over
    assertThatThrownBy(() -> PostgresCatalog.resolve(arm + " union all " + arm + orderBy(columns), database, USER)).isInstanceOf(
        PostgresCatalog.WorkLimitExceeded.class);

    final PostgresCatalog.Answer oneKey = PostgresCatalog.resolve(arm + " union all " + arm + " order by pg_c0", database, USER);
    assertThat(oneKey).isNotNull();
    assertThat(oneKey.rows).hasSize(6_000);
  }

  // ---------------------------------------------------------------- window functions

  /** A window numbering by a few keys is answered in the order the keys give, per partition. */
  @Test
  void aWindowOverAFewKeysIsAnswered() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(
        "select g as pg_g, r as pg_r, row_number() over (partition by g order by r desc) as pg_n " + TWELVE_ROWS
            + " order by pg_g, pg_r", database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.rows).hasSize(12);
    // each g is a partition of four rows, numbered from the greatest r
    for (int row = 0; row < 12; row++)
      assertThat(answer.rows.get(row).get("pg_n")).as("row %d", row).isEqualTo(4L - row % 4);
  }

  /** The keys of a window are evaluated for every row, and sorted for every partition: both are charged. */
  @Test
  void theOrderingKeysOfAWindowAreCharged() {
    final StringBuilder keys = new StringBuilder();
    for (int i = 0; i < 100; i++)
      keys.append(i > 0 ? ", " : "").append(i % 2 == 0 ? "r" : "r desc");
    final String query = "select row_number() over (order by " + keys + ") as pg_n from pg_database d, generate_series(1, 50000) as s(r)";

    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  /** Sorting the rows of a partition by many keys costs the rows, times the keys, times the depth of the sort, whatever evaluating the keys cost. */
  @Test
  void theSortOfAWindowPartitionIsCharged() {
    final StringBuilder keys = new StringBuilder();
    for (int i = 0; i < 40; i++)
      keys.append(i > 0 ? ", " : "").append("r");
    final String query = "select row_number() over (order by " + keys + ") as pg_n from pg_database d, generate_series(1, 20000) as s(r)";

    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  /** The same for the keys a window partitions by. */
  @Test
  void thePartitionKeysOfAWindowAreCharged() {
    final StringBuilder keys = new StringBuilder();
    for (int i = 0; i < 200; i++)
      keys.append(i > 0 ? ", " : "").append("r");
    final String query = "select row_number() over (partition by " + keys + ") as pg_n from pg_database d, generate_series(1, 30000) as s(r)";

    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  /** A window over a few keys and rows stays within the budget, whatever the rows hold. */
  @Test
  void aWindowOverManyRowsByFewKeysIsAnswered() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(
        "select row_number() over (partition by r / 100 order by r desc) as pg_n from pg_database d, generate_series(1, 20000) as s(r)", database,
        USER);

    assertThat(answer).isNotNull();
    assertThat(answer.rows).hasSize(20_000);
  }

  // ---------------------------------------------------------------- grouping

  /** The keys of a GROUP BY are evaluated for every row: the rows, times the keys, are charged. */
  @Test
  void theGroupingKeysOfAStatementAreCharged() {
    final StringBuilder keys = new StringBuilder();
    for (int i = 0; i < 200; i++)
      keys.append(i > 0 ? ", " : "").append("r");
    final String query = "select 1 as pg_n from pg_database d, generate_series(1, 30000) as s(r) group by " + keys;

    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);

    final PostgresCatalog.Answer few = PostgresCatalog.resolve(
        "select g as pg_g, count(*) as pg_n from pg_database d, generate_series(1, 3) as a(g), generate_series(1, 10000) as b(r) group by g",
        database, USER);
    assertThat(few).isNotNull();
    assertThat(few.rows).hasSize(3);
    assertThat(few.rows.get(0).get("pg_n")).isEqualTo(10_000L);
  }

  // ---------------------------------------------------------------- helpers

  /** {@code select 1 as pg_c0, ..., 1 as pg_c<columns-1> from pg_database d, generate_series(1, rows)}: every row ties on every column. */
  private static String constants(final int columns, final int rows) {
    final StringBuilder query = new StringBuilder("select ");
    for (int i = 0; i < columns; i++)
      query.append(i > 0 ? ", " : "").append("1 as pg_c").append(i);
    return query.append(" from pg_database d, generate_series(1, ").append(rows).append(") as s(r)").toString();
  }

  /** {@code order by pg_c0, ..., pg_c<columns-1>}: as many distinct keys. */
  private static String orderBy(final int columns) {
    final StringBuilder clause = new StringBuilder(" order by ");
    for (int i = 0; i < columns; i++)
      clause.append(i > 0 ? ", " : "").append("pg_c").append(i);
    return clause.toString();
  }
}
