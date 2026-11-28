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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The width of the projection of a catalog statement is bounded and charged to the statement's work budget: a {@code *} that lists
 * the columns of its FROM entries a great many times, or a select list that is as long, is refused before the columns are listed,
 * and what is projected for each row is charged by the rows times the columns, as the rows the statement reads are.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogProjectionWidthTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogProjectionWidthTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Alpha");
      database.getSchema().createDocumentType("Beta");
      database.getSchema().createDocumentType("Gamma");
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- the columns a * lists

  /** A {@code *} lists the columns of the relation, one row for each of the user's types. */
  @Test
  void aStarIsAnswered() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select * from pg_class", database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.columns).containsKey("relname");
    assertThat(answer.rows).isNotEmpty();
    assertThat(answer.rows.get(0)).hasSameSizeAs(answer.columns);
  }

  /** A few stars list the columns each time, the copies of a name announced under it and keyed apart. */
  @Test
  void aFewRepeatedStarsAreAnsweredWithDistinctKeys() {
    final int width = PostgresCatalog.resolve("select * from pg_class", database, USER).columns.size();

    final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select *, c.*, * from pg_class c", database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.columns).hasSize(3 * width);
    assertThat(answer.rows).isNotEmpty();
    assertThat(answer.rows.get(0)).hasSize(3 * width);

    final List<String> keys = new ArrayList<>(answer.columns.keySet());
    assertThat(keys).doesNotHaveDuplicates();
    final int first = keys.indexOf("relname");
    assertThat(first).isGreaterThanOrEqualTo(0);
    assertThat(keys).contains("relname\0" + 2, "relname\0" + 3);
    for (final String key : keys)
      assertThat(PostgresCatalog.announcedName(key)).doesNotContain("\0");
    assertThat(PostgresCatalog.announcedName("relname\0" + 3)).isEqualTo("relname");
  }

  /** Stars that would list far more columns than a statement may project are refused, and promptly. */
  @Test
  void manyRepeatedStarsAreRefused() {
    final String query = "select " + "*, ".repeat(2_999) + "* from pg_class";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    stopwatch.assertGaveUpWithin(10_000, "a refusal before the columns are listed from listing every one of them");
  }

  /** The same for a star of an alias. */
  @Test
  void manyRepeatedQualifiedStarsAreRefused() {
    final String query = "select " + "c.*, ".repeat(2_999) + "c.* from pg_class c";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    stopwatch.assertGaveUpWithin(10_000, "a refusal before the columns are listed from listing every one of them");
  }

  /** A select list of more columns than a statement may project is refused, whatever the items are. */
  @Test
  void aSelectListOfManyItemsIsRefused() {
    final String query = "select " + "relname, ".repeat(4_999) + "relname from pg_class";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    stopwatch.assertGaveUpWithin(10_000, "a refusal of a select list that is too long from naming every column of it");
  }

  /** A select list that repeats a name a few hundred times is answered, each copy keyed by the order it is in. */
  @Test
  void aSelectListOfRepeatedNamesWithinTheLimitIsAnswered() {
    final String query = "select " + "relname, ".repeat(499) + "relname from pg_class";

    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.columns).hasSize(500);
    assertThat(answer.columns).containsKeys("relname", "relname\0" + 2, "relname\0" + 500);
    assertThat(answer.columns).doesNotContainKey("relname\0" + 501);
  }

  /** A FROM clause that repeats a relation many times, each entry listed by a {@code *}, is refused. */
  @Test
  void manyRepeatedFromEntriesListedByAStarAreRefused() {
    final int entries = 200;
    final StringBuilder query = new StringBuilder("select * from ");
    for (int i = 0; i < entries; i++)
      query.append(i > 0 ? ", " : "").append("pg_class e").append(i);
    for (int i = 1; i < entries; i++)
      query.append(i > 1 ? " and " : " where ").append("e").append(i - 1).append(".oid = e").append(i).append(".oid");

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve(query.toString(), database, USER)).isInstanceOf(
        PostgresCatalog.WorkLimitExceeded.class);
    stopwatch.assertGaveUpWithin(10_000, "a refusal before the columns are listed from listing every one of them");
  }

  /** The same for the stars of a derived table, and of a sub-select. */
  @Test
  void manyRepeatedStarsOfASubSelectAreRefused() {
    final String inner = "select " + "*, ".repeat(2_999) + "* from pg_class";

    assertThatThrownBy(() -> PostgresCatalog.resolve("select * from (" + inner + ") as d", database, USER)).isInstanceOf(
        PostgresCatalog.WorkLimitExceeded.class);
    assertThatThrownBy(() -> PostgresCatalog.resolve("select oid from pg_class where exists (" + inner + ")", database, USER))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  // ---------------------------------------------------------------- what is projected for each row

  /** The columns projected for a row are charged for every row: many of them over many rows are refused. */
  @Test
  void aWideProjectionOverManyRowsIsRefused() {
    final String query = constants(100, 60_000);

    assertThatThrownBy(() -> PostgresCatalog.resolve(query, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  /** The same columns over a few rows, and many rows of a few columns, are within the budget. */
  @Test
  void aWideProjectionOverFewRowsOrFewColumnsOverManyRowsAreAnswered() {
    final PostgresCatalog.Answer fewRows = PostgresCatalog.resolve(constants(100, 1_000), database, USER);
    assertThat(fewRows).isNotNull();
    assertThat(fewRows.rows).hasSize(1_000);
    assertThat(fewRows.columns).hasSize(100);

    final PostgresCatalog.Answer fewColumns = PostgresCatalog.resolve(constants(2, 60_000), database, USER);
    assertThat(fewColumns).isNotNull();
    assertThat(fewColumns.rows).hasSize(60_000);
  }

  /** A column list charged by what it projects counts the columns a {@code *} lists, not the {@code *} items. */
  @Test
  void aStarOverManyRowsIsCharged() {
    final StringBuilder query = new StringBuilder("select ");
    for (int i = 0; i < 15; i++)
      query.append("d.*, ");
    query.append("r from pg_database d, generate_series(1, 40000) as s(r)");

    assertThatThrownBy(() -> PostgresCatalog.resolve(query.toString(), database, USER)).isInstanceOf(
        PostgresCatalog.WorkLimitExceeded.class);

    final PostgresCatalog.Answer one = PostgresCatalog.resolve("select d.*, r from pg_database d, generate_series(1, 40000) as s(r)",
        database, USER);
    assertThat(one).isNotNull();
    assertThat(one.rows).hasSize(40_000);
  }

  /** An aggregate over many rows reads each row once for each column that holds one. */
  @Test
  void manyAggregatesOverManyRowsAreCharged() {
    final StringBuilder many = new StringBuilder("select ");
    for (int i = 0; i < 200; i++)
      many.append(i > 0 ? ", " : "").append("count(r) as pg_n").append(i);
    many.append(" from pg_database d, generate_series(1, 60000) as s(r)");

    assertThatThrownBy(() -> PostgresCatalog.resolve(many.toString(), database, USER)).isInstanceOf(
        PostgresCatalog.WorkLimitExceeded.class);

    final PostgresCatalog.Answer one = PostgresCatalog.resolve("select count(r) as pg_n from pg_database d, generate_series(1, 60000) as s(r)",
        database, USER);
    assertThat(one).isNotNull();
    assertThat(one.rows).hasSize(1);
    assertThat(one.rows.get(0).get("pg_n")).isEqualTo(60_000L);
  }

  /** DISTINCT hashes every column of every row, over what the projection cost: both are charged. */
  @Test
  void distinctOverAWideProjectionIsCharged() {
    final String projection = constants(100, 30_000);

    final PostgresCatalog.Answer plain = PostgresCatalog.resolve(projection, database, USER);
    assertThat(plain).isNotNull();
    assertThat(plain.rows).hasSize(30_000);

    assertThatThrownBy(() -> PostgresCatalog.resolve(projection.replaceFirst("select ", "select distinct "), database, USER))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  /** A row of a set-returning call is projected once for each element of its array, and each projection is charged. */
  @Test
  void aSetReturningCallChargesEveryProjectionOfARow() {
    final String[] properties = new String[30];
    database.transaction(() -> {
      for (int i = 0; i < properties.length; i++) {
        properties[i] = "k" + i;
        database.getSchema().getType("Alpha").createProperty(properties[i], Type.INTEGER);
      }
    });
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Alpha", properties);

    final StringBuilder items = new StringBuilder();
    for (int i = 0; i < 200; i++)
      items.append(", 1 as pg_c").append(i);

    final PostgresCatalog.Answer few = PostgresCatalog.resolve(
        "select (information_schema._pg_expandarray(i.indkey)).n as pg_n" + items + " from pg_index i, generate_series(1, 20) as s(r)",
        database, USER);
    assertThat(few).isNotNull();
    assertThat(few.rows).hasSize(20 * properties.length);

    assertThatThrownBy(() -> PostgresCatalog.resolve(
        "select (information_schema._pg_expandarray(i.indkey)).n as pg_n" + items + " from pg_index i, generate_series(1, 900) as s(r)",
        database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  // ---------------------------------------------------------------- helpers

  /** {@code select 1 as pg_c0, ..., 1 as pg_c<columns-1> from pg_database d, generate_series(1, rows)}. */
  private static String constants(final int columns, final int rows) {
    final StringBuilder query = new StringBuilder("select ");
    for (int i = 0; i < columns; i++)
      query.append(i > 0 ? ", " : "").append("1 as pg_c").append(i);
    return query.append(" from pg_database d, generate_series(1, ").append(rows).append(") as s(r)").toString();
  }
}
