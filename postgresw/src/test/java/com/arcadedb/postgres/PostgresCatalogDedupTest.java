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
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The catalog removes duplicate rows for {@code DISTINCT}, {@code GROUP BY}, {@code count(DISTINCT ...)}, a set operation and a window
 * partition: over a large row set each of them has to finish within a bound that does not depend on how the row values are spread.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogDedupTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogDedupTest";
  private static final String USER          = "root";

  /** Rows of the series a statement reads: the values are 0, STEP, 2 * STEP, ... and all of them differ. */
  private static final int  ROWS = 30_000;
  private static final long STEP = 4_294_967_297L;

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

  @Test
  void distinctOverALargeSeriesAnswersWithinTheBound() {
    final List<List<Object>> rows = rowsOf("select distinct s.r from " + series(ROWS));

    assertThat(rows).hasSize(ROWS + 1);
  }

  @Test
  void groupByOverALargeSeriesAnswersWithinTheBound() {
    final List<List<Object>> rows = rowsOf("select s.r, count(*) from " + series(ROWS) + " group by s.r");

    assertThat(rows).hasSize(ROWS + 1);
    assertThat(rows.get(0).get(1)).isEqualTo(1L);
  }

  @Test
  void countDistinctOverALargeSeriesAnswersWithinTheBound() {
    // it does less per row than the statements above, so it reads more of them
    final int rows = 5 * ROWS;

    assertThat(rowsOf("select count(distinct s.r) from " + series(rows))).containsExactly(List.of((long) rows + 1));
  }

  @Test
  void unionOverALargeSeriesAnswersWithinTheBound() {
    final List<List<Object>> rows = rowsOf("select s.r from " + series(ROWS) + " union select s.r from " + series(ROWS));

    assertThat(rows).hasSize(ROWS + 1);
  }

  @Test
  void intersectOverALargeSeriesAnswersWithinTheBound() {
    final List<List<Object>> rows = rowsOf("select s.r from " + series(ROWS) + " intersect select s.r from " + series(ROWS));

    assertThat(rows).hasSize(ROWS + 1);
  }

  @Test
  void exceptOverALargeSeriesAnswersWithinTheBound() {
    final List<List<Object>> rows = rowsOf("select s.r from " + series(ROWS) + " except select s.r from " + series(ROWS));

    assertThat(rows).isEmpty();
  }

  @Test
  void windowPartitionOverALargeSeriesAnswersWithinTheBound() {
    final List<List<Object>> rows = rowsOf("select row_number() over (partition by s.r) from " + series(ROWS));

    assertThat(rows).hasSize(ROWS + 1);
    assertThat(rows.get(0).get(0)).isEqualTo(1L);
  }

  /** The FROM clause of a series of {@code rows + 1} values: 0, STEP, ..., rows * STEP. */
  private static String series(final int rows) {
    return "pg_database d, generate_series(0, " + rows * STEP + ", " + STEP + ") as s(r)";
  }

  /** Runs the statement against the bound that tells a pass that does a bounded amount of work per row from one that compares each row with all the others. */
  private List<List<Object>> rowsOf(final String query) {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    stopwatch.assertGaveUpWithin(10_000, "a pass that does a bounded amount of work per row from one that compares each row with all the others");

    assertThat(answer).as(query).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).as("rows of %s", query).isNotNull();
    final List<List<Object>> rows = new ArrayList<>(answer.rows.size());
    for (final Map<String, Object> row : answer.rows)
      rows.add(new ArrayList<>(row.values()));
    return rows;
  }
}
