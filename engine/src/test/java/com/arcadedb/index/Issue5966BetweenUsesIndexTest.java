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
package com.arcadedb.index;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.FilterStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #5966: {@code BETWEEN} on an indexed column never used the index, falling back to a full
 * bucket scan with a row-level filter, unlike the semantically identical {@code x > a AND x < b}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5966BetweenUsesIndexTest {
  private static final String DB_PATH = "target/databases/Issue5966BetweenUsesIndexTest";

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private String plan(final String query, final Object... params) {
    return database.query("sql", query, params).getExecutionPlan().get().prettyPrint(0, 3);
  }

  // #5966: BETWEEN on a single indexed column must use the index, not a full bucket scan
  @Test
  void betweenOnIndexedColumnUsesTheIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.n INTEGER");
      database.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      database.command("sql", "INSERT INTO V SET n = 10");
      database.command("sql", "INSERT INTO V SET n = 20");
      database.command("sql", "INSERT INTO V SET n = 30");
    });

    database.transaction(() -> {
      final String planString = plan("EXPLAIN SELECT n FROM V WHERE n BETWEEN 15 AND 25");
      assertThat(planString).contains("FETCH FROM INDEX");
      assertThat(planString).doesNotContain("SCAN WITH FILTER");

      final ExecutionPlan p = database.query("sql", "SELECT n FROM V WHERE n BETWEEN 15 AND 25").getExecutionPlan().get();
      for (final ExecutionStep step : p.getSteps())
        assertThat(step).as("no row-level FilterStep should be needed for a plain single-field BETWEEN")
            .isNotInstanceOf(FilterStep.class);
    });
  }

  // #5966: BETWEEN must return the exact same rows as the equivalent two-sided AND, including both inclusive bounds
  @Test
  void betweenOnIndexedColumnReturnsSameResultsAsEquivalentAndRange() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.n INTEGER");
      database.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      for (int i = 0; i < 50; i++)
        database.command("sql", "INSERT INTO V SET n = ?", i);
    });

    database.transaction(() -> {
      final List<Integer> betweenResult = collectN(database.query("sql", "SELECT n FROM V WHERE n BETWEEN 15 AND 25"));
      final List<Integer> andResult = collectN(database.query("sql", "SELECT n FROM V WHERE n >= 15 AND n <= 25"));

      betweenResult.sort(Integer::compareTo);
      andResult.sort(Integer::compareTo);

      assertThat(betweenResult).isEqualTo(andResult);
      // inclusive on both ends
      assertThat(betweenResult).contains(15, 25).doesNotContain(14, 26);
    });
  }

  // #5966: a BETWEEN on a middle field of a composite index must stop the key at that field (partial-key index
  // scan), never silently swallow a later equality field into the same ordered-range lookup - the terminal
  // "rangeOp" bit must be set for BetweenCondition exactly like it already is for a plain range BinaryCondition.
  @Test
  void betweenInsideCompositeIndexStopsKeyBuildingAtTheRangeField() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a INTEGER");
      database.command("sql", "CREATE PROPERTY T.b INTEGER");
      database.command("sql", "CREATE PROPERTY T.c INTEGER");
      database.command("sql", "CREATE INDEX ON T (a, b, c) NOTUNIQUE");

      for (int b = 0; b < 30; b++)
        for (int c = 0; c < 3; c++)
          database.command("sql", "INSERT INTO T SET a = 1, b = ?, c = ?", b, c);
      // noise on a different `a` value, must never be returned
      database.command("sql", "INSERT INTO T SET a = 2, b = 10, c = 1");
    });

    database.transaction(() -> {
      final String planString = plan("EXPLAIN SELECT a, b, c FROM T WHERE a = 1 AND b BETWEEN 10 AND 20 AND c = 1");
      // the index key used to fetch is only (a, b) - c stays out of the ordered-range key and is applied as a
      // separate post-fetch filter
      assertThat(planString).contains("FETCH FROM INDEX T[a,b,c]");
      assertThat(planString).contains("a = 1 AND b BETWEEN 10 AND 20");
      assertThat(planString).contains("FILTER ITEMS WHERE").contains("c = 1");

      final ResultSet rs = database.query("sql", "SELECT a, b, c FROM T WHERE a = 1 AND b BETWEEN 10 AND 20 AND c = 1");
      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat((Integer) r.getProperty("a")).isEqualTo(1);
        assertThat((Integer) r.getProperty("b")).isBetween(10, 20);
        assertThat((Integer) r.getProperty("c")).isEqualTo(1);
        count++;
      }
      // b in [10..20] inclusive -> 11 values, one row each for c = 1
      assertThat(count).isEqualTo(11);
    });
  }

  // #5966: NOT BETWEEN must still return correct results after this change. NotBlock does not override
  // isIndexAware(), so it correctly keeps falling back to the pre-existing bucket-scan path (no index-range
  // support was added for NOT BETWEEN by this fix) - what matters here is that the row-level evaluation of the
  // wrapped BetweenCondition itself is unaffected by the new isIndexAware()/resolveKeyFrom() wiring.
  @Test
  void notBetweenOnIndexedColumnStillReturnsCorrectResults() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.n INTEGER");
      database.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      database.command("sql", "INSERT INTO V SET n = 10");
      database.command("sql", "INSERT INTO V SET n = 20");
      database.command("sql", "INSERT INTO V SET n = 30");
    });

    database.transaction(() -> {
      final List<Integer> result = collectN(database.query("sql", "SELECT n FROM V WHERE n NOT BETWEEN 15 AND 25"));
      result.sort(Integer::compareTo);
      assertThat(result).containsExactly(10, 30);
    });
  }

  // #5966: BETWEEN on a case-insensitive (COLLATE CI) indexed STRING column must use the index and apply the
  // range comparison after case-folding, exercising the shared convertKeys()/convertKeysToDeclaredTypes() path
  // (both the insert-side fold and the query-side bound fold this PR's LSMTreeIndex.convertKeys dedupe touches).
  @Test
  void betweenOnCaseInsensitiveIndexedColumnUsesIndexAndFolds() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) NOTUNIQUE");

      database.command("sql", "INSERT INTO Product SET name = 'Apple'");
      database.command("sql", "INSERT INTO Product SET name = 'BANANA'");
      database.command("sql", "INSERT INTO Product SET name = 'cherry'");
      database.command("sql", "INSERT INTO Product SET name = 'Watermelon'");
    });

    database.transaction(() -> {
      final String planString = plan("EXPLAIN SELECT name FROM Product WHERE name BETWEEN 'a' AND 'c'");
      assertThat(planString).contains("FETCH FROM INDEX");

      final List<String> names = new ArrayList<>();
      final ResultSet rs = database.query("sql", "SELECT name FROM Product WHERE name BETWEEN 'a' AND 'c'");
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));

      // case-insensitively, "apple" and "banana" fall in ['a'..'c'], "cherry" and "watermelon" do not
      assertThat(names).containsExactlyInAnyOrder("Apple", "BANANA");
    });
  }

  // #5966: BETWEEN as the FIRST field of a composite UNIQUE index, followed by a further equality field, must
  // also stop key-building at the range field and return correct results. UNIQUE inserts go through
  // LSMTreeIndex.convertKeys() for the constraint check, so this also exercises the refactored delegate on the
  // insert side, not just the query side covered by the other tests in this class.
  @Test
  void betweenAsFirstFieldOfCompositeUniqueIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE U");
      database.command("sql", "CREATE PROPERTY U.a INTEGER");
      database.command("sql", "CREATE PROPERTY U.b INTEGER");
      database.command("sql", "CREATE INDEX ON U (a, b) UNIQUE");

      for (int a = 0; a < 30; a++)
        for (int b = 0; b < 3; b++)
          database.command("sql", "INSERT INTO U SET a = ?, b = ?", a, b);
    });

    database.transaction(() -> {
      final String planString = plan("EXPLAIN SELECT a, b FROM U WHERE a BETWEEN 10 AND 20 AND b = 1");
      assertThat(planString).contains("FETCH FROM INDEX U[a,b]");
      assertThat(planString).contains("a BETWEEN 10 AND 20");

      final ResultSet rs = database.query("sql", "SELECT a, b FROM U WHERE a BETWEEN 10 AND 20 AND b = 1");
      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat((Integer) r.getProperty("a")).isBetween(10, 20);
        assertThat((Integer) r.getProperty("b")).isEqualTo(1);
        count++;
      }
      // a in [10..20] inclusive -> 11 values, one row each for b = 1
      assertThat(count).isEqualTo(11);
    });
  }

  // #5966: an OR across two separately-indexed fields, one of them a BETWEEN, must fetch each branch from its
  // own index (via IndexSearchDescriptor.requiresDistinctStep(), already BetweenCondition-aware from this PR's
  // first commit) with exactly one top-level DISTINCT to dedupe the merged branches - not one redundant DISTINCT
  // per branch - and must still return the correct union of rows.
  @Test
  void orAcrossTwoIndexesWithBetweenBranchMergesWithoutRedundantDistinct() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE X");
      database.command("sql", "CREATE PROPERTY X.a INTEGER");
      database.command("sql", "CREATE PROPERTY X.b INTEGER");
      database.command("sql", "CREATE INDEX ON X (a) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON X (b) NOTUNIQUE");
      for (int i = 0; i < 30; i++)
        database.command("sql", "INSERT INTO X SET a = ?, b = ?", i, i);
    });

    database.transaction(() -> {
      final String planString = plan("EXPLAIN SELECT a, b FROM X WHERE a = 1 OR b BETWEEN 10 AND 20");
      assertThat(planString).contains("FETCH FROM INDEX X[a]").contains("FETCH FROM INDEX X[b]");
      // exactly one DISTINCT merging the two branches, none chained inside either branch
      assertThat(planString.split("DISTINCT", -1).length - 1).isEqualTo(1);

      final List<Integer> aValues = new ArrayList<>();
      final ResultSet rs = database.query("sql", "SELECT a, b FROM X WHERE a = 1 OR b BETWEEN 10 AND 20");
      while (rs.hasNext()) {
        final Result r = rs.next();
        if (r.getProperty("a") != null)
          aValues.add(r.getProperty("a"));
      }
      aValues.sort(Integer::compareTo);
      // a = 1 (from the equality branch) plus a in [10..20] (from the BETWEEN branch, since a = b here)
      final List<Integer> expected = new ArrayList<>();
      expected.add(1);
      for (int i = 10; i <= 20; i++)
        expected.add(i);
      assertThat(aValues).isEqualTo(expected);
    });
  }

  private static List<Integer> collectN(final ResultSet rs) {
    final List<Integer> result = new ArrayList<>();
    while (rs.hasNext())
      result.add(rs.next().getProperty("n"));
    return result;
  }
}
