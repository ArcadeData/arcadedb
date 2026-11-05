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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.FilterStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for indexed range queries (BETWEEN, bounds, ranges inside transactions, ranges over compacted indexes).
 * Each nested class keeps the issue number of the regression it covers in its display name.
 */
class IndexedRangeQueryTest {
  /**
   * Regression test for #5966: {@code BETWEEN} on an indexed column never used the index, falling back to a full
   * bucket scan with a row-level filter, unlike the semantically identical {@code x > a AND x < b}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5966 BETWEEN on an indexed column uses the index")
  class BetweenUsesIndexTest {
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

    // #5966: BETWEEN on name.toLowerCase() over a case-insensitive (COLLATE CI) indexed STRING column must use the
    // index and apply the range comparison after case-folding (a plain "name BETWEEN" is left to the scan, #8932), exercising the shared convertKeys()/convertKeysToDeclaredTypes() path
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
        final String planString = plan("EXPLAIN SELECT name FROM Product WHERE name.toLowerCase() BETWEEN 'a' AND 'c'");
        assertThat(planString).contains("FETCH FROM INDEX");

        final List<String> names = new ArrayList<>();
        final ResultSet rs = database.query("sql", "SELECT name FROM Product WHERE name.toLowerCase() BETWEEN 'a' AND 'c'");
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

  /**
   * Regression guard for #7611: an inclusive bound served by an LSM index seeks to wherever the binary search
   * converged inside the run of entries that share the bound key, instead of to the run's boundary, so the entries
   * on the far side of that landing point are never visited.
   * <p>
   * A full key owns one entry per transaction that wrote it - every commit appends its own {@code (key, rids)} entry to
   * the mutable page rather than merging into the previous one - so a run of equal entries is the normal state of any
   * non-unique index under batched loading, not a partial-key curiosity. {@code compareKey()} walked to the run boundary
   * only for a PARTIAL key, which is why an indexed {@code >=} under-reported by {@code floor((k-1)/2)} for a run of
   * {@code k} entries while the unindexed scan over the same rows was right.
   * <p>
   * Everything here compares the indexed type against an unindexed twin holding identical rows: the index exists to make
   * the query faster, so any disagreement is the defect. The descending half is asserted through
   * {@link RangeIndex#range(boolean, Object[], boolean, Object[], boolean)} directly, because it is the seek and not the SQL
   * that is under test and a descending seek lands in the same run from the other side.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#7611 inclusive lower bound of an indexed range")
  class InclusiveLowerBoundTest extends TestHelper {

    private static final int NEIGHBOURS = 5;

    /** One transaction per row: that is what makes each row its own index entry, and the run its length. */
    private void insertOneTxPerRow(final String indexedType, final String plainType, final String value, final int howMany) {
      for (int i = 0; i < howMany; i++) {
        final int seq = i;
        database.transaction(() -> {
          database.newDocument(indexedType).set("s", value).set("seq", seq).save();
          if (plainType != null)
            database.newDocument(plainType).set("s", value).set("seq", seq).save();
        });
      }
    }

    private void createTypes(final String indexedType, final String plainType) {
      database.command("sql", "CREATE DOCUMENT TYPE " + indexedType);
      database.command("sql", "CREATE PROPERTY " + indexedType + ".s STRING");
      database.command("sql", "CREATE INDEX ON " + indexedType + " (s) NOTUNIQUE");
      if (plainType != null) {
        database.command("sql", "CREATE DOCUMENT TYPE " + plainType);
        database.command("sql", "CREATE PROPERTY " + plainType + ".s STRING");
      }
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
      }
    }

    private long count(final String sql, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("sql", sql, params)) {
        return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
      }
    }

    /**
     * The minimal shape, at every run length the defect distinguishes: {@code k} rows exactly at the bound, each in its
     * own transaction, five below and five above. The shortfall was {@code floor((k-1)/2)}, so k=3 is the smallest
     * failing case and k=1,2 have to keep passing.
     */
    @Test
    void inclusiveLowerBoundReturnsEveryRowAtTheBound() {
      for (final int k : new int[] { 1, 2, 3, 4, 5, 10, 41 }) {
        final String indexed = "Min" + k;
        final String plain = "MinPlain" + k;
        createTypes(indexed, plain);
        insertOneTxPerRow(indexed, plain, "A", NEIGHBOURS);
        insertOneTxPerRow(indexed, plain, "B", k);
        insertOneTxPerRow(indexed, plain, "C", NEIGHBOURS);

        final long expected = k + NEIGHBOURS;
        assertThat(count("SELECT count(*) AS c FROM " + indexed + " WHERE s >= 'B'")).as("k=" + k + ", indexed >=")
            .isEqualTo(expected);
        assertThat(count("SELECT count(*) AS c FROM " + plain + " WHERE s >= 'B'")).as("k=" + k + ", unindexed >=")
            .isEqualTo(expected);
      }
    }

    /**
     * The same bound written the ways that reach the index differently. {@code BETWEEN} lowers to the same inclusive
     * from-key, a bound parameter takes the same path as a literal, and {@code IN} seeks each value as an exact key -
     * it was always correct and must stay so.
     */
    @Test
    void everySpellingOfTheBoundAgreesWithTheUnindexedScan() {
      createTypes("Ix", "Nx");
      insertOneTxPerRow("Ix", "Nx", "A", NEIGHBOURS);
      insertOneTxPerRow("Ix", "Nx", "B", 10);
      insertOneTxPerRow("Ix", "Nx", "C", NEIGHBOURS);

      final long expected = 10 + NEIGHBOURS;

      assertThat(count("SELECT count(*) AS c FROM Ix WHERE s >= 'B'")).as(">= literal").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Ix WHERE s >= :b", Map.of("b", "B"))).as(">= parameter").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Ix WHERE s BETWEEN 'B' AND 'C'")).as("BETWEEN").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Ix WHERE s >= 'B' AND s <= 'C'")).as(">= AND <=").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Ix WHERE s IN ['B', 'C']")).as("IN").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Ix WHERE s = 'B'")).as("=").isEqualTo(10);
      // a strict lower bound discards the run anyway, and an upper bound is enforced per entry rather than by seeking:
      // both were correct before the fix and are here so it cannot break them
      assertThat(count("SELECT count(*) AS c FROM Ix WHERE s > 'A'")).as("> previous key").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Ix WHERE s <= 'B'")).as("<=").isEqualTo(10 + NEIGHBOURS);

      assertThat(count("SELECT count(*) AS c FROM Nx WHERE s >= 'B'")).as("unindexed >=").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Nx WHERE s BETWEEN 'B' AND 'C'")).as("unindexed BETWEEN").isEqualTo(expected);
    }

    /**
     * The seek itself, both directions, through the index API. A descending seek converges in the same run and has to
     * walk to its LAST entry; SQL rarely drives it because an ORDER BY DESC usually sorts after fetching, so the guard
     * goes on {@code range()} directly.
     */
    @Test
    void bothScanDirectionsSeekToTheBoundaryOfTheRun() {
      createTypes("Dir", null);
      insertOneTxPerRow("Dir", null, "A", NEIGHBOURS);
      insertOneTxPerRow("Dir", null, "B", 11);
      insertOneTxPerRow("Dir", null, "C", NEIGHBOURS);

      final RangeIndex index = (RangeIndex) database.getSchema().getIndexByName("Dir[s]");

      // ascending from 'B' inclusive: the 11 at the bound plus the 5 above
      assertThat(collect(index.range(true, new Object[] { "B" }, true, null, true))).as("ascending >= B").hasSize(16);
      // descending from 'B' inclusive: the 11 at the bound plus the 5 below
      assertThat(collect(index.range(false, new Object[] { "B" }, true, null, true))).as("descending <= B").hasSize(16);
      // and exclusive, in both directions, must still drop the whole run
      assertThat(collect(index.range(true, new Object[] { "B" }, false, null, true))).as("ascending > B").hasSize(5);
      assertThat(collect(index.range(false, new Object[] { "B" }, false, null, true))).as("descending < B").hasSize(5);
    }

    private List<Identifiable> collect(final IndexCursor cursor) {
      final List<Identifiable> result = new ArrayList<>();
      while (cursor.hasNext())
        result.add(cursor.next());
      return result;
    }

    /**
     * The behavioural surface the fix widens: an ascending scan now starts at the FIRST entry of the run, so index
     * entries that were previously invisible - including the ADDs that a later transaction deleted - are handed to the
     * cursor's tombstone resolution for the first time. A run mixing live rows, deleted rows and a re-inserted row has
     * to agree with the unindexed scan exactly.
     */
    @Test
    void deletesInsideTheRunResolveAgainstTheWiderScan() {
      createTypes("Tomb", "TombPlain");
      insertOneTxPerRow("Tomb", "TombPlain", "A", NEIGHBOURS);
      insertOneTxPerRow("Tomb", "TombPlain", "B", 9);
      insertOneTxPerRow("Tomb", "TombPlain", "C", NEIGHBOURS);

      // delete 3 of the 9 rows at the bound, each in its own transaction so each delete is its own index entry
      for (final int seq : new int[] { 0, 4, 8 })
        database.transaction(() -> {
          database.command("sql", "DELETE FROM Tomb WHERE s = 'B' AND seq = ?", seq);
          database.command("sql", "DELETE FROM TombPlain WHERE s = 'B' AND seq = ?", seq);
        });

      // and re-insert one of them
      database.transaction(() -> {
        database.newDocument("Tomb").set("s", "B").set("seq", 4).save();
        database.newDocument("TombPlain").set("s", "B").set("seq", 4).save();
      });

      final long expected = 7 + NEIGHBOURS;
      assertThat(count("SELECT count(*) AS c FROM Tomb WHERE s >= 'B'")).as("indexed >= after deletes").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM TombPlain WHERE s >= 'B'")).as("unindexed >= after deletes").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Tomb WHERE s = 'B'")).as("indexed = after deletes").isEqualTo(7);
    }

    /**
     * A size that fills many index pages, then the same assertions across a {@code compact()}. Compaction writes each
     * key once, so it used to REDUCE the loss without removing it - the residue being whatever still sat in mutable
     * pages. Both sides of the compaction have to be exact.
     */
    @Test
    void holdsAcrossManyPagesAndACompaction() throws Exception {
      createTypes("Big", null);

      final int distinctKeys = 200;
      final int perKey = 40;
      for (int t = 0; t < perKey; t++) {
        final int round = t;
        database.transaction(() -> {
          for (int k = 0; k < distinctKeys; k++)
            database.newDocument("Big").set("s", String.format("k%05d", k)).set("seq", round).save();
        });
      }

      final String bound = String.format("k%05d", distinctKeys / 2);
      final String previous = String.format("k%05d", distinctKeys / 2 - 1);
      final long expected = (long) (distinctKeys / 2) * perKey;

      assertThat(count("SELECT count(*) AS c FROM Big WHERE s >= '" + bound + "'")).as("before compaction, >=")
          .isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Big WHERE s > '" + previous + "'")).as("before compaction, >")
          .isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Big WHERE s = '" + bound + "'")).as("before compaction, =")
          .isEqualTo(perKey);

      ((IndexInternal) database.getSchema().getIndexByName("Big[s]")).compact();

      assertThat(count("SELECT count(*) AS c FROM Big WHERE s >= '" + bound + "'")).as("after compaction, >=")
          .isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Big WHERE s > '" + previous + "'")).as("after compaction, >")
          .isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Big WHERE s = '" + bound + "'")).as("after compaction, =")
          .isEqualTo(perKey);
    }

    /**
     * The PARTIAL-key half of the same seek. Resolving an iterator landing point to the run boundary used to live in the
     * two {@code compareKey()} overrides behind a {@code convertedKeys.length < binaryKeyTypes.length} guard, so it
     * covered exactly this composite-prefix case (#6592, #6694) and nothing else; the fix moved it into
     * {@code seekRunBoundary()} for full and partial keys alike. Every other test here scans a single-property index,
     * which means a regression on the prefix path would pass the whole fixture unnoticed - hence a leading-key prefix
     * scan, both directions, before and after a compaction, against an unindexed twin holding the same rows.
     */
    @Test
    void leadingKeyPrefixOfACompositeIndexScansTheWholeGroup() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Comp");
      database.command("sql", "CREATE PROPERTY Comp.a STRING");
      database.command("sql", "CREATE PROPERTY Comp.b STRING");
      database.command("sql", "CREATE INDEX ON Comp (a, b) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE CompPlain");
      database.command("sql", "CREATE PROPERTY CompPlain.a STRING");
      database.command("sql", "CREATE PROPERTY CompPlain.b STRING");

      final int groups = 30;
      final int perGroup = 40;
      // one transaction per round, so every (a, b) pair owns one index entry per round and the prefix group is a run
      for (int round = 0; round < perGroup; round++) {
        final int r = round;
        database.transaction(() -> {
          for (int g = 0; g < groups; g++) {
            final String a = String.format("a%03d", g);
            final String b = String.format("b%03d", r);
            database.newDocument("Comp").set("a", a).set("b", b).save();
            database.newDocument("CompPlain").set("a", a).set("b", b).save();
          }
        });
      }

      final String prefix = String.format("a%03d", groups / 2);
      final RangeIndex index = (RangeIndex) database.getSchema().getIndexByName("Comp[a,b]");

      assertThat(count("SELECT count(*) AS c FROM Comp WHERE a = '" + prefix + "'")).as("before compaction, prefix")
          .isEqualTo(perGroup);
      assertThat(count("SELECT count(*) AS c FROM CompPlain WHERE a = '" + prefix + "'")).as("unindexed prefix")
          .isEqualTo(perGroup);
      // the partial key reaches iterator purposes 2 and 3 directly, which is the path the guard used to own
      assertThat(collect(index.range(true, new Object[] { prefix }, true, new Object[] { prefix }, true)))
          .as("before compaction, ascending prefix").hasSize(perGroup);
      assertThat(collect(index.range(false, new Object[] { prefix }, true, new Object[] { prefix }, true)))
          .as("before compaction, descending prefix").hasSize(perGroup);

      ((IndexInternal) index).compact();

      assertThat(count("SELECT count(*) AS c FROM Comp WHERE a = '" + prefix + "'")).as("after compaction, prefix")
          .isEqualTo(perGroup);
      assertThat(collect(index.range(true, new Object[] { prefix }, true, new Object[] { prefix }, true)))
          .as("after compaction, ascending prefix").hasSize(perGroup);
      assertThat(collect(index.range(false, new Object[] { prefix }, true, new Object[] { prefix }, true)))
          .as("after compaction, descending prefix").hasSize(perGroup);
    }

    /**
     * The shape it was found on: ISO date strings on an indexed STRING property, loaded in batches - the exact form of
     * every time-bounded report. The loss was entirely on the day EQUAL to the lower bound, every later day complete,
     * which is why only a row count catches it.
     */
    @Test
    void batchedDateLoadMatchesTheUnindexedScan() {
      database.command("sql", "CREATE DOCUMENT TYPE Ship");
      database.command("sql", "CREATE PROPERTY Ship.d STRING");
      database.command("sql", "CREATE INDEX ON Ship (d) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE ShipNoIndex");
      database.command("sql", "CREATE PROPERTY ShipNoIndex.d STRING");

      final int perDay = 20;
      final List<String> values = new ArrayList<>();
      for (int day = 0; day < 365 * 3; day++) {
        final String d = LocalDate.of(1993, 1, 1).plusDays(day).toString();
        for (int i = 0; i < perDay; i++)
          values.add(d);
      }
      Collections.shuffle(values, new Random(42));

      final int batchSize = 5_000;
      for (int offset = 0; offset < values.size(); offset += batchSize) {
        final int lo = offset, hi = Math.min(offset + batchSize, values.size());
        database.transaction(() -> {
          for (int i = lo; i < hi; i++) {
            database.newDocument("Ship").set("d", values.get(i)).save();
            database.newDocument("ShipNoIndex").set("d", values.get(i)).save();
          }
        });
      }

      final long expected = 365L * perDay;
      assertThat(count("SELECT count(*) AS c FROM Ship WHERE d >= '1994-01-01' AND d < '1995-01-01'")).as("indexed range")
          .isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM ShipNoIndex WHERE d >= '1994-01-01' AND d < '1995-01-01'"))
          .as("unindexed range").isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Ship WHERE d BETWEEN '1994-01-01' AND '1994-12-31'")).as("indexed BETWEEN")
          .isEqualTo(expected);
      assertThat(count("SELECT count(*) AS c FROM Ship WHERE d = '1994-01-01'")).as("the bound day alone").isEqualTo(perDay);
    }
  }

  /**
   * #6927: an index RANGE scan run inside a transaction ignored the transaction overlay's REMOVE entries, so it
   * emitted rows that the same transaction had already deleted or re-keyed. The point-lookup path
   * ({@link com.arcadedb.index.lsm.LSMTreeIndex#get(Object[], int)}) subtracts the pending removals from the disk
   * result via its {@code removedRids} filter; {@code LSMTreeIndexCursor} collected only the overlay's ADD entries
   * and dropped the REMOVEs on the floor, so the still-present disk entry was emitted unchanged.
   * <p>
   * The user-visible symptom is a row that does not satisfy the WHERE clause at all: {@code UPDATE ... SET age = 99
   * WHERE age = 20} becomes {@code REMOVE(20, rid) + ADD(99, rid)} in the overlay, and the following
   * {@code SELECT ... WHERE age &lt; 30} resolved the stale disk entry for key 20 back to the (now age=99) record.
   * Nothing re-checks the predicate downstream, because the index consumed it entirely.
   * <p>
   * The pure-delete variant was wrong in a noisier way: the dead RIDs reached
   * {@code GetValueFromIndexEntryStep}, which swallows the {@code RecordNotFoundException} with a WARNING per row,
   * so SQL looked right by accident. Any direct {@link RangeIndex#range} / {@link RangeIndex#iterator} caller had
   * no such net and received the dead RIDs.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  @DisplayName("#6927 range scan honours same-transaction removes")
  class RangeScanTxRemovesTest extends TestHelper {

    private static final String TYPE_NAME = "Person";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final var type = database.getSchema().createDocumentType(TYPE_NAME);
        type.createProperty("name", Type.STRING);
        type.createProperty("age", Type.INTEGER);
        type.createProperty("code", Type.INTEGER);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "age" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "code" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      });

      database.transaction(() -> {
        database.newDocument(TYPE_NAME).set("name", "a").set("age", 20).set("code", 1).save();
        database.newDocument(TYPE_NAME).set("name", "b").set("age", 25).set("code", 2).save();
      });
    }

    /**
     * The reporter's exact shape: an in-transaction UPDATE that re-keys an indexed property, followed by a range
     * scan whose predicate the index consumes entirely. Before the fix the scan returned the re-keyed row too,
     * with {@code age=99}, i.e. a row violating the WHERE clause it was selected by.
     */
    @Test
    void rangeScanDoesNotReturnRowReKeyedInTheSameTransaction() {
      database.begin();
      try {
        database.command("sql", "UPDATE " + TYPE_NAME + " SET age = 99 WHERE age = 20");

        final List<String> names = new ArrayList<>();
        final ResultSet rs = database.query("sql", "SELECT name, age FROM " + TYPE_NAME + " WHERE age < 30");
        while (rs.hasNext()) {
          final Result r = rs.next();
          assertThat(r.<Integer>getProperty("age")).isLessThan(30);
          names.add(r.getProperty("name"));
        }

        assertThat(names).containsExactly("b");

        // the point-lookup path has always been right: keep both paths pinned to the same answer
        assertThat(database.lookupByKey(TYPE_NAME, "age", 20).hasNext()).isFalse();

        // ...and the new key must be visible to the range scan
        final List<String> reKeyed = new ArrayList<>();
        final ResultSet rs2 = database.query("sql", "SELECT name FROM " + TYPE_NAME + " WHERE age > 30");
        while (rs2.hasNext())
          reKeyed.add(rs2.next().getProperty("name"));
        assertThat(reKeyed).containsExactly("a");
      } finally {
        database.rollback();
      }
    }

    /**
     * The same asymmetry seen from the raw {@link RangeIndex} API, which has no {@code RecordNotFoundException}
     * net downstream: a record deleted in this transaction must not be handed back by an ordered scan.
     */
    @Test
    void rangeCursorSkipsRecordDeletedInTheSameTransaction() {
      database.begin();
      try {
        final RID deleted = lookupRid("age", 20);
        database.lookupByRID(deleted, true).asDocument().delete();

        assertThat(collectRange("age", 0, 30, true)).doesNotContain(deleted).hasSize(1);
        assertThat(collectRange("age", 0, 30, false)).doesNotContain(deleted).hasSize(1);
        assertThat(collectIterator("age", true)).doesNotContain(deleted).hasSize(1);
        assertThat(collectIterator("age", false)).doesNotContain(deleted).hasSize(1);
      } finally {
        database.rollback();
      }
    }

    /**
     * A non-unique key holding two RIDs where only one is deleted in the transaction: the per-RID REMOVE must
     * suppress exactly that RID and leave the sibling alone. This is the case a "the whole key is gone" shortcut
     * would get wrong in the opposite direction.
     */
    @Test
    void rangeCursorSuppressesOnlyTheRemovedRidOfASharedKey() {
      database.transaction(() -> database.newDocument(TYPE_NAME).set("name", "c").set("age", 20).set("code", 3).save());

      database.begin();
      try {
        final List<RID> sharing = collectRange("age", 20, 20, true);
        assertThat(sharing).hasSize(2);

        final RID victim = sharing.get(0);
        final RID survivor = sharing.get(1);
        database.lookupByRID(victim, true).asDocument().delete();

        assertThat(collectRange("age", 0, 30, true)).contains(survivor).doesNotContain(victim);
        assertThat(collectRange("age", 0, 30, false)).contains(survivor).doesNotContain(victim);
      } finally {
        database.rollback();
      }
    }

    /**
     * On a UNIQUE index a pending REMOVE means the key is gone, exactly as {@code LSMTreeIndex.get()} already
     * decided by returning an empty cursor; the range scan has to agree.
     */
    @Test
    void rangeCursorHonoursPendingRemoveOnAUniqueIndex() {
      database.begin();
      try {
        final RID deleted = lookupRid("code", 1);
        database.lookupByRID(deleted, true).asDocument().delete();

        assertThat(database.lookupByKey(TYPE_NAME, "code", 1).hasNext()).isFalse();
        assertThat(collectRange("code", 0, 10, true)).doesNotContain(deleted).hasSize(1);
        assertThat(collectRange("code", 0, 10, false)).doesNotContain(deleted).hasSize(1);
      } finally {
        database.rollback();
      }
    }

    /**
     * A record deleted AND a fresh one inserted at the same key in the same transaction: the ADD has to resurrect
     * the key even though a REMOVE is pending on it, so the scan sees the new RID and only the new RID.
     */
    @Test
    void rangeCursorResurrectsAKeyReInsertedAfterTheRemove() {
      database.begin();
      try {
        final RID deleted = lookupRid("age", 20);
        database.lookupByRID(deleted, true).asDocument().delete();
        final RID inserted = database.newDocument(TYPE_NAME).set("name", "a2").set("age", 20).set("code", 4).save()
            .getIdentity();

        final List<RID> rids = collectRange("age", 0, 30, true);
        assertThat(rids).contains(inserted).doesNotContain(deleted).hasSize(2);
      } finally {
        database.rollback();
      }
    }

    /**
     * Every entry of the scanned range removed in the transaction: the cursor must report exhaustion instead of
     * emitting dead RIDs, and must not resurrect them on a descending pass either.
     */
    @Test
    void rangeCursorIsEmptyWhenEveryEntryWasRemovedInTheTransaction() {
      database.begin();
      try {
        for (final RID rid : collectRange("age", 0, 100, true))
          database.lookupByRID(rid, true).asDocument().delete();

        assertThat(collectRange("age", 0, 100, true)).isEmpty();
        assertThat(collectRange("age", 0, 100, false)).isEmpty();
        assertThat(collectIterator("age", true)).isEmpty();

        final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE age < 100");
        assertThat(rs.hasNext()).isFalse();
      } finally {
        database.rollback();
      }
    }

    /**
     * The {@code REPLACE} shape, which only a UNIQUE index can produce: within one transaction the record holding key
     * {@code K} is deleted and a DIFFERENT record is inserted at the same {@code K}. Because a unique index's overlay
     * map is keyed by key value alone (not by RID), {@code TransactionIndexContext.addIndexKeyLock} collapses the pair
     * into a single {@code REPLACE(rid=new, oldRid=old)} entry - so the pending removal of the old RID survives ONLY
     * as {@code oldRid}, which commit does replay ({@code index.removeReplay(keyValues, oldRid)}). A scan that reads
     * only {@code rid} therefore re-emits the old RID straight off the page.
     * <p>
     * The type is created with a single bucket on purpose: each bucket owns its own sub-index and therefore its own
     * transaction lane, so a delete and an insert landing in different buckets stay two independent entries and never
     * merge into a REPLACE.
     */
    @Test
    void rangeScanAndLookupAgreeOnAUniqueKeyReplacedInTheSameTransaction() {
      database.transaction(() -> {
        final var type = database.getSchema().createDocumentType("Registration", 1);
        type.createProperty("code", Type.INTEGER);
        database.getSchema().buildTypeIndex("Registration", new String[] { "code" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      });
      database.transaction(() -> database.newDocument("Registration").set("code", 7).save());

      database.begin();
      try {
        final IndexCursor before = database.lookupByKey("Registration", "code", 7);
        assertThat(before.hasNext()).isTrue();
        final RID replaced = before.next().getIdentity();

        database.lookupByRID(replaced, true).asDocument().delete();
        final RID replacement = database.newDocument("Registration").set("code", 7).save().getIdentity();

        final RangeIndex index = (RangeIndex) database.getSchema().getType("Registration")
            .getPolymorphicIndexByProperties("code");

        final List<RID> ranged = drain(index.range(true, new Object[] { 0 }, true, new Object[] { 10 }, true));
        assertThat(ranged).containsExactly(replacement);
        assertThat(drain(index.range(false, new Object[] { 10 }, true, new Object[] { 0 }, true)))
            .containsExactly(replacement);

        // the point lookup on a UNIQUE key must agree: exactly one RID, the replacement. Asserted on the RAW
        // sub-index too: TypeIndex.get() asks a unique index for limit=1 and returns whichever entry comes out
        // first, so it would MASK a two-RID answer behind HashSet iteration order rather than fail on it.
        for (final IndexInternal sub : ((TypeIndex) database.getSchema().getType("Registration")
            .getPolymorphicIndexByProperties("code")).getSubIndexes())
          assertThat(drain(sub.get(new Object[] { 7 }))).containsExactly(replacement);

        assertThat(drain(database.lookupByKey("Registration", "code", 7))).containsExactly(replacement);
      } finally {
        database.rollback();
      }
    }

    /**
     * A key-wide {@code remove(keys)} - the manual-index API form that carries no RID - on a NON-unique index. Both the
     * range scan and the point lookup have to drop every disk RID at that key. {@code get()} used to allocate an EMPTY
     * {@code removedRids} set for this shape and then filter nothing with it, so the removal was silently a no-op.
     */
    @Test
    void bothPathsHonourAKeyWideRemoveOnANonUniqueIndex() {
      database.transaction(() -> database.newDocument(TYPE_NAME).set("name", "c").set("age", 20).set("code", 3).save());

      database.begin();
      try {
        final RangeIndex index = rangeIndex("age");
        assertThat(drain(index.get(new Object[] { 20 }))).hasSize(2);

        // no RID: the whole key goes, both RIDs with it
        ((IndexInternal) index).remove(new Object[] { 20 });

        assertThat(index.get(new Object[] { 20 }).hasNext()).isFalse();
        assertThat(collectRange("age", 0, 30, true)).hasSize(1);
        assertThat(collectRange("age", 0, 30, false)).hasSize(1);
      } finally {
        database.rollback();
      }
    }

    /**
     * The same REPLACE shape as {@link #rangeScanAndLookupAgreeOnAUniqueKeyReplacedInTheSameTransaction} on a HASH
     * index, whose {@code get()} carries a copy of the very same overlay-merge code. A hash index has no range scan,
     * so the point lookup is the only path there - and it has to answer with one RID, not two.
     */
    @Test
    void hashIndexLookupHonoursAUniqueKeyReplacedInTheSameTransaction() {
      database.transaction(() -> {
        final var type = database.getSchema().createDocumentType("Badge", 1);
        type.createProperty("serial", Type.INTEGER);
        database.getSchema().buildTypeIndex("Badge", new String[] { "serial" })
            .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
      });
      database.transaction(() -> database.newDocument("Badge").set("serial", 11).save());

      database.begin();
      try {
        final RID replaced = database.lookupByKey("Badge", "serial", 11).next().getIdentity();
        database.lookupByRID(replaced, true).asDocument().delete();
        final RID replacement = database.newDocument("Badge").set("serial", 11).save().getIdentity();

        for (final IndexInternal sub : ((TypeIndex) database.getSchema().getType("Badge")
            .getPolymorphicIndexByProperties("serial")).getSubIndexes())
          assertThat(drain(sub.get(new Object[] { 11 }))).containsExactly(replacement);

        assertThat(drain(database.lookupByKey("Badge", "serial", 11))).containsExactly(replacement);
      } finally {
        database.rollback();
      }
    }

    private RID lookupRid(final String property, final Object key) {
      final IndexCursor cursor = database.lookupByKey(TYPE_NAME, property, key);
      assertThat(cursor.hasNext()).isTrue();
      return cursor.next().getIdentity();
    }

    private RangeIndex rangeIndex(final String property) {
      return (RangeIndex) database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties(property);
    }

    private List<RID> collectRange(final String property, final int from, final int to, final boolean ascending) {
      final IndexCursor cursor = rangeIndex(property).range(ascending, new Object[] { ascending ? from : to }, true,
          new Object[] { ascending ? to : from }, true);
      return drain(cursor);
    }

    private List<RID> collectIterator(final String property, final boolean ascending) {
      return drain(rangeIndex(property).iterator(ascending));
    }

    private List<RID> drain(final IndexCursor cursor) {
      final List<RID> rids = new ArrayList<>();
      while (cursor.hasNext()) {
        final Identifiable next = cursor.next();
        assertThat(next).isNotNull();
        rids.add(next.getIdentity());
        // a RID handed out by an in-tx scan must resolve: a dead one is exactly the defect this test guards
        final Document doc = database.lookupByRID(next.getIdentity(), true).asDocument();
        assertThat(doc).isNotNull();
      }
      return rids;
    }
  }

  /**
   * #8817: inside a transaction, an index range read missed the records that transaction inserted once the LSM index
   * had been compacted into a sub-index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#8817 transaction inserts visible to ranges over a compacted index")
  class TxRangeOverCompactedIndexTest extends TestHelper {
    private static final int COMMITTED = 20_000;

    private void load(final boolean unique, final boolean compact) throws Exception {
      database.transaction(() -> {
        final var type = database.getSchema().createVertexType("V", 1);
        type.createProperty("a", Type.LONG);
        database.getSchema().buildTypeIndex("V", new String[] { "a" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(unique)
            .withPageSize(4096).create();
      });
      for (int base = 0; base < COMMITTED; base += 5_000) {
        final int from = base;
        database.transaction(() -> {
          for (int i = from; i < from + 5_000; i++)
            database.newVertex("V").set("a", 1000L + i).save();
        });
      }
      if (compact)
        for (final Index idx : database.getSchema().getType("V").getAllIndexes(false))
          for (final IndexInternal bucketIndex : ((TypeIndex) idx).getIndexesOnBuckets())
            if (bucketIndex.scheduleCompaction()) {
              bucketIndex.compact();
              assertThat(((LSMTreeIndex) bucketIndex).getMutableIndex().getSubIndex()).as("index must be compacted").isNotNull();
            }
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void txInsertsAreVisibleToIndexRangesOverCompactedIndex(final boolean unique) throws Exception {
      load(unique, true);
      database.begin();
      try {
        for (int i = 0; i < 10; i++)
          database.newVertex("V").set("a", 500L + i).save();
        // 50 below every committed key, 50 above, 30 committed deleted, 20 updated to 5
        for (int i = 0; i < 50; i++)
          database.newVertex("V").set("a", (long) i).save();
        for (int i = 0; i < 50; i++)
          database.newVertex("V").set("a", 10_000_000L + i).save();
        database.command("sql", "DELETE FROM V WHERE a >= 1100 AND a < 1130");
        database.command("sql", "UPDATE V SET a = a + 5000000 WHERE a >= 1200 AND a < 1220");

        assertThat(count("SELECT count(*) AS c FROM V WHERE a BETWEEN 500 AND 509")).isEqualTo(10);
        assertThat(count("SELECT count(*) AS c FROM V WHERE a = 505")).isEqualTo(1);
        assertThat(count("SELECT count(*) AS c FROM V WHERE a < 1010")).isEqualTo(count("SELECT count(*) AS c FROM V WHERE a + 0 < 1010"));
        assertThat(count("SELECT count(*) AS c FROM V WHERE a > 9999999")).isEqualTo(50);
        assertThat(count("SELECT count(*) AS c FROM V WHERE a BETWEEN 0 AND 49")).isEqualTo(
            count("SELECT count(*) AS c FROM V WHERE a + 0 BETWEEN 0 AND 49"));
        assertThat(count("SELECT min(a) AS c FROM V")).isEqualTo(0);
        assertThat(count("SELECT max(a) AS c FROM V")).isEqualTo(10_000_049);
        assertThat(count("SELECT count(*) AS c FROM V")).isEqualTo(COMMITTED + 10 + 100 - 30);
        try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE a >= 0 ORDER BY a ASC LIMIT 3")) {
          assertThat(rs.next().<Number>getProperty("a").longValue()).isEqualTo(0);
          assertThat(rs.next().<Number>getProperty("a").longValue()).isEqualTo(1);
          assertThat(rs.next().<Number>getProperty("a").longValue()).isEqualTo(2);
        }
        try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) RETURN min(v.a) AS m, max(v.a) AS x")) {
          final var r = rs.next();
          assertThat(r.<Number>getProperty("m").longValue()).isEqualTo(0);
          assertThat(r.<Number>getProperty("x").longValue()).isEqualTo(10_000_049);
        }
        try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) WHERE v.a < 1010 RETURN count(*) AS c")) {
          assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(count("SELECT count(*) AS c FROM V WHERE a + 0 < 1010"));
        }
      } finally {
        database.rollback();
      }
    }

    @Test
    void indexApiRangeSeesTxInserts() throws Exception {
      load(false, true);
      database.begin();
      try {
        for (int i = 0; i < 10; i++)
          database.newVertex("V").set("a", 500L + i).save();
        final TypeIndex index = database.getSchema().getType("V").getIndexesByProperties("a").get(0);
        int n = 0;
        try (final IndexCursor c = index.range(true, new Object[] { 500L }, true, new Object[] { 1009L }, true)) {
          while (c.hasNext()) {
            c.next();
            n++;
          }
        }
        assertThat(n).isEqualTo(20);
      } finally {
        database.rollback();
      }
    }
  }

  /**
   * #5214: bounded (range/partial) scans over an LSM index that compacted into several series must return every key
   * exactly once and in the requested order.
   * <p>
   * Several compacted series inside one file are produced by a RAM-bounded compaction ({@code INDEX_COMPACTION_RAM_MB}),
   * i.e. on large indexes whose immutable pages exceed the compaction budget. Two defects lived on that layout:
   * <ul>
   * <li>a series lying wholly inside {@code [fromKeys, toKeys]} (containing neither endpoint) produced no cursor in
   * {@code LSMTreeIndexCompacted.newIterators}, so every interior series was dropped and range scans returned partial
   * results;</li>
   * <li>{@code iterator(false, fromKeys, inclusive)} delegated to the direction-auto-detecting range with a {@code null}
   * begin key, which defaults to ascending, so a descending partial scan iterated ascending.</li>
   * </ul>
   * The existing partial-scan tests only asserted element counts, never completeness of the set or the ordering, so both
   * defects stayed latent. These tests assert both.
   */
  @Nested
  @DisplayName("#5214 range scans across a multi-series compacted index")
  class MultiSeriesRangeTest extends TestHelper {

    private static final String TYPE_NAME = "Doc";
    private static final int    TOTAL_KEYS = 80_000;
    private static final int    LO         = 20_000;
    private static final int    HI         = 60_000;
    private static final int    PAGE_SIZE  = 256 * 1024;
    private static final String KEY_PAD    = "x".repeat(64);

    private static String key(final int i) {
      return "K" + KEY_PAD + String.format("%08d", i);
    }

    @BeforeEach
    @Override
    public void beforeTest() {
      // Disable AUTO compaction so the explicit compaction below is the only one and is not raced by a background one.
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    }

    /**
     * Builds a non-unique LSM index over a single bucket whose data lives in more than one compacted series, all emitted
     * by a single RAM-bounded compaction round.
     */
    private TypeIndex buildMultiSeriesIndex() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("email", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();

      database.transaction(() -> {
        for (int i = 0; i < TOTAL_KEYS; i++)
          database.newDocument(TYPE_NAME).set("email", key(i)).save();
      });

      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("email").get(0);
      final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

      assertThat(((IndexInternal) typeIndex).scheduleCompaction()).as("compaction scheduled").isTrue();
      assertThat(((IndexInternal) typeIndex).compact()).as("compaction executed").isTrue();

      assertThat(bucketIndex.getMutableIndex().getSubIndex().newIterators(true, null, null))
          .as("test setup must produce multiple compacted series").hasSizeGreaterThan(2);

      return typeIndex;
    }

    /** Drains a cursor, asserting strict monotonic order in the given direction, and returns the emitted keys. */
    private static List<String> drain(final IndexCursor cursor, final boolean ascending) {
      final List<String> keys = new ArrayList<>();
      try {
        String previous = null;
        while (cursor.hasNext()) {
          cursor.next();
          final String current = (String) cursor.getKeys()[0];
          if (previous != null) {
            if (ascending)
              assertThat(current).as("keys must be strictly ascending").isGreaterThan(previous);
            else
              assertThat(current).as("keys must be strictly descending").isLessThan(previous);
          }
          previous = current;
          keys.add(current);
        }
      } finally {
        cursor.close();
      }
      return keys;
    }

    private static List<String> expected(final int fromInclusive, final int toInclusive, final boolean ascending) {
      final List<String> keys = new ArrayList<>();
      if (ascending)
        for (int i = fromInclusive; i <= toInclusive; i++)
          keys.add(key(i));
      else
        for (int i = toInclusive; i >= fromInclusive; i--)
          keys.add(key(i));
      return keys;
    }

    @Tag("slow")
    @Test
    void ascendingRangeWindowReturnsEveryKey() throws Exception {
      final TypeIndex typeIndex = buildMultiSeriesIndex();
      // Window spanning several series; whole series fall strictly inside and must not be dropped.
      final IndexCursor cursor = typeIndex.range(true, new Object[] { key(LO) }, true, new Object[] { key(HI) }, true);
      assertThat(drain(cursor, true)).isEqualTo(expected(LO, HI, true));
    }

    @Tag("slow")
    @Test
    void descendingRangeWindowReturnsEveryKey() throws Exception {
      final TypeIndex typeIndex = buildMultiSeriesIndex();
      // Descending window: begin=high bound, end=low bound.
      final IndexCursor cursor = typeIndex.range(false, new Object[] { key(HI) }, true, new Object[] { key(LO) }, true);
      assertThat(drain(cursor, false)).isEqualTo(expected(LO, HI, false));
    }

    @Tag("slow")
    @Test
    void ascendingPartialFromReturnsEveryKey() throws Exception {
      final TypeIndex typeIndex = buildMultiSeriesIndex();
      final IndexCursor cursor = typeIndex.iterator(true, new Object[] { key(LO) }, true);
      assertThat(drain(cursor, true)).isEqualTo(expected(LO, TOTAL_KEYS - 1, true));
    }

    @Tag("slow")
    @Test
    void descendingPartialFromReturnsEveryKeyInDescendingOrder() throws Exception {
      final TypeIndex typeIndex = buildMultiSeriesIndex();
      final IndexCursor cursor = typeIndex.iterator(false, new Object[] { key(HI) }, true);
      assertThat(drain(cursor, false)).isEqualTo(expected(0, HI, false));
    }

    @Tag("slow")
    @Test
    void exclusiveRangeWindowExcludesEndpoints() throws Exception {
      final TypeIndex typeIndex = buildMultiSeriesIndex();
      final IndexCursor asc = typeIndex.range(true, new Object[] { key(LO) }, false, new Object[] { key(HI) }, false);
      assertThat(drain(asc, true)).isEqualTo(expected(LO + 1, HI - 1, true));
      final IndexCursor desc = typeIndex.range(false, new Object[] { key(HI) }, false, new Object[] { key(LO) }, false);
      assertThat(drain(desc, false)).isEqualTo(expected(LO + 1, HI - 1, false));
    }

    @Tag("slow")
    @Test
    void sqlRangeCountIsComplete() throws Exception {
      buildMultiSeriesIndex();
      try (final ResultSet rs = database.query("sql",
          "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE email >= ? AND email <= ?", key(LO), key(HI))) {
        assertThat((Long) rs.next().getProperty("c")).isEqualTo((long) (HI - LO + 1));
      }
    }

    @Tag("slow")
    @Test
    void sqlOrderByDescIsCompleteAndOrdered() throws Exception {
      buildMultiSeriesIndex();
      // The planner satisfies ORDER BY ... DESC directly from the index descending scan (no explicit sort), so this
      // exercises the end-to-end descending range path over multiple compacted series.
      final List<String> keys = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT email FROM " + TYPE_NAME + " WHERE email <= ? ORDER BY email DESC", key(HI))) {
        String previous = null;
        while (rs.hasNext()) {
          final String current = rs.next().getProperty("email");
          if (previous != null)
            assertThat(current).as("SQL ORDER BY DESC keys must be strictly descending").isLessThan(previous);
          previous = current;
          keys.add(current);
        }
      }
      assertThat(keys).hasSize(HI + 1);
      assertThat(keys.get(0)).isEqualTo(key(HI));
      assertThat(keys.get(keys.size() - 1)).isEqualTo(key(0));
    }
  }

  /**
   * #5386: a range seek whose from-key sorts OUTSIDE the indexed [min,max] keyspace threw an NPE
   * ("convertedKeys is null") on a NOTUNIQUE single-field STRING index that compacted into several series.
   * <p>
   * Root cause (present in 26.5.1 / 26.7.3): for an open-ended range ({@code path >= X}, no upper bound) whose from-key
   * fell outside a compacted series' key range, {@code LSMTreeIndexCompacted.newIterators} fell back to a second lookup
   * against {@code toKeys}, which is {@code null} for an open-ended range. {@code lookupInPage(..., null, ...)} then
   * dereferenced {@code convertedKeys.length}. The #5214 rewrite (commit b0171877b) removed that fallback in favour of an
   * explicit full-series cursor, fixing this path; #5214's own tests only exercised bounded windows (both from and to
   * keys), so this test pins the open-ended out-of-range case.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5386 seek outside the compacted key range")
  class OutOfRangeSeekTest extends TestHelper {

    private static final String TYPE_NAME  = "FileDoc";
    private static final int    TOTAL_KEYS = 80_000;
    private static final int    PAGE_SIZE  = 256 * 1024;
    private static final String KEY_PAD    = "x".repeat(64);

    private static String key(final int i) {
      return "K" + KEY_PAD + String.format("%08d", i);
    }

    @BeforeEach
    @Override
    public void beforeTest() {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    }

    private TypeIndex buildMultiSeriesIndex() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("path", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "path" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();

      database.transaction(() -> {
        for (int i = 0; i < TOTAL_KEYS; i++)
          database.newDocument(TYPE_NAME).set("path", key(i)).save();
      });

      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("path").get(0);
      final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

      assertThat(((IndexInternal) typeIndex).scheduleCompaction()).as("compaction scheduled").isTrue();
      assertThat(((IndexInternal) typeIndex).compact()).as("compaction executed").isTrue();

      assertThat(bucketIndex.getMutableIndex().getSubIndex().newIterators(true, null, null))
          .as("test setup must produce multiple compacted series").hasSizeGreaterThan(2);

      return typeIndex;
    }

    @Tag("slow")
    @Test
    void seekBelowMinDoesNotThrow() throws Exception {
      buildMultiSeriesIndex();

      // in range: works
      assertThatCode(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", key(0))) {
          assertThat(rs.hasNext()).isTrue();
          assertThat((String) rs.next().getProperty("path")).isEqualTo(key(0));
        }
      }).doesNotThrowAnyException();

      // below min: must not NPE, must return the smallest key
      assertThatCode(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "A")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat((String) rs.next().getProperty("path")).isEqualTo(key(0));
        }
      }).doesNotThrowAnyException();

      // empty string (below every non-empty key)
      assertThatCode(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat((String) rs.next().getProperty("path")).isEqualTo(key(0));
        }
      }).doesNotThrowAnyException();
    }

    @Tag("slow")
    @Test
    void seekAboveMaxDoesNotThrow() throws Exception {
      buildMultiSeriesIndex();

      // beyond max: must not NPE, must return no rows
      assertThatCode(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "z")) {
          assertThat(rs.hasNext()).isFalse();
        }
      }).doesNotThrowAnyException();
    }

    /**
     * Reproduces the reporter's observed layout: the compacted segment's min is HIGHER than a newer mutable segment's
     * min (empty-string keys live only in the newer mutable pages). A {@code >= ''} seek is in-range for the mutable
     * segment but out-of-range-below for every compacted series.
     */
    @Tag("slow")
    @Test
    void seekBelowCompactedMinWithLowerMutableKeys() throws Exception {
      buildMultiSeriesIndex();

      // Now add records with keys that sort BELOW the compacted min into the (newer) mutable segment.
      database.transaction(() -> {
        for (int i = 0; i < 98; i++)
          database.newDocument(TYPE_NAME).set("path", "").save();          // empty-string keys: global minimum
        database.newDocument(TYPE_NAME).set("path", "A").save();
      });

      // equality on the empty string must find the 98 rows
      try (final ResultSet rs = database.query("sql",
          "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE path = ''")) {
        assertThat((Long) rs.next().getProperty("c")).isEqualTo(98L);
      }

      // >= '' : in-range for mutable, out-of-range-below for compacted series. Must not NPE.
      assertThatCode(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat((String) rs.next().getProperty("path")).isEqualTo("");
        }
      }).doesNotThrowAnyException();

      // >= 'A' : also below the compacted min.
      assertThatCode(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "A")) {
          assertThat(rs.hasNext()).isTrue();
        }
      }).doesNotThrowAnyException();
    }
  }

  /**
   * Issue #10541: a lookup on an indexed property with a value of another kind than the property can hold answers what the same
   * query answers without the index.
   * <p>
   * The index converts the operand to its declared key type, and for a kind the type cannot hold the conversion either leaves the
   * operand as it was or answers {@code null}, neither of which is a key. The first reached the key comparison, which cast it
   * ({@code ClassCastException} for a BOOLEAN or DECIMAL property, {@code IllegalArgumentException} "Comparison between type N and N not
   * supported" for a DATE) and the scan answered no rows; the second was read as "no bound", so {@code p > ?} with a Boolean, a List, a
   * Map or a LocalDate on an INTEGER or DOUBLE property returned every row. An {@code IN} whose right side is not a list is refused by the
   * openCypher scan and was answered with no rows through the index.
   * <p>
   * The guarantee is the one differential the issue measures, over every property type, operand kind and predicate shape of both
   * languages, on one indexed type and one identical type without the index: the two answer the same, rows or the refusal.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#10541 operand kinds answer the same with and without the index")
  class IndexedOperandKindTest extends TestHelper {

    /** One row per type, holding what each property type holds. */
    private static final Map<String, Object> STORED = new LinkedHashMap<>();

    /** What a caller can bind: every kind a property type can or cannot hold. */
    private static final Map<String, Object> OPERANDS = new LinkedHashMap<>();

    static {
      STORED.put("BOOLEAN", true);
      STORED.put("INTEGER", 7);
      STORED.put("LONG", 7L);
      STORED.put("DOUBLE", 1.5);
      STORED.put("DECIMAL", new BigDecimal("1.5"));
      STORED.put("STRING", "abc");
      STORED.put("DATE", LocalDate.of(2020, 1, 1));
      STORED.put("DATETIME", LocalDateTime.of(2020, 1, 1, 0, 0));

      OPERANDS.put("String", "zzz");
      OPERANDS.put("NumericString", "5");
      OPERANDS.put("Integer", 7);
      OPERANDS.put("Long", 7L);
      OPERANDS.put("Double", 1.5);
      OPERANDS.put("BigDecimal", new BigDecimal("1.5"));
      OPERANDS.put("Boolean", true);
      OPERANDS.put("LocalDate", LocalDate.of(2020, 1, 1));
      OPERANDS.put("LocalDateTime", LocalDateTime.of(2020, 1, 1, 0, 0));
      // Holding what the INTEGER and LONG rows hold, so that an operand split into its items, or into its values, would find a row
      OPERANDS.put("List", List.of(7));
      OPERANDS.put("Map", Map.of("a", 7));
    }

    /**
     * The operand kinds each property type cannot hold. For an equality or an IN they match no row, and the index has to answer so, as the
     * scan does. For a range the scan orders some of them across kinds by what they convert to (a LocalDate against a DECIMAL by its epoch
     * number, a Boolean against it as 1), a leniency the index cannot reproduce because its keys have one order, one kind: there the index
     * may decline (no rows) but must neither throw nor answer a row the scan does not.
     */
    private static final Map<String, List<String>> FOREIGN = Map.of(
        "BOOLEAN", List.of("LocalDate", "LocalDateTime", "List", "Map"),
        "INTEGER", List.of("Boolean", "LocalDate", "LocalDateTime", "List", "Map"),
        "LONG", List.of("Boolean", "LocalDate", "LocalDateTime", "List", "Map"),
        "DOUBLE", List.of("Boolean", "LocalDate", "LocalDateTime", "List", "Map"),
        "DECIMAL", List.of("Boolean", "LocalDate", "LocalDateTime", "List", "Map"),
        "STRING", List.of("List", "Map"),
        "DATE", List.of("Boolean", "List", "Map", "NumericString"),
        "DATETIME", List.of("Boolean", "List", "Map", "NumericString"));

    /** SQL shapes, {@code :x} the operand and {@code :v} what the property holds, so an IN can hold one element that matches. */
    private static final List<String> SQL_EQUALITIES = List.of("p = :x", "p IN [:x]", "p IN [:x, :v]", "p IN :x");
    private static final List<String> SQL_RANGES     = List.of("p > :x", "p >= :x", "p < :x", "p <= :x");

    /** openCypher shapes. */
    private static final List<String> CYPHER_EQUALITIES = List.of("n.p = $x", "n.p IN [$x]", "n.p IN [$x, $v]", "n.p IN $x");
    private static final List<String> CYPHER_RANGES     = List.of("n.p > $x", "n.p >= $x", "n.p < $x", "n.p <= $x");

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "NOTUNIQUE", "UNIQUE", "UNIQUE_HASH" })
    void everyOperandKindAnswersWithTheIndexAsWithoutIt(final String indexKind) {
      createTypes(indexKind);

      final List<String> differences = new ArrayList<>();
      for (final Map.Entry<String, Object> stored : STORED.entrySet())
        for (final Map.Entry<String, Object> operand : OPERANDS.entrySet()) {
          final Map<String, Object> parameters = Map.of("x", operand.getValue(), "v", stored.getValue());
          final boolean foreign = FOREIGN.get(stored.getKey()).contains(operand.getKey());
          for (final String predicate : SQL_EQUALITIES)
            compare(differences, "sql", stored.getKey(), predicate, operand.getKey(), parameters,
                "SELECT id FROM %s WHERE " + predicate, false);
          for (final String predicate : SQL_RANGES)
            compare(differences, "sql", stored.getKey(), predicate, operand.getKey(), parameters,
                "SELECT id FROM %s WHERE " + predicate, foreign);
          for (final String predicate : CYPHER_EQUALITIES)
            compare(differences, "opencypher", stored.getKey(), predicate, operand.getKey(), parameters,
                "MATCH (n:%s) WHERE " + predicate + " RETURN n.id AS id", false);
          for (final String predicate : CYPHER_RANGES)
            compare(differences, "opencypher", stored.getKey(), predicate, operand.getKey(), parameters,
                "MATCH (n:%s) WHERE " + predicate + " RETURN n.id AS id", foreign);
        }

      assertThat(differences).as("cases where the indexed type and the one without the index answer differently on %s "
          + "(language | property | predicate | operand | with the index | without the index)", indexKind).isEmpty();
    }

    /** The differential is worth something only if the indexed type is read through its index: a plain equality is. */
    @Test
    void theIndexedTypesAreReadThroughTheirIndex() {
      createTypes("NOTUNIQUE");

      for (final String property : STORED.keySet()) {
        try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT id FROM I_" + property + " WHERE p = :v", Map.of("v", STORED.get(property)))) {
          assertThat(rs.next().<String>getProperty("executionPlanAsString")).as("SQL plan on %s", property).contains("FETCH FROM INDEX");
        }
        try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (n:I_" + property + ") WHERE n.p = $v RETURN n.id AS id",
            Map.of("v", STORED.get(property)))) {
          rs.stream().count();
          assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).as("Cypher plan on %s", property).contains("NodeIndexSeek");
        }
      }
    }

    /** A list element that matches is found whatever the other elements are: the elements of another kind are skipped, not fatal. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "NOTUNIQUE", "UNIQUE_HASH" })
    void anInListFindsTheElementThatMatchesBesideOnesOfAnotherKind(final String indexKind) {
      createTypes(indexKind);

      for (final Map.Entry<String, Object> stored : STORED.entrySet())
        for (final Map.Entry<String, Object> operand : OPERANDS.entrySet()) {
          final Map<String, Object> parameters = Map.of("x", operand.getValue(), "v", stored.getValue());
          assertThat(run("sql", "SELECT id FROM I_" + stored.getKey() + " WHERE p IN [:x, :v]", parameters))
              .as("SQL IN [%s, stored] on %s", operand.getKey(), stored.getKey()).isEqualTo("rows [1]");
          assertThat(run("opencypher", "MATCH (n:I_" + stored.getKey() + ") WHERE n.p IN [$x, $v] RETURN n.id AS id", parameters))
              .as("Cypher IN [%s, stored] on %s", operand.getKey(), stored.getKey()).isEqualTo("rows [1]");
        }
    }

    private void createTypes(final String indexKind) {
      for (final Map.Entry<String, Object> stored : STORED.entrySet()) {
        final String property = stored.getKey();
        for (final String prefix : new String[] { "I_", "N_" }) {
          final String type = prefix + property;
          database.command("sql", "CREATE VERTEX TYPE " + type).close();
          database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER").close();
          database.command("sql", "CREATE PROPERTY " + type + ".p " + property).close();
        }
        database.command("sql", "CREATE INDEX ON I_" + property + " (p) " + indexKind).close();
        database.transaction(() -> {
          for (final String prefix : new String[] { "I_", "N_" })
            database.newVertex(prefix + property).set("id", 1).set("p", stored.getValue()).save();
        });
      }
    }

    /** {@code mayDecline}: the index may answer no rows where the scan orders the operand across kinds, see {@link #FOREIGN}. */
    private void compare(final List<String> differences, final String language, final String property, final String predicate,
        final String operand, final Map<String, Object> parameters, final String template, final boolean mayDecline) {
      final String indexed = run(language, String.format(template, "I_" + property), parameters);
      final String scanned = run(language, String.format(template, "N_" + property), parameters);
      if (!indexed.equals(scanned) && !(mayDecline && indexed.equals("rows []")))
        differences.add(language + " | " + property + " | " + predicate + " | " + operand + " | " + indexed + " | " + scanned);
    }

    /** The ids the query answers, sorted, or the class of the exception at the bottom of what it threw. */
    private String run(final String language, final String query, final Map<String, Object> parameters) {
      try (final ResultSet rs = database.query(language, query, parameters)) {
        final List<String> ids = new ArrayList<>();
        while (rs.hasNext())
          ids.add(String.valueOf(rs.next().<Object>getProperty("id")));
        Collections.sort(ids);
        return "rows " + ids;
      } catch (final Throwable e) {
        Throwable root = e;
        while (root.getCause() != null)
          root = root.getCause();
        return root.getClass().getSimpleName();
      }
    }
  }
}
