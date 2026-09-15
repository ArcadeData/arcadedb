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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue7611IndexedRangeLowerBoundTest extends TestHelper {

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
