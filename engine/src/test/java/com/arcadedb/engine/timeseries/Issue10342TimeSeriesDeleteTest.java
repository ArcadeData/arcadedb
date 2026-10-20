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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10342: the samples of a TIMESERIES type can be deleted - {@code DELETE FROM <type> WHERE <time range> AND <tag> = <value>}
 * and {@code TRUNCATE TYPE} - where #10336 only refused the statements. Every case is checked against a model kept in Java: the rows
 * the type holds afterwards are exactly the rows the predicate leaves, and so are the aggregates the engine answers from the statistics
 * of the sealed blocks, which a rebuilt block has to recompute.
 * <p>
 * The same predicates run over the three layouts a type can be in - every sample in the mutable pages, every sample in sealed blocks,
 * and both - because the two layers are deleted by different code, and a delete that missed one would only show on a layout that has it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10342TimeSeriesDeleteTest extends TestHelper {
  /** 2023-11-14 22:13:20 UTC. */
  private static final long BASE    = 1_700_000_000_000L;
  private static final int  SAMPLES = 400;

  private record Sample(long ts, String host, String region, Double v, Integer n, String s, boolean b) {
  }

  private final List<Sample> model = new ArrayList<>();

  // ------------------------------------------------------------------------------------------------------------------ the layouts

  enum Layout {MUTABLE, SEALED, BOTH}

  static Stream<Arguments> cases() {
    final List<Arguments> cases = new ArrayList<>();
    for (final Layout layout : Layout.values())
      for (final int shards : new int[] { 1, 3 }) {
        cases.add(Arguments.of(layout, shards, "a time range", "ts >= " + (BASE + 100_000L) + " AND ts <= " + (BASE + 250_000L),
            (Predicate<Sample>) s -> s.ts >= BASE + 100_000L && s.ts <= BASE + 250_000L));
        cases.add(Arguments.of(layout, shards, "everything before", "ts < " + (BASE + 150_000L),
            (Predicate<Sample>) s -> s.ts < BASE + 150_000L));
        cases.add(Arguments.of(layout, shards, "everything after", "ts > " + (BASE + 300_000L),
            (Predicate<Sample>) s -> s.ts > BASE + 300_000L));
        cases.add(Arguments.of(layout, shards, "BETWEEN", "ts BETWEEN " + (BASE + 40_000L) + " AND " + (BASE + 41_000L),
            (Predicate<Sample>) s -> s.ts >= BASE + 40_000L && s.ts <= BASE + 41_000L));
        cases.add(Arguments.of(layout, shards, "one timestamp", "ts = " + (BASE + 7_000L), (Predicate<Sample>) s -> s.ts == BASE + 7_000L));
        cases.add(Arguments.of(layout, shards, "one tag", "host = 'h1'", (Predicate<Sample>) s -> s.host.equals("h1")));
        cases.add(Arguments.of(layout, shards, "two tags", "host = 'h2' AND region = 'eu'",
            (Predicate<Sample>) s -> s.host.equals("h2") && s.region.equals("eu")));
        cases.add(Arguments.of(layout, shards, "a tag in a time range", "host = 'h0' AND ts >= " + (BASE + 50_000L) + " AND ts < " + (BASE + 200_000L),
            (Predicate<Sample>) s -> s.host.equals("h0") && s.ts >= BASE + 50_000L && s.ts < BASE + 200_000L));
        cases.add(Arguments.of(layout, shards, "a tag nobody has", "host = 'nobody'", (Predicate<Sample>) s -> false));
        cases.add(Arguments.of(layout, shards, "a range nobody is in", "ts > " + (BASE + 10_000_000L), (Predicate<Sample>) s -> false));
        cases.add(Arguments.of(layout, shards, "an empty range", "ts > " + (BASE + 5_000L) + " AND ts < " + (BASE + 4_000L),
            (Predicate<Sample>) s -> false));
        cases.add(Arguments.of(layout, shards, "every sample", null, (Predicate<Sample>) s -> true));
      }
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} {1} shard(s): {2}")
  @MethodSource("cases")
  void deletesExactlyWhatTheWhereNames(final Layout layout, final int shards, final String what, final String where,
      final Predicate<Sample> predicate) throws Exception {
    create(shards);
    populate(layout);
    final long expected = model.stream().filter(predicate).count();
    model.removeIf(predicate);

    try (final ResultSet rs = database.command("sql", "DELETE FROM M" + (where != null ? " WHERE " + where : ""))) {
      assertThat(rs.next().<Long>getProperty("count")).as("deleted").isEqualTo(expected);
    }

    assertMatchesModel();
  }

  @ParameterizedTest(name = "{0} {1} shard(s)")
  @MethodSource("layouts")
  void truncateTypeRemovesEverySample(final Layout layout, final int shards) throws Exception {
    create(shards);
    populate(layout);
    final long expected = model.size();

    try (final ResultSet rs = database.command("sql", "TRUNCATE TYPE M")) {
      final Result result = rs.next();
      assertThat(result.<Long>getProperty("count")).isEqualTo(expected);
      assertThat(result.<Boolean>getProperty("transactional")).isFalse();
    }
    model.clear();

    assertMatchesModel();
    final TimeSeriesEngine engine = engine();
    for (int s = 0; s < engine.getShardCount(); s++) {
      assertThat(engine.getShard(s).getSealedStore().getBlockCount()).as("sealed blocks of shard %d", s).isZero();
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).as("mutable samples of shard %d", s).isZero();
    }
    // the type is still there, and takes samples again
    append(0, 10);
    assertMatchesModel();
  }

  static Stream<Arguments> layouts() {
    final List<Arguments> layouts = new ArrayList<>();
    for (final Layout layout : Layout.values())
      for (final int shards : new int[] { 1, 3 })
        layouts.add(Arguments.of(layout, shards));
    return layouts.stream();
  }

  // ------------------------------------------------------------------------------------------------------------------ the details

  /** A block wholly inside the range is dropped, one that crosses it is rebuilt, one outside it is left as it is. */
  @Test
  void blocksAreDroppedRebuiltOrKept() throws Exception {
    create(1);
    // three blocks of 100 samples, one per compaction
    for (int round = 0; round < 3; round++) {
      append(round * 100, (round + 1) * 100);
      engine().compactAll();
    }
    final TimeSeriesSealedStore sealed = engine().getShard(0).getSealedStore();
    assertThat(sealed.getBlockCount()).isEqualTo(3);

    // [50 .. 249]: the first block loses its tail, the second goes, the third loses its head
    final Predicate<Sample> predicate = s -> s.ts >= BASE + 50_000L && s.ts <= BASE + 249_000L;
    final long expected = model.stream().filter(predicate).count();
    model.removeIf(predicate);
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE ts >= " + (BASE + 50_000L) + " AND ts <= " + (BASE + 249_000L))) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(expected);
    }

    assertThat(sealed.getBlockCount()).as("blocks after").isEqualTo(2);
    assertThat(sealed.getTotalSampleCount()).isEqualTo(model.size());
    assertMatchesModel();
  }

  /** A delete that finds nothing rewrites nothing and disturbs no read. */
  @Test
  void aDeleteThatFindsNothingRewritesNothing() throws Exception {
    create(1);
    append(0, 100);
    engine().compactAll();
    append(100, 150);
    final TimeSeriesSealedStore sealed = engine().getShard(0).getSealedStore();
    final long rewrites = sealed.getDownsampleRewriteCount();

    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE host = 'nobody'")) {
      assertThat(rs.next().<Long>getProperty("count")).isZero();
    }
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE ts > " + (BASE + 10_000_000L))) {
      assertThat(rs.next().<Long>getProperty("count")).isZero();
    }

    assertThat(sealed.getDownsampleRewriteCount()).isEqualTo(rewrites);
    assertMatchesModel();
  }

  /** A sample with no value is still no value after its block is rebuilt, whatever codec its column is stored with. */
  @Test
  void aRebuiltBlockKeepsAbsentValuesAndEveryCodec() throws Exception {
    create(1);
    append(0, SAMPLES);
    engine().compactAll();

    final Predicate<Sample> predicate = s -> s.host.equals("h3") && s.ts < BASE + 200_000L;
    model.removeIf(predicate);
    database.command("sql", "DELETE FROM M WHERE host = 'h3' AND ts < " + (BASE + 200_000L)).close();

    assertThat(model).anyMatch(s -> s.v == null);
    assertThat(model).anyMatch(s -> s.n == null);
    assertMatchesModel();
  }

  @Test
  void samplesAppendedAfterADeleteAreStoredAndReadAndCompacted() throws Exception {
    create(2);
    append(0, 200);
    engine().compactAll();
    append(200, 300);

    model.removeIf(s -> s.region.equals("us"));
    database.command("sql", "DELETE FROM M WHERE region = 'us'").close();
    assertMatchesModel();

    append(300, 400);
    assertMatchesModel();
    engine().compactAll();
    assertMatchesModel();
  }

  @Test
  void theDeleteSurvivesAReopen() throws Exception {
    create(2);
    append(0, 250);
    engine().compactAll();
    append(250, SAMPLES);

    final Predicate<Sample> predicate = s -> s.ts >= BASE + 100_000L && s.ts < BASE + 330_000L && !s.host.equals("h0");
    model.removeIf(predicate);
    database.command("sql", "DELETE FROM M WHERE ts >= " + (BASE + 100_000L) + " AND ts < " + (BASE + 330_000L) + " AND host = 'h1'").close();
    database.command("sql", "DELETE FROM M WHERE ts >= " + (BASE + 100_000L) + " AND ts < " + (BASE + 330_000L) + " AND host = 'h2'").close();
    database.command("sql", "DELETE FROM M WHERE ts >= " + (BASE + 100_000L) + " AND ts < " + (BASE + 330_000L) + " AND host = 'h3'").close();
    assertMatchesModel();

    reopenDatabase();
    assertMatchesModel();
  }

  /**
   * A delete is not part of the transaction: like the append, it is durable when it returns, in both layers - the sealed block it
   * rebuilds and the mutable pages it compacts - and so is a truncate.
   */
  @Test
  void aRollbackDoesNotBringTheSamplesBack() throws Exception {
    create(1);
    append(0, 100);
    engine().compactAll();
    append(100, 200);

    database.begin();
    database.command("sql", "DELETE FROM M WHERE host = 'h0'").close();
    database.rollback();

    model.removeIf(s -> s.host.equals("h0"));
    assertMatchesModel();

    database.begin();
    database.command("sql", "TRUNCATE TYPE M").close();
    database.rollback();

    model.clear();
    assertMatchesModel();
  }

  /**
   * A block that fails its CRC aborts the delete. A block the delete rebuilds is written with a fresh CRC, so a damaged one that was
   * rebuilt anyway would come out looking sound: the delete reads it checked, fails, and removes nothing from either layer.
   */
  @Test
  void aDamagedSealedBlockAbortsTheDeleteInsteadOfBeingRebuilt() throws Exception {
    create(1);
    append(0, 100);
    engine().compactAll();
    append(100, 150);

    // inside the last block's compressed data: the final four bytes of the file are that block's stored CRC
    final File sealed = new File(database.getDatabasePath(), "M_shard_0.ts.sealed");
    assertThat(sealed).exists();
    flipByteAt(sealed, sealed.length() - 8);
    // a block this process wrote is trusted without being read back: make it look at what is on disk
    engine().getShard(0).getSealedStore().clearCRCValidationCache();
    try {
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM M WHERE host = 'h1'").close()).hasStackTraceContaining("CRC mismatch");
    } finally {
      flipByteAt(sealed, sealed.length() - 8);
      engine().getShard(0).getSealedStore().clearCRCValidationCache();
    }

    // nothing was deleted, in either layer
    assertMatchesModel();
  }

  /** Flips one bit at {@code offset} of {@code file}, the least the format has to notice. */
  private static void flipByteAt(final File file, final long offset) throws IOException {
    try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      raf.seek(offset);
      final int b = raf.read();
      raf.seek(offset);
      raf.write(b ^ 0x01);
    }
  }

  @Test
  void whatItCannotDeleteExactlyIsRefusedAndDeletesNothing() throws Exception {
    create(1);
    append(0, 100);
    engine().compactAll();

    final String[] refused = { "DELETE FROM M WHERE v > 5", "DELETE FROM M WHERE host = 'h0' OR host = 'h1'",
        "DELETE FROM M WHERE ts != " + BASE, "DELETE FROM M WHERE host = 'h0' AND host = 'h1'", "DELETE FROM M WHERE n = 3",
        "DELETE FROM M WHERE host = 'h0' AND v < 5", "DELETE FROM M WHERE host = null", "DELETE FROM M WHERE host LIKE 'h%'",
        "DELETE FROM M LIMIT 5", "DELETE FROM M RETURN BEFORE", "DELETE FROM M WHERE ts > 0 BATCH 10" };
    for (final String statement : refused)
      assertThatThrownBy(() -> database.command("sql", statement).close()).as(statement).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("TIMESERIES type 'M'");

    assertThatThrownBy(() -> database.command("sql", "UPDATE M SET v = 1 WHERE host = 'h0'").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("cannot be updated");

    assertMatchesModel();
  }

  /** A back-quoted type name is the type, as everywhere else in the planner. */
  @Test
  void aBackQuotedTypeNameIsTheTimeSeriesType() throws Exception {
    create(1);
    append(0, 100);

    final Predicate<Sample> predicate = s -> s.host.equals("h1");
    final long expected = model.stream().filter(predicate).count();
    // a quoted column is the column for the SELECT push-down too, so the DELETE removes exactly what this returns
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM `M` WHERE `host` = 'h1' AND `ts` >= " + BASE)) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(expected);
    }
    model.removeIf(predicate);
    try (final ResultSet rs = database.command("sql", "DELETE FROM `M` WHERE `host` = 'h1' AND `ts` >= " + BASE)) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(expected);
    }
    assertMatchesModel();
  }

  /** EXPLAIN shows the scope, and runs nothing. */
  @Test
  void explainShowsTheScopeAndDeletesNothing() throws Exception {
    create(1);
    append(0, 100);

    try (final ResultSet rs = database.command("sql", "EXPLAIN DELETE FROM M WHERE host = 'h1' AND ts > " + BASE)) {
      final String plan = rs.next().<String>getProperty("executionPlanAsString");
      assertThat(plan).contains("DELETE FROM TIMESERIES M").contains("host = 'h1'");
    }
    assertMatchesModel();
  }

  /**
   * The escape from the wedge of #10381: a constraint cannot be declared over a sample that violates it, and the sample can now be
   * deleted by its time, so the constraint can.
   */
  @Test
  void aSampleThatBlocksAConstraintCanBeDeletedByItsTime() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE W TIMESTAMP ts TAGS (host STRING) FIELDS (a INTEGER) SHARDS 1");
    database.command("sql", "INSERT INTO W SET ts = 1000, host = 'h', a = 10");
    database.command("sql", "INSERT INTO W SET ts = 2000, host = 'h', a = 999");

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY W.a MAX 50").close()).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("timestamp 2000").hasMessageContaining("DELETE FROM W WHERE ts = 2000");

    try (final ResultSet rs = database.command("sql", "DELETE FROM W WHERE ts = 2000 AND host = 'h'")) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
    }
    database.command("sql", "ALTER PROPERTY W.a MAX 50").close();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM W")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(1L);
    }
  }

  /** A read straddling a delete fails loudly instead of answering from blocks that no longer exist. */
  @Test
  void aWalkInFlightIsRefusedRatherThanAnsweredShort() throws Exception {
    create(1);
    for (int round = 0; round < 3; round++) {
      append(round * 100, (round + 1) * 100);
      engine().compactAll();
    }

    final Iterator<Object[]> walk = engine().iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(walk.next()).isNotNull();

    database.command("sql", "DELETE FROM M WHERE ts >= " + (BASE + 100_000L) + " AND ts < " + (BASE + 200_000L)).close();

    assertThatThrownBy(() -> {
      while (walk.hasNext())
        walk.next();
    }).hasMessageContaining("delete of samples");
  }

  /**
   * A mutable bucket of several data pages is cleared and written again whole: the page boundaries, the partial last page and the
   * header counters come out as a bucket that was never deleted from, and it takes samples, and a compaction, afterwards.
   */
  @Test
  void aMutableBucketOfSeveralPagesIsRewrittenWhole() throws Exception {
    create(1);
    final TimeSeriesBucket bucket = engine().getShard(0).getMutableBucket();
    final int rows = 3 * bucket.getMaxSamplesPerPage() + 123;
    append(0, rows);
    final int pagesBefore = bucket.getDataPageCount();
    assertThat(pagesBefore).as("data pages before").isGreaterThanOrEqualTo(4);

    final long cutoff = BASE + (rows / 2) * 1_000L;
    final Predicate<Sample> predicate = s -> s.host.equals("h2") && s.ts < cutoff;
    final long expected = model.stream().filter(predicate).count();
    model.removeIf(predicate);
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE host = 'h2' AND ts < " + cutoff)) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(expected);
    }

    assertThat(bucket.getSampleCount()).isEqualTo(model.size());
    assertMatchesModel();

    // the first half of the time, whatever the series: the bucket shrinks by pages
    final Predicate<Sample> firstHalf = s -> s.ts < cutoff;
    model.removeIf(firstHalf);
    database.command("sql", "DELETE FROM M WHERE ts < " + cutoff).close();
    assertThat(bucket.getDataPageCount()).as("data pages after").isLessThan(pagesBefore);
    assertThat(bucket.getSampleCount()).isEqualTo(model.size());
    assertMatchesModel();

    append(rows, rows + 500);
    assertMatchesModel();
    engine().compactAll();
    assertMatchesModel();
  }

  /**
   * A delete over a store of many blocks that all mix tags rebuilds every one of them, one at a time as the rewrite reaches it: each
   * keeps its place in the time order, retained blocks stay between rebuilt ones, and none is lost, duplicated or left declaring a tag
   * it no longer holds.
   */
  @Test
  void aTagDeleteOverManyMixedBlocksRebuildsEachInPlace() throws Exception {
    create(1);
    final int blocks = 25;
    final int perBlock = 40;
    for (int b = 0; b < blocks; b++) {
      append(b * perBlock, (b + 1) * perBlock);
      engine().compactAll();
    }
    final TimeSeriesSealedStore sealed = engine().getShard(0).getSealedStore();
    final int blocksBefore = sealed.getBlockCount();
    assertThat(blocksBefore).as("sealed blocks before").isGreaterThanOrEqualTo(blocks);
    assertMatchesModel();

    // every block holds all four hosts: all of them are rebuilt
    final long expectedOne = model.stream().filter(s -> s.host.equals("h1")).count();
    model.removeIf(s -> s.host.equals("h1"));
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE host = 'h1'")) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(expectedOne);
    }
    assertThat(sealed.getBlockCount()).as("no block disappears: each keeps the other hosts").isEqualTo(blocksBefore);
    assertMatchesModel();

    // the first half of the time only: the blocks of the first half are rebuilt, the ones of the second are retained between them
    final long half = BASE + (blocks * perBlock / 2) * 1_000L;
    final Predicate<Sample> firstHalfOfOne = s -> s.host.equals("h2") && s.ts < half;
    final long expectedTwo = model.stream().filter(firstHalfOfOne).count();
    model.removeIf(firstHalfOfOne);
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE host = 'h2' AND ts < " + half)) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(expectedTwo);
    }
    assertThat(sealed.getBlockCount()).isEqualTo(blocksBefore);
    assertMatchesModel();

    // and the file takes samples, and a compaction, afterwards
    append(blocks * perBlock, blocks * perBlock + 100);
    engine().compactAll();
    assertMatchesModel();
  }

  /** The rows {@code [from, to)} of a bucket of {@code rows} rows holding {@code perPage} per page, by index. */
  private record RowRange(String what, BiFunction<Integer, Integer, int[]> of) {
    @Override
    public String toString() {
      return what;
    }
  }

  static Stream<RowRange> rowRanges() {
    return Stream.of(
        new RowRange("the first row", (p, rows) -> new int[] { 0, 1 }),
        new RowRange("a row at each side of a page boundary", (p, rows) -> new int[] { p - 1, p + 1 }),
        new RowRange("a whole page in the middle", (p, rows) -> new int[] { p, 2 * p }),
        new RowRange("the partial last page", (p, rows) -> new int[] { 3 * p, rows }),
        new RowRange("a row of the partial last page", (p, rows) -> new int[] { 3 * p + 2, 3 * p + 3 }),
        new RowRange("the last full page and the partial one", (p, rows) -> new int[] { 2 * p, rows }),
        new RowRange("the second half of a page to the middle of the next", (p, rows) -> new int[] { p / 2, p + p / 2 }),
        new RowRange("all but the first row", (p, rows) -> new int[] { 1, rows }),
        new RowRange("every row, short of the shortcut", (p, rows) -> new int[] { 0, rows }));
  }

  /**
   * The mutable pages are compacted in place: the rows that stay move down over the ones removed, byte for byte, and the pages before
   * the first removed row are not written at all. Every shape of removal against the page boundaries - the first row, a boundary, a
   * whole page, the partial last page, everything - ends with the rows the model holds, in order, packed in the fewest pages, the
   * counters of the header and of every page right, and a bucket that takes samples and a compaction afterwards.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("rowRanges")
  void theMutablePagesAreCompactedInPlace(final RowRange range) throws Exception {
    create(1);
    final TimeSeriesBucket bucket = engine().getShard(0).getMutableBucket();
    final int perPage = bucket.getMaxSamplesPerPage();
    final int rows = 3 * perPage + 5;
    append(0, rows);
    assertThat(bucket.getDataPageCount()).as("data pages before").isEqualTo(4);

    final int[] bounds = range.of().apply(perPage, rows);
    final long from = BASE + bounds[0] * 1_000L;
    final long to = BASE + bounds[1] * 1_000L;
    // the pages that end before the first removed row: what a delete has no reason to write
    final int untouchedPages = bounds[0] / perPage;
    final int[] versionsBefore = new int[untouchedPages + 1];
    for (int p = 1; p <= untouchedPages; p++)
      versionsBefore[p] = ((DatabaseInternal) database).getPageManager()
          .getMostRecentVersionOfPage(new PageId(database, bucket.getFileId(), p), bucket.getPageSize());

    final Predicate<Sample> predicate = s -> s.ts >= from && s.ts < to;
    final long expected = model.stream().filter(predicate).count();
    model.removeIf(predicate);
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE ts >= " + from + " AND ts < " + to)) {
      assertThat(rs.next().<Long>getProperty("count")).as("deleted").isEqualTo(expected);
    }

    assertThat(bucket.getSampleCount()).as("samples of the bucket").isEqualTo(model.size());
    assertThat(bucket.getDataPageCount()).as("data pages: packed, no hole").isEqualTo((model.size() + perPage - 1) / perPage);
    for (int p = 1; p <= untouchedPages; p++)
      assertThat(((DatabaseInternal) database).getPageManager()
          .getMostRecentVersionOfPage(new PageId(database, bucket.getFileId(), p), bucket.getPageSize()))
          .as("page %d ends before the first removed row: not written", p).isEqualTo(versionsBefore[p]);
    assertMatchesModel();

    append(rows, rows + 2 * perPage);
    assertMatchesModel();
    engine().compactAll();
    assertMatchesModel();
  }

  /**
   * A delete that stopped after some of the shards (a failure part way through the loop) is finished by running it again: what is
   * gone is not deleted twice, and the shards it did not reach are.
   */
  @Test
  void aDeleteThatStoppedAfterSomeShardsIsFinishedByRunningItAgain() throws Exception {
    create(3);
    populate(Layout.BOTH);
    final long from = BASE + 50_000L;
    final long to = BASE + 250_000L;

    // what the interrupted statement did: the first shard only
    final TimeSeriesShard first = engine().getShard(0);
    final long[] doneByFirst = new long[1];
    first.runSealedRewrite(() -> doneByFirst[0] = first.deleteSamplesLocked(from, to, null));
    assertThat(doneByFirst[0]).as("the first shard held samples of the range").isPositive();

    final Predicate<Sample> inRange = s -> s.ts >= from && s.ts <= to;
    final long inTotal = model.stream().filter(inRange).count();
    model.removeIf(inRange);
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE ts >= " + from + " AND ts <= " + to)) {
      assertThat(rs.next().<Long>getProperty("count")).as("what the first run left").isEqualTo(inTotal - doneByFirst[0]);
    }
    assertMatchesModel();
  }

  /**
   * A failure in the sealed rewrite of a shard leaves that shard, and the ones after it, as they were; the shards before it stay
   * deleted, and running the statement again finishes the job.
   */
  @Test
  void aFailedSealedRewriteLeavesTheShardAsItWasAndRerunningFinishesTheDelete() throws Exception {
    create(3);
    populate(Layout.BOTH);
    final long from = BASE + 50_000L;
    final long to = BASE + 250_000L;
    final long[] before = new long[3];
    for (int s = 0; s < 3; s++)
      before[s] = engine().getShard(s).countSamples();

    // the temp file of the second shard's sealed rewrite cannot be created: a directory holds its path
    final Path obstruction = Path.of(database.getDatabasePath(), "M_shard_1.ts.sealed.tmp");
    Files.createDirectory(obstruction);
    try {
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM M WHERE ts >= " + from + " AND ts <= " + to).close())
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("TIMESERIES type 'M'");
    } finally {
      Files.deleteIfExists(obstruction);
    }

    assertThat(engine().getShard(0).countSamples()).as("the shard before the failure was deleted").isLessThan(before[0]);
    assertThat(engine().getShard(1).countSamples()).as("the shard that failed is as it was").isEqualTo(before[1]);
    assertThat(engine().getShard(2).countSamples()).as("the shard after it was not reached").isEqualTo(before[2]);
    // the session ended normally and published what the shard before the failure did (to the followers, under HA), so no shard waits to
    // ship a file: the one that failed rewrote nothing
    for (int s = 0; s < 3; s++)
      assertThat(engine().getShard(s).hasUnpublishedSealedDelete()).as("shard %d after the failed statement", s).isFalse();

    model.removeIf(s -> s.ts >= from && s.ts <= to);
    database.command("sql", "DELETE FROM M WHERE ts >= " + from + " AND ts <= " + to).close();
    for (int s = 0; s < 3; s++)
      assertThat(engine().getShard(s).hasUnpublishedSealedDelete()).as("shard %d after the statement that finished the job", s).isFalse();
    assertMatchesModel();
  }

  /** A sample with no tag is declared by its block as the empty text, as a compaction declares it, so the filter finds the rest. */
  @Test
  void aPartiallyDeletedBlockWithNullTagsDeclaresWhatItHolds() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE N2 TIMESTAMP ts TAGS (host STRING, region STRING) FIELDS (v DOUBLE) SHARDS 1").close();
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("N2")).getEngine();
    final int rows = 150;
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[3][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = BASE + i * 1_000L;
      columns[0][i] = "h" + (i % 2);
      columns[1][i] = i % 5 == 0 ? null : (i % 2 == 0 ? "eu" : "us");
      columns[2][i] = (double) i;
    }
    engine.appendBatch(Arrays.copyOfRange(timestamps, 0, 100), new Object[][] { Arrays.copyOfRange(columns[0], 0, 100),
        Arrays.copyOfRange(columns[1], 0, 100), Arrays.copyOfRange(columns[2], 0, 100) });
    engine.compactAll();
    engine.appendBatch(Arrays.copyOfRange(timestamps, 100, rows), new Object[][] { Arrays.copyOfRange(columns[0], 100, rows),
        Arrays.copyOfRange(columns[1], 100, rows), Arrays.copyOfRange(columns[2], 100, rows) });

    database.command("sql", "DELETE FROM N2 WHERE host = 'h1' AND ts < " + (BASE + 80_000L)).close();

    assertThat(engine.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).as("integrity problems").isEmpty();
    long eu = 0;
    long us = 0;
    for (int i = 0; i < rows; i++) {
      if (i % 2 == 1 && BASE + i * 1_000L < BASE + 80_000L)
        continue;
      if (i % 5 != 0 && i % 2 == 0)
        eu++;
      else if (i % 5 != 0)
        us++;
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM N2 WHERE region = 'eu'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(eu);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM N2 WHERE region = 'us'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(us);
    }
  }

  /** A block rebuilt from a downsampled one is as coarse as it was, so the next downsampling pass leaves it alone. */
  @Test
  void aRebuiltDownsampledBlockKeepsItsGranularityMarker() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE D TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1").close();
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("D")).getEngine();
    final int rows = 600;
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[2][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = i * 1_000L;
      columns[0][i] = i % 2 == 0 ? "a" : "b";
      columns[1][i] = (double) i;
    }
    engine.appendBatch(timestamps, columns);
    engine.compactAll();

    // two series share every bucket, so only the marker (not the spacing of the samples) tells the block is already coarse
    final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, 60_000L));
    engine.applyDownsampling(tiers, 1_000_000L);
    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final long afterFirstPass = sealed.getDownsampleRewriteCount();
    engine.applyDownsampling(tiers, 1_000_000L);
    assertThat(sealed.getDownsampleRewriteCount()).as("the downsampling is idempotent").isEqualTo(afterFirstPass);

    try (final ResultSet rs = database.command("sql", "DELETE FROM D WHERE host = 'a' AND ts < 120000")) {
      assertThat(rs.next().<Long>getProperty("count")).isPositive();
    }
    final long afterDelete = sealed.getDownsampleRewriteCount();
    engine.applyDownsampling(tiers, 1_000_000L);
    assertThat(sealed.getDownsampleRewriteCount()).as("the rebuilt block is not downsampled again").isEqualTo(afterDelete);
    assertThat(engine.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).as("integrity problems").isEmpty();
  }

  /** The bounds a SELECT takes in a WHERE are the bounds a DELETE takes: a date as text (read in UTC), and a parameter. */
  @Test
  void aDateAsTextAndParametersAreReadAsASelectReadsThem() throws Exception {
    create(2);
    append(0, 200);
    engine().compactAll();
    append(200, 300);

    // The pushdown of a WHERE on the timestamp reads a date as text in UTC, whatever zone the database has. A DELETE removes what a
    // SELECT of the same WHERE returns - the oracle - and that is neither nothing nor everything
    final String cutoff = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(BASE + 100_000L), ZoneOffset.UTC));
    final Set<Long> matching = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT ts FROM M WHERE ts < '" + cutoff + "'")) {
      while (rs.hasNext())
        matching.add(((LocalDateTime) rs.next().getProperty("ts")).toInstant(ZoneOffset.UTC).toEpochMilli());
    }
    assertThat(matching).isNotEmpty().hasSizeLessThan(model.size());
    model.removeIf(s -> matching.contains(s.ts));
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE ts < '" + cutoff + "'")) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo((long) matching.size());
    }
    assertMatchesModel();

    final Predicate<Sample> series = s -> s.host.equals("h1") && s.ts < BASE + 200_000L;
    final long expectedSeries = model.stream().filter(series).count();
    model.removeIf(series);
    try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE host = ? AND ts < ?", "h1", BASE + 200_000L)) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(expectedSeries);
    }
    assertMatchesModel();
  }

  /** A TAG of another type than STRING is matched by the filter in both layers. */
  @Test
  void aTagThatIsNotAStringIsMatchedInBothLayers() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE N TIMESTAMP ts TAGS (dc LONG) FIELDS (v DOUBLE) SHARDS 1").close();
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("N")).getEngine();
    for (int part = 0; part < 2; part++) {
      final long[] timestamps = new long[50];
      final Object[][] columns = new Object[2][50];
      for (int r = 0; r < 50; r++) {
        final int i = part * 50 + r;
        timestamps[r] = BASE + i * 1_000L;
        columns[0][r] = (long) (i % 5);
        columns[1][r] = (double) i;
      }
      engine.appendBatch(timestamps, columns);
      if (part == 0)
        engine.compactAll();
    }

    try (final ResultSet rs = database.command("sql", "DELETE FROM N WHERE dc = 3")) {
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(20L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM N WHERE dc = 3")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c, sum(v) AS s FROM N")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(80L);
      double expected = 0;
      for (int i = 0; i < 100; i++)
        if (i % 5 != 3)
          expected += i;
      assertThat(r.<Number>getProperty("s").doubleValue()).isEqualTo(expected);
    }
  }

  /** Samples appended while deletes run are neither lost nor deleted, and every delete removes what its range holds. */
  @Test
  void appendsRacingDeletesAreNeitherLostNorDeleted() throws Exception {
    create(2);
    final long writerBase = BASE + 10_000_000L;
    final int writes = 300;
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(1);

    final Thread writer = new Thread(() -> {
      try {
        started.await();
        for (int i = 0; i < writes; i++)
          for (int attempt = 0; ; attempt++)
            try {
              engine().appendSamples(new long[] { writerBase + i * 1_000L }, new Object[] { "w" }, new Object[] { "eu" },
                  new Object[] { (double) i }, new Object[] { i }, new Object[] { "s" }, new Object[] { true });
              break;
            } catch (final ConcurrentModificationException e) {
              // a retry of the same sample: the append that lost the race stored nothing. Running out of retries is a failure of the
              // test, not a sample quietly missing from the count below
              if (attempt >= 9)
                throw new AssertionError("sample " + i + " lost ten races against the delete", e);
            }
      } catch (final Throwable t) {
        failure.compareAndSet(null, t);
      }
    }, "writer");
    writer.start();
    started.countDown();

    long deleted = 0;
    for (int round = 0; round < 12; round++) {
      append(round * 10, round * 10 + 10);
      try (final ResultSet rs = database.command("sql", "DELETE FROM M WHERE ts < " + writerBase)) {
        deleted += rs.next().<Long>getProperty("count");
      }
      if (round % 4 == 3)
        engine().compactAll();
    }
    writer.join(60_000L);

    if (failure.get() != null)
      throw new AssertionError("the writer failed", failure.get());
    assertThat(writer.isAlive()).as("the writer finished").isFalse();
    assertThat(deleted).as("every sample the rounds appended was deleted by the round").isEqualTo(120L);
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM M")) {
      assertThat(rs.next().<Long>getProperty("c")).as("the samples the writer appended").isEqualTo((long) writes);
    }
    model.clear();
  }

  // ------------------------------------------------------------------------------------------------------------------ the fixture

  private void create(final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING, region STRING) "
        + "FIELDS (v DOUBLE, n INTEGER, s STRING, b BOOLEAN) SHARDS " + shards).close();
  }

  private void populate(final Layout layout) throws Exception {
    switch (layout) {
    case MUTABLE -> append(0, SAMPLES);
    case SEALED -> {
      append(0, SAMPLES);
      engine().compactAll();
    }
    case BOTH -> {
      append(0, SAMPLES / 2);
      engine().compactAll();
      append(SAMPLES / 2, SAMPLES);
    }
    }
  }

  /** Appends the samples {@code [from, to)}: one per second, four hosts, two regions, a value now and then absent. */
  private void append(final int from, final int to) throws Exception {
    final int rows = to - from;
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[6][rows];
    for (int r = 0; r < rows; r++) {
      final int i = from + r;
      final Sample sample = new Sample(BASE + i * 1_000L, "h" + (i % 4), i % 2 == 0 ? "eu" : "us", i % 17 == 0 ? null : i * 1.5,
          i % 10 == 0 ? null : i, "s" + (i % 3), i % 2 == 0);
      model.add(sample);
      timestamps[r] = sample.ts;
      columns[0][r] = sample.host;
      columns[1][r] = sample.region;
      columns[2][r] = sample.v;
      columns[3][r] = sample.n;
      columns[4][r] = sample.s;
      columns[5][r] = sample.b;
    }
    engine().appendBatch(timestamps, columns);
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();
  }

  /** The type holds exactly the model's samples, value for value, and answers the aggregates the model says. */
  private void assertMatchesModel() throws Exception {
    // the files are sound: block CRCs, the header counters of the mutable bucket, and - decoding every sealed block - that a rebuilt block
    // declares the statistics, the sorted timestamps and the tag values its rows have
    assertThat(engine().checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).as("integrity problems").isEmpty();

    final List<Sample> actual = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT ts, host, region, v, n, s, b FROM M")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        actual.add(new Sample(((LocalDateTime) r.getProperty("ts")).toInstant(ZoneOffset.UTC).toEpochMilli(), r.getProperty("host"),
            r.getProperty("region"), r.<Number>getProperty("v") != null ? r.<Number>getProperty("v").doubleValue() : null,
            r.<Number>getProperty("n") != null ? r.<Number>getProperty("n").intValue() : null, r.getProperty("s"),
            r.<Boolean>getProperty("b")));
      }
    }
    actual.sort(Comparator.comparingLong(Sample::ts));
    final List<Sample> expected = new ArrayList<>(model);
    expected.sort(Comparator.comparingLong(Sample::ts));
    assertThat(actual).as("the samples of the type").isEqualTo(expected);

    // aggregates: answered from the statistics of the sealed blocks, which a rebuilt block has to have recomputed
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS c, count(v) AS cv, sum(v) AS sv, min(v) AS mnv, max(v) AS mxv, sum(n) AS sn, min(n) AS mnn, max(n) AS mxn FROM M")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("c").longValue()).as("count(*)").isEqualTo(model.size());
      assertThat(r.<Number>getProperty("cv").longValue()).as("count(v)").isEqualTo(model.stream().filter(s -> s.v != null).count());
      if (model.stream().anyMatch(s -> s.v != null)) {
        assertThat(r.<Number>getProperty("sv").doubleValue()).as("sum(v)")
            .isCloseTo(model.stream().filter(s -> s.v != null).mapToDouble(s -> s.v).sum(), Offset.offset(1e-6));
        assertThat(r.<Number>getProperty("mnv").doubleValue()).as("min(v)")
            .isEqualTo(model.stream().filter(s -> s.v != null).mapToDouble(s -> s.v).min().getAsDouble());
        assertThat(r.<Number>getProperty("mxv").doubleValue()).as("max(v)")
            .isEqualTo(model.stream().filter(s -> s.v != null).mapToDouble(s -> s.v).max().getAsDouble());
      }
      if (model.stream().anyMatch(s -> s.n != null)) {
        assertThat(r.<Number>getProperty("sn").longValue()).as("sum(n)").isEqualTo(model.stream().filter(s -> s.n != null).mapToLong(s -> s.n).sum());
        assertThat(r.<Number>getProperty("mnn").longValue()).as("min(n)")
            .isEqualTo(model.stream().filter(s -> s.n != null).mapToLong(s -> s.n).min().getAsLong());
        assertThat(r.<Number>getProperty("mxn").longValue()).as("max(n)")
            .isEqualTo(model.stream().filter(s -> s.n != null).mapToLong(s -> s.n).max().getAsLong());
      }
    }

    // a tag filter answers the same through the blocks' declared tag values, which a rebuilt block has to have recomputed
    for (final String host : new String[] { "h0", "h1", "h2", "h3" })
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM M WHERE host = '" + host + "'")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).as("count of " + host)
            .isEqualTo(model.stream().filter(s -> s.host.equals(host)).count());
      }
  }
}
