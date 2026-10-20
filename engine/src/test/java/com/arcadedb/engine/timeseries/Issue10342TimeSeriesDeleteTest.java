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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
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

  /** A delete is not part of the transaction: like the append, it is durable when it returns. */
  @Test
  void aRollbackDoesNotBringTheSamplesBack() throws Exception {
    create(1);
    append(0, 100);

    database.begin();
    database.command("sql", "DELETE FROM M WHERE host = 'h0'").close();
    database.rollback();

    model.removeIf(s -> s.host.equals("h0"));
    assertMatchesModel();
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

  /** The bounds a SELECT takes in a WHERE are the bounds a DELETE takes: a date as text in the database's zone, and a parameter. */
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
          for (int attempt = 0; attempt < 10; attempt++)
            try {
              engine().appendSamples(new long[] { writerBase + i * 1_000L }, new Object[] { "w" }, new Object[] { "eu" },
                  new Object[] { (double) i }, new Object[] { i }, new Object[] { "s" }, new Object[] { true });
              break;
            } catch (final ConcurrentModificationException e) {
              // a retry of the same sample: the append that lost the race stored nothing
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
