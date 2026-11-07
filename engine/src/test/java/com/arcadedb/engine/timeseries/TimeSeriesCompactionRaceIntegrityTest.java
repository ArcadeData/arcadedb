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
import com.arcadedb.engine.BasePage;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integrity of the sealed store against compaction: CHECK DATABASE FIX racing a compaction, shard fan-out
 * attribution and the oldest sample after a compaction of out-of-order samples. Each nested group keeps its own
 * fixture and carries the issue number it regression-tests.
 */
class TimeSeriesCompactionRaceIntegrityTest {

  /**
   * Regression test for issue #6406 item 5: the compaction-race arm of {@link TimeSeriesShard#checkMutableIntegrity}
   * - a {@code FIX} whose commit loses a version race against a concurrent compaction on the mutable bucket's page 0
   * - had no coverage. Not because it cannot go wrong, but because forcing a real, deterministic
   * {@link ConcurrentModificationException} on that exact commit needs a live compaction racing a live
   * {@code CHECK DATABASE FIX} at exactly the right instant, which neither {@code TimeSeriesCheckDatabaseTest.SealedStoreIntegrity}
   * nor {@code TimeSeriesCheckDatabaseTest.CheckDatabaseTimeSeries} attempts (both run single-threaded, non-replicated).
   * <p>
   * This test takes the option the issue itself named as one of the ways to close the gap: a package-private (here,
   * test-only public) hook - {@link TimeSeriesShard#TEST_FIX_COMMIT_FAULT} - fired exactly where the real commit
   * would be, on exactly the path that reaches one (a repair with something to write). Same idiom as
   * {@code RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT} for the equivalent leader-side problem: throw the same
   * exception class a losing commit would, and let production code's own catch block do the rest.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CheckFixCompactionRace extends TestHelper {

    private static final int ROWS = 500;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // Deliberately leaves the mutable header disagreeing with its pages between the two CHECK DATABASE runs.
      return false;
    }

    @AfterEach
    void clearHook() {
      TimeSeriesShard.TEST_FIX_COMMIT_FAULT = null;
    }

    private TimeSeriesEngine createType(final String typeName) {
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName
          + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    }

    private void appendRows(final TimeSeriesEngine engine, final int rows) throws IOException {
      final long[] timestamps = new long[rows];
      final Object[][] columns = new Object[2][rows];
      for (int i = 0; i < rows; i++) {
        timestamps[i] = 1_700_000_000_000L + i * 1_000L;
        columns[0][i] = "host_" + (i % 7);
        columns[1][i] = (double) i;
      }
      engine.appendBatch(timestamps, columns);
    }

    private Result runCheck(final String suffix) {
      try (final ResultSet resultSet = database.command("sql", "CHECK DATABASE" + suffix)) {
        assertThat(resultSet.hasNext()).isTrue();
        return resultSet.next();
      }
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> warningsOf(final Result row) {
      return (Collection<String>) row.<Collection<?>>getProperty("warnings");
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> repairsOf(final Result row) {
      return (Collection<String>) row.<Collection<?>>getProperty("timeSeriesRepairs");
    }

    /**
     * A repair that loses the race is reported as NOT LANDED - told to the operator by name, with the totals
     * re-read from what actually survived rather than from the rolled-back transaction - and a second,
     * un-contended run then repairs it cleanly. That second run is the point: it proves the corruption this test
     * plants is real and repairable, so the first run's "no repair" outcome is because of the injected race, not
     * because there was nothing to fix in the first place.
     */
    // #6406
    @Test
    void aFixThatLosesTheCommitRaceIsReportedNotAppliedAndRetryable() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      final String bucketPath = engine.getShard(0).getMutableBucket().getComponentFile().getFilePath();

      database.close();
      try {
        // Page 0's sample counter, at content offset 7 of the header page (same corruption #6360 uses).
        try (final RandomAccessFile raf = new RandomAccessFile(new File(bucketPath), "rw")) {
          raf.seek(BasePage.PAGE_HEADER_SIZE + 7);
          raf.writeLong(ROWS + 500L);
        }
      } finally {
        database = factory.open();
      }

      final Result reported = runCheck("");
      assertThat(warningsOf(reported)).as("the corruption must actually be visible before the race is injected")
          .anyMatch(w -> w.contains("declares " + (ROWS + 500) + " sample(s)"));

      TimeSeriesShard.TEST_FIX_COMMIT_FAULT = shard ->
          // The exact exception class and shape the real commit2ndPhase would throw on a losing page-version
          // check (com.arcadedb.exception.ConcurrentModificationException), so the catch block in
          // checkMutableIntegrity cannot tell this from the real thing.
          { throw new ConcurrentModificationException("TEST fault injection: simulated compaction race on shard " + shard); };

      final Result raced = runCheck(" FIX");
      // Nothing landed - the fault fired instead of a real commit - so this run reports zero repairs...
      assertThat(raced.<Long>getProperty("repairedTimeSeries")).isZero();
      final Collection<String> repairs = repairsOf(raced);
      assertThat(repairs).as("repairs: %s", repairs).noneMatch(r -> r.contains("mutable bucket"));
      // ...and says so BY NAME, telling the operator to run it again rather than reporting success or silence.
      final Collection<String> warnings = warningsOf(raced);
      assertThat(warnings).as("warnings: %s", warnings)
          .anyMatch(w -> w.contains("mutable bucket") && w.contains("compaction rewrote page 0")
              && w.contains("run the check again"));
      // The totals are re-read from what survived the rollback, not from the (never-committed) repaired header.
      assertThat(raced.<Long>getProperty("totalTimeSeriesSamples")).isEqualTo((long) ROWS + 500L);

      TimeSeriesShard.TEST_FIX_COMMIT_FAULT = null;

      final Result fixed = runCheck(" FIX");
      assertThat(fixed.<Long>getProperty("repairedTimeSeries")).isEqualTo(1L);
      assertThat(repairsOf(fixed)).anyMatch(r -> r.contains("mutable bucket") && r.contains("rewrote page 0's counters"));

      final Result after = runCheck("");
      assertThat(warningsOf(after)).isEmpty();
      assertThat(after.<Long>getProperty("totalTimeSeriesSamples")).isEqualTo((long) ROWS);
    }
  }

  /**
   * Regression test for issue #6406 item 1: {@link TimeSeriesEngine#checkIntegrity} now runs the sealed half of
   * every shard's check ({@link TimeSeriesShard#checkSealedIntegrity}) fanned out across {@code shardExecutor}
   * instead of one shard at a time, while the mutable half ({@link TimeSeriesShard#checkMutableIntegrity}) stays
   * sequential.
   * <p>
   * What this has to prove that a single-shard test cannot: with several shards in flight on different pool
   * threads, a finding still comes back attributed to the RIGHT shard, an undamaged shard still reports nothing, and
   * the totals returned by the sequential mutable phase still add up to what the fanned-out sealed phase actually
   * found - i.e. the two arrays the implementation zips back together by index ({@code mutableReports[s]} and
   * {@code sealedFutures[s].join()}) are not silently misaligned by the fan-out.
   * <p>
   * The corruption is planted on shard 2 of 4, not shard 0: an off-by-one or a swapped index would have a decent
   * chance of still passing against shard 0.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ShardFanoutIntegrity extends TestHelper {

    private static final int SHARDS        = 4;
    private static final int ROWS          = 2_000;
    private static final int DAMAGED_SHARD = 2;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // One test leaves a damaged sealed store behind on purpose.
      return false;
    }

    private TimeSeriesEngine createType(final String typeName) {
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName
          + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS " + SHARDS);
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    }

    private void appendRows(final TimeSeriesEngine engine, final int rows) throws IOException {
      final long[] timestamps = new long[rows];
      final Object[][] columns = new Object[2][rows];
      for (int i = 0; i < rows; i++) {
        timestamps[i] = 1_700_000_000_000L + i * 1_000L;
        columns[0][i] = "host_" + (i % 7);
        columns[1][i] = (double) i;
      }
      engine.appendBatch(timestamps, columns);
    }

    private Result runCheck(final String suffix) {
      try (final ResultSet resultSet = database.command("sql", "CHECK DATABASE" + suffix)) {
        assertThat(resultSet.hasNext()).isTrue();
        return resultSet.next();
      }
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> warningsOf(final Result row) {
      return (Collection<String>) row.<Collection<?>>getProperty("warnings");
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> repairsOf(final Result row) {
      return (Collection<String>) row.<Collection<?>>getProperty("timeSeriesRepairs");
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> corruptedTypesOf(final Result row) {
      return (Collection<String>) row.<Collection<?>>getProperty("corruptedTimeSeries");
    }

    /**
     * A finding planted on one shard of several comes back attributed to that shard alone, the other shards report
     * nothing, and the totals the sequential mutable phase already knew still match what the fanned-out sealed
     * phase found on the healthy shards.
     */
    // #6406
    @Test
    void aSealedCorruptionOnOneShardOfSeveralIsAttributedToThatShardAlone() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();

      final long samplesBeforeCorruption = engine.countSamples();
      assertThat(samplesBeforeCorruption).isEqualTo((long) ROWS);

      final File damagedSealedFile = new File(getDatabasePath(), "Cpu_shard_" + DAMAGED_SHARD + ".ts.sealed");
      assertThat(damagedSealedFile).as("shard %d must have compacted at least one block to have a sealed file", DAMAGED_SHARD)
          .exists();

      database.close();
      try (final RandomAccessFile raf = new RandomAccessFile(damagedSealedFile, "rw")) {
        // magic(4) version(1) colCount(2), then the block count - same offset the sealed-header test of #6360 uses.
        raf.seek(7);
        raf.writeInt(4_242);
      } finally {
        database = factory.open();
      }

      final Result reported = runCheck("");

      assertThat(corruptedTypesOf(reported)).containsExactly("Cpu");

      final Collection<String> warnings = warningsOf(reported);
      // Attributed to the damaged shard, and to no other: a swapped or off-by-one index in the fan-out's merge
      // would either miss this line or put it under the wrong shard number.
      assertThat(warnings).as("warnings: %s", warnings)
          .anyMatch(w -> w.contains("shard " + DAMAGED_SHARD + " sealed store") && w.contains("declares 4242 block(s)"));
      for (int s = 0; s < SHARDS; s++) {
        if (s == DAMAGED_SHARD)
          continue;
        final int shard = s;
        assertThat(warnings).as("shard %d must report nothing: warnings=%s", shard, warnings)
            .noneMatch(w -> w.contains("shard " + shard + " "));
      }

      // The mutable half (sequential, computed BEFORE the sealed fan-out) already knew the true sample count; the
      // sealed corruption is a header/directory mismatch, not a lost block, so the total is unaffected by it.
      assertThat(reported.<Long>getProperty("totalTimeSeriesSamples")).isEqualTo((long) ROWS);

      final Result fixed = runCheck(" FIX");
      assertThat(fixed.<Long>getProperty("repairedTimeSeries")).isEqualTo(1L);
      final Collection<String> repairs = repairsOf(fixed);
      assertThat(repairs).as("repairs: %s", repairs)
          .anyMatch(r -> r.contains("shard " + DAMAGED_SHARD + " sealed store") && r.contains("rewrote the header from the block directory"));
      for (int s = 0; s < SHARDS; s++) {
        if (s == DAMAGED_SHARD)
          continue;
        final int shard = s;
        assertThat(repairs).as("shard %d must have nothing to repair: repairs=%s", shard, repairs)
            .noneMatch(r -> r.contains("shard " + shard + " "));
      }

      database.close();
      database = factory.open();
      assertThat(warningsOf(runCheck(""))).isEmpty();
      assertThat(runCheck("").<Long>getProperty("totalTimeSeriesSamples")).isEqualTo((long) ROWS);
    }

    /**
     * A healthy multi-shard type reports the same totals under the new split/fanned-out check as it did as one
     * per-shard call before #6406: the {@code DEEP} tier decodes every sealed block of every shard, on whichever
     * pool thread it lands on, and the merge still adds up.
     */
    // #6406
    @Test
    void aHealthyMultiShardTypeReportsCleanUnderDeep() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      appendRows(engine, 250);

      final Result row = runCheck(" DEEP");

      assertThat(warningsOf(row)).isEmpty();
      assertThat(corruptedTypesOf(row)).isEmpty();
      assertThat(row.<Long>getProperty("totalTimeSeriesSamples")).isEqualTo(ROWS + 250L);
      assertThat(row.<Long>getProperty("totalTimeSeriesSealedBlocks")).isGreaterThan(0L);
      assertThat(row.<Long>getProperty("repairedTimeSeries")).isZero();
    }
  }

  /**
   * #8813: after COMPACT TIMESERIES TYPE, a series whose samples arrived out of order answered
   * {@code ORDER BY ts ASC LIMIT 1} with a sample that is not the oldest.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CompactedOldestSample extends TestHelper {
    private static final int  N      = 200_000;
    private static final int  COMMIT = 1_000;
    private static final int  COMPACT_EVERY = 68_000;
    private static final long T0     = 1_700_000_000_000L;

    private long ts(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        final Object o = rs.next().getProperty("ts");
        return switch (o) {
          case Date d -> d.getTime();
          case Instant i -> i.toEpochMilli();
          case LocalDateTime l -> l.toInstant(ZoneOffset.UTC).toEpochMilli();
          default -> ((Number) o).longValue();
        };
      }
    }

    // #8813
    @Tag("slow")
    @Test
    void oldestSampleAfterCompactOfOutOfOrderSamples() {
      database.command("sql", "CREATE TIMESERIES TYPE R TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");
      final Random rnd = new Random(43);
      long min = Long.MAX_VALUE;
      final long[] all = new long[N];
      long window = 0;
      database.begin();
      for (int i = 0; i < N; i++) {
        if (i % COMMIT == 0)
          window = (long) (rnd.nextDouble() * 47 * 3600_000);
        final long t = T0 + window + (long) (rnd.nextDouble() * 3600_000);
        min = Math.min(min, t);
        all[i] = t;
        database.command("sql", "INSERT INTO R SET ts = ?, host = 'h0', v = ?", new Date(t), rnd.nextDouble());
        if ((i + 1) % COMMIT == 0) {
          database.commit();
          // the reporter's server compacted in the background while the load was running: the sealed store already holds
          // earlier, overlapping blocks when the final COMPACT runs
          if ((i + 1) % COMPACT_EVERY == 0)
            database.command("sql", "COMPACT TIMESERIES TYPE R").close();
          database.begin();
        }
      }
      database.commit();

      assertThat(ts("SELECT ts FROM R ORDER BY ts ASC LIMIT 1")).as("before COMPACT").isEqualTo(min);
      database.command("sql", "COMPACT TIMESERIES TYPE R").close();
      assertThat(ts("SELECT ts FROM R ORDER BY ts ASC LIMIT 1")).as("ORDER BY ts ASC LIMIT 1").isEqualTo(min);
      assertThat(ts("SELECT ts FROM R LIMIT 1")).as("bare LIMIT 1").isEqualTo(min);
      assertThat(ts("SELECT ts FROM R ORDER BY ts ASC LIMIT 2")).as("first of LIMIT 2").isEqualTo(min);
      assertThat(ts("SELECT min(ts) AS ts FROM R")).as("min(ts)").isEqualTo(min);
      assertRangesAndOldest(all, min, "compacted");

      // the directory is rebuilt from the file on open, where the blocks sit in the order they were written
      reopenDatabase();
      assertRangesAndOldest(all, min, "reopened");
    }

    private void assertRangesAndOldest(final long[] all, final long min, final String when) {
      assertThat(ts("SELECT ts FROM R ORDER BY ts ASC LIMIT 1")).as(when + " ORDER BY ts ASC LIMIT 1").isEqualTo(min);
      final long span = 47L * 3600_000;
      for (int w = 0; w < 12; w++) {
        final long from = T0 + w * span / 12;
        final long to = from + span / 24;
        long expected = 0;
        for (final long t : all)
          if (t >= from && t <= to)
            ++expected;
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM R WHERE ts >= ? AND ts <= ?", new Date(from), new Date(to))) {
          assertThat(rs.next().<Number>getProperty("c").longValue()).as(when + " count in window " + w).isEqualTo(expected);
        }
      }
    }
  }
}
