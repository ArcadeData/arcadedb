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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6406 item 5: the compaction-race arm of {@link TimeSeriesShard#checkMutableIntegrity}
 * - a {@code FIX} whose commit loses a version race against a concurrent compaction on the mutable bucket's page 0
 * - had no coverage. Not because it cannot go wrong, but because forcing a real, deterministic
 * {@link ConcurrentModificationException} on that exact commit needs a live compaction racing a live
 * {@code CHECK DATABASE FIX} at exactly the right instant, which neither {@code Issue6360SealedStoreIntegrityTest}
 * nor {@code Issue6360CheckDatabaseTimeSeriesTest} attempts (both run single-threaded, non-replicated).
 * <p>
 * This test takes the option the issue itself named as one of the ways to close the gap: a package-private (here,
 * test-only public) hook - {@link TimeSeriesShard#TEST_FIX_COMMIT_FAULT} - fired exactly where the real commit
 * would be, on exactly the path that reaches one (a repair with something to write). Same idiom as
 * {@code RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT} for the equivalent leader-side problem: throw the same
 * exception class a losing commit would, and let production code's own catch block do the rest.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6406CompactionRaceIntegrityTest extends TestHelper {

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
