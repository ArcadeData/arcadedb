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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6406ShardFanoutIntegrityTest extends TestHelper {

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
