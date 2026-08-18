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
 * Issue #6360 through the statement that exposes it: {@code CHECK DATABASE ... FIX} and {@code ... DEEP} over a
 * TimeSeries type.
 * <p>
 * #6340 gave TimeSeries its first coverage in the checker and deliberately left both of these open. What they
 * settle:
 * <ul>
 * <li><b>{@code FIX}</b> repairs DERIVED bookkeeping - the mutable bucket's page-0 counters and the sealed store's
 * header - and never a sample. Those counters are recomputable from the pages and blocks they describe, and a
 * wrong sealed global bound is not cosmetic: {@code loadDirectory} reads it out of the header instead of
 * recomputing it, so a query pruned against it silently misses data the file holds.</li>
 * <li><b>{@code DEEP}</b> is the tier that DECODES the data. The default tier already reads every byte to verify
 * the per-block CRCs, which proves the bytes are the ones that were written and nothing about whether they mean
 * what the block claims.</li>
 * </ul>
 * The teardown integrity check is off because several tests deliberately leave a damaged file behind; the ones
 * that assert health do it explicitly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6360CheckDatabaseTimeSeriesTest extends TestHelper {

  private static final int ROWS = 3_000;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  private TimeSeriesEngine createType(final String typeName) {
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    return engineOf(typeName);
  }

  private TimeSeriesEngine engineOf(final String typeName) {
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
   * The residue of #6314 in the shape it leaves behind - page 0 counting samples the data pages do not hold - is
   * now repairable, because every one of those three counters is derived from the pages and maintained
   * incrementally. {@code clearDataPagesUpTo} already recomputes exactly these three the same way on every
   * retention pass, so the repair is the routine operation and not a new one.
   */
  @Test
  void fixRewritesAMutableHeaderThatDisagreesWithItsPages() throws Exception {
    final TimeSeriesEngine engine = createType("Cpu");
    appendRows(engine, ROWS);
    final String bucketPath = engine.getShard(0).getMutableBucket().getComponentFile().getFilePath();

    database.close();
    try {
      // Page 0's sample counter, at content offset 7 of the header page.
      try (final RandomAccessFile raf = new RandomAccessFile(new File(bucketPath), "rw")) {
        raf.seek(BasePage.PAGE_HEADER_SIZE + 7);
        raf.writeLong(ROWS + 500L);
      }
    } finally {
      database = factory.open();
    }

    // Reported without FIX, and nothing is touched.
    final Result reported = runCheck("");
    assertThat(corruptedTypesOf(reported)).containsExactly("Cpu");
    assertThat(reported.<Long>getProperty("repairedTimeSeries")).isZero();
    assertThat(repairsOf(reported)).isEmpty();

    final Result fixed = runCheck(" FIX");
    assertThat(fixed.<Long>getProperty("repairedTimeSeries")).isEqualTo(1L);
    assertThat(repairsOf(fixed)).anyMatch(r -> r.contains("timeseries 'Cpu'") && r.contains("mutable bucket")
        && r.contains("rewrote page 0's counters") && r.contains(ROWS + " sample(s)"));

    // The finding stays in the report of the run that repaired it, and is gone from the next one.
    assertThat(warningsOf(fixed)).anyMatch(w -> w.contains("declares " + (ROWS + 500) + " sample(s)"));

    final Result after = runCheck("");
    assertThat(corruptedTypesOf(after)).isEmpty();
    assertThat(warningsOf(after)).isEmpty();
    assertThat(after.<Long>getProperty("totalTimeSeriesSamples")).isEqualTo((long) ROWS);
    assertThat(engineOf("Cpu").getShard(0).getMutableBucket().getSampleCount()).isEqualTo((long) ROWS);
  }

  /**
   * The repair survives a restart, which is the whole point of writing it through a transaction rather than into
   * the in-memory counter.
   */
  @Test
  void aRepairedMutableHeaderIsStillRepairedAfterReopening() throws Exception {
    final TimeSeriesEngine engine = createType("Cpu");
    appendRows(engine, 500);
    final String bucketPath = engine.getShard(0).getMutableBucket().getComponentFile().getFilePath();

    database.close();
    try (final RandomAccessFile raf = new RandomAccessFile(new File(bucketPath), "rw")) {
      raf.seek(BasePage.PAGE_HEADER_SIZE + 7);
      raf.writeLong(4_000L);
    } finally {
      database = factory.open();
    }

    assertThat(runCheck(" FIX").<Long>getProperty("repairedTimeSeries")).isEqualTo(1L);

    database.close();
    database = factory.open();

    final Result after = runCheck("");
    assertThat(warningsOf(after)).isEmpty();
    assertThat(after.<Long>getProperty("totalTimeSeriesSamples")).isEqualTo(500L);
  }

  /**
   * The sealed half of the same policy: the header's block count and global timestamp bounds are derived from the
   * block directory, so {@code FIX} recomputes them - and the reopened store prunes range queries against numbers
   * that describe the blocks it actually has.
   */
  @Test
  void fixRewritesASealedHeaderThatDisagreesWithItsBlocks() throws Exception {
    final TimeSeriesEngine engine = createType("Cpu");
    appendRows(engine, ROWS);
    engine.compactAll();

    final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");
    assertThat(sealed).exists();

    database.close();
    try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
      // magic(4) version(1) colCount(2), then block count and the two global bounds.
      raf.seek(7);
      raf.writeInt(4_242);
    } finally {
      database = factory.open();
    }

    final Result reported = runCheck("");
    assertThat(corruptedTypesOf(reported)).containsExactly("Cpu");
    assertThat(warningsOf(reported)).anyMatch(w -> w.contains("sealed store") && w.contains("declares 4242 block(s)"));

    final Result fixed = runCheck(" FIX");
    assertThat(fixed.<Long>getProperty("repairedTimeSeries")).isEqualTo(1L);
    assertThat(repairsOf(fixed)).anyMatch(r -> r.contains("sealed store")
        && r.contains("rewrote the header from the block directory"));

    database.close();
    database = factory.open();
    assertThat(warningsOf(runCheck(""))).isEmpty();
  }

  /**
   * {@code DEEP} on a healthy database says the same thing the default tier does, with the same totals - the tier
   * adds checks, not a different verdict.
   */
  @Test
  void theDeepTierWalksAHealthyTimeSeriesTypeAndReportsItClean() throws Exception {
    final TimeSeriesEngine engine = createType("Cpu");
    appendRows(engine, ROWS);
    engine.compactAll();
    appendRows(engine, 100);

    final Result row = runCheck(" DEEP");

    assertThat(warningsOf(row)).isEmpty();
    assertThat(corruptedTypesOf(row)).isEmpty();
    assertThat(row.<Long>getProperty("totalTimeSeriesSamples")).isEqualTo(ROWS + 100L);
    assertThat(row.<Long>getProperty("totalTimeSeriesSealedBlocks")).isGreaterThan(0L);
    assertThat(row.<Long>getProperty("repairedTimeSeries")).isZero();
  }

  /**
   * {@code DEEP} composes with the other clauses rather than replacing any of them, and the statement's own
   * round-trip carries it - the check a caller reading {@code EXPLAIN} or a replicated statement depends on.
   */
  @Test
  void theDeepClauseComposesWithTypeAndFix() throws Exception {
    appendRows(createType("Cpu"), 200);
    appendRows(createType("Memory"), 200);

    final Result row = runCheck(" TYPE Cpu FIX DEEP");
    assertThat(warningsOf(row)).isEmpty();
    assertThat(row.<Long>getProperty("totalTimeSeriesTypes")).isEqualTo(1L);
    assertThat(row.<Long>getProperty("repairedTimeSeries")).isZero();
  }

  /**
   * A database with no TimeSeries type answers {@code DEEP} the way it answers the default tier: with zeros rather
   * than with silence.
   */
  @Test
  void theDeepTierOnADatabaseWithoutTimeSeriesReportsZeros() {
    database.getSchema().createDocumentType("Note");
    database.transaction(() -> database.newDocument("Note").set("i", 1).save());

    final Result row = runCheck(" DEEP");

    assertThat(warningsOf(row)).isEmpty();
    assertThat(row.<Long>getProperty("totalTimeSeriesTypes")).isZero();
    assertThat(row.<Long>getProperty("repairedTimeSeries")).isZero();
    assertThat(repairsOf(row)).isEmpty();
  }
}
