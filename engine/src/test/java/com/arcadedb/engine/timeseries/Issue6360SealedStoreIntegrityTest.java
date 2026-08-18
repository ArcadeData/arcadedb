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

import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6360, all three items, at the level they live at: the sealed store itself.
 * <p>
 * <b>Item 3</b> is {@link #aBlockAppendedInThisSessionValidatesItsOwnCRC()}: {@code BlockEntry.blockStartOffset}
 * and {@code storedCRC} used to be assigned by {@code loadDirectory()} alone, so a block this process wrote
 * carried zero in both while its constructor pre-set {@code crcValidated = true}. The bug was unreachable only
 * because those two facts lined up - clear the flag and the reader computes a CRC over offset 0 and compares it
 * against zero. Since compaction installs a rewritten directory without re-reading it, that was the state of the
 * ENTIRE live directory after every compaction, retention pass and downsampling cycle.
 * <p>
 * <b>Items 1 and 2</b> are the tier question: what the check should verify and what {@code FIX} may do about it.
 * The answer this pins is that the CRC pass stays in the DEFAULT tier - it is the only thing that proves the bytes
 * are the bytes that were written - while decoding, which proves the bytes MEAN what the block claims, is the
 * {@code DEEP} tier; and that {@code FIX} rewrites derived bookkeeping and never touches a block.
 * <p>
 * Written against the store directly rather than through a database because every case here needs a block whose
 * metadata disagrees with its own data while still passing its CRC, which is exactly what {@code appendBlock}
 * produces when handed statistics that do not describe the columns it is given.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6360SealedStoreIntegrityTest {

  private static final String ROOT      = "target/databases/Issue6360SealedStoreIntegrityTest";
  private static final String BASE_PATH = ROOT + "/sealed";

  /** The "TSBL" a sealed block starts with, spelled out here so the test does not need the store's constant. */
  private static final byte[] BLOCK_MAGIC = { 0x54, 0x53, 0x42, 0x4C };

  private List<ColumnDefinition> columns;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(ROOT));
    new File(ROOT).mkdirs();

    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("usage", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(ROOT));
  }

  private TimeSeriesSealedStore open() throws IOException {
    return new TimeSeriesSealedStore(BASE_PATH, columns);
  }

  /**
   * Appends one block whose declared metadata is whatever the caller says it is. Every case below is one of these:
   * the block's CRC is computed over the metadata as given, so a lie told here is a lie that passes the CRC.
   */
  private void appendBlock(final TimeSeriesSealedStore store, final long[] timestamps, final String[] hosts,
      final double[] usage, final long declaredMinTs, final long declaredMaxTs, final double[] mins,
      final double[] maxs, final double[] sums, final String[] declaredHosts) throws IOException {
    final byte[][] compressed = {
        DeltaOfDeltaCodec.encode(timestamps),
        DictionaryCodec.encode(hosts),
        GorillaXORCodec.encode(usage) };

    store.appendBlock(timestamps.length, declaredMinTs, declaredMaxTs, compressed, mins, maxs, sums,
        new String[][] { null, declaredHosts, null });
  }

  /**
   * A block that describes itself truthfully, which is what every other case here departs from.
   */
  private void appendHealthyBlock(final TimeSeriesSealedStore store, final int samples) throws IOException {
    final long[] timestamps = new long[samples];
    final String[] hosts = new String[samples];
    final double[] usage = new double[samples];
    for (int i = 0; i < samples; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      hosts[i] = "host_" + (i % 3);
      usage[i] = i * 0.5;
    }

    double sum = 0;
    for (final double v : usage)
      sum += v;

    appendBlock(store, timestamps, hosts, usage, timestamps[0], timestamps[samples - 1],
        new double[] { Double.NaN, Double.NaN, usage[0] },
        new double[] { Double.NaN, Double.NaN, usage[samples - 1] },
        new double[] { Double.NaN, Double.NaN, sum },
        new String[] { "host_0", "host_1", "host_2" });
  }

  private static List<String> problems(final TimeSeriesSealedStore store, final boolean deep) throws IOException {
    return store.checkIntegrity(deep ? TimeSeriesIntegrity.Options.deepOnly() : TimeSeriesIntegrity.Options.REPORT_ONLY).problems();
  }

  /**
   * ITEM 3. The block is appended by THIS process, so its offset and CRC exist only in the file unless the write
   * path records them; then the flag that hides that is cleared and the block is read back.
   * <p>
   * Before the fix this threw {@code CRC mismatch in sealed store block at offset 0 (stored=0x0, ...)} - the
   * reader went to offset zero, hashed the header and the start of the first block, and compared the result
   * against a stored CRC of zero. Nothing in production reached it because {@code crcValidated} is pre-set on a
   * block just written, which is an optimisation being load-bearing by accident.
   */
  @Test
  void aBlockAppendedInThisSessionValidatesItsOwnCRC() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      appendHealthyBlock(store, 64);
      store.flushHeader();

      store.clearCRCValidationCache();

      final List<Object[]> rows = store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).hasSize(64);
    }
  }

  /**
   * ITEM 3, at the level the class invariant lives at. A block entry is handed its offset by the constructor, so a
   * future write path cannot build one without deciding where its block is, and the "already verified" flag is
   * granted only by {@code recordWrittenCRC} - together with the CRC it is a shortcut for. Setting the flag without
   * the CRC is precisely the defect this issue fixed, and it is now unreachable by construction rather than by
   * every write path remembering.
   */
  @Test
  void aBlockEntryIsTrustedOnlyOnceItsWrittenCRCIsRecorded() {
    final double[] stats = { Double.NaN, Double.NaN, 1.0 };
    final TimeSeriesSealedStore.BlockEntry entry =
        new TimeSeriesSealedStore.BlockEntry(1_000L, 2_000L, 4, 3, stats, stats, stats, 27L);

    assertThat(entry.blockStartOffset).isEqualTo(27L);
    assertThat(entry.crcValidated).as("not trusted until the CRC it would be trusted against is known").isFalse();

    entry.recordWrittenCRC(0x0BADF00D);

    assertThat(entry.storedCRC).isEqualTo(0x0BADF00D);
    assertThat(entry.crcValidated).isTrue();
  }

  /**
   * ITEM 3, the other half: the entry now says where its block is and which CRC guards it, so the check can hold
   * the directory in memory against the file - a comparison that was meaningless while one side was always zero.
   */
  @Test
  void theDirectoryInMemoryAgreesWithTheFileAboutEveryBlockItWrote() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      appendHealthyBlock(store, 40);
      appendHealthyBlock(store, 40);
      store.flushHeader();

      assertThat(problems(store, false)).isEmpty();
      assertThat(problems(store, true)).isEmpty();
    }
  }

  /**
   * ITEM 1. The declared per-column minimum is what the aggregation push-down answers {@code MIN()} from without
   * decompressing anything, so a block that lies about it returns a wrong number that no later read contradicts.
   * The CRC is computed over the metadata as written, so it matches: only decoding finds this.
   */
  @Test
  void aBlockWhoseDeclaredStatsDoNotDescribeItsValuesIsFoundOnlyByTheDeepTier() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      final long[] timestamps = { 1_000L, 2_000L, 3_000L, 4_000L };
      final String[] hosts = { "a", "a", "b", "b" };
      final double[] usage = { 1.0, 2.0, 3.0, 4.0 };

      appendBlock(store, timestamps, hosts, usage, 1_000L, 4_000L,
          new double[] { Double.NaN, Double.NaN, -99.0 },   // min: nowhere in the data
          new double[] { Double.NaN, Double.NaN, 4.0 },
          new double[] { Double.NaN, Double.NaN, 10.0 },
          new String[] { "a", "b" });
      store.flushHeader();

      assertThat(problems(store, false)).as("default tier").isEmpty();
      assertThat(problems(store, true)).as("deep tier")
          .anyMatch(p -> p.contains("declares min -99.0 for column 'usage'") && p.contains("values start at 1.0"));
    }
  }

  /**
   * The sum gets the same treatment as the extremes, with the tolerance sized for the rounding of a double
   * accumulation rather than for anything larger: a sum that is out by a whole sample is a wrong {@code SUM()} and
   * a wrong {@code AVG()}.
   */
  @Test
  void aBlockWhoseDeclaredSumIsWrongIsFoundByTheDeepTier() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      final long[] timestamps = { 1_000L, 2_000L, 3_000L };
      final String[] hosts = { "a", "a", "a" };
      final double[] usage = { 1.5, 2.5, 3.5 };

      appendBlock(store, timestamps, hosts, usage, 1_000L, 3_000L,
          new double[] { Double.NaN, Double.NaN, 1.5 },
          new double[] { Double.NaN, Double.NaN, 3.5 },
          new double[] { Double.NaN, Double.NaN, 99.0 },
          new String[] { "a" });
      store.flushHeader();

      assertThat(problems(store, false)).isEmpty();
      assertThat(problems(store, true))
          .anyMatch(p -> p.contains("declares sum 99.0 for column 'usage'") && p.contains("add up to 7.5"));
    }
  }

  /**
   * The range iterator binary-searches a block's timestamps, so timestamps that are not sorted make a query over
   * this block silently return a subset of the rows that match it. Nothing else in the engine ever says so.
   */
  @Test
  void aBlockWhoseTimestampsAreNotSortedIsFoundByTheDeepTier() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      final long[] timestamps = { 1_000L, 5_000L, 2_000L, 6_000L };
      final String[] hosts = { "a", "a", "a", "a" };
      final double[] usage = { 1.0, 2.0, 3.0, 4.0 };

      appendBlock(store, timestamps, hosts, usage, 1_000L, 6_000L,
          new double[] { Double.NaN, Double.NaN, 1.0 },
          new double[] { Double.NaN, Double.NaN, 4.0 },
          new double[] { Double.NaN, Double.NaN, 10.0 },
          new String[] { "a" });
      store.flushHeader();

      assertThat(problems(store, false)).isEmpty();
      assertThat(problems(store, true))
          .anyMatch(p -> p.contains("timestamps out of order at position 2"));
    }
  }

  /**
   * Block-level tag pruning SKIPS a block whose declared distinct values do not contain the value being filtered
   * on, so a value present in the data but absent from the declaration hides those rows from every query that
   * filters on it - the quietest of the three failures this tier looks for.
   */
  @Test
  void aBlockWhoseDeclaredTagValuesMissOneItHoldsIsFoundByTheDeepTier() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      final long[] timestamps = { 1_000L, 2_000L, 3_000L };
      final String[] hosts = { "a", "b", "c" };
      final double[] usage = { 1.0, 2.0, 3.0 };

      appendBlock(store, timestamps, hosts, usage, 1_000L, 3_000L,
          new double[] { Double.NaN, Double.NaN, 1.0 },
          new double[] { Double.NaN, Double.NaN, 3.0 },
          new double[] { Double.NaN, Double.NaN, 6.0 },
          new String[] { "a", "b" });   // "c" is in the data and not in the declaration
      store.flushHeader();

      assertThat(problems(store, false)).isEmpty();
      assertThat(problems(store, true))
          .anyMatch(p -> p.contains("tag value(s) [c]") && p.contains("column 'host'"));
    }
  }

  /**
   * A block whose declared timestamp bounds are not the bounds of its own timestamps. The directory-level checks
   * cannot see it - they only compare the block's declaration against the header's - and the range scan prunes on
   * exactly those two numbers.
   */
  @Test
  void aBlockWhoseDeclaredTimestampBoundsAreWrongIsFoundByTheDeepTier() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      final long[] timestamps = { 1_000L, 2_000L, 3_000L };
      final String[] hosts = { "a", "a", "a" };
      final double[] usage = { 1.0, 2.0, 3.0 };

      appendBlock(store, timestamps, hosts, usage, 1_000L, 9_000L,
          new double[] { Double.NaN, Double.NaN, 1.0 },
          new double[] { Double.NaN, Double.NaN, 3.0 },
          new double[] { Double.NaN, Double.NaN, 6.0 },
          new String[] { "a" });
      store.flushHeader();

      assertThat(problems(store, true))
          .anyMatch(p -> p.contains("declares max timestamp 9000") && p.contains("timestamps end at 3000"));
    }
  }

  /**
   * ITEM 2, stated as a test so it is not re-litigated by accident: the per-block CRC pass is in the DEFAULT tier.
   * A tier that skipped it would answer "clean" having read only the directory, which is the misleading-clean
   * result the whole check exists to end.
   */
  @Test
  void theDefaultTierStillVerifiesEveryBlockCRC() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      appendHealthyBlock(store, 32);
      store.flushHeader();
    }

    // One byte inside the first block's compressed data, which leaves the directory readable and the CRC wrong.
    try (final RandomAccessFile raf = new RandomAccessFile(new File(BASE_PATH + ".ts.sealed"), "rw")) {
      final long offset = raf.length() - 8;
      raf.seek(offset);
      final int b = raf.read();
      raf.seek(offset);
      raf.write(b ^ 0x01);
    }

    try (final TimeSeriesSealedStore store = open()) {
      assertThat(problems(store, false)).anyMatch(p -> p.contains("CRC mismatch"));
    }
  }

  /**
   * ITEM 1, the {@code FIX} half. The header's block count and global bounds are DERIVED from the block directory
   * - and a wrong global bound is not cosmetic, because {@code loadDirectory} reads those two out of the header
   * rather than recomputing them, so a range query pruned against them silently misses data the file holds.
   */
  @Test
  void fixRewritesASealedHeaderThatDisagreesWithItsBlocks() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      appendHealthyBlock(store, 50);
      appendHealthyBlock(store, 50);
      store.flushHeader();
    }

    // Header layout: magic(4) version(1) colCount(2) blockCount(4) globalMin(8) globalMax(8)
    try (final RandomAccessFile raf = new RandomAccessFile(new File(BASE_PATH + ".ts.sealed"), "rw")) {
      raf.seek(7);
      raf.writeInt(99);
      raf.writeLong(-1L);
    }

    try (final TimeSeriesSealedStore store = open()) {
      final TimeSeriesIntegrity.Outcome reported = store.checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY);
      assertThat(reported.problems()).anyMatch(p -> p.contains("declares 99 block(s)"));
      assertThat(reported.problems()).anyMatch(p -> p.contains("global min timestamp -1"));
      assertThat(reported.repairs()).as("a report-only run changes nothing").isEmpty();

      final TimeSeriesIntegrity.Outcome fixed = store.checkIntegrity(TimeSeriesIntegrity.Options.fixOnly());
      assertThat(fixed.repairs()).anyMatch(r -> r.contains("rewrote the header from the block directory: 2 block(s)"));
    }

    // Reopened: the header is now what the blocks say, and the bounds a query prunes against come back with it.
    try (final TimeSeriesSealedStore store = open()) {
      assertThat(problems(store, false)).isEmpty();
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(1_700_000_000_000L);
    }
  }

  /**
   * A tail that STARTS with a block magic is a block {@code loadDirectory} recognised and could not read to the
   * end of, so it is incomplete by its own evidence. Dropping it is not housekeeping: {@code appendBlock} writes
   * at the end of the file, so a tail nothing can read makes every block appended after it unreadable too.
   */
  @Test
  void fixDropsTheTailOfAnIncompleteAppend() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      appendHealthyBlock(store, 20);
      store.flushHeader();
    }

    final File sealed = new File(BASE_PATH + ".ts.sealed");
    final long healthyLength = sealed.length();
    try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
      raf.seek(healthyLength);
      raf.write(BLOCK_MAGIC);              // the block header got this far and no further
      raf.write(new byte[33]);
    }

    try (final TimeSeriesSealedStore store = open()) {
      assertThat(problems(store, false)).anyMatch(p -> p.contains("37 byte(s) follow the last readable block")
          && p.contains("a block whose write did not complete"));

      final TimeSeriesIntegrity.Outcome fixed = store.checkIntegrity(TimeSeriesIntegrity.Options.fixOnly());
      assertThat(fixed.repairs()).anyMatch(r -> r.contains("dropped 37 byte(s)"));
    }

    assertThat(sealed.length()).isEqualTo(healthyLength);

    try (final TimeSeriesSealedStore store = open()) {
      assertThat(problems(store, false)).isEmpty();
    }
  }

  /**
   * The other half of that rule, and the reason it is a rule. A tail that does NOT start with a block magic can be
   * an append that died before its magic reached the disk - or a COMPLETE block whose magic took a bit flip, which
   * a hex editor could still recover. {@code FIX} cannot tell them apart, so it reports and leaves the bytes
   * exactly where they are.
   */
  @Test
  void fixLeavesATailThatCouldStillBeABlockWithADamagedMagic() throws Exception {
    final File sealed = new File(BASE_PATH + ".ts.sealed");

    try (final TimeSeriesSealedStore store = open()) {
      appendHealthyBlock(store, 20);
    }
    final long firstBlockEnd = sealed.length();

    try (final TimeSeriesSealedStore store = open()) {
      appendHealthyBlock(store, 20);       // a second, complete block...
    }
    final long lengthBefore = sealed.length();

    // ...whose magic is then flipped, which is exactly what the directory scan cannot tell from a partial write.
    try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
      raf.seek(firstBlockEnd);
      raf.writeInt(0xDEADBEEF);
    }

    try (final TimeSeriesSealedStore store = open()) {
      assertThat(store.getBlockCount()).as("the damaged block is already out of reach").isEqualTo(1);

      final TimeSeriesIntegrity.Outcome fixed = store.checkIntegrity(TimeSeriesIntegrity.Options.fixOnly());
      assertThat(fixed.problems()).anyMatch(p -> p.contains("follow the last readable block")
          && p.contains("a complete block whose magic was overwritten"));
      assertThat(fixed.repairs()).noneMatch(r -> r.contains("dropped"));
    }

    assertThat(sealed.length()).as("the bytes a hex editor could still recover are still there")
        .isEqualTo(lengthBefore);
  }

  /**
   * The line {@code FIX} must not cross: a block whose bytes are damaged is the only copy of the samples in it, so
   * the run reports it and leaves the file exactly as long as it found it.
   */
  @Test
  void fixNeverDiscardsABlockThatFailsItsCRC() throws Exception {
    try (final TimeSeriesSealedStore store = open()) {
      appendHealthyBlock(store, 30);
      appendHealthyBlock(store, 30);
      store.flushHeader();
    }

    final File sealed = new File(BASE_PATH + ".ts.sealed");
    final long lengthBefore = sealed.length();
    try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
      final long offset = lengthBefore - 8;
      raf.seek(offset);
      final int b = raf.read();
      raf.seek(offset);
      raf.write(b ^ 0x01);
    }

    try (final TimeSeriesSealedStore store = open()) {
      final TimeSeriesIntegrity.Outcome fixed = store.checkIntegrity(TimeSeriesIntegrity.Options.deepAndFix());
      assertThat(fixed.problems()).anyMatch(p -> p.contains("CRC mismatch"));
      assertThat(fixed.repairs()).isEmpty();
    }

    assertThat(sealed.length()).isEqualTo(lengthBefore);
    assertThat(new TimeSeriesSealedStore(BASE_PATH, columns).getBlockCount()).isEqualTo(2);
  }
}
