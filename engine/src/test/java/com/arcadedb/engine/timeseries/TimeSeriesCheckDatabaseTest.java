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
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CHECK DATABASE coverage of TimeSeries types (default, FIX and DEEP tiers) and the sealed store integrity checks
 * they are built on. Each nested group keeps its own fixture and carries the issue number it regression-tests.
 */
class TimeSeriesCheckDatabaseTest {

  /**
   * Issue #6340 (item 4): {@code CHECK DATABASE} had no TimeSeries coverage at all - {@code DatabaseChecker}
   * contained zero occurrences of {@code TimeSeries}, {@code tstb} or {@code tstd}.
   * <p>
   * The checker walks record buckets and indexes. A TimeSeries type has neither: {@code LocalTimeSeriesType}
   * registers its shards with the schema as FILES rather than as a type's record buckets, and its compacted data
   * lives in {@code .ts.sealed} files outside the paginated layer entirely. Since {@code LocalTimeSeriesType extends
   * LocalDocumentType}, such a type fell into the checker's document arm and then had no record bucket to scan - so
   * the three on-disk formats TimeSeries owns, two of them with their own magics, headers and CRCs, were the only
   * storage in the engine the integrity check could not see.
   * <p>
   * That was not abstract. #6314 fixed a bug that wrote real rows to disk at the wrong stride; the fix stops it
   * recurring but cannot undo the pages a mismatched session already wrote, and there was no way to detect a file
   * left in that state. The header such a session also wrote counts rows the pages no longer hold, which is exactly
   * what {@link #aMutableBucketWhoseHeaderCountsSamplesItsPagesDoNotHoldIsReported()} pins.
   * <p>
   * The teardown integrity check is disabled for this group because three of its tests deliberately leave a damaged
   * file behind; the healthy case asserts cleanliness explicitly instead.
   * <p>
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
  @Nested
  class CheckDatabaseTimeSeries extends TestHelper {

    private static final int ROWS = 3_000;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
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

    private Result runCheck() {
      try (final ResultSet resultSet = database.command("sql", "CHECK DATABASE")) {
        assertThat(resultSet.hasNext()).isTrue();
        return resultSet.next();
      }
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> warningsOf(final Result row) {
      return (Collection<String>) row.<Collection<?>>getProperty("warnings");
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> corruptedTypesOf(final Result row) {
      return (Collection<String>) row.<Collection<?>>getProperty("corruptedTimeSeries");
    }

    /**
     * Flips one bit at {@code offset} of {@code file}, which is the least the format has to notice.
     */
    private static void flipByteAt(final File file, final long offset) throws IOException {
      try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        raf.seek(offset);
        final int b = raf.read();
        raf.seek(offset);
        raf.write(b ^ 0x01);
      }
    }

    /**
     * The pass runs, walks what is there and says so. The counters matter as much as the empty warning list: a
     * caller reading {@code totalTimeSeriesSamples} as zero on a database that holds three thousand of them would
     * have no way to tell "checked and clean" from "not looked at", which is the state this whole item was about.
     * <p>
     * The sealed block here was appended by THIS process rather than found on disk at open, which is the case whose
     * block-start offset and stored CRC live only in the file and not in the in-memory directory. Verifying it
     * against what memory believes reports a healthy store as damaged, so this is the arm that pins it.
     */
    // #6340
    @Test
    void aHealthyTimeSeriesTypeIsWalkedAndReportedClean() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      appendRows(engine, 100);

      final Result row = runCheck();

      assertThat(warningsOf(row)).as("warnings").isEmpty();
      assertThat(corruptedTypesOf(row)).isEmpty();
      assertThat(row.<Long>getProperty("totalTimeSeriesTypes")).isEqualTo(1L);
      assertThat(row.<Long>getProperty("totalTimeSeriesShards")).isEqualTo(1L);
      assertThat(row.<Long>getProperty("totalTimeSeriesSamples")).isEqualTo(ROWS + 100L);
      assertThat(row.<Long>getProperty("totalTimeSeriesSealedBlocks")).isGreaterThan(0L);
    }

    /**
     * A database with no TimeSeries type answers the question with zeros rather than with silence, and its step plan
     * is the one it always had - the pass adds no step where there is nothing to check.
     */
    // #6340
    @Test
    void aDatabaseWithoutTimeSeriesReportsZerosRatherThanNothing() {
      database.getSchema().createDocumentType("Note");
      database.transaction(() -> database.newDocument("Note").set("i", 1).save());

      final Result row = runCheck();

      assertThat(warningsOf(row)).isEmpty();
      assertThat(corruptedTypesOf(row)).isEmpty();
      assertThat(row.<Long>getProperty("totalTimeSeriesTypes")).isZero();
      assertThat(row.<Long>getProperty("totalTimeSeriesShards")).isZero();
      assertThat(row.<Long>getProperty("totalTimeSeriesSamples")).isZero();
      assertThat(row.<Long>getProperty("totalTimeSeriesSealedBlocks")).isZero();
    }

    /**
     * The residue of #6314, reproduced in the shape it leaves behind: page 0 counts samples the data pages do not
     * hold. A session that wrote at the wrong stride put real rows at offsets nothing will address again and
     * incremented the header for each of them, so the two disagree permanently and no read path ever says so - a
     * query simply returns fewer rows than were written.
     */
    // #6340
    @Test
    void aMutableBucketWhoseHeaderCountsSamplesItsPagesDoNotHoldIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      final String bucketPath = engine.getShard(0).getMutableBucket().getComponentFile().getFilePath();

      database.close();
      try {
        // Page 0's sample counter, at content offset 7 of the header page. Raised rather than lowered so the file
        // says what a wrong-stride session's file says: more samples counted than the pages can account for.
        try (final RandomAccessFile raf = new RandomAccessFile(new File(bucketPath), "rw")) {
          raf.seek(BasePage.PAGE_HEADER_SIZE + 7);
          raf.writeLong(ROWS + 500L);
        }
      } finally {
        database = factory.open();
      }

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'") && w.contains("shard 0 mutable bucket")
          && w.contains("declares " + (ROWS + 500) + " sample(s)") && w.contains("hold " + ROWS));
    }

    /**
     * The header page of a mutable bucket is what says how to read everything after it. Without it nothing can, and
     * before this pass nothing said so either.
     */
    // #6340
    @Test
    void aMutableBucketWithNoHeaderMagicIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, 500);
      final String bucketPath = engine.getShard(0).getMutableBucket().getComponentFile().getFilePath();

      database.close();
      try {
        flipByteAt(new File(bucketPath), BasePage.PAGE_HEADER_SIZE);
      } finally {
        database = factory.open();
      }

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'") && w.contains("'TSBC' magic"));
    }

    /**
     * Every sealed block carries a CRC32 over its metadata and its compressed columns, and it is verified lazily on
     * the first read of that block - so a block nothing queries is a block nothing verifies. Checking them here is
     * what turns "the data is probably fine" into an answer, and it is the reason this pass reads the sealed file
     * rather than only its header.
     */
    // #6340
    @Test
    void aSealedBlockWhoseBytesChangedFailsItsCRC() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");
      assertThat(sealed).exists();

      database.close();
      try {
        // Inside the last block's compressed data: the final four bytes are that block's stored CRC, and the
        // directory scan that precedes the check has to still recognise the block for the CRC to be the finding.
        flipByteAt(sealed, sealed.length() - 8);
      } finally {
        database = factory.open();
      }

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'") && w.contains("sealed store")
          && w.contains("CRC mismatch"));

      // And again, in the same process: the sealed store caches "this block's CRC has been verified" so a read never
      // pays for it twice, which is right for a read and would make a second check answer "clean" without looking.
      final Result second = runCheck();
      assertThat(corruptedTypesOf(second)).containsExactly("Cpu");
      assertThat(warningsOf(second)).anyMatch(w -> w.contains("CRC mismatch"));
    }

    /**
     * A tag dictionary is the only thing that can turn a 4-byte id in a mutable row back into the tag it stands
     * for. Losing it does not fail a query: every tag reads back as null, on every row, silently.
     */
    // #6340
    @Test
    void aTagDictionaryWithNoHeaderMagicIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, 500);
      final TimeSeriesTagDictionary dictionary = engine.getTagDictionary();
      assertThat(dictionary).isNotNull();
      final String dictionaryPath = dictionary.getComponentFile().getFilePath();

      database.close();
      try {
        flipByteAt(new File(dictionaryPath), BasePage.PAGE_HEADER_SIZE);
      } finally {
        database = factory.open();
      }

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'") && w.contains("tag dictionary")
          && w.contains("'TSTD' magic"));
    }

    /**
     * The TYPE scope reaches TimeSeries the way it reaches every other type: naming one checks its files and leaves
     * the other one's alone.
     */
    // #6340
    @Test
    void theTypeScopeSelectsWhichTimeSeriesTypeIsChecked() throws Exception {
      appendRows(createType("Cpu"), 500);
      final TimeSeriesEngine memory = createType("Memory");
      appendRows(memory, 500);
      final String memoryBucketPath = memory.getShard(0).getMutableBucket().getComponentFile().getFilePath();

      database.close();
      try {
        flipByteAt(new File(memoryBucketPath), BasePage.PAGE_HEADER_SIZE);
      } finally {
        database = factory.open();
      }

      try (final ResultSet resultSet = database.command("sql", "CHECK DATABASE TYPE Cpu")) {
        final Result row = resultSet.next();
        assertThat(corruptedTypesOf(row)).isEmpty();
        assertThat(row.<Long>getProperty("totalTimeSeriesTypes")).isEqualTo(1L);
      }

      try (final ResultSet resultSet = database.command("sql", "CHECK DATABASE TYPE Memory")) {
        final Result row = resultSet.next();
        assertThat(corruptedTypesOf(row)).containsExactly("Memory");
      }
    }

    /**
     * Patches a field of a closed component file's page 0, at {@code contentOffset} bytes past the page header, and
     * reopens the database. Every damage shape below is one of these: the header is what page 0 asserts about the
     * rest of the file, so each field is a distinct thing the check has to notice.
     */
    private void patchHeaderField(final String filePath, final int contentOffset, final Patch patch) throws Exception {
      database.close();
      try (final RandomAccessFile raf = new RandomAccessFile(new File(filePath), "rw")) {
        raf.seek(BasePage.PAGE_HEADER_SIZE + contentOffset);
        patch.apply(raf);
      } finally {
        database = factory.open();
      }
    }

    @FunctionalInterface
    private interface Patch {
      void apply(RandomAccessFile raf) throws Exception;
    }

    /**
     * The format version byte decides how a row is READ - whether a TAG column is a 4-byte dictionary id or an
     * inline string - so a page 0 that disagrees with the file name is the one thing that turns every row into a
     * different row. This is the header half of the guard #6314 added to the component itself.
     */
    // #6340
    @Test
    void aMutableBucketWhoseHeaderVersionDisagreesWithItsFileNameIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, 200);

      patchHeaderField(engine.getShard(0).getMutableBucket().getComponentFile().getFilePath(), 4,
          raf -> raf.writeByte(TimeSeriesBucket.CURRENT_VERSION + 7));

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'")
          && w.contains("mutable row format version " + (TimeSeriesBucket.CURRENT_VERSION + 7)));
    }

    /**
     * The stride is computed from the schema's columns, so a header claiming a different count means the rows were
     * written at a width nothing will read them back at.
     */
    // #6340
    @Test
    void aMutableBucketWhoseHeaderColumnCountDisagreesWithTheSchemaIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, 200);

      patchHeaderField(engine.getShard(0).getMutableBucket().getComponentFile().getFilePath(), 5,
          raf -> raf.writeShort(99));

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'")
          && w.contains("declares 99 column(s)"));
    }

    /**
     * A header announcing data pages the file does not hold, which is the other direction of the #6314 residue: the
     * samples those pages should carry are simply not there.
     * <p>
     * It also pins the ONE-finding rule: the walk stops at the first missing page, so the aggregate "declares X
     * samples but the pages hold Y" - which the short count would otherwise trip - is deliberately not reported on
     * top. A single root cause reads as a single finding.
     */
    // #6340
    @Test
    void aMutableBucketAnnouncingDataPagesItDoesNotHaveReportsThatAloneAndOnce() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, 200);

      patchHeaderField(engine.getShard(0).getMutableBucket().getComponentFile().getFilePath(), 40,
          raf -> raf.writeInt(9_999));

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");

      final List<String> bucketWarnings = warningsOf(row).stream()
          .filter(w -> w.contains("timeseries 'Cpu'") && w.contains("mutable bucket")).toList();
      assertThat(bucketWarnings).hasSize(1);
      assertThat(bucketWarnings.getFirst()).contains("declares 9999 data page(s) but page");
    }

    /**
     * A file that is not a whole number of its own pages was written at a different stride or lost its tail - the
     * O(1) check that needs no walk at all.
     */
    // #6340
    @Test
    void aMutableBucketFileThatIsNotAWholeNumberOfPagesIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, 200);
      final String bucketPath = engine.getShard(0).getMutableBucket().getComponentFile().getFilePath();

      database.close();
      try (final RandomAccessFile raf = new RandomAccessFile(new File(bucketPath), "rw")) {
        raf.setLength(raf.length() - 16);
      } finally {
        database = factory.open();
      }

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'")
          && w.contains("not a whole number of"));
    }

    /**
     * A dictionary whose header claims more entries than its pages hold: every id above what the pages actually
     * carry resolves to nothing, which a reader experiences as a null tag rather than as an error.
     */
    // #6340
    @Test
    void aTagDictionaryClaimingMoreEntriesThanItsPagesHoldIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, 200);
      final TimeSeriesTagDictionary dictionary = engine.getTagDictionary();
      assertThat(dictionary).isNotNull();

      patchHeaderField(dictionary.getComponentFile().getFilePath(), 5, raf -> raf.writeInt(500));

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'") && w.contains("tag dictionary")
          && w.contains("declares 500 entries"));
    }

    /**
     * Issue #6356: corrupting the sealed store's OWN header magic used to make the TimeSeries type disappear from
     * the schema entirely - {@code TimeSeriesSealedStore}'s constructor throws on a bad magic, {@code initEngine()}
     * fails during schema load, and the type was silently dropped instead of registered, so the database reopened
     * cleanly with the type simply gone (probed: {@code totalTimeSeriesTypes=0}, {@code existsType("Cpu")=false}) and
     * a check had nothing left to report on.
     * <p>
     * {@code LocalSchema#readConfiguration} now registers the type anyway with its engine left unavailable, which is
     * what lets {@code DatabaseChecker#checkTimeSeries}'s existing "the storage engine is not initialised" branch -
     * unreachable until this fix, since nothing walkable ever carried a broken type - fire for the first time.
     */
    // #6340
    @Test
    void aTimeSeriesTypeWhoseSealedStoreFailsToLoadStaysInTheSchemaAndIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");
      assertThat(sealed).exists();

      database.close();
      try {
        // Byte 0 of the header is part of the MAGIC_VALUE int, so this can never pass the magic check on reopen.
        flipByteAt(sealed, 0);
      } finally {
        database = factory.open();
      }

      // THE CORE OF #6356: the type must still be there, not have vanished.
      assertThat(database.getSchema().existsType("Cpu")).as("the type must not disappear from the schema").isTrue();
      final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("Cpu");
      assertThat(reopened.isEngineAvailable()).isFalse();
      assertThat(reopened.getEngineUnavailableReason()).contains("Cpu_shard_0.ts.sealed");

      // Every write against it fails loudly instead of quietly building a fresh empty type.
      assertThatThrownBy(reopened::requireEngine).hasMessageContaining("Cpu");
    }

    /**
     * The other half of #6356: {@code CHECK DATABASE} must be able to see and report the broken type, which is only
     * possible once it stays registered.
     */
    // #6340
    @Test
    void checkDatabaseReportsATimeSeriesTypeWhoseEngineFailedToInitialize() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");

      database.close();
      try {
        flipByteAt(sealed, 0);
      } finally {
        database = factory.open();
      }

      final Result row = runCheck();

      assertThat(row.<Long>getProperty("totalTimeSeriesTypes")).as("the broken type is still counted").isEqualTo(1L);
      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(
          w -> w.contains("timeseries 'Cpu'") && w.contains("storage engine is not initialised")
              // The reason names the file, so an operator does not have to go hunting through the server log for it.
              && w.contains("Cpu_shard_0.ts.sealed"));
    }

    /** A write against the broken type fails with a message naming the type, not with an NPE. */
    // #6340
    @Test
    void writingToATimeSeriesTypeWhoseEngineFailedToInitializeFailsWithAClearError() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");

      database.close();
      try {
        flipByteAt(sealed, 0);
      } finally {
        database = factory.open();
      }

      assertThatThrownBy(() -> database.command("sql",
          "INSERT INTO Cpu SET ts = 1700000000000, hostname = 'h', usage = 1.0"))
          .hasMessageContaining("Cpu");
    }

    /**
     * The read side of the same guard: {@code SelectExecutionPlanner.handleTypeAsTarget} must call
     * {@code requireEngine()} and fail loudly, rather than falling through to a generic document scan that would
     * read zero rows from a type with no record bucket of its own and report the query as having succeeded.
     */
    // #6340
    @Test
    void selectingFromATimeSeriesTypeWhoseEngineFailedToInitializeFailsWithAClearError() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");

      database.close();
      try {
        flipByteAt(sealed, 0);
      } finally {
        database = factory.open();
      }

      assertThatThrownBy(() -> database.command("sql", "SELECT FROM Cpu").close())
          .hasMessageContaining("Cpu");
    }

    /**
     * Bytes past the last readable block: the tail of a write that did not complete. The directory scan stops at
     * the first thing that is not a block magic, so this region is invisible to every other reader - neither used
     * nor reported - which is exactly why the check has to say it is there.
     */
    // #6340
    @Test
    void bytesFollowingTheLastSealedBlockAreReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");

      database.close();
      try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
        raf.seek(raf.length());
        raf.write(new byte[64]);
      } finally {
        database = factory.open();
      }

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'") && w.contains("sealed store")
          && w.contains("64 byte(s) follow the last readable block"));
    }

    /**
     * The sealed header's block count against what scanning the file actually finds. Compared only once the header
     * is clean on disk, which a close-and-reopen guarantees - while blocks are being appended it legitimately
     * under-reports, and comparing then would call a healthy store damaged.
     */
    // #6340
    @Test
    void aSealedStoreHeaderMiscountingItsBlocksIsReported() throws Exception {
      final TimeSeriesEngine engine = createType("Cpu");
      appendRows(engine, ROWS);
      engine.compactAll();
      final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");

      database.close();
      try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
        // Block count is the int at offset 7 of the 27-byte header: magic(4) + version(1) + colCount(2).
        raf.seek(7);
        raf.writeInt(42);
      } finally {
        database = factory.open();
      }

      final Result row = runCheck();

      assertThat(corruptedTypesOf(row)).containsExactly("Cpu");
      assertThat(warningsOf(row)).anyMatch(w -> w.contains("timeseries 'Cpu'") && w.contains("sealed store")
          && w.contains("declares 42 block(s)"));
    }

    private TimeSeriesEngine engineOf(final String typeName) {
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    }

    private Result runCheck(final String suffix) {
      try (final ResultSet resultSet = database.command("sql", "CHECK DATABASE" + suffix)) {
        assertThat(resultSet.hasNext()).isTrue();
        return resultSet.next();
      }
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> repairsOf(final Result row) {
      return (Collection<String>) row.<Collection<?>>getProperty("timeSeriesRepairs");
    }

    /**
     * The residue of #6314 in the shape it leaves behind - page 0 counting samples the data pages do not hold - is
     * now repairable, because every one of those three counters is derived from the pages and maintained
     * incrementally. {@code clearDataPagesUpTo} already recomputes exactly these three the same way on every
     * retention pass, so the repair is the routine operation and not a new one.
     */
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
  @Nested
  class SealedStoreIntegrity {

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

      // The declared count follows the declared sum: a NaN sum says "no real sample", a finite one says every sample
      // was real. Each case here departs from the truth in one statistic on purpose, and this keeps the count from
      // being a second departure that would mask which one the checker caught.
      final long[] counts = { 0, 0, Double.isNaN(sums[2]) ? 0 : timestamps.length };
      store.appendBlock(timestamps.length, declaredMinTs, declaredMaxTs, compressed, mins, maxs, sums, counts,
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
    // #6360
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
    // #6360
    @Test
    void aBlockEntryIsTrustedOnlyOnceItsWrittenCRCIsRecorded() {
      final double[] stats = { Double.NaN, Double.NaN, 1.0 };
      final TimeSeriesSealedStore.BlockEntry entry =
          new TimeSeriesSealedStore.BlockEntry(1_000L, 2_000L, 4, 3,
              new TimeSeriesSealedStore.BlockStats(stats, stats, stats, new long[] { 0, 0, 4 }), 27L, 1L);

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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
    // #6360
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
}
