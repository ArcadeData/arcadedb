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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * The teardown integrity check is disabled for this class because three of its tests deliberately leave a damaged
 * file behind; the healthy case asserts cleanliness explicitly instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6340TimeSeriesCheckDatabaseTest extends TestHelper {

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
    assertThat(bucketWarnings.get(0)).contains("declares 9999 data page(s) but page");
  }

  /**
   * A file that is not a whole number of its own pages was written at a different stride or lost its tail - the
   * O(1) check that needs no walk at all.
   */
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
        w -> w.contains("timeseries 'Cpu'") && w.contains("storage engine is not initialised"));
  }

  /** A write against the broken type fails with a message naming the type, not with an NPE. */
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
   * Bytes past the last readable block: the tail of a write that did not complete. The directory scan stops at
   * the first thing that is not a block magic, so this region is invisible to every other reader - neither used
   * nor reported - which is exactly why the check has to say it is there.
   */
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
}
