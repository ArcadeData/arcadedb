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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #6932: {@code WALFile.getTransaction} read the transaction's segment count
 * straight from the file and allocated a {@code WALPage[]} of that size with no sanity check.
 * <p>
 * {@code TransactionManager.checkIntegrity} feeds {@code getTransaction} fully arbitrary header bytes -
 * {@code findNextValidTransactionPosition} derives a candidate transaction start from every 8-byte window
 * in the file that happens to equal {@link WALFile#MAGIC_NUMBER}, a value that can occur inside a page
 * payload - so a single corrupt or torn WAL reaches the allocation with a corruption-controlled count. At
 * {@code pages = Integer.MAX_VALUE} the array of references alone is ~17GB, and the resulting
 * {@link OutOfMemoryError} is an {@code Error}: the {@code catch (Exception)} guard at the bottom of
 * {@code getTransaction} cannot catch it, and {@code checkIntegrity}'s {@code try/finally} has no catch
 * either, so it unwinds out of {@code LocalDatabase.open} before the WAL drop or the {@code .corrupt}
 * rename can run. The database then fails to open forever, hitting the same bytes on every attempt.
 * <p>
 * The count is derivable and therefore checkable: {@code segmentSize} is already bounded against the file
 * length, and every segment costs at least {@code PAGE_HEADER_SIZE + 1} bytes (the fixed page header plus
 * a delta of at least one byte, which {@code writeTransactionToBuffer} guarantees), so any legal count
 * satisfies {@code pages * (PAGE_HEADER_SIZE + 1) <= segmentSize}. The equivalent guard already existed on
 * the Raft entry parser ({@code ArcadeStateMachine.deserializeWalTransaction}, issue #4420) and on the
 * per-page {@code deltaSize} (issue #4958); only the WAL file reader was left without one.
 */
class Issue6932CorruptWALPageCountTest extends TestHelper {

  // Layout constants mirroring the private statics in WALFile.
  private static final int TX_HEADER_SIZE   = 24; // txId(8) + timestamp(8) + pages(4) + segmentSize(4)
  private static final int PAGE_HEADER_SIZE = 24; // fileId(4) + pageNumber(4) + from(4) + to(4) + version(4) + size(4)
  private static final int TX_FOOTER_SIZE   = 12; // segmentSize(4) + MAGIC_NUMBER(8)

  /** The cheapest legal segment: fixed header plus a one-byte delta. */
  private static final int MIN_SEGMENT_SIZE = PAGE_HEADER_SIZE + 1;

  /** Offset of the segment-size field inside the transaction header. */
  private static final int SEGMENT_SIZE_OFFSET = 20;

  /**
   * A file id that does not exist in the database, so {@code TransactionManager.applyChanges} would treat a
   * crafted record as a no-op ("skipping missing file") rather than writing to a real component. Same
   * convention as {@code Issue4508TornWALRecoveryTest}.
   */
  private static final int MISSING_FILE_ID = 990_000;

  @TempDir
  Path walDir;

  @Override
  protected void beginTest() {
    database.getSchema().getOrCreateDocumentType("CorruptWALType");
  }

  @Test
  void hugePageCountIsRejectedBeforeTheArrayIsAllocated() throws Exception {
    // A single well-formed one-page record whose header lies about the number of segments. Before the
    // fix this reached `new WALPage[Integer.MAX_VALUE]` and died with an OutOfMemoryError that the
    // catch(Exception) guard below it cannot catch.
    assertRejected("huge", buildTransaction(Integer.MAX_VALUE, MIN_SEGMENT_SIZE, 1),
        "a page count that cannot fit in the declared segment must be rejected as 'not a transaction'");
  }

  @Test
  void pageCountExceedingTheSegmentBudgetIsRejected() throws Exception {
    // A million segments cannot live inside a 25-byte segment area. Before the fix this allocated an
    // 8MB array of references before discovering, one segment in, that the file had run out.
    assertRejected("overbudget", buildTransaction(1_000_000, MIN_SEGMENT_SIZE, 1),
        "a page count beyond what the declared segment size can hold must be rejected");
  }

  @Test
  void negativeSegmentSizeIsRejected() throws Exception {
    // `segmentSize` was never checked for sign, so a negative value made the truncation bound
    // (`pos + segmentSize + 8 > size`) pass trivially. The record below then parsed CLEANLY - magic
    // number and all - even though writeTransactionToBuffer could never have produced it.
    final byte[] content = buildTransaction(1, MIN_SEGMENT_SIZE, 1);
    ByteBuffer.wrap(content).putInt(SEGMENT_SIZE_OFFSET, -1);

    assertRejected("negseg", content, "a negative segment size is corruption, not a parseable transaction");
  }

  @Test
  void negativePageCountIsRejected() throws Exception {
    assertRejected("negpages", buildTransaction(-1, MIN_SEGMENT_SIZE, 1), "a negative page count must be rejected");
  }

  @Test
  void aTransactionAtTheTightestLegalDensityStillParses() throws Exception {
    // The bound must not reject a legitimate record: three segments each of the minimum size, so
    // pages * (PAGE_HEADER_SIZE + 1) == segmentSize exactly.
    final int pages = 3;
    final WALFile file = openWal("tight", buildTransaction(pages, pages * MIN_SEGMENT_SIZE, pages));
    try {
      final WALFile.WALTransaction tx = file.getTransaction(0);
      assertThat(tx).as("a record at the tightest legal density must still parse").isNotNull();
      assertThat(tx.pages).hasSize(pages);
      assertThat(tx.txId).isEqualTo(42L);
      for (final WALFile.WALPage page : tx.pages) {
        assertThat(page.changesFrom).isZero();
        assertThat(page.changesTo).isZero();
        assertThat(page.currentContent.size()).isEqualTo(1);
      }
    } finally {
      file.close();
    }
  }

  @Test
  void aMultiByteDeltaTransactionStillParses() throws Exception {
    // Same guard, with deltas larger than the one-byte minimum: the bound is an upper bound on the
    // count, never an equality, so a sparser record must pass too.
    final int pages = 2;
    final int deltaSize = 16;
    final int segmentSize = pages * (PAGE_HEADER_SIZE + deltaSize);
    final ByteBuffer buf = ByteBuffer.allocate(TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
    buf.putLong(7L);
    buf.putLong(123456L);
    buf.putInt(pages);
    buf.putInt(segmentSize);
    for (int i = 0; i < pages; i++) {
      buf.putInt(3);             // fileId
      buf.putInt(i);             // pageNumber
      buf.putInt(0);             // changesFrom
      buf.putInt(deltaSize - 1); // changesTo
      buf.putInt(1);             // currentPageVersion
      buf.putInt(1024);          // currentPageSize
      for (int b = 0; b < deltaSize; b++)
        buf.put((byte) (b + i));
    }
    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);

    final WALFile file = openWal("wide", buf.array());
    try {
      final WALFile.WALTransaction tx = file.getTransaction(0);
      assertThat(tx).isNotNull();
      assertThat(tx.pages).hasSize(pages);
      assertThat(tx.pages[0].currentContent.size()).isEqualTo(deltaSize);
      assertThat(tx.endPositionInLog).isEqualTo(TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
    } finally {
      file.close();
    }
  }

  @Test
  void recoveryOfAWalWithACorruptPageCountDoesNotKillTheOpen() throws Exception {
    // The whole point of the guard, driven through the real path from the issue rather than through a
    // bare checkIntegrity() on a live database: kill the database so the next open runs
    // LocalDatabase.performRecovery, and plant the corrupt WAL beside the real one it left behind.
    // checkIntegrity wraps its body in a try/finally with NO catch, so the Error thrown by the
    // allocation escapes LocalDatabase.open before either the WAL drop or the '.corrupt' rename can
    // run - a permanent open-failure loop, since every later attempt reads the same bytes.
    final String dbPath = database.getDatabasePath();
    database.transaction(() -> database.newDocument("CorruptWALType").set("k", 1).save());
    ((DatabaseInternal) database).kill();

    // MISSING_FILE_ID keeps the crafted record a no-op even if a future change made it parseable:
    // applyChanges logs "skipping missing file" instead of writing to a real component.
    final File corruptWal = new File(dbPath, "txlog_6932.wal");
    Files.write(corruptWal.toPath(), buildTransaction(Integer.MAX_VALUE, MIN_SEGMENT_SIZE, 1, MISSING_FILE_ID));

    // kill() leaves the instance registered; close() deregisters it without flushing what the kill dropped.
    database.close();

    final AtomicBoolean recoveryRan = new AtomicBoolean(false);
    factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, () -> {
      recoveryRan.set(true);
      return null;
    });

    assertThatCode(() -> database = factory.open())
        .as("a corrupt page count must not throw an Error out of LocalDatabase.open")
        .doesNotThrowAnyException();
    assertThat(recoveryRan.get()).as("recovery must actually have run - otherwise the WAL was never read").isTrue();

    // Recovery completed rather than unwinding: the corrupt WAL was walked over and dropped with the rest.
    assertThat(corruptWal).as("a WAL rejected as 'not a transaction' is dropped, not preserved as a gap").doesNotExist();

    // The committed record survived and the database is still writable. Counted by scan, never by
    // countType/count(*): those sum LocalBucket.count(), a cached counter that recovery resets to -1
    // and recomputes, so it can read correct while the records it claims to describe did not survive.
    assertThat(liveRecordCount()).as("the record committed before the kill must have been recovered").isEqualTo(1L);
    database.transaction(() -> database.newDocument("CorruptWALType").set("k", 2).save());
    assertThat(liveRecordCount()).as("the database must still be writable after recovery walked the corrupt WAL")
        .isEqualTo(2L);
  }

  /** Ground truth: {@code count(@rid)} full-scans instead of reading the cached per-bucket counter. */
  private long liveRecordCount() {
    return ((Number) database.query("sql", "SELECT count(@rid) AS n FROM CorruptWALType").next().getProperty("n"))
        .longValue();
  }

  private void assertRejected(final String name, final byte[] content, final String reason) throws Exception {
    final WALFile file = openWal(name, content);
    try {
      assertThat(file.getTransaction(0)).as(reason).isNull();
    } finally {
      file.close();
    }
  }

  private WALFile openWal(final String name, final byte[] content) throws Exception {
    final Path wal = walDir.resolve("txlog_6932_" + name + ".wal");
    Files.write(wal, content);
    return new WALFile(wal.toAbsolutePath().toString());
  }

  /**
   * Builds a WAL transaction record whose header declares {@code declaredPages} / {@code declaredSegmentSize}
   * while the body actually carries {@code actualPages} minimum-size segments.
   */
  private static byte[] buildTransaction(final int declaredPages, final int declaredSegmentSize, final int actualPages) {
    return buildTransaction(declaredPages, declaredSegmentSize, actualPages, 3);
  }

  private static byte[] buildTransaction(final int declaredPages, final int declaredSegmentSize, final int actualPages,
      final int fileId) {
    final int bodySize = actualPages * MIN_SEGMENT_SIZE;
    final ByteBuffer buf = ByteBuffer.allocate(TX_HEADER_SIZE + bodySize + TX_FOOTER_SIZE);
    buf.putLong(42L);                 // txId
    buf.putLong(123456L);             // timestamp
    buf.putInt(declaredPages);        // page/segment count
    buf.putInt(declaredSegmentSize);
    for (int i = 0; i < actualPages; i++) {
      buf.putInt(fileId);   // fileId
      buf.putInt(i);        // pageNumber
      buf.putInt(0);        // changesFrom
      buf.putInt(0);        // changesTo -> one-byte delta
      buf.putInt(1);        // currentPageVersion
      buf.putInt(1024);     // currentPageSize
      buf.put((byte) 0x7F); // the delta itself
    }
    buf.putInt(declaredSegmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);
    return buf.array();
  }
}
