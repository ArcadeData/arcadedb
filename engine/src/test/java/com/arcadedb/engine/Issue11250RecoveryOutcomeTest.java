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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11250: a crash recovery that did not complete (its WAL has a gap, so it was preserved and
 * nothing further was replayed) used to leave only a log line behind. Callers that can heal the database from another copy,
 * an HA node resyncing from its leader, had no way to ask, and the node went on serving a database whose recovery had
 * stopped half way. {@link TransactionManager#isLastRecoveryIncomplete()} is the answer they read.
 */
class Issue11250RecoveryOutcomeTest extends TestHelper {

  private static final int TX_HEADER_SIZE   = 24;
  private static final int PAGE_HEADER_SIZE = 24;
  private static final int TX_FOOTER_SIZE   = 12;

  @Override
  protected void beginTest() {
    database.getSchema().getOrCreateDocumentType("RecoveryOutcomeType");
  }

  @Test
  void aRecoveryAbortedOnAWalGapIsReportedIncomplete() throws Exception {
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    assertThat(transactionManager.isLastRecoveryIncomplete()).isFalse();

    // A corrupt first record FOLLOWED by a valid transaction record is the pattern the gap detector aborts recovery on
    final byte[] garbage = new byte[32];
    Arrays.fill(garbage, (byte) 0x7F);
    final byte[] delta = { 1, 2, 3, 4, 5 };
    final int segmentSize = PAGE_HEADER_SIZE + delta.length;
    final ByteBuffer buf = ByteBuffer.allocate(garbage.length + TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
    buf.put(garbage);
    buf.putLong(42L);
    buf.putLong(123456L);
    buf.putInt(1);
    buf.putInt(segmentSize);
    buf.putInt(3);
    buf.putInt(0);
    buf.putInt(0);
    buf.putInt(delta.length - 1);
    buf.putInt(1);
    buf.putInt(1024);
    buf.put(delta);
    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);
    Files.write(new File(database.getDatabasePath(), "txlog_777.wal").toPath(), buf.array());

    transactionManager.checkIntegrity();

    assertThat(transactionManager.isLastRecoveryIncomplete())
        .as("recovery stopped on the gap and preserved the WAL: the database is not known to be whole")
        .isTrue();

    // The WAL is now .corrupt, so a second pass finds nothing to replay: it must not erase what the first one skipped
    transactionManager.checkIntegrity();
    assertThat(transactionManager.isLastRecoveryIncomplete()).isTrue();
  }

  @Test
  void aRecoveryThatReplayedEverythingIsNotReportedIncomplete() {
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();

    transactionManager.checkIntegrity();

    assertThat(transactionManager.isLastRecoveryIncomplete()).isFalse();
  }

  /**
   * Issue #11280: a replay that THROWS (a torn page whose bytes no longer decode) escaped recovery, so the open failed and every
   * restart repeated it, because the WAL stayed in place against the same torn pages. It is now the same outcome as a WAL gap:
   * the WAL is preserved aside, the database opens, and the recovery is reported incomplete for the HA node to quarantine it.
   */
  @Test
  void aRecoveryWhoseReplayThrowsIsReportedIncompleteInsteadOfFailingTheOpen() throws Exception {
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    database.transaction(() -> database.newDocument("RecoveryOutcomeType").set("a", 1).save());

    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("RecoveryOutcomeType").getBuckets(false).get(0);
    final int pageVersion = (int) ((DatabaseInternal) database).getPageManager()
        .getImmutablePage(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize(), false, false).getVersion();

    // A well-formed transaction one version ahead of the page, whose change lies far outside the page: applying it throws
    final byte[] delta = { 1, 2, 3, 4, 5 };
    final int changesFrom = Integer.MAX_VALUE / 2;
    final int segmentSize = PAGE_HEADER_SIZE + delta.length;
    final ByteBuffer buf = ByteBuffer.allocate(TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
    buf.putLong(42L);
    buf.putLong(123456L);
    buf.putInt(1);
    buf.putInt(segmentSize);
    buf.putInt(bucket.getFileId());
    buf.putInt(0);
    buf.putInt(changesFrom);
    buf.putInt(changesFrom + delta.length - 1);
    buf.putInt(pageVersion + 1);
    buf.putInt(bucket.getPageSize());
    buf.put(delta);
    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);
    final File wal = new File(database.getDatabasePath(), "txlog_778.wal");
    Files.write(wal.toPath(), buf.array());

    transactionManager.checkIntegrity();

    assertThat(transactionManager.isLastRecoveryIncomplete()).isTrue();
    assertThat(wal).as("the WAL must not stay in place to be replayed and thrown on again at every open").doesNotExist();
    assertThat(new File(wal.getPath() + ".corrupt")).as("preserved aside for inspection").exists();
  }
}
