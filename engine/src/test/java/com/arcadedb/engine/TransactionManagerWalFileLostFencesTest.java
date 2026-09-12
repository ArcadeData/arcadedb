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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.WarningCapture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for discussion #7479: a WAL file this instance still has open can become
 * inaccessible without this instance ever closing or dropping it - the reported scenario was a
 * second embedded process opening the same database directory (a per-process advisory lock that a
 * Docker Desktop bind mount does not enforce across the container boundary) and deleting every
 * {@code *.wal} file in it on its own clean close, including the ones the first, still-running
 * instance had open. Before this fix, {@code TransactionManager.checkWALFiles()} caught the
 * resulting {@code IOException} and just logged it at SEVERE - once a second, forever, via the
 * housekeeping timer - while the database kept silently accepting writes into a WAL pool that could
 * no longer be trusted. It must instead fence the database once, the same way a post-WAL-append
 * commit failure already does (#5053): one clear diagnostic, and the housekeeping timer cancels
 * itself on its next tick (see {@link TransactionManagerFencedTimerTaskTest}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionManagerWalFileLostFencesTest extends TestHelper {

  /**
   * A WAL file stand-in whose {@link #getSize()} always fails with the exact exception the reporter's
   * server log showed ({@code java.io.IOException: No such file or directory}), regardless of what
   * actually happened on disk. Unlike deleting the real file, which on a native POSIX filesystem does
   * not fail an already-open file descriptor's {@code size()} call, this reproduces the failure the
   * way the reporter's virtualized bind mount actually surfaced it, portably.
   */
  private static final class VanishedWALFile extends WALFile {
    private VanishedWALFile(final String filePath) throws IOException {
      super(filePath);
    }

    @Override
    public long getSize() throws IOException {
      throw new IOException("No such file or directory");
    }
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void vanishedWalFileFencesInsteadOfLoggingForever() throws Exception {
    final LocalDatabase db = (LocalDatabase) database;
    final TransactionManager txManager = db.getTransactionManager();

    assertThat(db.isFencedForRecovery()).isFalse();

    final WALFile vanished = new VanishedWALFile(db.getDatabasePath() + "/txlog_vanished_7479.wal");
    WALFile displaced = null;
    try {
      displaced = txManager.replaceActiveWALFileForTesting(0, vanished);

      final List<String> severeLines = WarningCapture.captureSevere(txManager::checkWALFilesForTesting);

      assertThat(db.isFencedForRecovery())
          .as("a WAL file this instance still has open becoming inaccessible must fence the database")
          .isTrue();
      assertThat(severeLines)
          .as("exactly one clear fence diagnostic, not a raw WAL-management stack trace per pool slot; got: %s",
              severeLines)
          .hasSize(1)
          .allMatch(line -> line.contains("fenced for recovery"));

      // A second pass (the housekeeping timer would otherwise still be running once a second) must be a
      // silent no-op: fenceForRecovery() only ever logs its first reason.
      final List<String> secondPassLines = WarningCapture.captureSevere(txManager::checkWALFilesForTesting);
      assertThat(secondPassLines).isEmpty();
    } finally {
      vanished.close();
      // Put the real pool file back so the impending close() below can close and drop it normally
      // instead of leaking its file descriptor.
      if (displaced != null)
        txManager.replaceActiveWALFileForTesting(0, displaced);
    }

    database.close();
  }
}
