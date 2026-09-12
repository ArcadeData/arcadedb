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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for a gap found in code review on #7502 (discussion #7479): {@link WALFile#acquireLock()}
 * can fail (return without a lock) and the file was still added to the pool and used normally - a second
 * embedded instance racing a WAL file-name collision (the counter is seeded past every name a directory
 * scan can see, but says nothing about a name a concurrent process claims after that scan) would silently
 * share the file with whoever holds it instead of being noticed at all.
 * <p>
 * Exercised at the {@code checkWALFiles()} runtime-rotation call site: the counter there does not rescan
 * the directory (unlike {@code createWALFilePool()}'s own defensive reseed at open time), so it is the one
 * of the three construction sites a same-name collision can realistically still reach.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionManagerWalRotationCollidesWithForeignWalFencesTest extends TestHelper {

  /** Reports a size past the rotation threshold so {@code checkWALFiles()} treats it as full. */
  private static final class OversizedWALFile extends WALFile {
    private OversizedWALFile(final String filePath) throws IOException {
      super(filePath);
    }

    @Override
    public long getSize() {
      return 64L * 1024 * 1024 + 1; // must exceed TransactionManager.MAX_LOG_FILE_SIZE (64 MB)
    }
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void rotationCollidingWithLiveForeignWalFileFencesInsteadOfSharingIt() throws Exception {
    final LocalDatabase db = (LocalDatabase) database;
    final TransactionManager txManager = db.getTransactionManager();

    assertThat(db.isFencedForRecovery()).isFalse();

    final String collidingPath = db.getDatabasePath() + "/txlog_" + txManager.getLogFileCounterForTesting() + ".wal";
    // Stands in for a second, live instance that already claimed the exact name this rotation is about
    // to try next - the self-lock every WALFile now takes for its whole life (issue #7479) is what a real
    // second instance would already be holding.
    final WALFile anotherInstanceWAL = new WALFile(collidingPath);
    final OversizedWALFile oversized = new OversizedWALFile(db.getDatabasePath() + "/txlog_test_oversized_7479.wal");
    WALFile displaced = null;
    try {
      displaced = txManager.replaceActiveWALFileForTesting(0, oversized);

      txManager.checkWALFilesForTesting();

      assertThat(db.isFencedForRecovery())
          .as("a rotation whose next WAL file name collides with one another live instance already has open "
              + "must fence the database instead of silently sharing that file with it")
          .isTrue();
    } finally {
      anotherInstanceWAL.close();
      new File(collidingPath).delete();
      oversized.close();
      new File(db.getDatabasePath(), "txlog_test_oversized_7479.wal").delete();
      if (displaced != null)
        txManager.replaceActiveWALFileForTesting(0, displaced);
    }

    database.close();
  }
}
