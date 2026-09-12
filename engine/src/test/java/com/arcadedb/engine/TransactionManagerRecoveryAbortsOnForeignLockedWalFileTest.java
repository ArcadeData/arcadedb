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
import com.arcadedb.utility.LockException;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for a correctness issue found in code review on #7502 (discussion #7479):
 * {@code TransactionManager.checkIntegrity()}'s foreign-lock-detected abort used to just {@code return}
 * without closing the handles it had opened or rebuilding a safe pool - {@code activeWALFilePool} was left
 * pointing at the directory-scan array, which can include a file another live instance still has open, and
 * neither {@code performRecovery()} nor {@code openInternal()} checked for the failure. The database would
 * open successfully and {@code writeTransactionToWAL()} would go on to append new transactions into that
 * same array - including, in the worst case, the file the other instance is still actively writing to -
 * reintroducing the exact two-instances-one-WAL-file corruption this whole issue is about, through the one
 * path (crash recovery) the other fixes in this PR do not otherwise reach.
 * <p>
 * It must instead refuse to complete recovery (and, transitively, the database open it runs inside of)
 * rather than ever hand back a database whose active pool includes a contested file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionManagerRecoveryAbortsOnForeignLockedWalFileTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
    database.transaction(() -> database.newDocument("Doc").set("v", 1).save());
  }

  @Test
  void recoveryRefusesToCompleteWhenAWalFileIsForeignlyLocked() throws Exception {
    final LocalDatabase db = (LocalDatabase) database;
    final TransactionManager txManager = db.getTransactionManager();

    // Stands in for a second, live instance that already has one of this database's WAL files open -
    // exactly what a directory scan during recovery would find while that instance is still running.
    final File foreignFile = new File(db.getDatabasePath(), "txlog_9994.wal");
    final WALFile anotherInstanceWAL = new WALFile(foreignFile.getPath());
    try {
      assertThatThrownBy(txManager::checkIntegrity)
          .as("recovery must refuse to complete rather than leave a WAL file another live instance holds as part "
              + "of this instance's own active pool")
          .isInstanceOf(LockException.class);
    } finally {
      anotherInstanceWAL.close();
      foreignFile.delete();
    }

    database.close();
  }
}
