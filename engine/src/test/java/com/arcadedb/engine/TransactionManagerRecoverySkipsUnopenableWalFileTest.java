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
import java.io.RandomAccessFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Regression test for a pre-existing gap CodeRabbit found while reviewing #7502 (discussion #7479):
 * {@code checkIntegrity()} already tolerated a WAL file whose constructor throws
 * {@code FileNotFoundException} during the scan (it logs and leaves that pool slot {@code null}), but the
 * replay-initialization loop right below indexed into every slot unconditionally, so a null one crashed
 * recovery with an unrelated {@code NullPointerException} instead of the diagnostic already logged for it.
 * Unrelated to the foreign-lock hardening the rest of this PR adds, but directly adjacent to it in the same
 * method.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionManagerRecoverySkipsUnopenableWalFileTest extends TestHelper {

  @Test
  void recoverySkipsAnUnopenableWalFileInsteadOfCrashingWithNPE() throws Exception {
    final LocalDatabase db = (LocalDatabase) database;
    final TransactionManager txManager = db.getTransactionManager();
    final String dbPath = db.getDatabasePath();

    final File openable = new File(dbPath, "txlog_9993.wal");
    assertThat(openable.createNewFile()).isTrue();

    final File unopenable = new File(dbPath, "txlog_9992.wal");
    assertThat(unopenable.createNewFile()).isTrue();
    assumeThat(unopenable.setWritable(false))
        .as("this test needs to be able to make a file read-only")
        .isTrue();

    try {
      // Same root-safety concern as TransactionManagerWalFileDeleteFailureReportedTest: only trust the
      // permission if it is actually enforced, not just that the chmod syscall succeeded.
      boolean permissionActuallyEnforced;
      try {
        new RandomAccessFile(unopenable, "rw").close();
        permissionActuallyEnforced = false;
      } catch (final IOException e) {
        permissionActuallyEnforced = true;
      }
      assumeThat(permissionActuallyEnforced)
          .as("this test requires file write permission to actually be enforced (e.g. not running as root)")
          .isTrue();

      assertThatCode(txManager::checkIntegrity)
          .as("a WAL file this scan cannot open must be skipped, not crash the whole recovery with an "
              + "unrelated NullPointerException")
          .doesNotThrowAnyException();
    } finally {
      unopenable.setWritable(true);
      unopenable.delete();
      openable.delete();
    }

    database.close();
  }
}
