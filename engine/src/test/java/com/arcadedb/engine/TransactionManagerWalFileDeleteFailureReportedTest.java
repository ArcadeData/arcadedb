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
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Regression test for a correctness issue found in code review on #7502 (discussion #7479):
 * {@code TransactionManager.deleteWALFileIfNotHeldByAnotherInstance} discarded the return value of
 * {@code File.delete()}, so a delete that the OS actually refused - most notably on Windows, where a
 * process cannot delete a file through which it still holds an open, non-share-delete handle, even its
 * own - was reported as {@code DELETED} anyway. The old code (a plain {@code File.delete()} with no
 * handle open at all) did not have this failure mode; masking it here would have been a regression for
 * whichever platform hits it.
 * <p>
 * Reproduced portably (this fails identically on Linux and macOS) by making the containing directory
 * read-only, which makes the OS refuse the delete regardless of platform-specific handle semantics.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionManagerWalFileDeleteFailureReportedTest extends TestHelper {

  @Test
  void deleteFailureIsReportedNotSilentlyTreatedAsSuccess() throws Exception {
    final TransactionManager txManager = ((LocalDatabase) database).getTransactionManager();

    final File undeletableDir = new File(database.getDatabasePath(), "undeletable-7479");
    assertThat(undeletableDir.mkdirs()).isTrue();
    final File walFile = new File(undeletableDir, "txlog_9996.wal");
    assertThat(walFile.createNewFile()).isTrue();

    assumeThat(undeletableDir.setWritable(false))
        .as("this test needs to be able to make a directory read-only")
        .isTrue();
    // Everything from here on runs with the directory read-only: one try/finally around all of it, so
    // that an assumption failure (or any other exception) below still restores write access instead of
    // leaving a permanently undeletable directory behind for the next run/mvn clean to trip over.
    try {
      // setWritable(false) only reports whether the chmod syscall itself succeeded, not whether the OS
      // will actually enforce it - a process running as root bypasses the permission check entirely,
      // which would make the delete below succeed anyway and turn this into a false failure instead of a
      // skip. Probe with an unrelated write before trusting the directory is really locked down:
      // createNewFile() returns false only for "already exists", so a refused create surfaces as an
      // IOException instead.
      boolean permissionActuallyEnforced;
      try {
        permissionActuallyEnforced = !new File(undeletableDir, "permission-probe").createNewFile();
      } catch (final IOException e) {
        permissionActuallyEnforced = true;
      }
      assumeThat(permissionActuallyEnforced)
          .as("this test requires directory write permission to actually be enforced (e.g. not running as root)")
          .isTrue();

      final TransactionManager.WalFileSweepOutcome outcome = txManager.deleteWALFileForTesting(walFile);

      assertThat(outcome)
          .as("a delete the OS actually refused must be reported as an error, not silently as success")
          .isEqualTo(TransactionManager.WalFileSweepOutcome.ERROR);
      assertThat(walFile)
          .as("the file the OS refused to delete must still be there")
          .exists();
    } finally {
      undeletableDir.setWritable(true);
      walFile.delete();
      new File(undeletableDir, "permission-probe").delete();
      undeletableDir.delete();
    }
  }
}
