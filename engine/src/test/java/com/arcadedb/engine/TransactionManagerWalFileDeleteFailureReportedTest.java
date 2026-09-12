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
    try {
      final String outcome = txManager.deleteWALFileForTesting(walFile);

      assertThat(outcome)
          .as("a delete the OS actually refused must be reported as an error, not silently as success")
          .isEqualTo("ERROR");
      assertThat(walFile)
          .as("the file the OS refused to delete must still be there")
          .exists();
    } finally {
      undeletableDir.setWritable(true);
      walFile.delete();
      undeletableDir.delete();
    }
  }
}
