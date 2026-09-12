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

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for discussion #7479: a clean {@code close()} used to delete every {@code *.wal}
 * file it found by name in the database directory, not only the ones this instance's own WAL pool
 * had created. The reported scenario was a second embedded process opening the same database
 * directory a server already had open - a per-process advisory lock that a Docker Desktop bind mount
 * does not enforce across the container boundary - whose own clean close then swept away the still
 * running server's active WAL files, surfacing there as repeated "No such file or directory" errors.
 * <p>
 * The stand-in for "another live instance's WAL file" here is a real {@link WALFile}, not a hand-rolled
 * lock: since this same issue, every {@code WALFile} takes an exclusive OS-level lock on itself for its
 * whole life (see {@code WALFile.acquireLock}), which is the actual mechanism that now protects it - not
 * an artifact of how this test happens to simulate a second process. An orphan nobody holds open must
 * still be removed exactly as before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionManagerCloseSkipsLockedForeignWalFileTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
    database.transaction(() -> database.newDocument("Doc").set("v", 1).save());
  }

  @Test
  void lockedForeignWalFileSurvivesCloseButOrphanedOneIsRemoved() throws Exception {
    final String dbPath = database.getDatabasePath();

    final File orphanForeign = new File(dbPath, "txlog_9998.wal");
    assertThat(orphanForeign.createNewFile()).isTrue();

    // Stands in for a second, still-running embedded instance/process that has this WAL file open -
    // exactly what happens today, the moment a second instance opens the same directory this issue is
    // about in the first place.
    final File anotherInstanceFile = new File(dbPath, "txlog_9999.wal");
    final WALFile anotherInstanceWAL = new WALFile(anotherInstanceFile.getPath());
    try {
      database.close();

      assertThat(anotherInstanceFile)
          .as("a WAL file another live instance still has open must survive this instance's close()")
          .exists();
      assertThat(orphanForeign)
          .as("an orphaned WAL-named file nobody holds must still be removed, as before")
          .doesNotExist();

      final File[] remainingWalFiles = new File(dbPath).listFiles((dir, name) -> name.endsWith(".wal"));
      assertThat(remainingWalFiles)
          .as("the only WAL file left behind must be the one still open in 'another instance'")
          .containsExactly(anotherInstanceFile);
    } finally {
      anotherInstanceWAL.close();
      anotherInstanceFile.delete();
    }

    // Reopen so TestHelper.afterTest() can drop the database normally.
    reopenDatabase();
  }
}
