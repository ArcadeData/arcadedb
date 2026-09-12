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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for discussion #7479: a clean {@code close()} used to delete every {@code *.wal}
 * file it found by name in the database directory, not only the ones this instance's own WAL pool
 * had created. The reported scenario was a second embedded process opening the same database
 * directory a server already had open - a per-process advisory lock that a Docker Desktop bind mount
 * does not enforce across the container boundary - whose own clean close then swept away the still
 * running server's active WAL files, surfacing there as repeated "No such file or directory" errors.
 * <p>
 * A file the OS reports as still locked by someone else must survive the sweep; an orphan nobody
 * holds must still be removed exactly as before.
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

    final File lockedForeign = new File(dbPath, "txlog_9999.wal");
    final File orphanForeign = new File(dbPath, "txlog_9998.wal");
    assertThat(lockedForeign.createNewFile()).isTrue();
    assertThat(orphanForeign.createNewFile()).isTrue();

    try (final RandomAccessFile raf = new RandomAccessFile(lockedForeign, "rw")) {
      final FileChannel channel = raf.getChannel();
      final FileLock heldByAnotherInstance = channel.lock();
      try {
        database.close();

        assertThat(lockedForeign)
            .as("a WAL-named file another instance still has locked must survive this instance's close()")
            .exists();
        assertThat(orphanForeign)
            .as("an orphaned WAL-named file nobody holds must still be removed, as before")
            .doesNotExist();

        final File[] remainingWalFiles = new File(dbPath).listFiles((dir, name) -> name.endsWith(".wal"));
        assertThat(remainingWalFiles)
            .as("the only WAL file left behind must be the one still locked by 'another instance'")
            .containsExactly(lockedForeign);
      } finally {
        heldByAnotherInstance.release();
      }
    } finally {
      lockedForeign.delete();
    }

    // Reopen so TestHelper.afterTest() can drop the database normally.
    reopenDatabase();
  }
}
