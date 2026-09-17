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
package com.arcadedb.server.ha.raft;

import com.arcadedb.engine.WALFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7505.
 * <p>
 * {@code SnapshotInstaller.cleanupWalFiles} listed every {@code *.wal} file in the freshly swapped-in database
 * directory and deleted it unconditionally - a second, unprotected copy of the failure class PR #7504 had just
 * fixed in {@code TransactionManager.close()} for discussion #7479. Under normal HA operation there is no live
 * local instance holding those files at that point, but on a database directory shared with another live
 * process - the case #7479 reported, where a Docker Desktop bind mount does not enforce the per-process advisory
 * lock across the container boundary - this sweep reproduced the exact same corruption.
 * <p>
 * It now goes through {@link WALFile#deleteIfNotHeldByAnotherInstance(File)}, the same lock-aware sweep the
 * engine uses, so an orphan is still removed and a file somebody else has open is left alone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7505SnapshotInstallerWalOwnershipTest {

  @TempDir
  private Path dbDir;

  @Test
  void anOrphanedWalFileIsStillRemovedButOneAnotherInstanceHoldsIsLeftAlone() throws Exception {
    final File orphan = dbDir.resolve("txlog_0.wal").toFile();
    assertThat(orphan.createNewFile()).isTrue();

    // Stands in for a second, still-running instance holding this WAL file open. Every WALFile has taken an
    // exclusive lock on itself for its whole life since #7479, which is what the sweep probes for.
    final File heldByAnotherInstance = dbDir.resolve("txlog_1.wal").toFile();
    final WALFile anotherInstanceWAL = new WALFile(heldByAnotherInstance.getPath());
    try {
      SnapshotInstaller.cleanupWalFiles(dbDir);

      assertThat(orphan)
          .as("an orphan nobody holds open must still be swept, exactly as before")
          .doesNotExist();
      assertThat(heldByAnotherInstance)
          .as("a WAL file another live instance still has open must not be deleted out from under it")
          .exists();
    } finally {
      anotherInstanceWAL.close();
      heldByAnotherInstance.delete();
    }
  }

  /** A directory with no WAL files at all, and one that does not exist, are both no-ops rather than failures. */
  @Test
  void anEmptyOrMissingDirectoryIsANoOp() {
    SnapshotInstaller.cleanupWalFiles(dbDir);
    SnapshotInstaller.cleanupWalFiles(dbDir.resolve("no-such-directory"));

    assertThat(dbDir).isEmptyDirectory();
  }

  /** Files that are not WAL files are untouched: the sweep is still scoped by the {@code .wal} suffix. */
  @Test
  void nonWalFilesAreUntouched() throws Exception {
    final File notAWal = dbDir.resolve("schema.json").toFile();
    assertThat(notAWal.createNewFile()).isTrue();

    SnapshotInstaller.cleanupWalFiles(dbDir);

    assertThat(notAWal).exists();
  }
}
