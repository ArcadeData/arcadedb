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
package com.arcadedb.database;

import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8626: recovery writes the replayed pages straight to the data files, so before it deletes the WAL it replayed
 * it forces them, as a clean close and a WAL rotation do. When that fsync fails the WAL is the only durable copy of
 * the replayed pages and must survive the open (#4934 semantics), instead of being deleted as it always was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8626RecoveryFsyncFailureTest {
  private static final String DB_PATH = "target/databases/Issue8626RecoveryFsyncFailureTest";

  @AfterEach
  void cleanup() {
    LocalDatabase.TEST_BEFORE_RECOVERY_REPLAY_HOOK = null;
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      if (factory.exists())
        factory.open().drop();
    } catch (final Exception e) {
      // THE TEST LEAVES A BROKEN FILE BEHIND: REMOVE THE DATABASE ON DISK BELOW
    }
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void recoveryWhoseFsyncFailsKeepsTheReplayedWal() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    try (final Database db = factory.create()) {
      db.getSchema().createDocumentType("Doc");
      db.getSchema().createDocumentType("Other");
      db.transaction(() -> db.newDocument("Other").set("v", 0).save());
    }

    // A crash after a committed transaction on Doc only: its WAL is what the next open replays.
    final Database crashed = factory.open();
    crashed.transaction(() -> crashed.newDocument("Doc").set("v", 1).save());
    ((LocalDatabase) crashed).kill();
    crashed.close();

    final File dbDir = new File(DB_PATH);
    final String[] crashWal = dbDir.list((d, n) -> n.endsWith(".wal"));
    assertThat(crashWal).isNotEmpty();

    // Break the fsync of a file the replay does not write to: recovery still has to force it (after a crash every
    // file is unsynced), and the failure must keep the WAL.
    final AtomicBoolean broken = new AtomicBoolean();
    LocalDatabase.TEST_BEFORE_RECOVERY_REPLAY_HOOK = db -> {
      try {
        final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("Other").getBuckets(false).getFirst();
        final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId());
        final Field channelField = PaginatedComponentFile.class.getDeclaredField("channel");
        channelField.setAccessible(true);
        ((FileChannel) channelField.get(file)).close();
        broken.set(new File(file.getFilePath()).delete());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    };

    final Database recovered = factory.open();
    try {
      assertThat(broken.get()).isTrue();
      for (final String wal : crashWal)
        assertThat(new File(dbDir, wal))
            .as("a WAL whose replayed pages could not be fsynced must survive the recovery").exists();

      assertThat(recovered.countType("Doc", false)).isEqualTo(1);
    } finally {
      LocalDatabase.TEST_BEFORE_RECOVERY_REPLAY_HOOK = null;
      recovered.close();
    }

    // The close cannot fsync the broken file either, so it stays crash-equivalent: WAL and lock file preserved.
    for (final String wal : crashWal)
      assertThat(new File(dbDir, wal)).exists();
    assertThat(new File(dbDir, "database.lck")).exists();
    factory.close();
  }
}
