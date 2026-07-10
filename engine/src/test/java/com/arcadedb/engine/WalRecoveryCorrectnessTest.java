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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WAL/recovery correctness tests.
 * <p>
 * #4926: a 64KB page flush spans many disk sectors and the MVCC version header lives in sector 0. A torn
 * write at power loss can persist the NEW version header while the delta sectors still hold the PREVIOUS
 * content. Recovery's old {@code <=} skip declared such a page "already applied" and left it silently
 * corrupt (new version, old bytes) despite an intact, fully-fsynced WAL - undetectable, since every later
 * MVCC check accepts the page. The equal-version case must RE-APPLY the delta (idempotent when the page is
 * intact, repairing when it is torn).
 */
class WalRecoveryCorrectnessTest {

  private static final String DB_PATH = "target/databases/WalRecoveryCorrectnessTest";

  @AfterEach
  void cleanup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      if (factory.exists())
        factory.open().drop();
    } catch (final Exception e) {
      // A test may deliberately leave the database unreopenable (broken file): remove it on disk below.
    }
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void tornPageWriteIsRepairedByWalReplayAtEqualVersion() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    String bucketFilePath;
    // 1. Create the row and make sure its page (v1) is fully on disk.
    {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", "A").save());
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("Doc").getBuckets(false).getFirst();
      bucketFilePath = db.getFileManager().getFile(bucket.getFileId()).getFilePath();

      // 2. Commit the update to 'B' (bumping the page to v2, WAL entry written) but make sure the DATA page
      // never reaches the disk: flushing is suspended, then the database is killed (crash simulation that
      // preserves the WAL and the lock file, purging the pending flush).
      assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
          .as("flush suspension must engage or the page could reach disk, invalidating the test").isTrue();
      db.transaction(() -> db.query("sql", "SELECT FROM Doc").next().getRecord().get().asDocument().modify()
          .set("v", "B").save());
      ((LocalDatabase) db).kill();
      // close() on a killed instance only unregisters it from the factory (WAL and lock file survive):
      // same pattern as ACIDTransactionTest's kill-then-reopen flows.
      db.close();
    }

    // 3. Forge the TORN write: the disk page still holds the v1 content ('A'), but the version header
    // (bytes 0-3 of the page, the first sector a torn flush can persist alone) claims v2.
    try (final RandomAccessFile raf = new RandomAccessFile(bucketFilePath, "rw")) {
      raf.seek(0);
      final int diskVersion = raf.readInt();
      assertThat(diskVersion).as("the update's page must not have reached the disk").isEqualTo(1);
      raf.seek(0);
      raf.writeInt(2);
    }

    // 4. Reopen: the lock file triggers recovery, the WAL holds the v2 delta, the disk page SAYS v2 but
    // still contains the v1 bytes. Replay must repair it - the old <= skip left 'A' behind forever.
    try (final Database reopened = factory.open()) {
      assertThat(reopened.query("sql", "SELECT v FROM Doc").next().<String>getProperty("v"))
          .as("recovery must re-apply the equal-version WAL delta and repair the torn page (#4926)")
          .isEqualTo("B");
    } finally {
      factory.close();
    }
  }

  @Test
  void cleanCloseAfterFailedFsyncPreservesWal() throws Exception {
    // #4934: if the pre-close data fsync fails, the OS may have dropped the dirty pages (fsyncgate
    // semantics), yet close() used to delete every WAL file anyway - making the committed data
    // unrecoverable after a power loss, with only a SEVERE log line as evidence. A failed fsync must make
    // the close crash-equivalent: WAL and lock file preserved for recovery on the next open.
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    db.getSchema().createDocumentType("Doc");
    db.transaction(() -> db.newDocument("Doc").set("v", 1).save());

    // Break one file so its fsync fails: channel closed underneath + OS file deleted, so the #4930 reopen
    // guard surfaces the failure from force() instead of silently re-creating the dropped file.
    final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("Doc").getBuckets(false).getFirst();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId());
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
    final Field channelField = PaginatedComponentFile.class.getDeclaredField("channel");
    channelField.setAccessible(true);
    ((FileChannel) channelField.get(file)).close();
    assertThat(new File(file.getFilePath()).delete()).isTrue();

    db.close();

    final File dbDir = new File(DB_PATH);
    assertThat(dbDir.listFiles((d, n) -> n.endsWith(".wal")))
        .as("the WAL must survive a close whose data fsync failed (#4934)").isNotEmpty();
    assertThat(new File(dbDir, "database.lck"))
        .as("the lock file must be preserved so the next open runs recovery (#4934)").exists();
  }
}
