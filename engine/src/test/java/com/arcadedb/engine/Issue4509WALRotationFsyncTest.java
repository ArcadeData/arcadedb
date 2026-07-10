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
import com.arcadedb.database.DatabaseInternal;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Timer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Regression test for issue #4509: runtime WAL rotation drops the previous WAL file once its
 * {@code pendingPagesToFlush} reaches 0, but {@code WALFile.notifyPageFlushed()} is decremented after a
 * {@code write()} that only reaches the OS page cache (no fsync). A power loss between the WAL drop and the OS
 * write-back would lose committed transactions.
 *
 * <p>The fix fsyncs the data files ({@link FileManager#syncFiles()}) before the rotated WAL file is physically
 * dropped. These tests verify that the rotation cleanup path performs the fsync exactly when a WAL file is about
 * to be dropped, and not when there is nothing to drop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4509WALRotationFsyncTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("RotV");
  }

  @Test
  void rotationFsyncsDataFilesBeforeDroppingWAL() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    // Stop the background WAL maintenance timer so the cleanup path is driven deterministically by the test.
    cancelTransactionManagerTimer(db);

    // Replace the FileManager with a spy that still performs the real fsync but records the invocation.
    final FileManager spyFileManager = spy(db.getFileManager());
    swapFileManager(db, spyFileManager);

    // Produce a transaction, then make sure its pages have been written by the flush thread so that the WAL
    // that recorded them reports pendingPagesToFlush == 0 (the condition that makes it eligible to be dropped).
    db.transaction(() -> db.newDocument("RotV").set("id", 1).set("name", "rotation").save());
    PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(db);

    // Simulate a rotation: move the active WAL files (which recorded the transaction) into the inactive pool.
    final TransactionManager txManager = db.getTransactionManager();
    final int moved = moveActiveWALFilesToInactive(txManager);
    assertThat(moved).as("at least one active WAL file must hold the committed transaction").isGreaterThan(0);

    // Drive the runtime rotation cleanup (dropFiles=true, syncDataOnDrop=true).
    invokeCleanWALFiles(txManager, true, false, true);

    // The data files must have been fsync'd before the WAL files were dropped...
    verify(spyFileManager, atLeastOnce()).syncFiles();
    // ...and the inactive WAL files must now be gone.
    assertThat(getInactiveWALFilePool(txManager)).as("rotated WAL files must be dropped").isEmpty();
  }

  @Test
  void cleanupWithoutDropDoesNotFsync() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    cancelTransactionManagerTimer(db);

    final FileManager spyFileManager = spy(db.getFileManager());
    swapFileManager(db, spyFileManager);

    final TransactionManager txManager = db.getTransactionManager();
    // Nothing in the inactive pool: the per-second cleanup must not fsync the data files when there is nothing
    // to drop, otherwise it would pay an fsync on every timer tick.
    assertThat(getInactiveWALFilePool(txManager)).isEmpty();

    invokeCleanWALFiles(txManager, true, false, true);

    verify(spyFileManager, never()).syncFiles();
  }

  // --- reflection helpers ---

  private static void cancelTransactionManagerTimer(final DatabaseInternal db) throws Exception {
    final TransactionManager txManager = db.getTransactionManager();
    final Field taskField = TransactionManager.class.getDeclaredField("task");
    taskField.setAccessible(true);
    final Timer task = (Timer) taskField.get(txManager);
    if (task != null)
      task.cancel();
  }

  private static void swapFileManager(final DatabaseInternal db, final FileManager fileManager) throws Exception {
    final Field f = db.getClass().getDeclaredField("fileManager");
    f.setAccessible(true);
    f.set(db, fileManager);
    assertThat(db.getFileManager()).isSameAs(fileManager);
  }

  @SuppressWarnings("unchecked")
  private static List<WALFile> getInactiveWALFilePool(final TransactionManager txManager) throws Exception {
    final Field f = TransactionManager.class.getDeclaredField("inactiveWALFilePool");
    f.setAccessible(true);
    return (List<WALFile>) f.get(txManager);
  }

  private static int moveActiveWALFilesToInactive(final TransactionManager txManager) throws Exception {
    final Field activeField = TransactionManager.class.getDeclaredField("activeWALFilePool");
    activeField.setAccessible(true);
    final WALFile[] active = (WALFile[]) activeField.get(txManager);
    final List<WALFile> inactive = getInactiveWALFilePool(txManager);

    int moved = 0;
    for (int i = 0; i < active.length; i++) {
      final WALFile file = active[i];
      if (file != null) {
        active[i] = null;
        file.setActive(false);
        inactive.add(file);
        moved++;
      }
    }
    return moved;
  }

  private static void invokeCleanWALFiles(final TransactionManager txManager, final boolean dropFiles, final boolean force,
      final boolean syncDataOnDrop) throws Exception {
    final Method m = TransactionManager.class.getDeclaredMethod("cleanWALFiles", boolean.class, boolean.class, boolean.class);
    m.setAccessible(true);
    m.invoke(txManager, dropFiles, force, syncDataOnDrop);
  }
}
