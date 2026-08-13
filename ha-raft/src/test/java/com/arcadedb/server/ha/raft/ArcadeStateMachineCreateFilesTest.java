/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests idempotency of createNewFiles() in ArcadeStateMachine.
 */
class ArcadeStateMachineCreateFilesTest {

  private static final String           DB_PATH = "./target/databases/test-create-files-idempotency";
  private              DatabaseInternal db;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    db = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void createNewFilesSkipsExistingFileOnDisk() throws Exception {
    // Simulate a file that already exists on disk with data (e.g., from a prior commit
    // before a cold restart). Use a high fileId that won't collide with existing files.
    final int fileId = 9000;
    final String fileName = "test_bucket.9000.65536.v0.pcf";
    final File osFile = new File(db.getDatabasePath() + File.separator + fileName);

    // Write some data to the file to simulate a partially-written state
    try (final RandomAccessFile raf = new RandomAccessFile(osFile, "rw")) {
      raf.write(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    }
    assertThat(osFile.exists()).isTrue();
    assertThat(osFile.length()).isEqualTo(8);

    // Invoke createNewFiles via reflection (it's private)
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    final Method createNewFiles = ArcadeStateMachine.class.getDeclaredMethod(
        "createNewFiles", DatabaseInternal.class, Map.class);
    createNewFiles.setAccessible(true);

    final Map<Integer, String> filesToAdd = new HashMap<>();
    filesToAdd.put(fileId, fileName);

    // First call: file exists on disk with non-zero size, should be skipped
    createNewFiles.invoke(stateMachine, db, filesToAdd);

    // Verify the file was NOT overwritten - content is preserved
    assertThat(osFile.length()).isEqualTo(8);
    try (final RandomAccessFile raf = new RandomAccessFile(osFile, "r")) {
      final byte[] content = new byte[8];
      raf.readFully(content);
      assertThat(content).isEqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    }

    // File should NOT be registered in FileManager (we skipped creation)
    assertThat(db.getFileManager().existsFile(fileId)).isFalse();
  }

  @Test
  void createNewFilesSkipsAlreadyRegisteredFile() throws Exception {
    // Register a file via normal creation first
    final int fileId = 9001;
    final String fileName = "test_bucket2.9001.65536.v0.pcf";
    final String filePath = db.getDatabasePath() + File.separator + fileName;

    db.getFileManager().getOrCreateFile(fileId, filePath);
    assertThat(db.getFileManager().existsFile(fileId)).isTrue();

    final long sizeAfterCreate = new File(filePath).length();

    // Invoke createNewFiles with the same fileId - should skip via in-memory guard
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    final Method createNewFiles = ArcadeStateMachine.class.getDeclaredMethod(
        "createNewFiles", DatabaseInternal.class, Map.class);
    createNewFiles.setAccessible(true);

    final Map<Integer, String> filesToAdd = new HashMap<>();
    filesToAdd.put(fileId, fileName);

    createNewFiles.invoke(stateMachine, db, filesToAdd);

    // File size should be unchanged
    assertThat(new File(filePath).length()).isEqualTo(sizeAfterCreate);
  }

  @Test
  void createNewFilesCreatesNewFileWhenNotPresent() throws Exception {
    final int fileId = 9002;
    final String fileName = "test_bucket3.9002.65536.v0.pcf";
    final String filePath = db.getDatabasePath() + File.separator + fileName;

    assertThat(new File(filePath).exists()).isFalse();
    assertThat(db.getFileManager().existsFile(fileId)).isFalse();

    // Invoke createNewFiles - should create the file normally
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    final Method createNewFiles = ArcadeStateMachine.class.getDeclaredMethod(
        "createNewFiles", DatabaseInternal.class, Map.class);
    createNewFiles.setAccessible(true);

    final Map<Integer, String> filesToAdd = new HashMap<>();
    filesToAdd.put(fileId, fileName);

    createNewFiles.invoke(stateMachine, db, filesToAdd);

    // File should now exist and be registered
    assertThat(new File(filePath).exists()).isTrue();
    assertThat(db.getFileManager().existsFile(fileId)).isTrue();
  }

  /**
   * Issue #6063 regression: a file id already registered under a name DIFFERENT from what a committed
   * schema entry expects is not the safe idempotent-replay case the other tests above cover - the two
   * names cannot both be legitimate replays of the same entry, so it means this node's file-id space has
   * diverged from the leader's (e.g. left behind by the first-formation bootstrap "local data is fresher,
   * keep it" guard in {@code applyBootstrapFingerprintEntry}). Silently skipping the collision, as the
   * pre-fix code did, left the new component (e.g. a freshly created type's bucket) never actually
   * created on this node while the schema JSON was still merged, so the type was later dropped from the
   * schema on load ("Cannot find bucket ..., removing it from type configuration") - the exact "Types:
   * DB1 6 <> DB2 5" divergence the issue's CI log shows. It must now be reported loudly instead.
   */
  @Test
  void createNewFilesThrowsOnFileIdCollisionWithDifferentName() throws Exception {
    final int fileId = 9003;
    final String existingFileName = "unrelated_bucket.9003.65536.v0.pcf";
    final String expectedFileName = "new_type_bucket.9003.65536.v0.pcf";
    final String existingFilePath = db.getDatabasePath() + File.separator + existingFileName;

    // Plant the divergence directly, rather than racing for it: this node already has an unrelated file
    // registered under fileId 9003 from its own independent history.
    db.getFileManager().getOrCreateFile(fileId, existingFilePath);
    assertThat(db.getFileManager().existsFile(fileId)).isTrue();

    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    final Method createNewFiles = ArcadeStateMachine.class.getDeclaredMethod(
        "createNewFiles", DatabaseInternal.class, Map.class);
    createNewFiles.setAccessible(true);

    // A committed schema change now expects the SAME fileId to name a DIFFERENT (brand new) file.
    final Map<Integer, String> filesToAdd = new HashMap<>();
    filesToAdd.put(fileId, expectedFileName);

    assertThatThrownBy(() -> {
      try {
        createNewFiles.invoke(stateMachine, db, filesToAdd);
      } catch (final InvocationTargetException e) {
        throw e.getCause();
      }
    }).isInstanceOf(SchemaException.class)
        .hasMessageContaining(String.valueOf(fileId))
        .hasMessageContaining(existingFileName)
        .hasMessageContaining(expectedFileName);

    // The collision must not silently fabricate the new file: the existing registration is untouched
    // and the new one was never created.
    assertThat(db.getFileManager().getFile(fileId).getFileName()).isEqualTo(existingFileName);
    assertThat(new File(db.getDatabasePath() + File.separator + expectedFileName)).doesNotExist();
  }
}
