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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Opening a database that needs recovery must take the exclusive lock, and settle the read-only
 * rejection, before the schema is loaded.
 * <p>
 * {@code openInternal} used to run {@code schema.load()} first and only then {@code checkForRecovery()},
 * which is where {@code database.lck} is locked. Everything the load does to a crashed database - the
 * sorted-index-build marker cleanup that drops component files, and the dictionary header page that is
 * written and committed when the page never reached disk - therefore happened while the database was
 * still unlocked, so a second process holding the lock could not keep it out.
 */
class RecoveryLockOrderingTest {
  private static final String DATABASE_NAME = "RecoveryLockOrderingTest";
  private static final String DATABASE_PATH = "./target/databases/" + DATABASE_NAME;

  @BeforeEach
  @AfterEach
  void cleanDatabaseDirectory() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void openOfALockedCrashedDatabaseWritesNothingBeforeItIsRejected() throws IOException {
    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      crashDatabaseWithUnflushedDictionary(factory);

      final long walBytesBefore = totalWalBytes();

      // Stand in for the process that owns the database: hold the exclusive lock on the marker file.
      try (final RandomAccessFile lockOwner = new RandomAccessFile(new File(DATABASE_PATH, "database.lck"), "rw");
          final FileChannel channel = lockOwner.getChannel();
          final FileLock lock = channel.lock()) {
        assertThat(lock.isValid()).isTrue();

        assertThatThrownBy(factory::open).isInstanceOf(RuntimeException.class);
      }

      assertThat(dictionaryLength())
          .as("a rejected open must not write the dictionary header page of a database it does not own").isZero();
      assertThat(totalWalBytes())
          .as("a rejected open must not append transactions to the WAL of a database it does not own")
          .isEqualTo(walBytesBefore);
    }
  }

  @Test
  void readOnlyOpenOfACrashedDatabaseIsRejectedBeforeTheSchemaIsLoaded() throws IOException {
    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      crashDatabaseWithUnflushedDictionary(factory);

      final long walBytesBefore = totalWalBytes();

      assertThatThrownBy(() -> factory.open(ComponentFile.MODE.READ_ONLY))
          .isInstanceOf(DatabaseMetadataException.class)
          .hasMessageContaining("read only");

      assertThat(dictionaryLength()).as("a rejected read-only open must leave the dictionary file untouched").isZero();
      assertThat(totalWalBytes()).as("a rejected read-only open must leave the WAL untouched").isEqualTo(walBytesBefore);
    }
  }

  /**
   * Leaves a database that needs recovery and whose dictionary page never reached disk.
   */
  private void crashDatabaseWithUnflushedDictionary(final DatabaseFactory factory) throws IOException {
    final Database database = factory.create();
    database.getSchema().createDocumentType("LockOrderingType");
    database.transaction(() -> database.newDocument("LockOrderingType").set("k", 1).save());

    ((DatabaseInternal) database).kill();
    database.close();

    assertThat(new File(DATABASE_PATH, "database.lck")).as("a killed database must leave the recovery marker").exists();

    final File[] dictionaries = new File(DATABASE_PATH).listFiles((dir, name) -> name.endsWith(".dict"));
    assertThat(dictionaries).as("the database must have a dictionary file").isNotNull().isNotEmpty();
    for (final File dictionary : dictionaries)
      try (final RandomAccessFile raf = new RandomAccessFile(dictionary, "rw")) {
        raf.setLength(0);
      }
  }

  private long dictionaryLength() {
    final File[] dictionaries = new File(DATABASE_PATH).listFiles((dir, name) -> name.endsWith(".dict"));
    long total = 0;
    if (dictionaries != null)
      for (final File dictionary : dictionaries)
        total += dictionary.length();
    return total;
  }

  private long totalWalBytes() {
    final File[] files = new File(DATABASE_PATH).listFiles((dir, name) -> name.endsWith(".wal"));
    long total = 0;
    if (files != null)
      for (final File file : files)
        total += file.length();
    return total;
  }
}
