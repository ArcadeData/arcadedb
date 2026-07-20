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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A crash that leaves the dictionary page unflushed must not cost the WAL.
 * <p>
 * {@code openInternal} loads the schema before the WAL is replayed, so the dictionary is built from
 * whatever raw bytes are on disk. When the page never reached disk the file is zero length, and the
 * load used to answer by committing a freshly created header page at version 0. Recovery then replayed
 * a WAL whose dictionary entries sit at a much higher version, the version-gap guard fired, and the
 * whole replay was aborted: every transaction still in the WAL was lost and the files were moved aside
 * as {@code .corrupt}.
 * <p>
 * The zero-length file is produced directly rather than by hoping a kill lands in that window, which is
 * a timing accident - the reason the symptom shows up on CI and not locally.
 */
class UnflushedDictionaryRecoveryTest {
  private static final String DATABASE_NAME = "UnflushedDictionaryRecoveryTest";
  private static final String DATABASE_PATH = "./target/databases/" + DATABASE_NAME;

  @BeforeEach
  @AfterEach
  void cleanDatabaseDirectory() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void recoveryReplaysTheWalWhenTheDictionaryPageWasNeverFlushed() throws IOException {
    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      // A clean create+close flushes the dictionary page and retires the WAL, so the entries written
      // below are the only dictionary versions the recovery WAL will carry.
      factory.create().close();

      final Database database = factory.open();
      database.getSchema().createDocumentType("UnflushedDictType");
      database.transaction(() -> database.newDocument("UnflushedDictType").set("survivor", 42).save());

      // Crash: the lock marker stays on disk so the next open recovers, and the WAL keeps the changes.
      ((DatabaseInternal) database).kill();
      database.close();

      assertThat(walFiles()).as("the killed database must leave WAL files to replay").isNotEmpty();

      truncateDictionaryFile();

      final Database reopened = factory.open();
      try {
        assertThat(reopened.isOpen()).isTrue();

        // The WAL held both the type and the record. Aborting the replay silently drops them.
        assertThat(reopened.getSchema().existsType("UnflushedDictType"))
            .as("the type committed before the crash must survive recovery").isTrue();

        reopened.transaction(() -> {
          final long count = reopened.countType("UnflushedDictType", false);
          assertThat(count).as("the record committed before the crash must survive recovery").isEqualTo(1);

          final Document doc = (Document) reopened.iterateType("UnflushedDictType", false).next();
          // Property names live in the dictionary: a stale in-RAM dictionary reads them back as null or garbage.
          assertThat(doc.getInteger("survivor")).isEqualTo(42);
        });
      } finally {
        reopened.close();
      }

      assertThat(corruptWalFiles()).as("a recoverable WAL must not be quarantined as corrupt").isEmpty();
    }
  }

  /**
   * Empties the dictionary file, reproducing a page that was written but never flushed.
   */
  private void truncateDictionaryFile() throws IOException {
    final File[] dictionaries = new File(DATABASE_PATH).listFiles((dir, name) -> name.endsWith(".dict"));
    assertThat(dictionaries).as("the database must have a dictionary file").isNotNull().isNotEmpty();

    for (final File dictionary : dictionaries)
      try (final RandomAccessFile raf = new RandomAccessFile(dictionary, "rw")) {
        raf.setLength(0);
      }
  }

  private File[] walFiles() {
    final File[] files = new File(DATABASE_PATH).listFiles((dir, name) -> name.endsWith(".wal"));
    return files == null ? new File[0] : files;
  }

  private File[] corruptWalFiles() {
    final File[] files = new File(DATABASE_PATH).listFiles((dir, name) -> name.endsWith(".wal.corrupt"));
    return files == null ? new File[0] : files;
  }
}
