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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12234: the node that first writes or reads a fixed-width float array relabels its dictionary file from v1 to v2, the
 * other nodes later. The bytes are the same, so the checksums nodes compare must not see two different files.
 */
class DictionaryLabelChecksumsTest {

  @Test
  void aDictionaryComparesTheSameUnderEitherLabel(@TempDir final File leader, @TempDir final File follower) throws Exception {
    final byte[] dictionary = { 1, 2, 3, 4, 5 };
    final byte[] bucket = { 9, 8, 7 };
    Files.write(new File(leader, "dictionary.0.65536.v2.dict").toPath(), dictionary);
    Files.write(new File(follower, "dictionary.0.65536.v1.dict").toPath(), dictionary);
    Files.write(new File(leader, "Doc_0.1.65536.v0.bucket").toPath(), bucket);
    Files.write(new File(follower, "Doc_0.1.65536.v0.bucket").toPath(), bucket);

    final Map<String, Long> onLeader = SnapshotManager.computeFileChecksums(leader);
    final Map<String, Long> onFollower = SnapshotManager.computeFileChecksums(follower);

    assertThat(onLeader).isEqualTo(onFollower);
    // UNDER THE LABEL A PEER OF A BUILD WITHOUT THE RELABEL REPORTS, SO A MIXED-VERSION CLUSTER COMPARES EQUAL TOO
    assertThat(onLeader).containsOnlyKeys("dictionary.0.65536.v1.dict", "Doc_0.1.65536.v0.bucket");
  }

  @Test
  void aDictionaryWithOtherBytesStillDiffers(@TempDir final File leader, @TempDir final File follower) throws Exception {
    Files.write(new File(leader, "dictionary.0.65536.v2.dict").toPath(), new byte[] { 1, 2, 3 });
    Files.write(new File(follower, "dictionary.0.65536.v1.dict").toPath(), new byte[] { 1, 2, 4 });

    assertThat(SnapshotManager.computeFileChecksums(leader)).isNotEqualTo(SnapshotManager.computeFileChecksums(follower));
  }

  /**
   * The relabel can land while a point-in-time window is open, when the setting is turned on: the window knows the dictionary
   * under its old name, the directory lists it under the new one. Matched by file name, the dictionary was taken for a page
   * file created after t0 and left out of the checksums, which a peer then reports as a missing file.
   */
  @Test
  void aDictionaryRelabelledWhileASnapshotIsOpenIsStillChecksummed(@TempDir final File root) throws Exception {
    final File dbDir = new File(root, "relabel-under-snapshot");
    final DatabaseFactory factory = new DatabaseFactory(dbDir.getPath());
    // A DATABASE KEPT OPENABLE BY AN OLDER BUILD: CREATED WITH THE SETTING OFF, SO ITS DICTIONARY IS LABELLED 1
    factory.getContextConfiguration().setValue(GlobalConfiguration.SERIALIZER_FIXED_WIDTH_FLOAT_ARRAYS, false);
    try (final Database database = factory.create()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      database.transaction(() -> database.getSchema().createDocumentType("Doc").createProperty("emb", Type.ARRAY_OF_FLOATS));
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
      assertThat(db.getSchema().getDictionary().isFixedWidthFloatArraysFenced()).isFalse();

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final Map<String, Long> t0 = SnapshotManager.computeFileChecksums(dbDir, snapshot);

        // TURNING THE SETTING ON RELABELS THE DICTIONARY AFTER t0
        database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` true");
        assertThat(db.getSchema().getDictionary().isFixedWidthFloatArraysFenced()).isTrue();

        final Map<String, Long> afterRelabel = SnapshotManager.computeFileChecksums(dbDir, snapshot);
        assertThat(t0.keySet()).anyMatch(name -> name.endsWith("." + Dictionary.DICT_EXT));
        // configuration.json IS NOT A PAGE FILE: THE ALTER REWROTE IT, AND THE WINDOW DOES NOT ANSWER FOR IT
        t0.remove(LocalDatabase.CONFIGURATION_FILE_NAME);
        afterRelabel.remove(LocalDatabase.CONFIGURATION_FILE_NAME);
        assertThat(afterRelabel).as("the window answers for the relabelled dictionary as it did at t0").isEqualTo(t0);
      }
    } finally {
      FileUtils.deleteRecursively(dbDir);
    }
  }
}
