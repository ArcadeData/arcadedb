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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The list of files a {@link FileManager} hands out holds files and empty slots, nothing else. A slot that was reserved for a
 * component but never got its file (the component failed to open it) is an empty slot, like the one a dropped file leaves,
 * and every walk over the list skips an empty slot.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FileManagerFileListTest extends TestHelper {

  @Test
  void aReservedSlotIsListedAsAnEmptySlot() {
    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    final int before = fileManager.getFiles().size();

    final int fileId = fileManager.newFileId();

    final List<ComponentFile> files = fileManager.getFiles();
    assertThat(files).hasSize(before + 1);
    assertThat(fileId).isEqualTo(before);
    assertThat(files.get(fileId)).as("the slot reserved for a file that is not open yet").isNull();
    assertNoUnnamedEntry(fileManager);
  }

  @Test
  void aBucketCreationRefusedForAnExistingFileLeavesNoUnnamedEntry() throws IOException {
    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    final int nextFileId = fileManager.getFiles().size();
    final String bucketName = "occupied_path_bucket";
    final File occupied = new File(getDatabasePath(),
        bucketName + "." + nextFileId + "." + GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger() + ".v"
            + LocalBucket.CURRENT_VERSION + "." + LocalBucket.BUCKET_EXT);
    assertThat(occupied.createNewFile()).as("the file that is already at the path of the next bucket file").isTrue();

    assertThatThrownBy(() -> database.getSchema().createBucket(bucketName)).isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().existsBucket(bucketName)).isFalse();
    assertNoUnnamedEntry(fileManager);
    assertThat(fileManager.getFiles().get(nextFileId)).as("the id of the refused creation").isNull();

    // THE FILE OF ANOTHER OWNER IS LEFT AS IT WAS, AND THE NEXT CREATION GETS AN ID OF ITS OWN
    assertThat(occupied).exists();
    database.getSchema().createBucket("free_path_bucket");
    assertThat(database.getSchema().existsBucket("free_path_bucket")).isTrue();
    assertThat(fileManager.getFiles().get(nextFileId + 1)).isNotNull();
    assertNoUnnamedEntry(fileManager);
  }

  @Test
  void aBucketCreationThatCannotOpenItsFileLeavesNoUnnamedEntry() {
    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    final int nextFileId = fileManager.getFiles().size();
    final String bucketName = "b".repeat(400);

    assertThatThrownBy(() -> database.getSchema().createBucket(bucketName)).isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().existsBucket(bucketName)).isFalse();
    assertNoUnnamedEntry(fileManager);
    assertThat(fileManager.getFiles().get(nextFileId)).as("the id of the failed creation").isNull();

    database.getSchema().createBucket("opened_bucket");
    assertThat(database.getSchema().existsBucket("opened_bucket")).isTrue();
    assertNoUnnamedEntry(fileManager);
  }

  @Test
  void aDatabaseReopenedAfterAFailedBucketCreationKeepsItsData() {
    database.transaction(() -> database.getSchema().createDocumentType("Item"));
    database.transaction(() -> database.newDocument("Item").set("n", 1).save());

    assertThatThrownBy(() -> database.getSchema().createBucket("b".repeat(400))).isInstanceOf(SchemaException.class);

    database.transaction(() -> database.newDocument("Item").set("n", 2).save());

    reopenDatabase();

    assertThat(database.countType("Item", true)).isEqualTo(2);
    assertNoUnnamedEntry(((DatabaseInternal) database).getFileManager());
  }

  private static void assertNoUnnamedEntry(final FileManager fileManager) {
    assertThat(fileManager.getFiles()).as("every entry of the file list is a file with a name, or an empty slot")
        .noneMatch(file -> file != null && file.getFileName() == null);
  }
}
