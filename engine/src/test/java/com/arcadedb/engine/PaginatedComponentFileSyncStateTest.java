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

import com.arcadedb.database.BasicDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a data file owes the disk (issue #8626). A file is forced only when something happened to it since its last
 * successful fsync: a page write needs the data synced, a creation or a rename also needs the metadata. A file that
 * was only read owes nothing, which is what makes a clean close of a read-only session cost no fsync at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PaginatedComponentFileSyncStateTest {
  private static final int PAGE_SIZE = 1024;
  private static final int FILE_ID   = 1;

  @TempDir
  Path tempDir;

  private final BasicDatabase                db    = Mockito.mock(BasicDatabase.class);
  private final List<PaginatedComponentFile> files = new ArrayList<>();

  @AfterEach
  void tearDown() {
    for (final PaginatedComponentFile f : files)
      f.close();
  }

  @Test
  void createdFileOwesAMetadataSyncOnce() throws IOException {
    final PaginatedComponentFile file = open(filePath("created"));

    assertThat(file.isModifiedSinceLastSync()).isTrue();
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);

    assertThat(file.isModifiedSinceLastSync()).isFalse();
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_CLEAN);
  }

  @Test
  void existingFileOpensClean() throws IOException {
    final String path = filePath("existing");
    final PaginatedComponentFile creator = open(path);
    writePage(creator, 0);
    creator.force(true);
    creator.close();

    final PaginatedComponentFile reopened = open(path);
    assertThat(reopened.isModifiedSinceLastSync()).isFalse();

    final MutablePage page = new MutablePage(new PageId(db, FILE_ID, 0), PAGE_SIZE);
    reopened.read(new CachedPage(page, false));
    assertThat(reopened.isModifiedSinceLastSync()).as("a read owes the disk nothing").isFalse();
    assertThat(reopened.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_CLEAN);
  }

  @Test
  void pageWriteOwesADataSync() throws IOException {
    final PaginatedComponentFile file = open(filePath("written"));
    file.forceIfModified();

    writePage(file, 0);
    assertThat(file.isModifiedSinceLastSync()).isTrue();
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_DATA);
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_CLEAN);
  }

  @Test
  void pageWriteDoesNotDowngradeAPendingMetadataSync() throws IOException {
    final PaginatedComponentFile file = open(filePath("createdThenWritten"));

    writePage(file, 0);
    writePage(file, 1);
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
  }

  @Test
  void unconditionalForceSettlesThePendingState() throws IOException {
    final PaginatedComponentFile file = open(filePath("forced"));
    writePage(file, 0);

    file.force(false);
    assertThat(file.isModifiedSinceLastSync()).isFalse();
  }

  @Test
  void renameOwesAMetadataSync() throws IOException {
    final PaginatedComponentFile file = open(filePath("renamed"));
    writePage(file, 0);
    file.forceIfModified();

    file.renameComponent("renamedAgain");
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
  }

  @Test
  void markUnsyncedForcesTheMetadataOfAnExistingFile() throws IOException {
    final String path = filePath("marked");
    open(path).close();

    final PaginatedComponentFile reopened = open(path);
    reopened.markUnsynced();
    assertThat(reopened.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
  }

  @Test
  void failedSyncKeepsTheFileOwingTheSync() throws Exception {
    final PaginatedComponentFile file = open(filePath("failing"));
    file.forceIfModified();
    writePage(file, 0);

    // Close the channel underneath and delete the OS file: the #4930 reopen guard then surfaces the failure from the
    // force instead of re-creating the file, which is how the #4934 test breaks an fsync too.
    final Field channelField = PaginatedComponentFile.class.getDeclaredField("channel");
    channelField.setAccessible(true);
    ((FileChannel) channelField.get(file)).close();
    assertThat(new File(file.getFilePath()).delete()).isTrue();

    assertThatThrownBy(file::forceIfModified).isInstanceOf(IOException.class);
    assertThat(file.isModifiedSinceLastSync())
        .as("a failed fsync must leave the file owing it, or the next sync pass would skip the unsynced pages").isTrue();
  }

  private PaginatedComponentFile open(final String path) throws IOException {
    final PaginatedComponentFile file = new PaginatedComponentFile(path, ComponentFile.MODE.READ_WRITE);
    files.add(file);
    return file;
  }

  private String filePath(final String name) {
    return tempDir.resolve(name + "." + FILE_ID + "." + PAGE_SIZE + ".v0.arc").toString();
  }

  private void writePage(final PaginatedComponentFile file, final int pageNumber) throws IOException {
    file.write(new MutablePage(new PageId(db, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 1, PAGE_SIZE));
  }
}
