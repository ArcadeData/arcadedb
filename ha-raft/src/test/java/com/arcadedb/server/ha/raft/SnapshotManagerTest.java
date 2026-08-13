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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotManagerTest {

  @Test
  void computeFileChecksumsForDirectory(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("file1.dat"), "hello");
    Files.writeString(tempDir.resolve("file2.dat"), "world");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).hasSize(2);
    assertThat(checksums).containsKey("file1.dat");
    assertThat(checksums).containsKey("file2.dat");
  }

  @Test
  void identicalFilesHaveSameChecksum(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("a.dat"), "same content");
    Files.writeString(tempDir.resolve("b.dat"), "same content");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums.get("a.dat")).isEqualTo(checksums.get("b.dat"));
  }

  @Test
  void differentFilesHaveDifferentChecksums(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("a.dat"), "content A");
    Files.writeString(tempDir.resolve("b.dat"), "content B");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums.get("a.dat")).isNotEqualTo(checksums.get("b.dat"));
  }

  /**
   * #6116: the transient files a checksum comparison must not see. The {@code .pshadow} entry is the newest of them
   * and the easiest to miss - it is the copy-on-write scratch of an open snapshot window (#6075), it lives in the
   * database directory, and its content is whatever pages happened to be dirtied, so a node that has one and a node
   * that does not would be reported as inconsistent for no reason at all.
   */
  @Test
  void transientFilesAreNotChecksummed(@TempDir final Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("database.json"), "{}");
    Files.writeString(tempDir.resolve("txlog_0.wal"), "wal");
    Files.writeString(tempDir.resolve("schema.prev.json"), "{}");
    Files.writeString(tempDir.resolve("database.lock"), "");
    Files.writeString(tempDir.resolve("txlog_1.corrupt"), "corrupt");
    Files.writeString(tempDir.resolve("snapshot-1.pshadow"), "shadow");

    final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(tempDir.toFile());

    assertThat(checksums).containsOnlyKeys("database.json");
  }

  /**
   * #6125: {@code findDifferingFiles} was removed rather than wired into the resync path, and these two tests -
   * its only callers - went with it. What replaces them is the guarantee that nothing has quietly grown a
   * file-level diff back: resync ships the whole database, and an incremental one belongs at the page level
   * (#6115), where the manifest and the page image come from the same window.
   */
  @Test
  void noFileLevelDiffHelperIsExposed() {
    assertThat(SnapshotManager.class.getDeclaredMethods())
        .as("a whole-file diff cannot be the basis of resync: see the class comment")
        .noneMatch(method -> method.getName().toLowerCase(Locale.ROOT).contains("differing"));
  }
}
