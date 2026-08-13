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

  @Test
  void findDifferingFiles() {
    final Map<String, Long> leader = Map.of("file1", 100L, "file2", 200L, "file3", 300L);
    final Map<String, Long> replica = Map.of("file1", 100L, "file2", 999L);

    final var differing = SnapshotManager.findDifferingFiles(leader, replica);

    assertThat(differing).containsExactlyInAnyOrder("file2", "file3");
  }

  @Test
  void noDifferencesWhenIdentical() {
    final Map<String, Long> checksums = Map.of("a", 1L, "b", 2L);

    final var differing = SnapshotManager.findDifferingFiles(checksums, checksums);

    assertThat(differing).isEmpty();
  }
}
