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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the symlink protections in snapshot extraction.
 * These tests verify the security invariants used in {@link SnapshotInstaller#downloadSnapshot}
 * to prevent symlink-based directory escape during ZIP extraction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotSymlinkProtectionTest {

  @TempDir
  Path tempDir;

  @Test
  @DisabledOnOs(OS.WINDOWS)
  void symlinkAtFileTargetIsDetected() throws Exception {
    // Simulate a symlink at the file path that points outside tempDir
    final Path outsideDir = Files.createTempDirectory("outside");
    try {
      final Path symlinkFile = tempDir.resolve("malicious-link");
      Files.createSymbolicLink(symlinkFile, outsideDir.resolve("escaped-file"));

      // This is the check used in SnapshotInstaller before writing
      assertThat(Files.isSymbolicLink(symlinkFile)).isTrue();
    } finally {
      Files.deleteIfExists(outsideDir.resolve("escaped-file"));
      Files.deleteIfExists(outsideDir);
    }
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  void symlinkInParentDirectoryDetectedByRealPath() throws Exception {
    // Simulate a symlink in a parent directory that escapes tempDir
    final Path outsideDir = Files.createTempDirectory("outside-parent");
    try {
      Files.createDirectories(outsideDir.resolve("data"));

      // Create a symlink inside tempDir that points to the outside directory
      final Path symlinkDir = tempDir.resolve("subdir");
      Files.createSymbolicLink(symlinkDir, outsideDir);

      // The target file path looks like it's inside tempDir via normalize()
      final Path targetFile = tempDir.resolve("subdir").resolve("data").resolve("file.dat").normalize();
      assertThat(targetFile.startsWith(tempDir)).isTrue(); // normalize check passes

      // But toRealPath on the parent resolves through the symlink
      final Path realParent = targetFile.getParent().toRealPath();
      final Path realTempDir = tempDir.toRealPath();
      assertThat(realParent.startsWith(realTempDir)).isFalse(); // real path check catches it
    } finally {
      com.arcadedb.utility.FileUtils.deleteRecursively(outsideDir.toFile());
    }
  }

  @Test
  @DisabledOnOs(OS.WINDOWS)
  void addFileToZipFailsSnapshotWhenSourceIsSymlink() throws Exception {
    // Regression test: the leader must NOT silently skip a symlinked database file. Doing so
    // would hand the follower a ZIP that looks complete but is missing data, producing silent
    // corruption after the atomic directory swap. The only safe behavior is to fail the snapshot.
    final Path realFile = tempDir.resolve("real.data");
    Files.writeString(realFile, "payload");

    final Path symlinkFile = tempDir.resolve("aliased.data");
    Files.createSymbolicLink(symlinkFile, realFile);

    try (final ZipOutputStream zipOut = new ZipOutputStream(new ByteArrayOutputStream())) {
      assertThatThrownBy(() -> SnapshotHttpHandler.addFileToZip(zipOut, symlinkFile.toFile()))
          .isInstanceOf(ReplicationException.class)
          .hasMessageContaining("symlink");
    }
  }

  @Test
  void normalPathPassesBothChecks() throws Exception {
    // A normal (non-symlink) path should pass all checks
    final Path subDir = tempDir.resolve("subdir");
    Files.createDirectories(subDir);

    final Path targetFile = tempDir.resolve("subdir").resolve("file.dat").normalize();
    assertThat(targetFile.startsWith(tempDir)).isTrue();
    assertThat(Files.isSymbolicLink(targetFile)).isFalse();

    final Path realParent = targetFile.getParent().toRealPath();
    final Path realTempDir = tempDir.toRealPath();
    assertThat(realParent.startsWith(realTempDir)).isTrue();
  }
}
