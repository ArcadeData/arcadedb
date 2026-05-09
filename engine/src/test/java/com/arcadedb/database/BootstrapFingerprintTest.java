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
package com.arcadedb.database;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BootstrapFingerprint}, the helper that backs the HA bootstrap path
 * (issue #4147). The contract these tests pin down:
 * <ul>
 *   <li>Determinism: same files → same digest, regardless of insertion order.</li>
 *   <li>Sensitivity: a one-byte change in any included file flips the digest.</li>
 *   <li>WAL/lock files do NOT affect the digest (lastTxId is the recency signal, not the WAL bytes).</li>
 *   <li>Subdirectories are not traversed (avoids accidentally pulling in external-bucket dirs).</li>
 *   <li>Empty / missing directory yields the canonical empty-content digest.</li>
 * </ul>
 */
class BootstrapFingerprintTest {

  @Test
  void emptyDirectoryYieldsStableDigest(@TempDir final Path dir) {
    final String f1 = BootstrapFingerprint.compute(dir.toFile());
    final String f2 = BootstrapFingerprint.compute(dir.toFile());
    assertThat(f1).hasSize(64);
    assertThat(f1).isEqualTo(f2);
  }

  @Test
  void missingDirectoryYieldsSameDigestAsEmpty(@TempDir final Path tmp) {
    final File missing = new File(tmp.toFile(), "does-not-exist");
    assertThat(BootstrapFingerprint.compute(missing))
        .isEqualTo(BootstrapFingerprint.compute(tmp.toFile()));
  }

  @Test
  void identicalContentTwoDirectoriesSameDigest(@TempDir final Path dirA, @TempDir final Path dirB) throws Exception {
    Files.writeString(dirA.resolve("v_0.bucket"), "alpha", StandardCharsets.UTF_8);
    Files.writeString(dirA.resolve("schema.json"), "{ \"types\": [] }", StandardCharsets.UTF_8);

    Files.writeString(dirB.resolve("v_0.bucket"), "alpha", StandardCharsets.UTF_8);
    Files.writeString(dirB.resolve("schema.json"), "{ \"types\": [] }", StandardCharsets.UTF_8);

    assertThat(BootstrapFingerprint.compute(dirA.toFile()))
        .isEqualTo(BootstrapFingerprint.compute(dirB.toFile()));
  }

  @Test
  void contentChangeFlipsDigest(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "alpha", StandardCharsets.UTF_8);
    final String before = BootstrapFingerprint.compute(dir.toFile());

    Files.writeString(dir.resolve("v_0.bucket"), "beta", StandardCharsets.UTF_8);
    final String after = BootstrapFingerprint.compute(dir.toFile());

    assertThat(after).isNotEqualTo(before);
  }

  @Test
  void walFilesAreExcluded(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "data", StandardCharsets.UTF_8);
    final String before = BootstrapFingerprint.compute(dir.toFile());

    // WAL rotation is a normal in-flight detail and must not move the fingerprint - the recency
    // signal is the persisted lastTxId, not the WAL bytes.
    Files.writeString(dir.resolve("txlog_0.wal"), "transient", StandardCharsets.UTF_8);
    Files.writeString(dir.resolve("txlog_1.wal"), "transient2", StandardCharsets.UTF_8);
    final String after = BootstrapFingerprint.compute(dir.toFile());

    assertThat(after).isEqualTo(before);
  }

  @Test
  void subdirectoriesAreNotTraversed(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "data", StandardCharsets.UTF_8);
    final String before = BootstrapFingerprint.compute(dir.toFile());

    final Path subdir = dir.resolve("external");
    Files.createDirectory(subdir);
    Files.writeString(subdir.resolve("anything.bucket"), "external content", StandardCharsets.UTF_8);

    assertThat(BootstrapFingerprint.compute(dir.toFile())).isEqualTo(before);
  }

  @Test
  void filenameIsPartOfTheDigest(@TempDir final Path dirA, @TempDir final Path dirB) throws Exception {
    // Same content under different file names must produce different digests, otherwise an
    // accidental file rename would falsely look like the same database.
    Files.writeString(dirA.resolve("v_0.bucket"), "payload", StandardCharsets.UTF_8);
    Files.writeString(dirB.resolve("v_1.bucket"), "payload", StandardCharsets.UTF_8);

    assertThat(BootstrapFingerprint.compute(dirA.toFile()))
        .isNotEqualTo(BootstrapFingerprint.compute(dirB.toFile()));
  }

  @Test
  void unincludedExtensionsAreIgnored(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "data", StandardCharsets.UTF_8);
    final String before = BootstrapFingerprint.compute(dir.toFile());

    Files.writeString(dir.resolve("readme.txt"), "operator notes", StandardCharsets.UTF_8);
    Files.writeString(dir.resolve("metrics.lock"), "lockfile", StandardCharsets.UTF_8);
    assertThat(BootstrapFingerprint.compute(dir.toFile())).isEqualTo(before);
  }

  @Test
  void customSuffixListIsRespected(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "data", StandardCharsets.UTF_8);
    Files.writeString(dir.resolve("notes.md"), "operator notes", StandardCharsets.UTF_8);

    // With the default suffix list, .md is excluded.
    final String defaultDigest = BootstrapFingerprint.compute(dir.toFile());
    // With a list that includes .md, the digest must include it.
    final String mdAwareDigest = BootstrapFingerprint.compute(dir.toFile(), List.of(".bucket", ".md"));

    assertThat(mdAwareDigest).isNotEqualTo(defaultDigest);
  }
}
