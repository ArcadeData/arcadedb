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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for issue #4830: snapshot extraction, marker writes and the atomic swap were
 * never fsynced before the backup was deleted and the database reopened, so a power loss between the
 * swap and the durable write-back could expose torn/partial files with no rollback.
 * <p>
 * A unit test cannot literally pull the power, but it can assert the two durability guarantees the
 * fix relies on: (1) the directory fsync used after every rename/marker is best-effort and never
 * aborts the install on platforms that disallow opening a directory as a channel, and (2) the
 * fsync added to the extraction loop does not change the bytes that land on disk. The end-to-end
 * swap durability ordering is covered by {@link SnapshotSwapRecoveryTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotDurabilityTest {

  @Test
  void fsyncDirectorySucceedsOnRealDirectory(@TempDir final Path dir) {
    // POSIX: opening the directory and forcing it must succeed without throwing.
    assertThatCode(() -> SnapshotInstaller.fsyncDirectory(dir)).doesNotThrowAnyException();
  }

  @Test
  void fsyncDirectoryIsBestEffortOnNonDirectoryPaths(@TempDir final Path dir) throws Exception {
    // A regular file and a non-existent path must not abort the install: the helper swallows the
    // IOException so callers on platforms where a directory cannot be force()'d keep working.
    final Path regularFile = dir.resolve("data.dat");
    Files.writeString(regularFile, "payload");
    final Path missing = dir.resolve("does-not-exist");

    assertThatCode(() -> SnapshotInstaller.fsyncDirectory(regularFile)).doesNotThrowAnyException();
    assertThatCode(() -> SnapshotInstaller.fsyncDirectory(missing)).doesNotThrowAnyException();
    assertThatCode(() -> SnapshotInstaller.fsyncDirectory(null)).doesNotThrowAnyException();
  }

  @Test
  void extractionFsyncsEachFileWithoutAlteringContent(@TempDir final Path dir) throws Exception {
    // Build a plain (manifest-less) snapshot ZIP and extract it. The fsync added to the extraction
    // loop must not corrupt or truncate any file: every byte read back must equal what was written.
    final Map<String, byte[]> files = new HashMap<>();
    files.put("configuration.json", "{\"k\":\"v\"}".getBytes(StandardCharsets.UTF_8));
    files.put("schema.json", "{\"types\":[]}".getBytes(StandardCharsets.UTF_8));
    final byte[] page = new byte[128 * 1024];
    for (int i = 0; i < page.length; i++)
      page[i] = (byte) (i * 7 + 3);
    files.put("mytype_0.1.65536.v0.bucket", page);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = new ZipOutputStream(baos)) {
      for (final Map.Entry<String, byte[]> e : files.entrySet()) {
        zos.putNextEntry(new ZipEntry(e.getKey()));
        zos.write(e.getValue());
        zos.closeEntry();
      }
      zos.finish();
    }

    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(baos.toByteArray()));
    SnapshotInstaller.extractAndVerifySnapshot(counter, counter, dir, false);

    for (final Map.Entry<String, byte[]> e : files.entrySet())
      assertThat(Files.readAllBytes(dir.resolve(e.getKey()))).isEqualTo(e.getValue());
  }
}
