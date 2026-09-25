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
package com.arcadedb.server.monitor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8301: saved profiler runs were rotated by name order, and before the fix a run saved under th-TH was named with
 * the Buddhist year, which sorts after every newer run: the newest runs were deleted while the legacy one was kept
 * forever. Retention now goes by modification time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8301ProfilerRunRetentionTest {

  @Test
  void legacyBuddhistYearRunIsTheOneEvicted(@TempDir final Path dir) throws IOException {
    final long base = System.currentTimeMillis() - 1_000_000L;
    final File legacy = write(dir, "profiler-run-25690101-000000.json", base);
    File newest = null;
    for (int i = 0; i < 50; i++)
      newest = write(dir, "profiler-run-202601%02d-%06d.json".formatted(1 + i / 24, i), base + (i + 1) * 1000L);

    ServerQueryProfiler.cleanOldFiles(dir.toFile());

    assertThat(legacy).doesNotExist();
    assertThat(newest).exists();
    assertThat(dir.toFile().listFiles()).hasSize(50);
  }

  private static File write(final Path dir, final String name, final long lastModified) throws IOException {
    final File file = Files.writeString(dir.resolve(name), "{}").toFile();
    assertThat(file.setLastModified(lastModified)).isTrue();
    return file;
  }
}
