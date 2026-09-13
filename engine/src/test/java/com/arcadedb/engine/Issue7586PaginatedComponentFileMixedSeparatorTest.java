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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7586, found by review on PR #7587: {@link PaginatedComponentFile#open} overrides
 * {@link ComponentFile#open} and duplicated its {@code File.separator}-only directory lookup rather than reusing
 * {@link com.arcadedb.utility.FileUtils#lastIndexOfSeparator}. This is the override that actually matters for the
 * reported bug - every real component ({@code Dictionary}, buckets, indexes) is a {@link PaginatedComponent} and
 * so opens through here, not through the base class - so fixing only {@code ComponentFile.open()} left the
 * production code path unfixed.
 * <p>
 * {@code PaginatedComponentFile.open()} does real file I/O (it opens a {@code RandomAccessFile}), so this reproduces
 * the parsing bug through a real file, mirroring {@link PaginatedComponentFileRoundTripTest}'s construction. The
 * mixed-separator technique matches {@link Issue7586ComponentFileMixedSeparatorTest}: the directory is joined with
 * this JVM's own separator and the closest separator to the file name is the other one, so the gap is exercised on
 * every platform this test runs on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7586PaginatedComponentFileMixedSeparatorTest {
  private static final int PAGE_SIZE = 1024;

  @TempDir
  Path tempDir;

  private PaginatedComponentFile pcf;

  @AfterEach
  void tearDown() {
    if (pcf != null)
      pcf.close();
  }

  @Test
  void fileNameAndComponentNameStripADirectoryPrefixRegardlessOfWhichSeparatorIsClosest() throws IOException {
    // A real "dir" directory, reached with a literal separator that is NOT this JVM's own File.separator: on
    // Linux/macOS that is '\', a plain filename character that keeps "dir" and the file name in one path
    // component; on Windows '/' is a real alternate separator, so it still resolves into "dir". Either way the
    // closest separator to the file name is the foreign one, exactly as a mismatched separator would be on
    // whichever platform the JVM's own separator differs from.
    Files.createDirectories(tempDir.resolve("dir"));
    final char foreignSeparator = File.separatorChar == '/' ? '\\' : '/';
    final String filePath =
        tempDir + File.separator + "dir" + foreignSeparator + "dictionary.0." + PAGE_SIZE + ".v0.dict";

    pcf = new PaginatedComponentFile(filePath, ComponentFile.MODE.READ_WRITE);

    assertThat(pcf.getFileName()).isEqualTo("dictionary.0." + PAGE_SIZE + ".v0.dict");
    assertThat(pcf.getComponentName()).isEqualTo("dictionary");
    assertThat(pcf.getFileId()).isEqualTo(0);
    assertThat(pcf.getPageSize()).isEqualTo(PAGE_SIZE);
    assertThat(pcf.getVersion()).isEqualTo(0);
  }
}
