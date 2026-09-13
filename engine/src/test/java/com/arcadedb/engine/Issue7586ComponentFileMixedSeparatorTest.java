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

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7586: {@code LocalSchema} builds the dictionary's path with a literal '/', while the rest of a database
 * path is built with the platform {@code File.separator}. On Windows this leaves a mixed-separator path whose
 * directory {@link ComponentFile#open} stripped with a lookup keyed on {@code File.separator} alone ('\\') - which
 * never finds the '/' - so {@code fileName}/{@code componentName} kept the database directory, and the backup
 * archive entry built from {@code fileName} nested under it instead of sitting at the archive root.
 * <p>
 * Reproduced here with the roles of the two separators swapped, so the gap is exercised on every platform this
 * test runs on rather than only on Windows: the directory is joined with '/' and the last, closest separator is
 * '\\', which a lookup keyed on this JVM's own {@code File.separator} ('/' on Linux and macOS) would miss.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7586ComponentFileMixedSeparatorTest {

  @Test
  void fileNameAndComponentNameStripADirectoryPrefixRegardlessOfWhichSeparatorIsClosest() throws FileNotFoundException {
    final ComponentFile file = new ComponentFile("db/dir\\dictionary.0.65536.v1.dict", ComponentFile.MODE.READ_ONLY) {
    };

    assertThat(file.getFileName()).isEqualTo("dictionary.0.65536.v1.dict");
    assertThat(file.getComponentName()).isEqualTo("dictionary.0");
    assertThat(file.getFileId()).isEqualTo(65536);
    assertThat(file.getVersion()).isEqualTo(1);
    assertThat(file.getFileExtension()).isEqualTo("dict");
  }

  @Test
  void fileNameAndComponentNameStripADirectoryPrefixWithNoTrailingFileId() throws FileNotFoundException {
    final ComponentFile file = new ComponentFile("db/dir\\schema.dict", ComponentFile.MODE.READ_ONLY) {
    };

    assertThat(file.getFileName()).isEqualTo("schema.dict");
    assertThat(file.getComponentName()).isEqualTo("schema");
    assertThat(file.getFileId()).isEqualTo(-1);
  }
}
