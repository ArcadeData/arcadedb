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
package com.arcadedb.integration.backup.format;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7586, defect B: {@code FullBackupFormat.compressEntry} used to hand a {@code ComponentFile}-derived name
 * straight to the archive writer, so a directory left in that name by a separator mismatch upstream (a database
 * path built with a literal '/' while {@code ComponentFile.open()} looked for the platform {@code File.separator})
 * nested the entry inside the archive instead of at its root - an archive a restore could not open.
 * <p>
 * {@code archiveEntryName} is the defense in depth this test pins directly: whatever name reaches it, only the
 * final path segment is ever archived, independent of which separator convention built the name and independent
 * of whatever {@code LocalSchema}/{@code ComponentFile.open()} currently produce.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7586ArchiveEntryNameTest {
  @ParameterizedTest
  @CsvSource({
      "dictionary.0.65536.v1.dict, dictionary.0.65536.v1.dict",
      "'db/dir\\dictionary.0.65536.v1.dict', dictionary.0.65536.v1.dict",
      "'db\\dir/dictionary.0.65536.v1.dict', dictionary.0.65536.v1.dict",
      "configuration.json, configuration.json" })
  void stripsAnyDirectoryComponentRegardlessOfWhichSeparatorBuiltIt(final String rawName, final String expected) {
    assertThat(FullBackupFormat.archiveEntryName(rawName)).isEqualTo(expected);
  }
}
