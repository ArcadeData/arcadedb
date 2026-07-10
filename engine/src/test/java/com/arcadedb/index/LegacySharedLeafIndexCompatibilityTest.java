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
package com.arcadedb.index;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies exact lookup compatibility with compacted files written before the shared-leaf writer safeguard.
 *
 * <p>The synthetic fixture contains 30,000 records with the same composite key. It was generated with the safeguard
 * temporarily disabled; an unpatched reader returns only 28,857 records from its compacted index.</p>
 */
class LegacySharedLeafIndexCompatibilityTest {
  private static final int EXPECTED_DUPLICATES = 30_000;

  @TempDir
  Path tempDir;

  private void extractFixture() throws IOException {
    try (InputStream resource = getClass().getResourceAsStream("/com/arcadedb/index/legacy-shared-leaf-fixture.fixture")) {
      assertThat(resource).as("legacy compacted-index fixture").isNotNull();
      try (ZipInputStream zip = new ZipInputStream(resource)) {
        for (ZipEntry entry; (entry = zip.getNextEntry()) != null; ) {
          final Path target = tempDir.resolve(entry.getName()).normalize();
          assertThat(target.startsWith(tempDir.normalize())).as("zip entry stays under the test directory").isTrue();
          if (entry.isDirectory())
            Files.createDirectories(target);
          else {
            Files.createDirectories(target.getParent());
            Files.copy(zip, target, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    }
  }

  private long count(final IndexCursor cursor) {
    long count = 0;
    while (cursor.hasNext()) {
      cursor.next();
      count++;
    }
    return count;
  }

  private void assertCounts(final Database database) {
    final ResultSet scan = database.query("sql", "SELECT count(*) AS c FROM Tok WHERE word.trim() = 'dup'");
    assertThat(((Number) scan.next().getProperty("c")).longValue()).as("full scan count").isEqualTo(EXPECTED_DUPLICATES);

    final ResultSet indexed = database.query("sql", "SELECT count(*) AS c FROM Tok WHERE word = 'dup' AND lang = 'xx'");
    assertThat(((Number) indexed.next().getProperty("c")).longValue()).as("legacy compacted-index lookup count")
        .isEqualTo(EXPECTED_DUPLICATES);

    final TypeIndex index = database.getSchema().getType("Tok").getIndexByProperties("word", "lang");
    final Object[] fullKey = { "dup", "xx" };
    assertThat(count(index.range(true, fullKey, true, fullKey, true))).as("ascending full-key range count")
        .isEqualTo(EXPECTED_DUPLICATES);
    assertThat(count(index.range(false, fullKey, true, fullKey, true))).as("descending full-key range count")
        .isEqualTo(EXPECTED_DUPLICATES);
  }

  @Test
  void lookupsReadFirstChunkFromSharedPrecedingLeaf() throws IOException {
    extractFixture();

    try (DatabaseFactory factory = new DatabaseFactory(tempDir.toString())) {
      try (Database database = factory.open()) {
        assertCounts(database);
      }
      try (Database reopened = factory.open()) {
        assertCounts(reopened);
      }
    }
  }
}
