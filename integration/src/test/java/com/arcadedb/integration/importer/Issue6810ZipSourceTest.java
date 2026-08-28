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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.integration.TestHelper;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6810: {@code SourceDiscovery}'s reset callbacks called {@code getNextEntry()} on the <b>old</b>, already
 * closed {@code ZipInputStream} instead of the freshly created one. The reset path runs on every import, so any ZIP
 * source imported as 0 records - and {@code Source.reset()} swallowed the underlying {@code IOException}, so the
 * import reported success.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6810ZipSourceTest {

  private static final String NODES = """
      id,name
      1,Alice
      2,Bob
      3,Carol
      """;

  private static final String OTHER = """
      id,city
      1,Rome
      """;

  @Test
  void aSingleEntryZipImportsItsRecords() throws Exception {
    final String databasePath = "target/databases/test-import-6810-single-entry";
    final File zipFile = writeZip("importer-6810-single.csv.zip", Map.of("nodes.csv", NODES));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + zipFile.getAbsolutePath());

      assertThat(db.countType("Document", true)).isEqualTo(3);
    } finally {
      db.drop();
      zipFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The rebuilt stream must be seeked to the {@code :::<resource>} entry the user asked for, not merely to the first
   * entry in the archive.
   */
  @Test
  void aMultiEntryZipImportsTheRequestedEntry() throws Exception {
    final String databasePath = "target/databases/test-import-6810-multi-entry";

    final Map<String, String> entries = new LinkedHashMap<>();
    entries.put("other.csv", OTHER);
    entries.put("nodes.csv", NODES);
    final File zipFile = writeZip("importer-6810-multi.zip", entries);

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + zipFile.getAbsolutePath() + ":::nodes.csv");

      assertThat(db.countType("Document", true)).isEqualTo(3);

      final Document first = db.iterateType("Document", true).next().asDocument(true);
      assertThat(first.getPropertyNames()).contains("name");
      assertThat(first.getPropertyNames()).doesNotContain("city");
    } finally {
      db.drop();
      zipFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  private static File writeZip(final String fileName, final Map<String, String> entries) throws IOException {
    final File file = new File("target/" + fileName);
    file.getParentFile().mkdirs();
    try (final ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(file))) {
      for (final Map.Entry<String, String> entry : entries.entrySet()) {
        zip.putNextEntry(new ZipEntry(entry.getKey()));
        zip.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
      }
    }
    return file;
  }
}
