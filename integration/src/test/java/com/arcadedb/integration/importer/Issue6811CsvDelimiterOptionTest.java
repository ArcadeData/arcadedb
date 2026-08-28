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
import java.io.FileWriter;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6811: {@code SourceDiscovery.analyzeSourceContent()} unconditionally wrote {@code knownDelimiter} into
 * {@code settings.options}, but left it {@code null} for {@code EntityType.DATABASE}. Since the user's
 * {@code -delimiter} / {@code WITH delimiter = ';'} lands in that same map, it was destroyed for every CSV source,
 * and the whole line ended up parsed as a single column.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6811CsvDelimiterOptionTest {

  @Test
  void importDatabaseHonoursTheDelimiterSetWithSql() throws IOException {
    final String databasePath = "target/databases/test-import-6811-sql";
    final File csvFile = writeSemicolonCsv("importer-6811-sql.csv");

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + csvFile.getAbsolutePath() + " WITH delimiter = ';'");
      assertImportedColumns(db);
    } finally {
      db.drop();
      csvFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void importDatabaseHonoursTheDelimiterSetOnTheCommandLine() throws Exception {
    final String databasePath = "target/databases/test-import-6811-cli";
    final File csvFile = writeSemicolonCsv("importer-6811-cli.csv");

    try {
      new Importer(new String[] { "-url", "file://" + csvFile.getAbsolutePath(), "-database", databasePath,
          "-forceDatabaseCreate", "true", "-delimiter", ";" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertImportedColumns(db);
      }
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      csvFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The per-entity delimiter is an override, not a mandatory value: leaving it unset must not clear whatever the
   * user asked for, and a plain comma-separated file must keep working with no delimiter set at all.
   */
  @Test
  void aCommaSeparatedFileStillImportsWithNoDelimiterSet() {
    final String databasePath = "target/databases/test-import-6811-default";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://src/test/resources/importer-vertices.csv");
      assertThat(db.countType("Document", true)).isEqualTo(6);
    } finally {
      db.drop();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * With no ".csv" extension to short-circuit on, the delimiter is auto-detected from the first line - and ';' was
   * missing from the candidate set, so a semicolon-separated file produced no candidate at all and the import died
   * with "Cannot determine the file type".
   */
  @Test
  void aSemicolonSeparatedFileWithoutACsvExtensionIsAutoDetected() throws IOException {
    final String databasePath = "target/databases/test-import-6811-autodetect";
    final File csvFile = writeSemicolonCsv("importer-6811-autodetect.txt");

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + csvFile.getAbsolutePath());
      assertImportedColumns(db);
    } finally {
      db.drop();
      csvFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  private static void assertImportedColumns(final Database db) {
    assertThat(db.countType("Document", true)).isEqualTo(2);

    final Document first = db.iterateType("Document", true).next().asDocument(true);
    assertThat(first.getPropertyNames()).contains("id", "name", "age");
    // BEFORE THE FIX THE WHOLE LINE WAS ONE COLUMN LITERALLY NAMED "id;name;age"
    assertThat(first.getPropertyNames()).doesNotContain("id;name;age");
  }

  private static File writeSemicolonCsv(final String fileName) throws IOException {
    final File file = new File("target/" + fileName);
    file.getParentFile().mkdirs();
    try (final FileWriter writer = new FileWriter(file)) {
      writer.write("id;name;age\n");
      writer.write("1;Alice;30\n");
      writer.write("2;Bob;25\n");
    }
    return file;
  }
}
