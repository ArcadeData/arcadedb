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
 * Regression test for issue #6946, the sibling of #6811: when the source file carries no known extension the format is
 * sniffed from its content, and the sniffed separator was written over the delimiter the user had explicitly supplied.
 * The file below is exactly the shape that makes someone choose ';': its values carry commas, so the sniffer's best
 * candidate is ',' and the whole line came back as one column.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6946AutoDetectDelimiterOptionTest {

  @Test
  void theUserDelimiterWinsOverTheSniffedOneWithSql() throws IOException {
    final String databasePath = "target/databases/test-import-6946-sql";
    final File file = writeFile("importer-6946-sql.txt");

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath() + " WITH delimiter = ';'");
      assertImportedColumns(db);
    } finally {
      db.drop();
      file.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void theUserDelimiterWinsOverTheSniffedOneOnTheCommandLine() throws Exception {
    final String databasePath = "target/databases/test-import-6946-cli";
    final File file = writeFile("importer-6946-cli.txt");

    try {
      new Importer(new String[] { "-url", "file://" + file.getAbsolutePath(), "-database", databasePath, "-forceDatabaseCreate",
          "true", "-delimiter", ";" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertImportedColumns(db);
      }
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      file.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * One import walks its documents, vertices and edges files against one shared settings instance. A delimiter settled
   * for the documents file (explicit ';') must not stand in for the vertices file, which supplies none and has to be
   * sniffed on its own ('|'): the delimiter is resolved per entity and handed to the format, never parked in the shared
   * options map.
   */
  @Test
  void aDelimiterSettledForOneEntityDoesNotLeakIntoTheNext() throws Exception {
    final String databasePath = "target/databases/test-import-6946-entities";
    final File documents = writeFile("importer-6946-documents.txt");
    final File vertices = new File("target/importer-6946-vertices.txt");
    try (final FileWriter writer = new FileWriter(vertices)) {
      writer.write("id|label|weight\n");
      writer.write("1|first|10\n");
      writer.write("2|second|20\n");
    }

    try {
      new Importer(new String[] { "-documents", "file://" + documents.getAbsolutePath(), "-documentsDelimiter", ";", "-vertices",
          "file://" + vertices.getAbsolutePath(), "-database", databasePath, "-forceDatabaseCreate", "true" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertImportedColumns(db);

        assertThat(db.countType("Node", true)).isEqualTo(2);
        final Document node = db.iterateType("Node", true).next().asDocument(true);
        assertThat(node.getPropertyNames()).as("the vertices file is parsed with its own, sniffed, separator")
            .contains("id", "label", "weight").doesNotContain("id|label|weight");
      }
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      documents.delete();
      vertices.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * Guard against over-fixing: with nothing supplied by the user the sniffed separator is still what drives the import.
   */
  @Test
  void theSniffedDelimiterStillAppliesWhenTheUserSuppliedNone() throws IOException {
    final String databasePath = "target/databases/test-import-6946-sniffed";
    final File file = new File("target/importer-6946-sniffed.txt");
    file.getParentFile().mkdirs();
    try (final FileWriter writer = new FileWriter(file)) {
      writer.write("id|name|age\n");
      writer.write("1|Alice|30\n");
      writer.write("2|Bob|25\n");
    }

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath());
      assertThat(db.countType("Document", true)).isEqualTo(2);
      final Document first = db.iterateType("Document", true).next().asDocument(true);
      assertThat(first.getPropertyNames()).contains("id", "name", "age");
    } finally {
      db.drop();
      file.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  private static void assertImportedColumns(final Database db) {
    assertThat(db.countType("Document", true)).isEqualTo(2);

    final Document first = db.iterateType("Document", true).next().asDocument(true);
    assertThat(first.getPropertyNames()).contains("id", "last,first,middle,suffix", "age");
    // BEFORE THE FIX THE SNIFFER PICKED ',' (4 COMMAS AGAINST 2 SEMICOLONS ON THE FIRST LINE) OVER THE USER'S ';'
    assertThat(first.getPropertyNames()).doesNotContain("id;last");
  }

  private static File writeFile(final String fileName) throws IOException {
    final File file = new File("target/" + fileName);
    file.getParentFile().mkdirs();
    try (final FileWriter writer = new FileWriter(file)) {
      writer.write("id;last,first,middle,suffix;age\n");
      writer.write("1;Doe, John, Q, Jr;30\n");
      writer.write("2;Roe, Jane, R, Sr;25\n");
    }
    return file;
  }
}
