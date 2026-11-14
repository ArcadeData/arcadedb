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
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11729: an ArcadeDB export whose file name does not carry {@code .jsonl} ({@code backup.tgz}, {@code backup.gz}) was
 * recognised as generic JSON, which imported the export's header line as one {@code Document}, dropped every record that followed
 * and reported OK. The first line of an export names it, the way the first line of an OrientDB or Neo4j export already does.
 * And a JSON source whose single top-level value is followed by more content is refused: reading the first value and calling
 * the import done is what made the loss silent.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11729ExportNamedWithoutJsonlTest {
  private static final String SOURCE_PATH  = "target/databases/issue11729-source";
  private static final String RESTORE_PATH = "target/databases/issue11729-restore";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    dropDatabase(SOURCE_PATH);
    dropDatabase(RESTORE_PATH);
  }

  @ParameterizedTest
  @ValueSource(strings = { "issue11729-backup.tgz", "issue11729-backup.gz", "issue11729-backup.json.tgz" })
  void exportIsRecognisedByItsHeaderWhateverItsName(final String fileName) {
    final File export = new File("target/" + fileName);
    export.delete();
    try {
      try (final Database db = new DatabaseFactory(SOURCE_PATH).create()) {
        db.transaction(() -> {
          db.command("sql", "CREATE VERTEX TYPE Person");
          db.command("sql", "CREATE EDGE TYPE Knows");
          db.command("sql", "INSERT INTO Person CONTENT [{'name': 'a'}, {'name': 'b'}, {'name': 'c'}]");
          db.command("sql", "CREATE EDGE Knows FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')");
        });
      }
      // THE NAME IS TAKEN AS GIVEN: THE EXPORTER APPENDS NOTHING TO A NAME THAT ALREADY HAS AN EXTENSION
      new Exporter(("-f " + export.getPath() + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
      assertThat(export).exists();

      try (final Database db = new DatabaseFactory(RESTORE_PATH).create()) {
        final Result result;
        try (final ResultSet rs = db.command("sql", "IMPORT DATABASE file://" + export.getAbsolutePath())) {
          result = rs.next();
        }
        assertThat(result.<String>getProperty("result")).isEqualTo("OK");

        assertThat(db.getSchema().existsType("Document")).as("the header line must not become a record").isFalse();
        assertThat(db.countType("Person", true)).isEqualTo(3);
        assertThat(db.countType("Knows", true)).isEqualTo(1);
      }
    } finally {
      export.delete();
    }
  }

  /** Newline-delimited objects that are not an ArcadeDB export: JSON holds one top-level value, so the rest is not dropped. */
  @Test
  void contentAfterTheTopLevelObjectIsRefused() throws Exception {
    final File source = new File("target/issue11729-rows.txt");
    Files.writeString(source.toPath(), "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n", StandardCharsets.UTF_8);
    try {
      assertThatThrownBy(() -> new Importer(
          ("-url file://" + source.getAbsolutePath() + " -database " + RESTORE_PATH + " -forceDatabaseCreate true").split(" ")).load())
          .hasStackTraceContaining("more content follows");

      try (final Database db = new DatabaseFactory(RESTORE_PATH).open()) {
        assertThat(db.getSchema().existsType("Document")).as("nothing may be imported from a source that was refused").isFalse();
      }
    } finally {
      source.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /** The header of an ArcadeDB export read as JSON because the user forced the type: refused, naming the right type. */
  @Test
  void exportHeaderForcedToJsonIsRefusedNamingJsonl() throws Exception {
    final File source = new File("target/issue11729-forced.json");
    Files.writeString(source.toPath(), """
        {"t":"info","c":{"description":"ArcadeDB Database Export","exporterVersion":2}}
        {"t":"db","c":{"name":"x"}}
        """, StandardCharsets.UTF_8);
    try {
      assertThatThrownBy(() -> new Importer(("-url file://" + source.getAbsolutePath() + " -database " + RESTORE_PATH
          + " -forceDatabaseCreate true -fileType json").split(" ")).load())
          .hasStackTraceContaining("ArcadeDB export").hasStackTraceContaining("jsonl");
    } finally {
      source.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  /** A single object followed only by whitespace is the one value a JSON source holds, and is imported. */
  @Test
  void singleObjectWithTrailingWhitespaceIsImported() throws Exception {
    final File source = new File("target/issue11729-single.txt");
    Files.writeString(source.toPath(), "{\"id\": 1, \"name\": \"one\"}\n\n  \n", StandardCharsets.UTF_8);
    try {
      new Importer(("-url file://" + source.getAbsolutePath() + " -database " + RESTORE_PATH + " -forceDatabaseCreate true").split(" "))
          .load();
      try (final Database db = new DatabaseFactory(RESTORE_PATH).open()) {
        assertThat(db.countType("Document", true)).isEqualTo(1);
      }
    } finally {
      source.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  private static void dropDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    else
      FileUtils.deleteRecursively(new File(databasePath));
  }
}
