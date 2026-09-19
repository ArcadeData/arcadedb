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
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7864, a follow-up to #7302: {@code executePostImportCommands} was the one reader of mandatory configuration
 * keys left on a bare {@code getString}, so an entry missing {@code "language"} or {@code "command"} was answered
 * with a {@code JSONException} naming the key and nothing else - and answered AFTER the whole import had run, which
 * is the most expensive moment in the file to be told only a key name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7864PostImportCommandValidationTest {

  @Test
  void aMissingCommandKeyNamesTheEntryAndWhatToWriteInIt() {
    final JSONObject config = new JSONObject("""
        { "postImportCommands": [ { "language": "sql", "cmd": "CREATE INDEX ON Person (id) UNIQUE" } ] }
        """);

    assertThatThrownBy(() -> GraphImporter.parsePostImportCommands(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("postImportCommands")
        .hasMessageContaining("\"command\"")
        .hasMessageContaining("the statement to execute after the import");
  }

  @Test
  void aMissingLanguageKeyIsAnsweredTheSameWay() {
    final JSONObject config = new JSONObject("""
        { "postImportCommands": [ { "command": "SELECT 1" } ] }
        """);

    assertThatThrownBy(() -> GraphImporter.parsePostImportCommands(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("\"language\"")
        .hasMessageContaining("the query language the command is written in");
  }

  @Test
  void aBlankValueIsRefusedToo() {
    final JSONObject config = new JSONObject("""
        { "postImportCommands": [ { "language": "sql", "command": "   " } ] }
        """);

    assertThatThrownBy(() -> GraphImporter.parsePostImportCommands(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty value");
  }

  /**
   * The entry is named by its position, because an array carries no other handle on which of several is meant.
   */
  @Test
  void theOffendingEntryIsNamedByItsPosition() {
    final JSONObject config = new JSONObject("""
        {
          "postImportCommands": [
            { "language": "sql", "command": "SELECT 1" },
            { "language": "sql" }
          ]
        }
        """);

    assertThatThrownBy(() -> GraphImporter.parsePostImportCommands(config))
        .hasMessageContaining("entry #2");
  }

  @Test
  void aWellFormedArrayParsesInOrder() {
    final JSONObject config = new JSONObject("""
        {
          "postImportCommands": [
            { "language": "sql", "command": "SELECT 1" },
            { "language": "opencypher", "command": "RETURN 2" }
          ]
        }
        """);

    assertThat(GraphImporter.parsePostImportCommands(config))
        .containsExactly(new GraphImporter.PostImportCommand("sql", "SELECT 1"),
            new GraphImporter.PostImportCommand("opencypher", "RETURN 2"));
  }

  @Test
  void anAbsentKeyIsNoCommands() {
    assertThat(GraphImporter.parsePostImportCommands(new JSONObject("{}"))).isEmpty();
  }

  /**
   * The whole point of the second half of the fix: the typo is reported before a single row is read, so the import
   * does not have to be repeated to find out whether there is a second one.
   */
  @Test
  void theTypoIsReportedBeforeTheImportRuns() throws Exception {
    final String databasePath = "target/databases/test-import-7864";
    final File dataDir = new File("target/importer-7864");
    dataDir.mkdirs();

    final File csv = new File(dataDir, "people.csv");
    Files.writeString(csv.toPath(), "id,name\n1,alice\n2,bob\n", StandardCharsets.UTF_8);

    final File configFile = new File(dataDir, "config.json");
    Files.writeString(configFile.toPath(), """
        {
          "vertices": [ { "type": "Person", "file": "people.csv", "id": "id" } ],
          "postImportCommands": [
            { "language": "sql", "cmd": "CREATE INDEX ON Person (id) UNIQUE" }
          ]
        }
        """, StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(databasePath));
    try {
      assertThatThrownBy(() -> GraphImporter.main(new String[] { configFile.getAbsolutePath(), databasePath,
          dataDir.getAbsolutePath() }))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("\"command\"");

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Person", false))
            .as("not one row was read: the configuration mistake was answered before the import started")
            .isZero();
      }
    } finally {
      final DatabaseFactory factory = new DatabaseFactory(databasePath);
      if (factory.exists())
        factory.open().drop();
      FileUtils.deleteRecursively(new File(databasePath));
      FileUtils.deleteRecursively(dataDir);
    }
  }
}
