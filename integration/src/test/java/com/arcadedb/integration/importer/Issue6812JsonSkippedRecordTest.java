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
 * Issue #6812: {@code JSONImporterFormat.createRecord()} used the same return signal for two opposite outcomes -
 * "there is no mapping, save this as an anonymous document" and "this record is rejected or deduplicated, drop it" -
 * so a record the importer had just logged as skipped was saved as an anonymous {@code Document} instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6812JsonSkippedRecordTest {

  @Test
  void aDuplicateIdIsSkippedInsteadOfBeingSavedAsAnAnonymousDocument() throws Exception {
    final String databasePath = "target/databases/test-import-6812-duplicate-id";
    final File jsonFile = writeJsonFile("importer-6812-duplicate-id.json",
        "{\"Users\":[{\"id\":\"1\",\"name\":\"a\"},{\"id\":\"1\",\"name\":\"b\"}]}");

    final String mapping = "{\"Users\":[{\"@cat\":\"v\",\"@type\":\"User\",\"@id\":\"id\",\"@idType\":\"string\"}]}";

    try {
      new Importer(new String[] { "-url", "file://" + jsonFile.getAbsolutePath(), "-database", databasePath,
          "-forceDatabaseCreate", "true", "-mapping", mapping }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("User", true)).isEqualTo(1);

        final Document user = db.iterateType("User", true).next().asDocument(true);
        // THE SECOND ENTRY IS SKIPPED, NOT MERGED: "@strategy" IS NOT "merge" HERE
        assertThat(user.getString("name")).isEqualTo("a");

        // THE SKIPPED DUPLICATE MUST NOT RESURFACE AS AN ANONYMOUS DOCUMENT
        assertThat(db.getSchema().existsType("Document")).isFalse();
      }
    } finally {
      dropDatabase(databasePath);
      jsonFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void aMappingObjectWithoutCatIsSkippedInsteadOfBeingSavedAsAnAnonymousDocument() throws Exception {
    final String databasePath = "target/databases/test-import-6812-no-cat";
    final File jsonFile = writeJsonFile("importer-6812-no-cat.json", "{\"Users\":[{\"id\":\"1\",\"name\":\"a\"}]}");

    final String mapping = "{\"Users\":[{\"@type\":\"User\",\"@id\":\"id\"}]}";

    try {
      new Importer(new String[] { "-url", "file://" + jsonFile.getAbsolutePath(), "-database", databasePath,
          "-forceDatabaseCreate", "true", "-mapping", mapping }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.getSchema().existsType("Document")).isFalse();
        assertThat(db.getSchema().existsType("User")).isFalse();
      }
    } finally {
      dropDatabase(databasePath);
      jsonFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The anonymous-document path is intentional when there is no mapping object at all - which is exactly why the two
   * cases had to be told apart. This pins it so the fix cannot be "tightened" into dropping legitimate records.
   */
  @Test
  void withoutAMappingObjectTheRecordIsStillSavedAsAnAnonymousDocument() throws Exception {
    final String databasePath = "target/databases/test-import-6812-anonymous";
    final File jsonFile = writeJsonFile("importer-6812-anonymous.json", "{\"Users\":[{\"id\":\"1\"},{\"id\":\"2\"}]}");

    try {
      new Importer(new String[] { "-url", "file://" + jsonFile.getAbsolutePath(), "-database", databasePath,
          "-forceDatabaseCreate", "true", "-mapping", "{'Users':[]}" }).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.countType("Document", true)).isEqualTo(2);
      }
    } finally {
      dropDatabase(databasePath);
      jsonFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  private static void dropDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
  }

  private static File writeJsonFile(final String fileName, final String content) throws IOException {
    final File file = new File("target/" + fileName);
    file.getParentFile().mkdirs();
    try (final FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }
}
