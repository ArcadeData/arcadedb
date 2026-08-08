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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5968: a single out-of-range/malformed value in a bulk CSV/JSON import used to abort the whole job (follow-up
 * from #5967, which made {@code Type.convert()} reject out-of-range numeric narrowing instead of silently wrapping it).
 * This adds an opt-in {@code -onRowError skip} importer setting that logs and skips the offending row instead of
 * aborting, while keeping today's abort-on-first-error behavior as the default for backward compatibility.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5968ImporterSkipOnRowErrorTest {

  @Test
  void csvVertexImportAbortsOnOutOfRangeValueByDefault() {
    final String databasePath = "target/databases/test-import-5968-csv-abort";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Score SHORT");
    }

    try {
      final Importer importer = new Importer(("-vertices src/test/resources/importer-vertices-outofrange.csv -database "
          + databasePath + " -typeIdProperty Id -typeIdType Long").split(" "));

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);
    } finally {
      databaseFactory.open().drop();
    }
  }

  @Test
  void csvVertexImportSkipsOutOfRangeValueWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-csv-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Score SHORT");
    }

    try {
      final Importer importer = new Importer(
          ("-vertices src/test/resources/importer-vertices-outofrange.csv -database " + databasePath
              + " -typeIdProperty Id -typeIdType Long -onRowError skip").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);

      try (final Database db = databaseFactory.open()) {
        // BOB'S "99999" DOES NOT FIT A SHORT: THAT ROW IS SKIPPED, THE OTHER TWO ARE IMPORTED
        assertThat(db.countType("Node", true)).isEqualTo(2);
        assertThat(db.lookupByKey("Node", "Id", 1L).next().getRecord().asVertex().getString("Name")).isEqualTo("Alice");
        assertThat(db.lookupByKey("Node", "Id", 3L).next().getRecord().asVertex().getString("Name")).isEqualTo("Carol");
        assertThat(db.lookupByKey("Node", "Id", 2L).hasNext()).isFalse();
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  @Test
  void jsonDocumentImportAbortsOnOutOfRangeValueByDefault() {
    final String databasePath = "target/databases/test-import-5968-json-abort";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Food");
      db.getSchema().getType("Food").createProperty("qty", Type.SHORT);
    }

    try {
      final Importer importer = new Importer(
          ("-url file://src/test/resources/importer-documents-outofrange.json -database " + databasePath
              + " -documentType Food -mapping {'*':[]}").split(" "));

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);
    } finally {
      databaseFactory.open().drop();
    }
  }

  @Test
  void jsonDocumentImportSkipsOutOfRangeValueWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-json-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Food");
      db.getSchema().getType("Food").createProperty("qty", Type.SHORT);
    }

    try {
      final Importer importer = new Importer(
          ("-url file://src/test/resources/importer-documents-outofrange.json -database " + databasePath
              + " -documentType Food -mapping {'*':[]} -onRowError skip").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);

      try (final Database db = databaseFactory.open()) {
        // BANANA'S "99999" DOES NOT FIT A SHORT: THAT RECORD IS SKIPPED, THE OTHER TWO ARE IMPORTED
        assertThat(db.countType("Food", true)).isEqualTo(2);
        db.iterateType("Food", true).forEachRemaining(record -> assertThat(record.asDocument().getString("name"))
            .isIn("Apple", "Cherry"));
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  @Test
  void invalidOnRowErrorValueIsRejected() {
    assertThatThrownBy(() -> new Importer(
        ("-vertices src/test/resources/importer-vertices.csv -database target/databases/test-import-5968-invalid"
            + " -onRowError bogus").split(" "))).isInstanceOf(IllegalArgumentException.class);
  }
}
