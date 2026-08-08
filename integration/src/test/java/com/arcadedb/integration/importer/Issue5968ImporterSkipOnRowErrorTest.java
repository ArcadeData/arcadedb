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
import com.arcadedb.exception.ValidationException;
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

  /**
   * Symmetric to {@link #csvVertexImportSkipsOutOfRangeValueWhenOptedIn()} but for {@code CSVImporterFormat.loadDocuments},
   * which is fully synchronous ({@code document.save()}, no {@code database.async()}), so unlike vertices, "skip" mode
   * here guarantees an exact surviving count.
   */
  @Test
  void csvDocumentImportSkipsOutOfRangeValueWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-csv-doc-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Widget");
      db.command("sql", "CREATE PROPERTY Widget.Score SHORT");
    }

    try {
      final Importer importer = new Importer(
          ("-documents src/test/resources/importer-vertices-outofrange.csv -database " + databasePath
              + " -documentType Widget -onRowError skip").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);

      try (final Database db = databaseFactory.open()) {
        // BOB'S "99999" DOES NOT FIT A SHORT: THAT ROW IS SKIPPED, THE OTHER TWO ARE IMPORTED
        assertThat(db.countType("Widget", true)).isEqualTo(2);
        db.iterateType("Widget", true).forEachRemaining(record -> assertThat(record.asDocument().getString("Name"))
            .isIn("Alice", "Carol"));
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

  /**
   * Vertices are queued via {@code database.async().createRecord(...)}, so a violation that only surfaces at persist
   * time on the async worker thread (e.g. a missing mandatory property, as opposed to a synchronous type-conversion
   * error caught inline in the CSV row loop) must still abort the import in the default {@code abort} mode.
   */
  @Test
  void csvVertexImportAbortsOnAsyncPersistTimeFailureByDefault() {
    final String databasePath = "target/databases/test-import-5968-csv-async-abort";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Email STRING (MANDATORY TRUE)");
    }

    try {
      final Importer importer = new Importer(
          ("-vertices src/test/resources/importer-vertices-missing-mandatory.csv -database " + databasePath
              + " -typeIdProperty Id -typeIdType Long").split(" "));

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(ValidationException.class);
    } finally {
      databaseFactory.open().drop();
    }
  }

  @Test
  void csvVertexImportCountsPersistTimeFailureWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-csv-async-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Email STRING (MANDATORY TRUE)");
    }

    try {
      final Importer importer = new Importer(
          ("-vertices src/test/resources/importer-vertices-missing-mandatory.csv -database " + databasePath
              + " -typeIdProperty Id -typeIdType Long -onRowError skip").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);

      try (final Database db = databaseFactory.open()) {
        // BOB IS MISSING THE MANDATORY Email PROPERTY: IT FAILS AT PERSIST TIME (DocumentValidator, NOT THE SYNCHRONOUS
        // Type-CONVERSION PATH), THE OTHER TWO ARE IMPORTED. "skip" MODE SAVES EACH VERTEX SYNCHRONOUSLY IN ITS OWN
        // TRANSACTION (SEE loadVertices), SO A FAILING ROW'S rollback() CAN NEVER TOUCH A PREVIOUSLY COMMITTED SIBLING -
        // WHICH IS WHY THE SURVIVING COUNT CAN BE ASSERTED EXACTLY HERE.
        assertThat(db.lookupByKey("Node", "Id", 2L).hasNext()).isFalse();
        assertThat(db.countType("Node", true)).isEqualTo(2);
        assertThat(db.lookupByKey("Node", "Id", 1L).next().getRecord().asVertex().getString("Name")).isEqualTo("Alice");
        assertThat(db.lookupByKey("Node", "Id", 3L).next().getRecord().asVertex().getString("Name")).isEqualTo("Carol");
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * Regression test for the PR #5974 review finding (round 4, "Confirmed"): a failure that surfaces AFTER the bucket
   * write - a duplicate value on the unique index {@code loadVertices} auto-creates on {@code -typeIdProperty} -
   * used to leave a "ghost" vertex: present in the bucket (inflating {@code countType()}) but never indexed, because
   * the whole file shared one transaction that only got rolled back on total abort, never on a per-row skip. Each
   * vertex now commits in its own transaction in skip mode, so a duplicate key rolls back only that one row.
   */
  @Test
  void csvVertexImportRollsBackGhostRecordOnDuplicateKeyWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-csv-ghost-vertex";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try {
      final Importer importer = new Importer(
          ("-vertices src/test/resources/importer-vertices-duplicate-id.csv -database " + databasePath
              + " -typeIdProperty Id -typeIdType Long -typeIdUnique true -forceDatabaseCreate true -onRowError skip").split(
              " "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);

      try (final Database db = databaseFactory.open()) {
        // BOB'S DUPLICATE Id=2 ROW MUST LEAVE NO TRACE: NEITHER A SECOND INDEX ENTRY NOR AN UNINDEXED GHOST IN THE
        // BUCKET. IF THE GHOST-RECORD BUG WERE PRESENT, countType() WOULD REPORT 4 INSTEAD OF 3.
        assertThat(db.countType("Node", true)).isEqualTo(3);
        assertThat(db.lookupByKey("Node", "Id", 2L).next().getRecord().asVertex().getString("Name")).isEqualTo("Bob");
        assertThat(db.lookupByKey("Node", "Id", 1L).next().getRecord().asVertex().getString("Name")).isEqualTo("Alice");
        assertThat(db.lookupByKey("Node", "Id", 3L).next().getRecord().asVertex().getString("Name")).isEqualTo("Carol");
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * Same as {@link #csvVertexImportRollsBackGhostRecordOnDuplicateKeyWhenOptedIn()} but for the fully-synchronous
   * {@code loadDocuments} path, against a manually created unique index (documents have no {@code -typeIdProperty}
   * concept of their own).
   */
  @Test
  void csvDocumentImportRollsBackGhostRecordOnDuplicateKeyWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-csv-ghost-doc";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Widget2");
      db.command("sql", "CREATE PROPERTY Widget2.Code STRING");
      db.command("sql", "CREATE INDEX ON Widget2 (Code) UNIQUE");
    }

    try {
      final Importer importer = new Importer(
          ("-documents src/test/resources/importer-documents-duplicate-code.csv -database " + databasePath
              + " -documentType Widget2 -onRowError skip").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);

      try (final Database db = databaseFactory.open()) {
        assertThat(db.countType("Widget2", true)).isEqualTo(3);
        assertThat(db.lookupByKey("Widget2", "Code", "B").next().getRecord().asDocument().getString("Name")).isEqualTo("Bob");
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * Regression test for the PR #5974 review finding: a Type/schema conversion error thrown from a NESTED mapped
   * object (parsed via a recursive {@code parseRecord()} call, before the enclosing object's own {@code endObject()}
   * runs) must not desync the {@code JsonReader} for the rest of the array. Each "Order" has a nested "customer"
   * object independently mapped to its own document type with a {@code SHORT} property that overflows for Bob.
   */
  private static final String NESTED_MAPPING = """
      {
        "Orders":[
          {
            "@cat":"d",
            "@type":"Order",
            "customer":{
              "@cat":"d",
              "@type":"Customer"
            }
          }
        ]
      }""";

  @Test
  void jsonNestedObjectImportAbortsOnOutOfRangeValueByDefault() {
    final String databasePath = "target/databases/test-import-5968-json-nested-abort";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Customer");
      db.getSchema().getType("Customer").createProperty("age", Type.SHORT);
    }

    try {
      final Importer importer = new Importer(new String[] {
          "-url", "file://src/test/resources/importer-documents-nested-outofrange.json",
          "-database", databasePath,
          "-mapping", NESTED_MAPPING
      });

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);
    } finally {
      databaseFactory.open().drop();
    }
  }

  @Test
  void jsonNestedObjectImportSkipsWholeRecordAndKeepsProcessingSiblingsWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-json-nested-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Customer");
      db.getSchema().getType("Customer").createProperty("age", Type.SHORT);
    }

    try {
      final Importer importer = new Importer(new String[] {
          "-url", "file://src/test/resources/importer-documents-nested-outofrange.json",
          "-database", databasePath,
          "-mapping", NESTED_MAPPING,
          "-onRowError", "skip"
      });

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isNotNull();

      try (final Database db = databaseFactory.open()) {
        // THE READER MUST NOT DESYNC AND SILENTLY DROP ORDER 3: BOB'S NESTED Customer FAILS, WHICH DISCARDS BOB'S
        // WHOLE Order TOO (A NESTED FAILURE CAN ONLY BE UNDONE BY ROLLING BACK THE ENCLOSING TOP-LEVEL RECORD'S OWN
        // TRANSACTION - THE NESTED Customer MAY ALREADY HAVE A BUCKET WRITE THAT NOTHING ELSE CAN SAFELY UNDO - SEE
        // #5974 REVIEW ROUND 2), BUT ALICE'S AND CAROL'S Orders (AND THEIR Customers) STILL GET CREATED NORMALLY.
        assertThat(db.countType("Order", true)).isEqualTo(2);
        assertThat(db.countType("Customer", true)).isEqualTo(2);
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
