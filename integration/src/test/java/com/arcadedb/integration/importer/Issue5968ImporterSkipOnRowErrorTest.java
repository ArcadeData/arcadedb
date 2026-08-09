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
import com.univocity.parsers.common.TextParsingException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
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

  /**
   * Default "abort" mode must guarantee no partial import: {@code loadDocuments} persists the whole file in one
   * synchronous transaction ({@code document.save()} per row, no {@code database.async()}), so if row 2 (Bob, whose
   * Score overflows SHORT) fails, row 1 (Alice) must never survive either - even though it was already {@code save()}d
   * into the still-open transaction before the failure was thrown.
   */
  @Test
  void csvDocumentImportAbortsOnOutOfRangeValueByDefaultAndRollsBackPartialRows() {
    final String databasePath = "target/databases/test-import-5968-csv-doc-abort-partial";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Widget");
      db.command("sql", "CREATE PROPERTY Widget.Score SHORT");
    }

    try {
      final Importer importer = new Importer(("-documents src/test/resources/importer-vertices-outofrange.csv -database "
          + databasePath + " -documentType Widget").split(" "));

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);

      try (final Database db = databaseFactory.open()) {
        // ALICE (ROW 1) WAS ALREADY save()D BEFORE BOB (ROW 2) FAILED: THE WHOLE TRANSACTION, INCLUDING ALICE, MUST BE
        // ROLLED BACK, NOT JUST LEFT OPEN FOR closeDatabase() TO COMMIT ON THE WAY OUT.
        assertThat(db.countType("Widget", true)).isZero();
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * Default "abort" mode has no exclusive-transaction-ownership guard - only "skip" mode rejects an already-active
   * transaction on an externally-managed {@link Database} (see
   * {@link #csvVertexImportRejectsSkipModeInsideActiveTransaction()}) - so it can run directly inside a caller's own
   * transaction that already holds unrelated pending work. A failing row must still abort the import, but the fix in
   * {@link #csvDocumentImportAbortsOnOutOfRangeValueByDefaultAndRollsBackPartialRows()} must not roll back that
   * pre-existing work out from under the caller: only the caller who started that transaction gets to decide what
   * happens to it.
   */
  @Test
  void csvDocumentImportAbortsWithoutDiscardingCallersPendingWorkInExternallyManagedTransaction() {
    final String databasePath = "target/databases/test-import-5968-csv-doc-abort-caller-tx";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Widget");
      db.command("sql", "CREATE PROPERTY Widget.Score SHORT");
      db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

      // The caller starts their own transaction with unrelated pending work before handing the Database to the
      // importer, then calls it directly in default "abort" mode (not "skip", which would be rejected outright).
      db.begin();
      db.newDocument("CallerWork").set("name", "pre-existing").save();

      final Importer importer = new Importer(db, null);
      importer.settings.documents = "src/test/resources/importer-vertices-outofrange.csv";
      importer.settings.documentTypeName = "Widget";

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);

      // The import failed, but the caller's own transaction - and therefore their pre-existing pending work - must
      // still be there for them to decide what to do with, not silently discarded by the importer's failure handling.
      assertThat(db.isTransactionActive()).isTrue();
      db.commit();

      assertThat(db.countType("CallerWork", true)).isEqualTo(1);
      // Alice (row 1) was already save()d into the shared caller transaction before Bob (row 2) failed. Since this
      // transaction isn't ours to roll back (ownsTransaction is false here), Alice survives the caller's own
      // commit() above: "abort" mode's "nothing is imported" guarantee only holds for a self-managed database (see
      // csvDocumentImportAbortsOnOutOfRangeValueByDefaultAndRollsBackPartialRows()), not inside a caller-managed
      // transaction the caller chose not to protect with "skip" mode's exclusive-ownership guard.
      assertThat(db.countType("Widget", true)).isEqualTo(1);
    } finally {
      db.drop();
    }
  }

  /**
   * Symmetric to {@link #csvDocumentImportAbortsWithoutDiscardingCallersPendingWorkInExternallyManagedTransaction()}
   * but for the success path: {@code loadDocuments}' final {@code database.commit()} after the row loop must also be
   * gated on {@code ownsTransaction}, not just the failure-path rollbacks, otherwise a fully-successful import would
   * commit the caller's own pre-existing transaction (and whatever unrelated work it holds) as a side effect.
   */
  @Test
  void csvDocumentImportOnAllSuccessLeavesCallersExternallyManagedTransactionOpenForThemToCommit() {
    final String databasePath = "target/databases/test-import-5968-csv-doc-success-caller-tx";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      // NO Score property/constraint created: unlike the aborting counterpart, every row of the same fixture
      // (including Bob's "99999") imports successfully here, so this exercises the success path, not the failure one.
      db.command("sql", "CREATE DOCUMENT TYPE Widget");
      db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

      db.begin();
      db.newDocument("CallerWork").set("name", "pre-existing").save();

      final Importer importer = new Importer(db, null);
      importer.settings.documents = "src/test/resources/importer-vertices-outofrange.csv";
      importer.settings.documentTypeName = "Widget";

      final Map<String, Object> result = importer.load();
      // ImporterContext#toMap() only adds "errors" when it's non-zero.
      assertThat(result.get("errors")).isNull();

      // The import succeeded, but the transaction was never ours to commit: it must still be active, with the
      // caller's own pre-existing work not yet durable, for the caller to commit (or roll back) themselves.
      assertThat(db.isTransactionActive()).isTrue();
      db.commit();

      assertThat(db.countType("CallerWork", true)).isEqualTo(1);
      assertThat(db.countType("Widget", true)).isEqualTo(3);
    } finally {
      db.drop();
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

  /**
   * Symmetric to {@link #csvDocumentImportAbortsWithoutDiscardingCallersPendingWorkInExternallyManagedTransaction()}:
   * {@code JSONImporterFormat.parseRecords} always begins its own transaction before parsing, regardless of mode -
   * {@code database.begin()} nests rather than reusing one that's already active (see
   * {@code LocalDatabase#begin()}), so its own {@code commit()}/{@code rollback()} can never affect a caller's
   * pre-existing transaction. This locks in that invariant so a future change doesn't accidentally reuse the
   * caller's transaction the way {@code CSVImporterFormat} does.
   */
  @Test
  void jsonDocumentImportAbortsWithoutDiscardingCallersPendingWorkInExternallyManagedTransaction() {
    final String databasePath = "target/databases/test-import-5968-json-doc-abort-caller-tx";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Food");
      db.getSchema().getType("Food").createProperty("qty", Type.SHORT);
      db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

      db.begin();
      db.newDocument("CallerWork").set("name", "pre-existing").save();

      final Importer importer = new Importer(db, "file://src/test/resources/importer-documents-outofrange.json");
      importer.settings.documentTypeName = "Food";
      importer.settings.mapping = "{'*':[]}";

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);

      // The caller's own transaction must survive the import failure intact, with their pre-existing pending work
      // still in it - see the CSV counterpart of this test for the full reasoning.
      assertThat(db.isTransactionActive()).isTrue();
      db.commit();

      assertThat(db.countType("CallerWork", true)).isEqualTo(1);
      // Unlike the CSV counterpart, Apple (record 1) is durably committed here regardless of the caller's own
      // commit() above: each JSON record commits in its own nested transaction (see parseRecords()), and a nested
      // commit() is independently durable even though Banana (record 2) then fails and aborts the whole import.
      assertThat(db.countType("Food", true)).isEqualTo(1);
    } finally {
      db.drop();
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

  /**
   * Default "abort" mode's vertex guarantee is weaker than the document path's: vertices persist via
   * {@code database.async()} in {@code commitEvery}-sized batches, and a persist-time failure only rolls back the
   * batch containing the bad record (see {@code DatabaseAsyncExecutorImpl.executeTask}) - any earlier batch that
   * already committed stays durably persisted. With {@code -commitEvery 2} and row 5 (of 6) failing, rows 1-4 (two
   * full batches) must survive even though the import as a whole still aborts with an {@code ImportException}. This
   * pins down and documents that "abort" here means "fail loudly", not "nothing is imported" the way it does for
   * documents - see {@link #csvDocumentImportAbortsOnOutOfRangeValueByDefaultAndRollsBackPartialRows()} for the
   * contrasting guarantee on the document path. {@code -parallel 1} keeps batch assignment deterministic for the
   * assertion.
   */
  @Test
  void csvVertexImportAbortsOnOutOfRangeValueByDefaultButPriorBatchesSurvive() {
    final String databasePath = "target/databases/test-import-5968-csv-vertex-multibatch-abort";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Score SHORT");
    }

    try {
      final Importer importer = new Importer(
          ("-vertices src/test/resources/importer-vertices-multibatch-outofrange.csv -database " + databasePath
              + " -typeIdProperty Id -typeIdType Long -commitEvery 2 -parallel 1").split(" "));

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);

      try (final Database db = databaseFactory.open()) {
        // Rows 1-4 (batches [1,2] and [3,4]) were already committed before row 5 (batch [5,6]) failed: "abort" mode
        // for vertices fails loudly, but does not roll back everything the way the document path does.
        assertThat(db.countType("Node", true)).isEqualTo(4);
        assertThat(db.lookupByKey("Node", "Id", 1L).hasNext()).isTrue();
        assertThat(db.lookupByKey("Node", "Id", 4L).hasNext()).isTrue();
        assertThat(db.lookupByKey("Node", "Id", 5L).hasNext()).isFalse();
        assertThat(db.lookupByKey("Node", "Id", 6L).hasNext()).isFalse();
      }
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
   * A failure that surfaces AFTER the bucket write - a duplicate value on the unique index {@code loadVertices}
   * auto-creates on {@code -typeIdProperty} - must not leave a "ghost" vertex: present in the bucket (inflating
   * {@code countType()}) but never indexed, because the whole file shared one transaction that only got rolled back
   * on total abort, never on a per-row skip. Each vertex commits in its own transaction in skip mode, so a duplicate
   * key rolls back only that one row.
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
      // THE DUPLICATE IS ONLY DETECTED AT commit() TIME (AFTER save() ALREADY SUCCEEDED), SO createdVertices MUST
      // ONLY BE INCREMENTED ONCE commit() ITSELF SUCCEEDS - OTHERWISE THIS WOULD OVERCOUNT TO 4.
      assertThat(result.get("createdVertices")).isEqualTo(3L);

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
      assertThat(result.get("createdDocuments")).isEqualTo(3L);

      try (final Database db = databaseFactory.open()) {
        assertThat(db.countType("Widget2", true)).isEqualTo(3);
        assertThat(db.lookupByKey("Widget2", "Code", "B").next().getRecord().asDocument().getString("Name")).isEqualTo("Bob");
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * A Type/schema conversion error thrown from a NESTED mapped object (parsed via a recursive {@code parseRecord()}
   * call, before the enclosing object's own {@code endObject()} runs) must not desync the {@code JsonReader} for the
   * rest of the array. Each "Order" has a nested "customer" object independently mapped to its own document type
   * with a {@code SHORT} property that overflows for Bob.
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
      // THE NESTED catch SETS recordFailed BUT DOES NOT ITSELF INCREMENT context.errors: THE OUTER parseRecords()
      // CATCH IS THE SOLE INCREMENT POINT, SO THIS MUST BE EXACTLY 1, NOT JUST NON-NULL.
      assertThat(result.get("errors")).isEqualTo(1L);
      // createRecord() COUNTS A NEW Order/Customer AS SOON AS IT IS ALLOCATED, BEFORE PROPERTIES ARE SET OR THE
      // TRANSACTION COMMITS: BOB'S Order AND ITS NESTED Customer MUST BOTH BE EXCLUDED HERE (2 Orders + 2 Customers),
      // NOT JUST ABSENT FROM THE DATABASE.
      assertThat(result.get("createdDocuments")).isEqualTo(4L);

      try (final Database db = databaseFactory.open()) {
        // THE READER MUST NOT DESYNC AND SILENTLY DROP ORDER 3: BOB'S NESTED Customer FAILS, WHICH DISCARDS BOB'S
        // WHOLE Order TOO (A NESTED FAILURE CAN ONLY BE UNDONE BY ROLLING BACK THE ENCLOSING TOP-LEVEL RECORD'S OWN
        // TRANSACTION - THE NESTED Customer MAY ALREADY HAVE A BUCKET WRITE THAT NOTHING ELSE CAN SAFELY UNDO), BUT
        // ALICE'S AND CAROL'S Orders (AND THEIR Customers) STILL GET CREATED NORMALLY.
        assertThat(db.countType("Order", true)).isEqualTo(2);
        assertThat(db.countType("Customer", true)).isEqualTo(2);
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  private static final String NESTED_VERTEX_MAPPING = """
      {
        "Orders":[
          {
            "@cat":"v",
            "@type":"Order",
            "customer":{
              "@cat":"v",
              "@type":"Customer"
            }
          }
        ]
      }""";

  /**
   * Same scenario as {@link #jsonNestedObjectImportSkipsWholeRecordAndKeepsProcessingSiblingsWhenOptedIn()} but with
   * a {@code "@cat":"v"} mapping instead of {@code "d"}, so the counter snapshot/restore in {@code parseRecords()}
   * exercises {@code context.createdVertices} - the sibling counter to {@code createdDocuments}, otherwise
   * untested by any other case here.
   */
  @Test
  void jsonNestedVertexImportSkipsWholeRecordWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-json-nested-vertex-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE VERTEX TYPE Customer");
      db.getSchema().getType("Customer").createProperty("age", Type.SHORT);
    }

    try {
      final Importer importer = new Importer(new String[] {
          "-url", "file://src/test/resources/importer-documents-nested-outofrange.json",
          "-database", databasePath,
          "-mapping", NESTED_VERTEX_MAPPING,
          "-onRowError", "skip"
      });

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);
      // BOB'S Order AND ITS NESTED Customer MUST BOTH BE EXCLUDED FROM createdVertices, NOT JUST createdDocuments
      // (WHICH THIS MAPPING DOESN'T EVEN TOUCH) - 2 Orders + 2 Customers SURVIVE.
      assertThat(result.get("createdVertices")).isEqualTo(4L);
      assertThat(result.get("createdDocuments")).isNull();

      try (final Database db = databaseFactory.open()) {
        assertThat(db.countType("Order", true)).isEqualTo(2);
        assertThat(db.countType("Customer", true)).isEqualTo(2);
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * "skip" mode commits/rolls back per row, so it must own the transaction outright. Simulates what happens when
   * {@code -onRowError skip} is used from inside a caller-managed transaction (e.g. a server HTTP command executed
   * with the default atomic/{@code autoCommit} behavior, which wraps the whole command in one transaction): rather
   * than the first row's commit silently discarding/committing whatever the caller had pending, this must fail
   * loudly and immediately.
   */
  @Test
  void csvVertexImportRejectsSkipModeInsideActiveTransaction() {
    final String databasePath = "target/databases/test-import-5968-csv-active-tx";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE VERTEX TYPE Node");

      db.begin();
      try {
        final Importer importer = new Importer(db, null);
        importer.settings.vertices = "src/test/resources/importer-vertices-outofrange.csv";
        importer.settings.typeIdProperty = "Id";
        importer.settings.typeIdType = "Long";
        importer.settings.onRowError = "skip";

        assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
            .hasRootCauseInstanceOf(IllegalStateException.class);

        // THE GUARD MUST FIRE BEFORE loadVertices()'S OWN UNIQUE-INDEX AUTO-CREATION SIDE EFFECT, NOT AFTER: THAT
        // database.transaction(...) CALL COMMITS INDEPENDENTLY OF THIS METHOD'S OWN TRANSACTION, SO A GUARD CHECKED
        // TOO LATE WOULD STILL LEAVE THE INDEX BEHIND EVEN THOUGH THE IMPORT ITSELF WAS REJECTED. (THE Id PROPERTY
        // ITSELF ALREADY EXISTS REGARDLESS OF THE GUARD - Importer#loadFromSource() AUTO-CREATES IT FROM SCHEMA
        // ANALYSIS BEFORE format.load() EVEN RUNS - SO ONLY THE INDEX IS A MEANINGFUL SIGNAL HERE.)
        assertThat(db.getSchema().getType("Node").getIndexesByProperties("Id")).isEmpty();
      } finally {
        if (db.isTransactionActive())
          db.rollback();
      }
    } finally {
      db.drop();
    }
  }

  @Test
  void jsonDocumentImportRejectsSkipModeInsideActiveTransaction() {
    final String databasePath = "target/databases/test-import-5968-json-active-tx";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Food");
      db.getSchema().getType("Food").createProperty("qty", Type.SHORT);

      db.begin();
      try {
        final Importer importer = new Importer(db, "file://src/test/resources/importer-documents-outofrange.json");
        importer.settings.mapping = "{'*':[]}";
        importer.settings.documentTypeName = "Food";
        importer.settings.onRowError = "skip";

        assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
            .hasRootCauseInstanceOf(IllegalStateException.class);
      } finally {
        if (db.isTransactionActive())
          db.rollback();
      }
    } finally {
      db.drop();
    }
  }

  /**
   * Symmetric to {@link #sqlImportDatabaseSupportsOnRowErrorSkipSetting()} but for CSV vertices (routed through
   * {@code loadVertices} instead of {@code loadDocuments}) via the embedding {@code Importer(Database, String)}
   * constructor directly, with no pre-existing active transaction: confirms skip mode works normally through this
   * entry point when it isn't rejected by the guard exercised in
   * {@link #csvVertexImportRejectsSkipModeInsideActiveTransaction()}.
   */
  @Test
  void csvVertexImportSupportsSkipModeViaEmbeddingConstructorWithoutActiveTransaction() {
    final String databasePath = "target/databases/test-import-5968-csv-embed-vertex";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Score SHORT");

      final Importer importer = new Importer(db, null);
      importer.settings.vertices = "src/test/resources/importer-vertices-outofrange.csv";
      importer.settings.typeIdProperty = "Id";
      importer.settings.typeIdType = "Long";
      importer.settings.onRowError = "skip";

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);
      assertThat(db.countType("Node", true)).isEqualTo(2);
    } finally {
      db.drop();
    }
  }

  /**
   * Unlike every other embedding-constructor test above, this one does NOT pre-create the vertex type: with no
   * {@code Node} type yet, {@code Importer#loadFromSource -> updateDatabaseSchema -> getOrCreateVertexType} begins a
   * transaction for the schema DDL and never commits it, so by the time {@code loadVertices} runs, a transaction is
   * already active - not the caller's (there wasn't one), but this importer's own dangling schema-creation one.
   * Traced empirically: a subsequent {@code database.rollback()} does not actually undo the type/property/index
   * creation bundled into that same transaction (schema mutations aren't governed by the enclosing data
   * transaction's rollback), so a first-row failure in "skip" mode was already safe here even before
   * {@code beginRowTransaction} in {@code CSVImporterFormat} started committing that dangling transaction on its own
   * before the per-row loop begins. This test locks in the observed behavior regardless of that underlying
   * mechanism, and the accompanying fix removes any future dependency on it.
   */
  @Test
  void csvVertexImportSkipModeSurvivesFirstRowFailureWhenSchemaAutoCreatedViaEmbeddingConstructor() {
    final String databasePath = "target/databases/test-import-5968-csv-embed-vertex-schema-autocreate";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      final Importer importer = new Importer(db, null);
      importer.settings.vertices = "src/test/resources/importer-vertices-outofrange-firstrow.csv";
      importer.settings.vertexTypeName = "Node";
      importer.settings.typeIdProperty = "Id";
      importer.settings.typeIdType = "Long";
      importer.settings.onRowError = "skip";

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);

      // The Node type must have survived (it wasn't rolled back along with row 1's own failed save()), and Bob/Carol
      // (rows 2 and 3, both valid) must have imported normally afterward - proving the bad first row only cost
      // itself, not the type it needed to exist.
      assertThat(db.getSchema().existsType("Node")).isTrue();
      assertThat(db.countType("Node", true)).isEqualTo(2);
      assertThat(db.lookupByKey("Node", "Id", 2L).next().getRecord().asVertex().getString("Name")).isEqualTo("Bob");
      assertThat(db.lookupByKey("Node", "Id", 3L).next().getRecord().asVertex().getString("Name")).isEqualTo("Carol");
      assertThat(db.lookupByKey("Node", "Id", 1L).hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  /**
   * A source-level parse failure (see {@link #csvVertexImportStillAbortsOnSyntaxLevelParseFailureWhenOptedIn()}) that
   * escapes the per-row loop while skip mode's own transaction machinery is active must not leave a dangling active
   * transaction behind on an externally-managed {@link Database} - the caller never had one active before this call
   * (see the entry guard exercised in {@link #csvVertexImportRejectsSkipModeInsideActiveTransaction()}), and
   * shouldn't be left holding one afterwards either.
   */
  @Test
  void csvVertexImportRollsBackDanglingTransactionOnSyntaxLevelParseFailureViaEmbeddingConstructor() {
    final String databasePath = "target/databases/test-import-5968-csv-embed-syntax-abort";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE VERTEX TYPE Node");

      final Importer importer = new Importer(db, null);
      importer.settings.vertices = "src/test/resources/importer-vertices-oversized-value.csv";
      importer.settings.typeIdProperty = "Id";
      importer.settings.typeIdType = "Long";
      importer.settings.onRowError = "skip";
      importer.settings.parseParameter("maxPropertySize", "10");

      // TextParsingException ITSELF WRAPS A LOWER-LEVEL CAUSE, SO ASSERT ON THE DIRECT CAUSE OF ImportException RATHER
      // THAN THE ROOT ONE.
      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .cause().isInstanceOf(TextParsingException.class);

      assertThat(db.isTransactionActive()).isFalse();
    } finally {
      db.drop();
    }
  }

  /**
   * A CSV-syntax-level parse failure (a value exceeding {@code maxPropertySize}, causing univocity-parsers'
   * {@code TextParsingException}) must still abort the import even in skip mode: {@code csvParser.parseNext()} is
   * deliberately left outside the per-row try/catch (see the comment in {@code loadDocuments}/{@code loadVertices}).
   */
  @Test
  void csvVertexImportStillAbortsOnSyntaxLevelParseFailureWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-csv-syntax-abort";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try {
      final Importer importer = new Importer(
          ("-vertices src/test/resources/importer-vertices-oversized-value.csv -database " + databasePath
              + " -typeIdProperty Id -typeIdType Long -forceDatabaseCreate true -maxPropertySize 10 -onRowError skip")
              .split(" "));

      // TextParsingException ITSELF WRAPS A LOWER-LEVEL CAUSE, SO ASSERT ON THE DIRECT CAUSE OF ImportException RATHER
      // THAN THE ROOT ONE.
      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .cause().isInstanceOf(TextParsingException.class);
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * A genuinely malformed JSON structure (here, a missing closing brace) must still abort the import even in skip
   * mode: {@code IOException} is never caught by the per-record/nested try/catch blocks, only {@code RuntimeException}.
   */
  @Test
  void jsonDocumentImportStillAbortsOnMalformedStructureWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-json-malformed-abort";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try {
      final Importer importer = new Importer(new String[] {
          // .json.txt (NOT .json): AN INTENTIONALLY-MALFORMED JSON FIXTURE WOULD FAIL THE REPO'S check-json PRE-COMMIT
          // HOOK OTHERWISE. THE IMPORTER DETECTS THE FORMAT FROM CONTENT (A LEADING '{'), NOT THE FILE EXTENSION.
          "-url", "file://src/test/resources/importer-documents-malformed.json.txt",
          "-database", databasePath,
          "-mapping", NESTED_MAPPING,
          "-onRowError", "skip"
      });

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IOException.class);
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * Multiple, non-consecutive-in-effect bad rows in the same file must all be individually skipped and counted,
   * with every good row still imported - not just the single-bad-row case the other tests exercise.
   */
  @Test
  void csvVertexImportSkipsMultipleOutOfRangeRowsWhenOptedIn() {
    final String databasePath = "target/databases/test-import-5968-csv-multi-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Score SHORT");
    }

    try {
      final Importer importer = new Importer(
          ("-vertices src/test/resources/importer-vertices-multiple-outofrange.csv -database " + databasePath
              + " -typeIdProperty Id -typeIdType Long -onRowError skip").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(2L);

      try (final Database db = databaseFactory.open()) {
        assertThat(db.countType("Node", true)).isEqualTo(2);
        assertThat(db.lookupByKey("Node", "Id", 1L).next().getRecord().asVertex().getString("Name")).isEqualTo("Alice");
        assertThat(db.lookupByKey("Node", "Id", 4L).next().getRecord().asVertex().getString("Name")).isEqualTo("Dave");
        assertThat(db.lookupByKey("Node", "Id", 2L).hasNext()).isFalse();
        assertThat(db.lookupByKey("Node", "Id", 3L).hasNext()).isFalse();
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

  /**
   * The CLI-arg tests above all go through {@code new Importer(String[])}, which parses {@code -onRowError} directly.
   * {@code IMPORT DATABASE ... WITH onRowError=skip} is a separate, reflection-based entry point
   * ({@code ImportDatabaseStatement} forwards every {@code WITH} key/value to {@code ImporterSettings.parseParameter}),
   * so it needs its own regression coverage to lock in that integration point.
   */
  @Test
  void sqlImportDatabaseSupportsOnRowErrorSkipSetting() {
    final String databasePath = "target/databases/test-import-5968-sql-onrowerror";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Document");
      db.command("sql", "CREATE PROPERTY Document.Score SHORT");

      db.command("sql", """
          IMPORT DATABASE file://src/test/resources/importer-vertices-outofrange.csv
          WITH onRowError=skip
          """);

      // BOB'S "99999" DOES NOT FIT A SHORT: THAT ROW IS SKIPPED, THE OTHER TWO ARE IMPORTED AS DOCUMENTS (THE DEFAULT
      // ENTITY TYPE FOR THE PRIMARY -url/DATABASE SOURCE)
      assertThat(db.countType("Document", true)).isEqualTo(2);
    } finally {
      db.drop();
    }
  }
}
