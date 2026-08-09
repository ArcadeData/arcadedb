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

  /**
   * A synchronous per-row failure (e.g. {@code v.set(...)} throwing on an out-of-range value, as opposed to one only
   * caught by the async {@code onError} handler) rethrows straight out of {@code loadVertices}' per-row loop in
   * default "abort" mode, skipping the {@code database.async().waitCompletion()} call that normally runs right after
   * the loop completes successfully. For a self-managed database this was masked by {@code database.close()}
   * internally draining the async queue on the way out - but for an externally-managed one (the embedding
   * constructor here, and critically {@code IMPORT DATABASE} over SQL/HTTP, which reuses the live server
   * {@code Database}), {@code closeDatabase()} is a no-op, so without an explicit drain this method could return
   * control to the caller while an earlier row's async write was still in flight and uncounted.
   */
  @Test
  void csvVertexImportDrainsAsyncQueueBeforeReturningOnSynchronousFailureInExternallyManagedDatabase() {
    final String databasePath = "target/databases/test-import-5968-csv-vertex-async-drain";

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

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);

      // Alice (row 1) was already queued via database.async() before Bob (row 2) failed synchronously. By the time
      // load() has thrown, that queued write must already be durable - not just eventually consistent - since
      // nothing else in this call path (no closeDatabase() for an externally-managed database) will ever drain it.
      assertThat(db.countType("Node", true)).isEqualTo(1);
    } finally {
      db.drop();
    }
  }

  /**
   * Symmetric to {@link #csvDocumentImportAbortsWithoutDiscardingCallersPendingWorkInExternallyManagedTransaction()}
   * but for vertices: in default "abort" mode, {@code loadVertices} never touches the foreground transaction at all
   * (vertices persist via {@code database.async()}, a separate mechanism), so a caller's own pre-existing
   * transaction and its unrelated pending work must survive completely untouched - not just "not rolled back" the
   * way the document path's shared-transaction case is, but never interacted with in the first place.
   */
  @Test
  void csvVertexImportAbortsWithoutTouchingCallersPendingWorkInExternallyManagedTransaction() {
    final String databasePath = "target/databases/test-import-5968-csv-vertex-abort-caller-tx";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Score SHORT");
      db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

      // The caller starts their own transaction with unrelated pending work before handing the Database to the
      // importer, then calls it directly in default "abort" mode.
      db.begin();
      db.newDocument("CallerWork").set("name", "pre-existing").save();

      final Importer importer = new Importer(db, null);
      importer.settings.vertices = "src/test/resources/importer-vertices-outofrange.csv";
      importer.settings.typeIdProperty = "Id";
      importer.settings.typeIdType = "Long";

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);

      // The caller's own transaction must still be active, with their pre-existing pending work still in it.
      assertThat(db.isTransactionActive()).isTrue();
      db.commit();

      assertThat(db.countType("CallerWork", true)).isEqualTo(1);
    } finally {
      db.drop();
    }
  }

  /**
   * {@code loadEdges} is out of scope for the {@code -onRowError skip} feature itself (edges already skip-and-log
   * unconditionally, see the class-level reasoning in {@code CSVImporterFormat}), so it has none of the
   * {@code ownsTransaction}/{@code callerTransactionActiveOnEntry} machinery the other two paths use. It calls
   * {@code database.begin()} unconditionally, though, exactly like {@code JSONImporterFormat.parseRecords} - since
   * {@code LocalDatabase#begin()} nests rather than reuses an already-active transaction, this locks in that
   * loadEdges is safe by the same mechanism, without needing any of the CSV document/vertex-style guards: a caller's
   * own pre-existing transaction and its unrelated pending work must survive an edges import untouched, exactly like
   * the vertex/document counterparts above.
   */
  @Test
  void csvEdgeImportDoesNotTouchCallersPendingWorkInExternallyManagedTransaction() {
    final String databasePath = "target/databases/test-import-5968-csv-edge-caller-tx";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    // Vertices imported first, in a separate self-managed run, so the edges import below only has to resolve
    // existing from/to references, not also create the vertex type.
    final Importer verticesImporter = new Importer(
        ("-vertices src/test/resources/importer-vertices.csv -database " + databasePath
            + " -typeIdProperty Id -typeIdType Long -typeIdPropertyIsUnique true -forceDatabaseCreate true").split(" "));
    verticesImporter.load();

    final Database db = databaseFactory.open();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

      // The caller starts their own transaction with unrelated pending work before handing the Database to the
      // importer.
      db.begin();
      db.newDocument("CallerWork").set("name", "pre-existing").save();

      final Importer edgesImporter = new Importer(db, null);
      edgesImporter.settings.edges = "src/test/resources/importer-edges.csv";
      edgesImporter.settings.typeIdProperty = "Id";
      edgesImporter.settings.typeIdType = "Long";
      edgesImporter.settings.edgeFromField = "From";
      edgesImporter.settings.edgeToField = "To";

      edgesImporter.load();

      // The caller's own transaction must still be active, with their pre-existing pending work still in it -
      // committed here, by the caller, exactly as if the edges import had never run inside it.
      assertThat(db.isTransactionActive()).isTrue();
      db.commit();

      assertThat(db.countType("CallerWork", true)).isEqualTo(1);
      assertThat(db.countType("Relationship", true)).isGreaterThan(0);
    } finally {
      db.drop();
    }
  }

  /**
   * Edges already skip-and-log unconditionally regardless of {@code -onRowError} (see the class-level reasoning in
   * {@code CSVImporterFormat}), so setting {@code -onRowError skip} for an edges import is a documented no-op, not a
   * rejection or a crash - this locks in that the import still succeeds normally and creates the same edges either
   * way.
   */
  @Test
  void csvEdgeImportSkipModeIsANoOpButStillSucceeds() {
    final String databasePath = "target/databases/test-import-5968-csv-edge-skip-noop";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Importer verticesImporter = new Importer(
        ("-vertices src/test/resources/importer-vertices.csv -database " + databasePath
            + " -typeIdProperty Id -typeIdType Long -typeIdPropertyIsUnique true -forceDatabaseCreate true").split(" "));
    verticesImporter.load();

    try {
      final Importer edgesImporter = new Importer(
          ("-edges src/test/resources/importer-edges.csv -database " + databasePath
              + " -typeIdProperty Id -typeIdType Long -edgeFromField From -edgeToField To -onRowError skip").split(" "));
      edgesImporter.load();

      try (final Database db = databaseFactory.open()) {
        assertThat(db.countType("Relationship", true)).isGreaterThan(0);
      }
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
        // Bob's "99999" does not fit a SHORT: that row is skipped, the other two are imported
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
   * here guarantees an exact surviving count. Deliberately reuses {@code importer-vertices-outofrange.csv} rather
   * than a document-specific fixture with the same shape - the CSV content itself is entity-agnostic (rows/columns),
   * only {@code -documentType Widget} below routes it through the document path instead of the vertex one.
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
        // Bob's "99999" does not fit a SHORT: that row is skipped, the other two are imported
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

      // Alice (row 1) was already save()d and had already incremented createdDocuments locally before Bob (row 2)
      // failed and rolled back the whole transaction, including Alice: createdDocuments must not still report her
      // as created, even though the exception path never returns context.toMap() to a normal caller - getContext()
      // remains directly reachable, and a stale count here would misrepresent what actually persisted.
      assertThat(importer.getContext().createdDocuments.get()).isZero();

      try (final Database db = databaseFactory.open()) {
        // Alice (row 1) was already save()d before Bob (row 2) failed: the whole transaction, including Alice, must
        // be rolled back, not just left open for closeDatabase() to commit on the way out.
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

      // Deliberate: this failed import's own summary does not credit Alice, even though she is about to survive the
      // caller's commit() below. Whether Alice ultimately becomes durable is the caller's own decision on a
      // transaction this import never controlled, not something a FAILED import can vouch for in its own summary.
      assertThat(importer.getContext().createdDocuments.get()).isEqualTo(0);

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
        // Banana's "99999" does not fit a SHORT: that record is skipped, the other two are imported
        assertThat(db.countType("Food", true)).isEqualTo(2);
        db.iterateType("Food", true).forEachRemaining(record -> assertThat(record.asDocument().getString("name"))
            .isIn("Apple", "Cherry"));
      }
    } finally {
      databaseFactory.open().drop();
    }
  }

  /**
   * {@code -onRowError skip} has no effect on a single top-level JSON object (no {@code -mapping} set, so
   * {@code mapping == null} in {@code JSONImporterFormat.load()}): there is no sibling record to continue with on
   * failure, so this path skips the {@code callerTransactionActiveOnEntry} guard and error recovery entirely, and
   * just logs an INFO notice. Locks in that the import still succeeds normally in that case - it's a documented
   * no-op, not a rejection or a crash.
   */
  @Test
  void jsonDocumentImportSkipModeHasNoEffectOnSingleTopLevelObjectButStillSucceeds() {
    final String databasePath = "target/databases/test-import-5968-json-single-object-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Document");
    }

    try {
      final Importer importer = new Importer(
          ("-url file://src/test/resources/importer-single-object.json -database " + databasePath
              + " -onRowError skip").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isNull();

      try (final Database db = databaseFactory.open()) {
        assertThat(db.countType("Document", true)).isEqualTo(1);
        assertThat(db.iterateType("Document", true).next().getRecord().asDocument().getString("name")).isEqualTo("Alice");
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
        // Bob is missing the mandatory Email property: it fails at persist time (DocumentValidator, not the
        // synchronous Type-conversion path), the other two are imported. "skip" mode saves each vertex synchronously
        // in its own transaction (see loadVertices), so a failing row's rollback() can never touch a previously
        // committed sibling - which is why the surviving count can be asserted exactly here.
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
      // The duplicate is only detected at commit() time (after save() already succeeded), so createdVertices must
      // only be incremented once commit() itself succeeds - otherwise this would overcount to 4.
      assertThat(result.get("createdVertices")).isEqualTo(3L);

      try (final Database db = databaseFactory.open()) {
        // Bob's duplicate Id=2 row must leave no trace: neither a second index entry nor an unindexed ghost in the
        // bucket. If the ghost-record bug were present, countType() would report 4 instead of 3.
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
      // The nested catch sets recordFailed but does not itself increment context.errors: the outer parseRecords()
      // catch is the sole increment point, so this must be exactly 1, not just non-null.
      assertThat(result.get("errors")).isEqualTo(1L);
      // createRecord() counts a new Order/Customer as soon as it is allocated, before properties are set or the
      // transaction commits: Bob's Order and its nested Customer must both be excluded here (2 Orders + 2 Customers),
      // not just absent from the database.
      assertThat(result.get("createdDocuments")).isEqualTo(4L);

      try (final Database db = databaseFactory.open()) {
        // The reader must not desync and silently drop Order 3: Bob's nested Customer fails, which discards Bob's
        // whole Order too (a nested failure can only be undone by rolling back the enclosing top-level record's own
        // transaction - the nested Customer may already have a bucket write that nothing else can safely undo), but
        // Alice's and Carol's Orders (and their Customers) still get created normally.
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
      // Bob's Order and its nested Customer must both be excluded from createdVertices, not just createdDocuments
      // (which this mapping doesn't even touch) - 2 Orders + 2 Customers survive.
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

        // The guard must fire before loadVertices()'s own unique-index auto-creation side effect, not after: that
        // database.transaction(...) call commits independently of this method's own transaction, so a guard checked
        // too late would still leave the index behind even though the import itself was rejected. (The Id property
        // itself already exists regardless of the guard - Importer#loadFromSource() auto-creates it from schema
        // analysis before format.load() even runs - so only the index is a meaningful signal here.)
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
   * {@code database.async().onError()} has no getter to save/restore whatever handler was registered before this
   * call, and it replaces rather than stacks - so without {@code handlerActive} making it inert once
   * {@code loadVertices} has returned, this import's own handler would keep routing a caller's later, unrelated
   * {@code database.async()} failures (on the same, reused, externally-managed {@code Database}) into this call's
   * own now-stale {@code ImporterContext} instead of wherever the caller's own error handling expects them.
   */
  @Test
  void csvVertexImportDeactivatesAsyncErrorHandlerAfterCompletingSoLaterUnrelatedFailuresAreNotMisattributed() {
    final String databasePath = "target/databases/test-import-5968-csv-vertex-handler-cleanup";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Email STRING (MANDATORY TRUE)");

      final Importer importer = new Importer(db, null);
      importer.settings.vertices = "src/test/resources/importer-vertices-missing-mandatory.csv";
      importer.settings.typeIdProperty = "Id";
      importer.settings.typeIdType = "Long";

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(ValidationException.class);

      final long errorsAfterImport = importer.getContext().errors.get();
      assertThat(errorsAfterImport).isGreaterThan(0);

      // The caller keeps using the same Database after the import finished and triggers their own, unrelated
      // async failure directly - the same mandatory-property violation, but on a vertex this import never touched.
      db.async().createRecord(db.newVertex("Node"), doc -> {
      });
      db.async().waitCompletion();

      // A leaked, still-active handler would have routed that failure into the finished import's own context too.
      assertThat(importer.getContext().errors.get()).isEqualTo(errorsAfterImport);
    } finally {
      db.drop();
    }
  }

  /**
   * Explicitly setting {@code -commitEvery}/{@code -parallel} alongside {@code -onRowError skip} has no effect on
   * vertices (skip mode saves synchronously, one at a time, never touching {@code database.async()}) - this locks
   * in that the combination is still functionally correct (no crash, no leftover async worker state, correct skip
   * accounting) despite settings that would otherwise tune batching/parallelism being silently inapplicable.
   */
  @Test
  void csvVertexImportSkipModeIgnoresExplicitCommitEveryAndParallelWithoutError() {
    final String databasePath = "target/databases/test-import-5968-csv-skip-commitevery-parallel";

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
              + " -typeIdProperty Id -typeIdType Long -onRowError skip -commitEvery 1 -parallel 4").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(1L);
      assertThat(result.get("createdVertices")).isEqualTo(2L);

      try (final Database db = databaseFactory.open()) {
        assertThat(db.countType("Node", true)).isEqualTo(2);
      }
    } finally {
      databaseFactory.open().drop();
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
      importer.settings.vertices = "src/test/resources/importer-outofrange-firstrow.csv";
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
   * Symmetric to {@link #csvVertexImportSkipModeSurvivesFirstRowFailureWhenSchemaAutoCreatedViaEmbeddingConstructor()}
   * but for default "abort" mode: row 1's failure is a synchronous {@code v.set()} type-conversion error (the
   * updateDatabaseSchema()-inferred {@code Node.Id} property is LONG, and "notanumber" can't convert), caught by
   * {@code loadVertices}' per-row {@code catch (RuntimeException)} - the same {@code ownsTransaction} rollback path
   * added by this PR that previously never existed for vertices at all. Pins down that the auto-created type/index
   * still survive a synchronous (not just an async persist-time) failure, mirroring the document-side guarantee.
   */
  @Test
  void csvVertexImportAbortsOnFirstRowFailureWhenSchemaAutoCreatedViaEmbeddingConstructor() {
    final String databasePath = "target/databases/test-import-5968-csv-embed-vertex-abort-schema-autocreate";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      final Importer importer = new Importer(db, null);
      importer.settings.vertices = "src/test/resources/importer-outofrange-firstrow.csv";
      importer.settings.vertexTypeName = "Node";
      importer.settings.typeIdProperty = "Id";
      importer.settings.typeIdType = "Long";

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);

      // The Node type (and its Id index) survive even though the whole import aborts on row 1 - schema mutations
      // aren't undone by a data-transaction rollback (see the "skip" mode counterpart). Row 1 fails synchronously,
      // before ever reaching the loop's database.async().createRecord() call, so - unlike a purely async persist-time
      // failure - rows 2/3 are never even attempted: the loop rethrows straight out on row 1.
      assertThat(db.getSchema().existsType("Node")).isTrue();
      assertThat(db.getSchema().getType("Node").existsProperty("Id")).isTrue();
      assertThat(db.countType("Node", true)).isZero();
    } finally {
      db.drop();
    }
  }

  /**
   * Symmetric to {@link #csvVertexImportSkipModeSurvivesFirstRowFailureWhenSchemaAutoCreatedViaEmbeddingConstructor()}
   * but for documents in default "abort" mode, which - unlike that "skip" mode test - reaches
   * {@code loadDocuments}'s method-level catch and its whole-file rollback instead of a per-row one. Pins down that
   * the auto-created {@code Widget} type itself is NOT rolled back along with row 1's own failed data, even though
   * {@link #csvDocumentImportAbortsOnOutOfRangeValueByDefaultAndRollsBackPartialRows()}'s "nothing is imported"
   * framing might otherwise suggest the whole transaction - schema included - goes with it.
   */
  @Test
  void csvDocumentImportAbortsOnFirstRowFailureWhenSchemaAutoCreatedViaEmbeddingConstructor() {
    final String databasePath = "target/databases/test-import-5968-csv-embed-doc-schema-autocreate";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      final Importer importer = new Importer(db, null);
      importer.settings.documents = "src/test/resources/importer-outofrange-firstrow.csv";
      importer.settings.documentTypeName = "Widget";
      importer.settings.typeIdProperty = "Id";
      importer.settings.typeIdType = "Long";

      assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
          .hasRootCauseInstanceOf(IllegalArgumentException.class);

      // The Widget type (auto-created by updateDatabaseSchema() since it didn't pre-exist) survives even though the
      // whole import aborts and no document is imported: schema mutations aren't undone by the data transaction's
      // rollback (see the "skip" mode counterpart for how this was traced), so re-running the import after fixing
      // row 1 wouldn't need to re-create the type from scratch.
      assertThat(db.getSchema().existsType("Widget")).isTrue();
      assertThat(db.countType("Widget", true)).isZero();
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

      // TextParsingException itself wraps a lower-level cause, so assert on the direct cause of ImportException
      // rather than the root one.
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

      // TextParsingException itself wraps a lower-level cause, so assert on the direct cause of ImportException
      // rather than the root one.
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
          // .json.txt (not .json): an intentionally-malformed JSON fixture would fail the repo's check-json
          // pre-commit hook otherwise. The importer detects the format from content (a leading '{'), not the file
          // extension.
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

      // Bob's "99999" does not fit a SHORT: that row is skipped, the other two are imported as documents (the
      // default entity type for the primary -url/DATABASE source).
      assertThat(db.countType("Document", true)).isEqualTo(2);
    } finally {
      db.drop();
    }
  }

  /**
   * The scenario {@code newExclusiveTransactionRequiredException()}'s own message and the release notes call out as
   * the primary reason for the guard: a plain {@code IMPORT DATABASE ... WITH onRowError=skip} over HTTP, where
   * {@code DatabaseAbstractHandler} wraps the whole command in its own atomic transaction by default.
   * {@link #csvVertexImportRejectsSkipModeInsideActiveTransaction()}/
   * {@link #jsonDocumentImportRejectsSkipModeInsideActiveTransaction()} exercise the guard through the lower-level
   * embedding constructor with a manual {@code db.begin()}, not through {@code ImportDatabaseStatement} itself -
   * {@code db.transaction(() -> ...)} reproduces the same "already active when the statement runs" condition
   * {@code DatabaseAbstractHandler} creates, without needing an actual HTTP server in this test.
   */
  @Test
  void sqlImportDatabaseRejectsOnRowErrorSkipInsideAtomicTransactionLikeHttpDoes() {
    final String databasePath = "target/databases/test-import-5968-sql-onrowerror-atomic";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "CREATE DOCUMENT TYPE Document");

      assertThatThrownBy(() -> db.transaction(() -> db.command("sql", """
          IMPORT DATABASE file://src/test/resources/importer-vertices-outofrange.csv
          WITH onRowError=skip
          """))).hasRootCauseInstanceOf(IllegalStateException.class)
          .hasRootCauseMessage(ImporterSettings.newExclusiveTransactionRequiredException().getMessage());

      // Nothing from the rejected import should have been committed either.
      assertThat(db.countType("Document", true)).isZero();
    } finally {
      db.drop();
    }
  }

  /**
   * Every other skip-mode test above sets exactly one of {@code -documents}/{@code -vertices}. Both go through
   * {@code loadFromSource()} in the same {@code Importer#load()} call, each with its own independently-computed
   * {@code TransactionOwnership} (see {@code CSVImporterFormat#computeTransactionOwnership}) and its own commit
   * cycle, so this locks in that a bad row in one source doesn't affect the other when both are imported together.
   */
  @Test
  void csvDocumentAndVertexImportBothSkipIndependentlyInTheSameRun() {
    final String databasePath = "target/databases/test-import-5968-csv-combined-skip";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    try (final Database db = databaseFactory.create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Widget");
      db.command("sql", "CREATE PROPERTY Widget.Score SHORT");
      db.command("sql", "CREATE VERTEX TYPE Node");
      db.command("sql", "CREATE PROPERTY Node.Score SHORT");
    }

    try {
      final Importer importer = new Importer(
          ("-documents src/test/resources/importer-vertices-outofrange.csv -documentType Widget"
              + " -vertices src/test/resources/importer-vertices-outofrange.csv -vertexType Node"
              + " -typeIdProperty Id -typeIdType Long -database " + databasePath + " -onRowError skip").split(" "));

      final Map<String, Object> result = importer.load();
      assertThat(result.get("errors")).isEqualTo(2L);

      try (final Database db = databaseFactory.open()) {
        assertThat(db.countType("Widget", true)).isEqualTo(2);
        assertThat(db.countType("Node", true)).isEqualTo(2);
      }
    } finally {
      databaseFactory.open().drop();
    }
  }
}
