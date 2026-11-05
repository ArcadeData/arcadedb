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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.integration.importer.format.JsonlImporterFormat;
import com.arcadedb.integration.importer.format.RDFImporterFormat;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11208: every importer row loop resolves its transaction through {@link ImporterTransactions}, so a commit the engine
 * refuses before it starts - a rollback-only transaction, a terminated statement, a server that stopped accepting writes -
 * leaves nothing on the caller's thread, whichever format the import used. {@code JSONImporterFormat} cleared its ownership
 * flag before such a commit and never restored it, so the rollback that should have discarded the refused transaction skipped
 * it: the outermost transaction is not popped by a failed commit, and it stayed active for the life of the thread, swallowing
 * the caller's next {@code begin()}/{@code commit()} pair. The CSV edge loop and the RDF loop restored the flag by hand; they
 * now share the protocol, and are pinned here against the same real refusal. So does the JSONL row loop (issue #11271), whose
 * commit sat in a {@code finally} with no handler after it; {@code ImporterCommitsThroughOneProtocolCensusTest} (census module)
 * keeps every loop on the protocol.
 * <p>
 * The refusal is the engine's own: a listener marks the import's transaction rollback-only, and {@code commit1stPhase()}
 * refuses it before doing anything, without rolling back first. Nothing is proxied.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ImporterRefusedCommitTest {
  private static final String DB_PATH   = "target/databases/importer-refused-commit-test";
  private static final String DATA_PATH = "target/test-data/importer-refused-commit-test";

  private Database database;
  private int      refusals;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
    new File(DATA_PATH).mkdirs();
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Document");
      database.getSchema().createDocumentType("Marker");
      database.getSchema().createVertexType("Node").createProperty("Id", Type.STRING);
      database.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Id");
      database.getSchema().createEdgeType("Relationship");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
  }

  @Test
  void aRefusedJsonCommitLeavesNoTransactionOnTheThread() throws IOException {
    refuseTheCommitOf("Document");
    final Importer importer = new Importer(database, write("records.json", "[{\"name\":\"a\"},{\"name\":\"b\"}]"));

    assertThatThrownBy(importer::load).hasStackTraceContaining("issue 11208: refused commit");

    assertThat(refusals).isEqualTo(1);
    assertNothingLeftOnTheThread();
    assertThat(countOf("Document")).isZero();
  }

  /** What the stranded transaction did: the caller's next unit of work nested inside it and never ended. */
  @Test
  void theCallersNextUnitOfWorkAfterARefusedJsonImportIsItsOwn() throws IOException {
    refuseTheCommitOf("Document");
    final Importer importer = new Importer(database, write("records.json", "[{\"name\":\"a\"},{\"name\":\"b\"}]"));
    assertThatThrownBy(importer::load).hasStackTraceContaining("issue 11208: refused commit");

    database.begin();
    database.newDocument("Marker").set("name", "next").save();
    database.commit();

    assertThat(database.isTransactionActive()).as("one begin/commit pair leaves nothing active").isFalse();
    assertThat(countOf("Marker")).isEqualTo(1);
  }

  @Test
  void aRefusedCsvEdgeCommitLeavesNoTransactionOnTheThread() throws IOException {
    database.transaction(() -> {
      for (final String id : new String[] { "0", "1", "2" })
        database.newVertex("Node").set("Id", id).save();
    });
    refuseTheCommitOf("Relationship");

    final Importer importer = new Importer(database, null);
    importer.setSettings(Map.of(//
        "edges", write("edges.csv", "From,To", "0,1", "1,2"),//
        "edgeFromField", "From",//
        "edgeToField", "To",//
        "typeIdProperty", "Id",//
        "typeIdType", "String"));

    assertThatThrownBy(importer::load).hasStackTraceContaining("issue 11208: refused commit");

    assertThat(refusals).as("both edges land in the one batch whose commit is refused").isEqualTo(2);
    assertNothingLeftOnTheThread();
    assertThat(countOf("Relationship")).isZero();
  }

  @Test
  void aRefusedRdfCommitLeavesNoTransactionOnTheThread() throws Exception {
    refuseTheCommitOf("Relationship");
    final ImporterSettings settings = new ImporterSettings();
    settings.vertexTypeName = "Node";
    settings.edgeTypeName = "Relationship";
    settings.typeIdProperty = "Id";
    final byte[] bytes = "v1,rel,v2\nv2,rel,v3\n".getBytes(StandardCharsets.UTF_8);
    final Parser parser = new Parser(new Source("test.rdf", new ByteArrayInputStream(bytes), bytes.length, false, null, null), 0);

    assertThatThrownBy(
        () -> new RDFImporterFormat().load(null, null, parser, (DatabaseInternal) database, new ImporterContext(), settings))
        .hasStackTraceContaining("issue 11208: refused commit");

    assertThat(refusals).as("both edges land in the one batch whose commit is refused").isEqualTo(2);
    assertNothingLeftOnTheThread();
    assertThat(countOf("Relationship")).isZero();
  }

  /**
   * The other arm of the protocol, the reason the flag is restored only by identity: under a caller's transaction the edge
   * loop's own is nested, a refused commit pops it, and the caller's transaction it uncovers stays the caller's to resolve.
   */
  @Test
  void aRefusedNestedCsvEdgeCommitLeavesTheCallersTransactionAlone() throws IOException {
    database.transaction(() -> {
      for (final String id : new String[] { "0", "1", "2" })
        database.newVertex("Node").set("Id", id).save();
    });
    refuseTheCommitOf("Relationship");

    database.begin();
    final TransactionContext callers = ((DatabaseInternal) database).getTransaction();
    database.newDocument("Marker").set("name", "caller").save();

    final Importer importer = new Importer(database, null);
    importer.setSettings(Map.of(//
        "edges", write("edges.csv", "From,To", "0,1", "1,2"),//
        "edgeFromField", "From",//
        "edgeToField", "To",//
        "typeIdProperty", "Id",//
        "typeIdType", "String"));
    assertThatThrownBy(importer::load).hasStackTraceContaining("issue 11208: refused commit");

    assertThat(((DatabaseInternal) database).getTransactionIfExists()).isSameAs(callers);
    assertThat(callers.isActive()).isTrue();
    database.commit();
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countOf("Marker")).isEqualTo(1);
    assertThat(countOf("Relationship")).isZero();
  }

  /**
   * Issue #11271: the JSONL row loop committed in a {@code finally} with no handler after it, so a refused commit left the
   * import's transaction active on the thread, and the {@code finally} then committed it a second time.
   */
  @Test
  void aRefusedJsonlCommitLeavesNoTransactionOnTheThread() {
    refuseTheCommitOf("Document");

    assertThatThrownBy(() -> loadJsonl(new ImporterSettings(), jsonlDocument(0, "a"), jsonlDocument(1, "b")))
        .hasStackTraceContaining("issue 11208: refused commit");

    assertThat(refusals).as("both records land in the one batch whose commit is refused").isEqualTo(2);
    assertNothingLeftOnTheThread();
    assertThat(countOf("Document")).isZero();
  }

  @Test
  void theCallersNextUnitOfWorkAfterARefusedJsonlImportIsItsOwn() {
    refuseTheCommitOf("Document");
    assertThatThrownBy(() -> loadJsonl(new ImporterSettings(), jsonlDocument(0, "a"), jsonlDocument(1, "b")))
        .hasStackTraceContaining("issue 11208: refused commit");

    database.begin();
    database.newDocument("Marker").set("name", "next").save();
    database.commit();

    assertThat(database.isTransactionActive()).as("one begin/commit pair leaves nothing active").isFalse();
    assertThat(countOf("Marker")).isEqualTo(1);
  }

  /**
   * Under {@code -onRowError skip} each record commits on its own, so a record whose commit is refused is a row error like any
   * other: it is skipped and counted, and the records around it land. The commit used to sit outside the record's handler, so
   * the refusal aborted the whole import.
   */
  @Test
  void aRefusedJsonlCommitUnderSkipModeSkipsThatRecordOnly() throws Exception {
    database.getSchema().getType("Document").getEvents().registerListener((AfterRecordCreateListener) record -> {
      if ("b".equals(record.asDocument().getString("name"))) {
        ++refusals;
        ((DatabaseInternal) database).getTransaction().setRollbackOnly("issue 11208: refused commit");
      }
    });
    final ImporterSettings settings = new ImporterSettings();
    settings.onRowError = "skip";

    final ImporterContext context = loadJsonl(settings, jsonlDocument(0, "a"), jsonlDocument(1, "b"), jsonlDocument(2, "c"));

    assertThat(refusals).isEqualTo(1);
    assertThat(context.errors.get()).isEqualTo(1);
    assertNothingLeftOnTheThread();
    assertThat(countOf("Document")).isEqualTo(2);
    assertThat(database.query("sql", "select from Document where name = 'b'").hasNext()).isFalse();
  }

  /**
   * The refusal skip mode meets in practice: a duplicate key, which a unique index finds when the record's transaction commits.
   * The duplicate is skipped and counted, and the count of created documents is the records that landed.
   */
  @Test
  void aDuplicateKeyFoundAtCommitUnderSkipModeSkipsThatRecordOnly() throws Exception {
    database.transaction(() -> {
      database.getSchema().getType("Document").createProperty("name", Type.STRING);
      database.getSchema().getType("Document").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    });
    final ImporterSettings settings = new ImporterSettings();
    settings.onRowError = "skip";

    final ImporterContext context = loadJsonl(settings, jsonlDocument(0, "a"), jsonlDocument(1, "a"), jsonlDocument(2, "c"));

    assertThat(context.errors.get()).isEqualTo(1);
    assertThat(context.createdDocuments.get()).as("the skipped duplicate is not counted as created").isEqualTo(2);
    assertNothingLeftOnTheThread();
    assertThat(countOf("Document")).isEqualTo(2);
  }

  private ImporterContext loadJsonl(final ImporterSettings settings, final String... lines) throws IOException {
    final byte[] bytes = (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8);
    final Parser parser = new Parser(new Source("records.jsonl", new ByteArrayInputStream(bytes), bytes.length, false, null, null), 0);
    final ImporterContext context = new ImporterContext();
    new JsonlImporterFormat().load(null, null, parser, (DatabaseInternal) database, context, settings);
    return context;
  }

  private static String jsonlDocument(final int position, final String name) {
    return "{\"t\":\"d\",\"c\":{\"r\":\"#1:" + position + "\",\"t\":\"Document\",\"p\":{\"name\":\"" + name + "\"}}}";
  }

  /** Marks the import's transaction rollback-only from inside it: the engine then refuses the commit before it starts. */
  private void refuseTheCommitOf(final String typeName) {
    database.getSchema().getType(typeName).getEvents().registerListener((AfterRecordCreateListener) record -> {
      ++refusals;
      ((DatabaseInternal) database).getTransaction().setRollbackOnly("issue 11208: refused commit");
    });
  }

  private void assertNothingLeftOnTheThread() {
    assertThat(database.isTransactionActive()).as("the refused transaction is rolled back, not stranded").isFalse();
    final TransactionContext top = ((DatabaseInternal) database).getTransactionIfExists();
    assertThat(top == null || !top.isActive()).as("no live transaction left on the thread's stack").isTrue();
  }

  private String write(final String name, final String... lines) throws IOException {
    final File file = new File(DATA_PATH, name);
    Files.writeString(file.toPath(), String.join("\n", lines) + "\n");
    return file.getAbsolutePath();
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }
}
