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
package com.arcadedb.integration.importer.format;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.univocity.parsers.common.TextParsingException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7272 (finding 1): {@code RDFImporterFormat.load()} opens (or reuses) a transaction and never
 * resolves it if a row throws - the only catch maps {@code IOException} to {@code ImportException} and
 * does not roll back, so a {@code RuntimeException} escaping {@code csvParser.parseNext()} (a malformed
 * row) leaves the transaction it opened active.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class RDFImporterFormatTransactionLeakTest {

  private static final String DB_PATH = "target/databases/rdf-importer-tx-leak-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
      database.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
      database.getSchema().createEdgeType("Related");
      database.getSchema().createDocumentType("Marker");
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
  }

  /**
   * A row whose value exceeds {@code maxPropertySize} makes {@code csvParser.parseNext()} itself throw a
   * {@link TextParsingException} (unchecked), outside the per-row work the loop otherwise wraps.
   */
  private static Parser rdfParser(final String content) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    final Source source = new Source("test.rdf", new ByteArrayInputStream(bytes), bytes.length, false, null, null);
    return new Parser(source, 0);
  }

  private ImporterSettings settingsWithTinyMaxPropertySize() {
    final ImporterSettings settings = settings();
    settings.options.put("maxPropertySize", 5);
    return settings;
  }

  private ImporterSettings settings() {
    final ImporterSettings settings = new ImporterSettings();
    settings.vertexTypeName = "Node";
    settings.edgeTypeName = "Related";
    settings.typeIdProperty = "id";
    // These fixtures open with a literal "s,p,o" line and have always relied on it being dropped. Since #7345 an RDF
    // source has no default header skip, so the skip these tests want is asked for explicitly rather than inherited;
    // not one fixture line and not one assertion below changes.
    settings.edgesSkipEntries = 1L;
    return settings;
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  /**
   * Issue #7328: the trailing {@code database.commit()} is the fourth of the four transaction operations
   * {@code load()} gates on ownership, and it is the one that used to be ungated - so a successful import against a
   * caller-managed transaction committed the caller's, silently, at a point of the importer's choosing. The import
   * reported success and the caller had no signal that its own unit of work had been resolved out from under it.
   * <p>
   * Asserted by rolling the caller's transaction back afterwards: if the importer committed it, the caller's own
   * pending record and the imported edges both survive that rollback.
   */
  @Test
  void aSuccessfulImportNeverCommitsTheCallersTransaction() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = true;

    database.begin();
    database.newDocument("Marker").set("name", "caller").save();

    format.load(null, null, rdfParser("""
        s,p,o
        v1,rel,v2
        v3,rel,v4
        """), (DatabaseInternal) database, context, settings());

    assertThat(database.isTransactionActive())
        .as("a transaction that predates the import is never the importer's to commit, success or not")
        .isTrue();

    // The caller's decision, exercised: everything staged inside its transaction - its own record and the edges the
    // import accumulated into it - must still be discardable.
    database.rollback();

    assertThat(countOf("Marker"))
        .as("the caller's own pending work must still be the caller's to discard")
        .isZero();
    assertThat(countOf("Node"))
        .as("the import staged its edges in the caller's transaction, so the caller's rollback takes them too")
        .isZero();
  }

  /**
   * The other half of the same asymmetry: {@code callerTransactionActiveOnEntry} is a snapshot taken before the
   * import began, and a caller transaction it recorded may already be resolved by the time a format's row loop
   * runs. The transaction the loop then pushes is its own - and gating on the stale snapshot left it on the stack
   * with nothing allowed to resolve it: not the loop's own commit or rollback, and not {@code Importer.load()}'s
   * cleanup, which is gated on the same flag (issue #7328).
   */
  @Test
  void theImportOwnsTheTransactionItPushesOnceTheCallersIsGone() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    // Recorded on entry, but the caller has since resolved it: no transaction is active now.
    context.callerTransactionActiveOnEntry = true;
    assertThat(database.isTransactionActive()).isFalse();

    format.load(null, null, rdfParser("""
        s,p,o
        v1,rel,v2
        v3,rel,v4
        """), (DatabaseInternal) database, context, settings());

    assertThat(database.isTransactionActive())
        .as("the transaction the import pushed is its own, and a successful import resolves it")
        .isFalse();
    assertThat(countOf("Node"))
        .as("the import committed its own transaction, so its edges are durable")
        .isEqualTo(4);
  }

  @Test
  void aFailedRowLeavesNoTransactionActiveWhenTheImporterOwnsIt() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = false;

    final Parser parser = rdfParser("""
        s,p,o
        v1,rel,v2
        v3,rel,TOOLONGVALUEHERE
        """);

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithTinyMaxPropertySize()))
        .isInstanceOf(TextParsingException.class);

    assertThat(database.isTransactionActive())
        .as("the transaction load() opened for itself must be resolved before the failure propagates")
        .isFalse();
    assertThat(countOf("Node"))
        .as("nothing must be durable: the only edge parsed before the failure was never committed")
        .isZero();
    assertThat(context.createdEdges.get())
        .as("the report must count what survived: the edge created before the failure was rolled back with it")
        .isZero();
  }

  @Test
  void aFailedRowLeavesTheCallersOwnTransactionUntouched() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = true;

    database.begin();
    database.newDocument("Marker").set("name", "caller").save();

    final Parser parser = rdfParser("""
        s,p,o
        v1,rel,v2
        v3,rel,TOOLONGVALUEHERE
        """);

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithTinyMaxPropertySize()))
        .isInstanceOf(TextParsingException.class);

    assertThat(database.isTransactionActive())
        .as("a transaction that predates the import is never the importer's to roll back")
        .isTrue();

    database.commit();

    assertThat(countOf("Marker"))
        .as("the caller's own pending work must survive a failure inside a transaction it owns")
        .isEqualTo(1);
  }

  /**
   * The CLI pipeline's shape, which is not the same as either of the two above:
   * {@code AbstractImporter.openDatabase()} ends with a {@code database.begin()} deliberately left open for the
   * whole import, so {@code load()} finds a transaction already active and reuses it - while
   * {@code callerTransactionActiveOnEntry} stays {@code false}, because that transaction belongs to the import
   * and not to an external caller. A failing row must still resolve it: leaving it active is what let
   * {@code AbstractImporter.closeDatabase()} commit a half-finished import.
   */
  @Test
  void aFailedRowResolvesTheTransactionTheImportPipelineLeftOpen() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = false;

    // What AbstractImporter.openDatabase() leaves behind before the format runs.
    database.begin();

    final Parser parser = rdfParser("""
        s,p,o
        v1,rel,v2
        v3,rel,TOOLONGVALUEHERE
        """);

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settingsWithTinyMaxPropertySize()))
        .isInstanceOf(TextParsingException.class);

    assertThat(database.isTransactionActive())
        .as("the import pipeline's own transaction is the import's to resolve, and a failure must resolve it")
        .isFalse();
    assertThat(countOf("Node"))
        .as("nothing from the aborted import may survive to be committed by closeDatabase()")
        .isZero();
  }

  /**
   * A {@code database.commit()} that fails is a different path from a row that throws:
   * {@code LocalDatabase.commit()} pops the transaction inside its own {@code finally}, so there is nothing
   * left to roll back and the {@code txOpen} flag is already {@code false} - only the {@code completed} flag
   * can still bring the counter back. The proxy reproduces that state exactly.
   */
  private DatabaseInternal databaseWhoseCommitFails() {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("commit".equals(method.getName()) && (args == null || args.length == 0)) {
            // What a real commit failure leaves behind: the transaction gone, its records not durable.
            database.rollback();
            throw new TransactionException("Simulated commit failure");
          }
          try {
            return method.invoke(database, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  void aFailedCommitReportsOnlyTheEdgesThatSurvived() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = false;

    // Every row is valid, so the loop completes and the only failure is the trailing commit itself.
    final Parser parser = rdfParser("""
        s,p,o
        v1,rel,v2
        """);

    assertThatThrownBy(
        () -> format.load(null, null, parser, databaseWhoseCommitFails(), context, settingsWithTinyMaxPropertySize()))
        .isInstanceOf(TransactionException.class);

    assertThat(database.isTransactionActive())
        .as("a failed commit still leaves its own transaction off the stack")
        .isFalse();
    assertThat(countOf("Node"))
        .as("the failed commit made nothing durable")
        .isZero();
    assertThat(context.createdEdges.get())
        .as("the report must not credit the import with the batch the failed commit never wrote")
        .isZero();
  }

}
