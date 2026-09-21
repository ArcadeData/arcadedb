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
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8069: {@code RDFImporterFormat.load()} dereferenced {@code row[0]}/{@code row[1]}/{@code row[2]} with no
 * arity check, so a line with fewer than three fields threw {@link ArrayIndexOutOfBoundsException} straight out of
 * the loop and aborted the whole import - and {@code -onRowError skip} was never even consulted, since nothing in
 * this format read {@link ImporterSettings#isSkipOnRowError()}. {@code CSVImporterFormat}'s sibling row loops
 * (fixed by #7782) already gate an arity failure on that setting; this format now reuses the same policy and its
 * {@code logSkippedRow} logging.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RDFImporterFormatRowErrorPolicyTest {

  private static final String DB_PATH = "target/databases/rdf-importer-row-error-policy-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
      database.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
      database.getSchema().createEdgeType("Related");
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

  private static Parser rdfParser(final String content) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    final Source source = new Source("test.rdf", new ByteArrayInputStream(bytes), bytes.length, false, null, null);
    return new Parser(source, 0);
  }

  private ImporterSettings settings() {
    final ImporterSettings settings = new ImporterSettings();
    settings.vertexTypeName = "Node";
    settings.edgeTypeName = "Related";
    settings.typeIdProperty = "id";
    settings.commitEvery = 1_000;
    return settings;
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  @Test
  void aShortLineAbortsTheWholeImportByDefaultAndNamesTheLine() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();

    // Line 2 (0-based line 1) has only two fields: no object term.
    final Parser parser = rdfParser("""
        v1,rel,v2
        v2,rel
        v3,rel,v4
        """);

    assertThatThrownBy(() -> format.load(null, null, parser, (DatabaseInternal) database, context, settings()))
        .isInstanceOf(ImportException.class)
        .hasMessageContaining("line 1")
        .hasMessageContaining("2 column");

    assertThat(countOf("Related"))
        .as("the default policy aborts the whole import: not even the row before the short one is durable, since "
            + "nothing owns the transaction to commit it piecemeal here")
        .isZero();
  }

  @Test
  void skipOnRowErrorSkipsTheShortLineAndImportsTheRest() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    final ImporterSettings settings = settings();
    settings.onRowError = "skip";

    final Parser parser = rdfParser("""
        v1,rel,v2
        v2,rel
        v3,rel,v4
        """);

    format.load(null, null, parser, (DatabaseInternal) database, context, settings);

    assertThat(countOf("Related"))
        .as("both well-formed statements must have been imported despite the short line between them")
        .isEqualTo(2);
    assertThat(context.createdEdges.get()).isEqualTo(2);
    assertThat(context.errors.get())
        .as("the short line must be counted as an error, not silently dropped")
        .isEqualTo(1);
  }

  /**
   * A commit failure at a {@code -commitEvery} boundary is an infrastructure failure, not a bad row: it must
   * propagate out of {@code load()} even under {@code -onRowError skip}, rather than being caught by the per-row
   * try/catch and logged as "skipping it", which would also leave the loop with no transaction active and turn
   * every remaining row into a cascading, misreported failure.
   */
  @Test
  void commitFailureDuringSkipOnRowErrorIsNotAbsorbedAsARowError() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    final ImporterSettings settings = settings();
    settings.onRowError = "skip";
    settings.commitEvery = 1;

    final RuntimeException commitFailure = new RuntimeException("simulated commit failure");
    final DatabaseInternal failingOnCommit = commitFailsOnFirstCall((DatabaseInternal) database, commitFailure);

    final Parser parser = rdfParser("""
        v1,rel,v2
        v3,rel,v4
        """);

    assertThatThrownBy(() -> format.load(null, null, parser, failingOnCommit, context, settings))
        .as("a commit failure must propagate, not be swallowed as a per-row skip")
        .isSameAs(commitFailure);

    assertThat(context.errors.get())
        .as("neither row is a bad row: the failure is in the commit, not in anything the loop parsed or wrote")
        .isZero();

    // The proxy's commit() throws BEFORE delegating to the real one, so the real transaction is never popped the
    // way a genuine commit() failure would leave it (DatabaseContext#popIfNotLastTransaction() does not pop the
    // outermost transaction either way, so this is the case a real failure exercises too): it must still come
    // down through the finally block's own rollback(), not linger active after load() has thrown.
    assertThat(database.isTransactionActive())
        .as("the transaction the commit failed on must have been rolled back, not left dangling")
        .isFalse();
  }

  /**
   * Same failure, but with {@code commitEvery} higher than the row count so the periodic commit inside the loop
   * never fires: the proxy's single interception lands on the TRAILING commit instead, a distinct code path from
   * {@link #commitFailureDuringSkipOnRowErrorIsNotAbsorbedAsARowError()} above.
   */
  @Test
  void trailingCommitFailureLeavesNoTransactionDangling() throws Exception {
    final RDFImporterFormat format = new RDFImporterFormat();
    final ImporterContext context = new ImporterContext();
    final ImporterSettings settings = settings();
    // Default settings() already sets commitEvery = 1_000, well above the 2 rows below - kept explicit here so
    // the test does not silently stop exercising the trailing commit if that default ever changes.
    settings.commitEvery = 1_000;

    final RuntimeException commitFailure = new RuntimeException("simulated trailing commit failure");
    final DatabaseInternal failingOnCommit = commitFailsOnFirstCall((DatabaseInternal) database, commitFailure);

    final Parser parser = rdfParser("""
        v1,rel,v2
        v3,rel,v4
        """);

    assertThatThrownBy(() -> format.load(null, null, parser, failingOnCommit, context, settings))
        .as("a trailing commit failure must propagate, not be swallowed")
        .isSameAs(commitFailure);

    assertThat(database.isTransactionActive())
        .as("the transaction the trailing commit failed on must have been rolled back, not left dangling")
        .isFalse();
  }

  /**
   * Wraps a real {@link DatabaseInternal} so its very first {@code commit()} call throws {@code failure} instead of
   * committing, and every other call - including every later {@code commit()} - passes straight through to the real
   * database. A JDK dynamic proxy rather than a hand-rolled subclass because {@link DatabaseInternal} is a large
   * interface and the test needs to intercept exactly one method.
   */
  private static DatabaseInternal commitFailsOnFirstCall(final DatabaseInternal real, final RuntimeException failure) {
    final InvocationHandler handler = new InvocationHandler() {
      private boolean commitCalled = false;

      @Override
      public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if ("commit".equals(method.getName()) && (args == null || args.length == 0) && !commitCalled) {
          commitCalled = true;
          throw failure;
        }
        try {
          return method.invoke(real, args);
        } catch (final InvocationTargetException e) {
          throw e.getCause();
        }
      }
    };

    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(), new Class<?>[] { DatabaseInternal.class },
        handler);
  }
}
