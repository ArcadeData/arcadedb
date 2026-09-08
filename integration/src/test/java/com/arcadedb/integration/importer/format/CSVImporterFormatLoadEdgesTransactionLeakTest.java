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
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.Source;
import com.arcadedb.integration.importer.SourceSchema;
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
 * Issue #7272 (finding 2): {@code CSVImporterFormat.loadEdges()} always begins its own transaction (it
 * nests rather than reusing a caller's - see the comment above {@code database.begin()} in that method)
 * and only catches {@code IOException}: a {@code RuntimeException} escaping {@code csvParser.parseNext()}
 * (a malformed row, e.g. one exceeding {@code maxPropertySize}) leaves that nested transaction on the
 * stack, shadowing whatever the caller had open.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class CSVImporterFormatLoadEdgesTransactionLeakTest {

  private static final String DB_PATH = "target/databases/csv-loadedges-tx-leak-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
      database.getSchema().getType("Node").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[] { "id" });
      database.getSchema().createEdgeType("Relationship");
      database.getSchema().createDocumentType("Marker");

      database.newVertex("Node").set("id", "v1").save();
      database.newVertex("Node").set("id", "v2").save();
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

  private static Parser csvParserOver(final String content) throws Exception {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    final Source source = new Source("edges.csv", new ByteArrayInputStream(bytes), bytes.length, false, null, null);
    return new Parser(source, 0);
  }

  private ImporterSettings edgeSettings() {
    final ImporterSettings settings = new ImporterSettings();
    settings.edgeTypeName = "Relationship";
    settings.edgeFromField = "from";
    settings.edgeToField = "to";
    settings.typeIdProperty = "id";
    settings.options.put("maxPropertySize", 5);
    return settings;
  }

  private SourceSchema schemaFor(final CSVImporterFormat format, final ImporterSettings settings) throws Exception {
    final Parser schemaParser = csvParserOver("from,to\nv1,v2\n");
    return format.analyze(AnalyzedEntity.EntityType.EDGE, schemaParser, settings, new AnalyzedSchema(100));
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  @Test
  void aFailedRowLeavesNoTransactionActive() throws Exception {
    final CSVImporterFormat format = new CSVImporterFormat();
    final ImporterSettings settings = edgeSettings();
    final SourceSchema sourceSchema = schemaFor(format, settings);
    final ImporterContext context = new ImporterContext();

    final Parser loadParser = csvParserOver("from,to\nv1,v2\nv1,TOOLONGVALUEHERE\n");

    assertThatThrownBy(
        () -> format.load(sourceSchema, AnalyzedEntity.EntityType.EDGE, loadParser, (DatabaseInternal) database, context, settings))
        .isInstanceOf(TextParsingException.class);

    assertThat(database.isTransactionActive())
        .as("the nested transaction loadEdges() opened must be resolved before the failure propagates")
        .isFalse();
    assertThat(countOf("Relationship"))
        .as("the edge parsed before the failure was never committed")
        .isZero();
    assertThat(context.createdEdges.get())
        .as("the report must count what survived: the edge created before the failure was rolled back with it")
        .isZero();
  }

  @Test
  void aFailedRowDoesNotShadowTheCallersOwnTransaction() throws Exception {
    final CSVImporterFormat format = new CSVImporterFormat();
    final ImporterSettings settings = edgeSettings();
    final SourceSchema sourceSchema = schemaFor(format, settings);
    final ImporterContext context = new ImporterContext();

    database.begin();
    database.newDocument("Marker").set("name", "caller").save();

    final Parser loadParser = csvParserOver("from,to\nv1,v2\nv1,TOOLONGVALUEHERE\n");

    assertThatThrownBy(
        () -> format.load(sourceSchema, AnalyzedEntity.EntityType.EDGE, loadParser, (DatabaseInternal) database, context, settings))
        .isInstanceOf(TextParsingException.class);

    // The caller's own commit must land on its own transaction, not on the importer's abandoned nested one.
    database.commit();

    assertThat(database.isTransactionActive())
        .as("one commit for the one transaction the caller opened must leave nothing active")
        .isFalse();
    assertThat(countOf("Marker"))
        .as("the caller's own record must be what its commit made durable")
        .isEqualTo(1);
    assertThat(countOf("Relationship"))
        .as("the importer's abandoned edge must not ride out on the caller's commit")
        .isZero();
  }

  /**
   * A {@code database.commit()} that fails is not the same failure as a row that throws, and the fix has to
   * treat it separately: {@code LocalDatabase.commit()} pops the transaction inside its own {@code finally},
   * so by the time the exception surfaces there is nothing left to roll back and the {@code txOpen} flag is
   * already {@code false}. Hanging the counter correction off that flag would therefore leave the batch the
   * commit failed to write counted as if it had survived.
   * <p>
   * The proxy below reproduces exactly that state - transaction resolved and its changes discarded, exception
   * propagating out of {@code commit()} - by rolling back and then throwing in place of the real commit.
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
  void aFailedCommitAlsoReportsOnlyTheEdgesThatSurvived() throws Exception {
    final CSVImporterFormat format = new CSVImporterFormat();
    final ImporterSettings settings = edgeSettings();
    final SourceSchema sourceSchema = schemaFor(format, settings);
    final ImporterContext context = new ImporterContext();

    // The one data row is valid, so the loop completes and the only failure is the trailing commit itself.
    final Parser loadParser = csvParserOver("from,to\nv1,v2\n");

    assertThatThrownBy(
        () -> format.load(sourceSchema, AnalyzedEntity.EntityType.EDGE, loadParser, databaseWhoseCommitFails(), context,
            settings))
        .isInstanceOf(TransactionException.class);

    assertThat(database.isTransactionActive())
        .as("a failed commit still leaves its own transaction off the stack")
        .isFalse();
    assertThat(countOf("Relationship"))
        .as("the failed commit made nothing durable")
        .isZero();
    assertThat(context.createdEdges.get())
        .as("the report must not credit the import with the batch the failed commit never wrote")
        .isZero();
  }
}
