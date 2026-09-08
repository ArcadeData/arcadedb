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
    final ImporterSettings settings = new ImporterSettings();
    settings.vertexTypeName = "Node";
    settings.edgeTypeName = "Related";
    settings.typeIdProperty = "id";
    settings.options.put("maxPropertySize", 5);
    return settings;
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
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
}
