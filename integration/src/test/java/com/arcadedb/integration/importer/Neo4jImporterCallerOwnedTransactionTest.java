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
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8073: {@code Neo4jImporter.parseVertices()}/{@code parseEdges()} used to open and commit their own
 * transaction unconditionally, even when the caller had already begun one around the whole import. Because
 * {@code begin()} nests and each nested {@code commit()} is independently durable, a caller who wrapped the import
 * in a transaction meaning to commit or discard it as a unit used to find the rows already on disk, and a later
 * {@code rollback()} took nothing back.
 * <p>
 * Fixed by gating on {@link ImporterContext#importOwnsTransaction}, the same way every other row loop in the
 * importer tree already does (see {@code RDFImporterFormat.load()}): when {@link ImporterContext#callerTransactionActiveOnEntry}
 * is set, the import joins the caller's transaction instead of nesting and committing its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Neo4jImporterCallerOwnedTransactionTest {

  private static final String DB_PATH = "target/databases/neo4j-importer-caller-owned-tx-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Person").createProperty("id", Type.STRING);
      database.getSchema().createEdgeType("KNOWS");
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

  private static void invoke(final Neo4jImporter importer, final String methodName) throws Throwable {
    final Method method = Neo4jImporter.class.getDeclaredMethod(methodName);
    method.setAccessible(true);
    try {
      method.invoke(importer);
    } catch (final InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  /**
   * One importer instance for both {@code parseVertices()} and {@code parseEdges()}, the same way {@code run()}
   * uses it: the id-to-RID mapping {@code parseVertices()} populates is an instance field, not part of
   * {@link ImporterContext}, so a fresh importer per call would leave {@code parseEdges()} unable to resolve any
   * endpoint.
   */
  private Neo4jImporter importerFor(final ImporterContext context, final String jsonl) throws Exception {
    final Neo4jImporter importer = new Neo4jImporter(database, context) {
      @Override
      public InputStream openInputStream() {
        return new ByteArrayInputStream(jsonl.getBytes(StandardCharsets.UTF_8));
      }
    };
    final Field field = Neo4jImporter.class.getDeclaredField("batchSize");
    field.setAccessible(true);
    field.set(importer, 1_000);
    return importer;
  }

  private static final String TWO_NODES_ONE_RELATIONSHIP =
      "{\"type\":\"node\",\"id\":\"1\",\"labels\":[\"Person\"]}\n"
          + "{\"type\":\"node\",\"id\":\"2\",\"labels\":[\"Person\"]}\n"
          + "{\"type\":\"relationship\",\"id\":\"10\",\"label\":\"KNOWS\",\"start\":{\"id\":\"1\"},\"end\":{\"id\":\"2\"}}\n";

  @Test
  void callerRollbackAfterImportTakesEverythingBack() throws Throwable {
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = true;

    final Neo4jImporter importer = importerFor(context, TWO_NODES_ONE_RELATIONSHIP);

    database.begin();
    invoke(importer, "parseVertices");
    invoke(importer, "parseEdges");

    assertThat(database.isTransactionActive())
        .as("a caller-owned transaction must still be active: the import must not have committed it away")
        .isTrue();

    database.rollback();

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countOf("Person"))
        .as("the caller's rollback() must take back everything the import staged, vertices included")
        .isZero();
    assertThat(countOf("KNOWS"))
        .as("the caller's rollback() must take back everything the import staged, edges included")
        .isZero();
  }

  @Test
  void callerCommitAfterImportKeepsEverything() throws Throwable {
    final ImporterContext context = new ImporterContext();
    context.callerTransactionActiveOnEntry = true;

    final Neo4jImporter importer = importerFor(context, TWO_NODES_ONE_RELATIONSHIP);

    database.begin();
    invoke(importer, "parseVertices");
    invoke(importer, "parseEdges");
    database.commit();

    assertThat(countOf("Person")).isEqualTo(2);
    assertThat(countOf("KNOWS")).isEqualTo(1);
  }

  @Test
  void withoutACallerTransactionTheImportStillCommitsItsOwn() throws Throwable {
    // No callerTransactionActiveOnEntry set: the default, standalone shape - the import must still durably commit
    // its own work exactly as before, with no caller left to resolve anything.
    final ImporterContext context = new ImporterContext();

    final Neo4jImporter importer = importerFor(context, TWO_NODES_ONE_RELATIONSHIP);

    invoke(importer, "parseVertices");
    invoke(importer, "parseEdges");

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countOf("Person")).isEqualTo(2);
    assertThat(countOf("KNOWS")).isEqualTo(1);
  }
}
