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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7272, the entry point the issue's own grep did not name: {@code Neo4jImporter.parseVertices()} has the
 * same shape as the four findings the issue lists - {@code database.begin()}, a row loop with a periodic
 * {@code commit()}/{@code begin()} every {@code batchSize} vertices, and a trailing
 * {@code if (database.isTransactionActive()) database.commit();} that an {@code IOException} escaping
 * {@code readFileSimple()} skips, leaving the transaction it opened on the stack.
 * <p>
 * It also covers the issue's third suggested fix, the one about the progress report: {@code context.createdVertices}
 * counts every vertex the batch allocated, so after a rollback it credits the import with vertices that are not on
 * the disk. The counter has to come back to the value at the last successful commit.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Neo4jImporterParseVerticesTransactionLeakTest {

  private static final String DB_PATH = "target/databases/neo4j-importer-parsevertices-tx-leak-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Person").createProperty("id", Type.STRING);
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
   * Delivers {@code failAfterBytes} bytes and then fails every further read, the shape {@code reader.readLine()}
   * sees when the underlying source breaks. {@code InputStream#read(byte[], int, int)} swallows an
   * {@link IOException} raised after at least one byte was read, so the first bulk read returns the whole prefix
   * and only the read after it throws - which is what puts the failure past the batch commit below.
   */
  private static class FailingAfterInputStream extends InputStream {
    private final byte[] data;
    private final int    failAfterBytes;
    private       int    pos;

    FailingAfterInputStream(final byte[] data, final int failAfterBytes) {
      this.data = data;
      this.failAfterBytes = failAfterBytes;
    }

    @Override
    public int read() throws IOException {
      if (pos >= failAfterBytes)
        throw new IOException("Simulated I/O failure mid-stream");
      if (pos >= data.length)
        return -1;
      return data[pos++] & 0xff;
    }

    @Override
    public boolean markSupported() {
      return true;
    }

    @Override
    public void mark(final int readLimit) {
    }

    @Override
    public void reset() {
      pos = 0;
    }
  }

  private static void setField(final Neo4jImporter importer, final String name, final Object value) throws Exception {
    final Field field = Neo4jImporter.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(importer, value);
  }

  private static void invokeParseVertices(final Neo4jImporter importer) throws Throwable {
    final Method method = Neo4jImporter.class.getDeclaredMethod("parseVertices");
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
   * Three vertices with a batch size of two: the first two are committed by the periodic commit, the third is
   * created into the transaction the failure then rolls back.
   */
  private Neo4jImporter importerFailingAfterThreeVertices(final ImporterContext context) throws Exception {
    final Neo4jImporter importer = new Neo4jImporter(database, context);

    final String lines = "{\"type\":\"node\",\"id\":\"1\",\"labels\":[\"Person\"]}\n"
        + "{\"type\":\"node\",\"id\":\"2\",\"labels\":[\"Person\"]}\n"
        + "{\"type\":\"node\",\"id\":\"3\",\"labels\":[\"Person\"]}\n";
    final byte[] bytes = lines.getBytes(StandardCharsets.UTF_8);

    setField(importer, "batchSize", 2);
    setField(importer, "inputStream", new FailingAfterInputStream(bytes, bytes.length));
    return importer;
  }

  @Test
  void aFailedReadLeavesNoTransactionActive() throws Throwable {
    final ImporterContext context = new ImporterContext();
    final Neo4jImporter importer = importerFailingAfterThreeVertices(context);

    assertThatThrownBy(() -> invokeParseVertices(importer)).isInstanceOf(IOException.class);

    assertThat(database.isTransactionActive())
        .as("the transaction parseVertices() opened must be resolved before the failure propagates")
        .isFalse();
    assertThat(countOf("Person"))
        .as("only the batch the periodic commit made durable survives; the third vertex was rolled back")
        .isEqualTo(2);
  }

  @Test
  void aFailedReadReportsOnlyTheVerticesThatSurvived() throws Throwable {
    final ImporterContext context = new ImporterContext();
    final Neo4jImporter importer = importerFailingAfterThreeVertices(context);

    assertThatThrownBy(() -> invokeParseVertices(importer)).isInstanceOf(IOException.class);

    assertThat(context.createdVertices.get())
        .as("the report must count what is on the disk, not the third vertex the rollback took away")
        .isEqualTo(2);
    assertThat(context.createdVertices.get())
        .as("the counter and the durable row count must agree")
        .isEqualTo(countOf("Person"));
  }

  @Test
  void aFailedReadDoesNotShadowTheCallersOwnTransaction() throws Throwable {
    final ImporterContext context = new ImporterContext();
    final Neo4jImporter importer = importerFailingAfterThreeVertices(context);

    database.begin();
    database.newDocument("Marker").set("name", "caller").save();

    assertThatThrownBy(() -> invokeParseVertices(importer)).isInstanceOf(IOException.class);

    // The caller's own commit must land on its own transaction, not on the importer's abandoned nested one.
    database.commit();

    assertThat(database.isTransactionActive())
        .as("one commit for the one transaction the caller opened must leave nothing active")
        .isFalse();
    assertThat(countOf("Marker"))
        .as("the caller's own record must be what its commit made durable")
        .isEqualTo(1);
  }

  /**
   * The counterpart of the two commit-failure tests on the format classes, for the one site whose flags live in
   * single-element arrays. {@code LocalDatabase.commit()} pops the transaction inside its own {@code finally},
   * so a commit that throws leaves {@code txOpen[0]} already {@code false} and nothing to roll back - only the
   * {@code completed} flag can still bring the counter back. Without it the batch the commit failed to write
   * would stay counted as if it had survived.
   * <p>
   * The proxy fails the first commit, which is the periodic one after {@code batchSize} vertices, and leaves
   * behind what a real commit failure leaves: the transaction gone and its records not durable.
   */
  private Database databaseWhoseFirstCommitFails() {
    return (Database) Proxy.newProxyInstance(Database.class.getClassLoader(), new Class<?>[] { Database.class },
        (proxy, method, args) -> {
          if ("commit".equals(method.getName()) && (args == null || args.length == 0)) {
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
  void aFailedCommitReportsOnlyTheVerticesThatSurvived() throws Throwable {
    final ImporterContext context = new ImporterContext();
    final Neo4jImporter importer = new Neo4jImporter(databaseWhoseFirstCommitFails(), context);

    // The input is fully readable: the only failure is the periodic commit after the second vertex.
    final String lines = "{\"type\":\"node\",\"id\":\"1\",\"labels\":[\"Person\"]}\n"
        + "{\"type\":\"node\",\"id\":\"2\",\"labels\":[\"Person\"]}\n"
        + "{\"type\":\"node\",\"id\":\"3\",\"labels\":[\"Person\"]}\n";
    final byte[] bytes = lines.getBytes(StandardCharsets.UTF_8);

    setField(importer, "batchSize", 2);
    setField(importer, "inputStream", new FailingAfterInputStream(bytes, bytes.length));

    assertThatThrownBy(() -> invokeParseVertices(importer)).isInstanceOf(TransactionException.class);

    assertThat(database.isTransactionActive())
        .as("a failed commit still leaves its own transaction off the stack")
        .isFalse();
    assertThat(countOf("Person"))
        .as("the failed commit made nothing durable")
        .isZero();
    assertThat(context.createdVertices.get())
        .as("the report must not credit the import with the batch the failed commit never wrote")
        .isZero();
  }

}
