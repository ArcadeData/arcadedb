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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
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
 * Issue #7272 (finding 4): {@code Neo4jImporter.readFile()} always begins its own transaction, reads the
 * JSONL input inside a try-with-resources, and only resolves that transaction in the line right after it
 * - {@code if (database.isTransactionActive()) database.commit();}. An {@code IOException} escaping
 * {@code reader.readLine()} skips that line entirely, leaving the transaction opened above on the stack.
 * <p>
 * {@code readFile()} is private; it is invoked here via reflection on an importer built with the public
 * {@code Neo4jImporter(Database, ImporterContext)} embedding constructor, with the private
 * {@code inputStream} field swapped for a stream that fails mid-read.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Neo4jImporterReadFileTransactionLeakTest {

  private static final String DB_PATH = "target/databases/neo4j-importer-readfile-tx-leak-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Node");
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
   * Fully readable up to {@code failAfterBytes}, then every further read throws {@link IOException} - the
   * shape {@code reader.readLine()} sees when the underlying source breaks mid-line. Supports mark/reset
   * trivially: {@code Neo4jImporter#openInputStream()} calls {@code reset()} once, before any read.
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

  private void setInputStream(final Neo4jImporter importer, final InputStream inputStream) throws Exception {
    final Field field = Neo4jImporter.class.getDeclaredField("inputStream");
    field.setAccessible(true);
    field.set(importer, inputStream);
  }

  private void invokeReadFile(final Neo4jImporter importer, final Callable<Void, JSONObject> callback) throws Throwable {
    final Method method = Neo4jImporter.class.getDeclaredMethod("readFile", Callable.class);
    method.setAccessible(true);
    try {
      method.invoke(importer, callback);
    } catch (final InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  @Test
  void aFailedReadLeavesNoTransactionActive() throws Throwable {
    final Neo4jImporter importer = new Neo4jImporter(database, new ImporterContext());

    final String line1 = "{\"type\":\"node\",\"id\":\"1\"}\n";
    final String line2 = "{\"type\":\"node\",\"id\":\"2\"}\n";
    final byte[] bytes = (line1 + line2).getBytes(StandardCharsets.UTF_8);
    setInputStream(importer, new FailingAfterInputStream(bytes, line1.getBytes(StandardCharsets.UTF_8).length + 2));

    final Callable<Void, JSONObject> callback = json -> {
      database.newDocument("Node").set("marker", true).save();
      return null;
    };

    assertThatThrownBy(() -> invokeReadFile(importer, callback)).isInstanceOf(IOException.class);

    assertThat(database.isTransactionActive())
        .as("the transaction readFile() opened must be resolved before the failure propagates")
        .isFalse();
    assertThat(countOf("Node"))
        .as("the row read before the failure was never committed")
        .isZero();
  }

  @Test
  void aFailedReadDoesNotShadowTheCallersOwnTransaction() throws Throwable {
    final Neo4jImporter importer = new Neo4jImporter(database, new ImporterContext());

    final String line1 = "{\"type\":\"node\",\"id\":\"1\"}\n";
    final String line2 = "{\"type\":\"node\",\"id\":\"2\"}\n";
    final byte[] bytes = (line1 + line2).getBytes(StandardCharsets.UTF_8);
    setInputStream(importer, new FailingAfterInputStream(bytes, line1.getBytes(StandardCharsets.UTF_8).length + 2));

    final Callable<Void, JSONObject> callback = json -> {
      database.newDocument("Node").set("marker", true).save();
      return null;
    };

    database.begin();
    database.newDocument("Marker").set("name", "caller").save();

    assertThatThrownBy(() -> invokeReadFile(importer, callback)).isInstanceOf(IOException.class);

    // The caller's own commit must land on its own transaction, not on the importer's abandoned nested one.
    database.commit();

    assertThat(database.isTransactionActive())
        .as("one commit for the one transaction the caller opened must leave nothing active")
        .isFalse();
    assertThat(countOf("Marker"))
        .as("the caller's own record must be what its commit made durable")
        .isEqualTo(1);
    assertThat(countOf("Node"))
        .as("the importer's abandoned row must not ride out on the caller's commit")
        .isZero();
  }

  /**
   * What a real commit failure leaves behind: {@code LocalDatabase.commit()} pops the transaction inside its own
   * {@code finally}, so the transaction this method opened is already gone and the caller's is back on top.
   */
  private Database databaseWhoseCommitFails() {
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

  /**
   * Issue #7328: the trailing {@code commit()} must clear {@code txOpen} BEFORE it runs, not after. A commit that
   * throws skips everything below it, so clearing afterwards leaves the flag set and hands the {@code finally} a
   * transaction this method no longer owns.
   * <p>
   * Proved on a {@link Proxy} that implements {@link Database} but not {@code DatabaseInternal} - a remote database
   * has the same shape - because that is the case where the guard cannot read the transaction stack and degrades
   * to the ambient {@code isTransactionActive()}. The failed commit has already popped the import's own
   * transaction, so what that ambient test sees is the CALLER's, and a rollback issued on it discards the caller's
   * unrelated pending work.
   */
  @Test
  void aFailedCommitDoesNotRollBackTheCallersTransaction() throws Throwable {
    final Neo4jImporter importer = new Neo4jImporter(databaseWhoseCommitFails(), new ImporterContext());

    // The input is fully readable: the only failure is the trailing commit itself.
    setInputStream(importer,
        new ByteArrayInputStream("{\"type\":\"node\",\"id\":\"1\"}\n".getBytes(StandardCharsets.UTF_8)));

    final Callable<Void, JSONObject> callback = json -> {
      database.newDocument("Node").set("marker", true).save();
      return null;
    };

    database.begin();
    database.newDocument("Marker").set("name", "caller").save();

    assertThatThrownBy(() -> invokeReadFile(importer, callback)).isInstanceOf(TransactionException.class);

    assertThat(database.isTransactionActive())
        .as("the caller's transaction must have survived the import's failed commit")
        .isTrue();

    database.commit();

    assertThat(countOf("Marker"))
        .as("the caller's own record must be what its own commit made durable")
        .isEqualTo(1);
    assertThat(countOf("Node"))
        .as("the failed commit made nothing of the import's own row durable")
        .isZero();
  }
}
