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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11046: {@code Neo4jImporter.readFile()} cleared its ownership flag before a commit and did not restore it when the
 * commit threw. A refusal raised before the commit starts (here the real engine's rollback-only refusal, the same arm a
 * terminated statement or a server that stopped accepting writes takes) leaves the transaction active, and the outermost one
 * is not popped, so the rollback in the finally skipped it on the cleared flag and the transaction stayed on the caller's
 * thread for good: it even survived the caller's own {@code begin()}/{@code commit()} pair, which nested inside it.
 * <p>
 * The existing leak tests inject a commit fault that rolls back first, which pops; this one does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11046RefusedCommitTransactionLeakTest {
  private static final String DB_PATH = "target/databases/issue-11046-refused-commit-leak-test";
  private static final String INPUT   = "{\"type\":\"node\",\"id\":\"1\"}\n{\"type\":\"node\",\"id\":\"2\"}\n";

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

  @Test
  void aRefusedCommitLeavesNoTransactionOnTheThread() throws Throwable {
    final Neo4jImporter importer = importer();

    assertThatThrownBy(() -> invokeReadFile(importer, refuseTheCommit())).isInstanceOf(TransactionException.class);

    assertThat(database.isTransactionActive()).as("the refused transaction is rolled back, not stranded").isFalse();
    assertThat(countOf("Node")).isZero();
  }

  /** What the stranded transaction did to the thread: the caller's next unit of work nested inside it and never ended. */
  @Test
  void theCallersNextUnitOfWorkIsItsOwn() throws Throwable {
    final Neo4jImporter importer = importer();
    assertThatThrownBy(() -> invokeReadFile(importer, refuseTheCommit())).isInstanceOf(TransactionException.class);

    database.begin();
    database.newDocument("Marker").set("name", "next").save();
    database.commit();

    assertThat(database.isTransactionActive()).as("one begin/commit pair leaves nothing active").isFalse();
    assertThat(countOf("Marker")).isEqualTo(1);
  }

  /**
   * Under a caller's transaction the import's is nested, and the refused commit pops it: the restored flag must not hand the
   * rollback the caller's transaction, which stays the caller's to resolve.
   */
  @Test
  void aRefusedNestedCommitLeavesTheCallersTransactionAlone() throws Throwable {
    final Neo4jImporter importer = importer();

    database.begin();
    final TransactionContext callers = ((DatabaseInternal) database).getTransaction();
    database.newDocument("Marker").set("name", "caller").save();

    assertThatThrownBy(() -> invokeReadFile(importer, refuseTheCommit())).isInstanceOf(TransactionException.class);

    assertThat(((DatabaseInternal) database).getTransactionIfExists()).isSameAs(callers);
    assertThat(callers.isActive()).isTrue();
    database.commit();
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countOf("Marker")).isEqualTo(1);
  }

  /** The shared protocol the importers' row loops use (GraphImporter's included), against the same real refusal. */
  @Test
  void theSharedCommitProtocolRestoresTheFlagOfARefusedCommit() {
    database.begin();
    final TransactionContext own = ImporterTransactions.currentTransaction(database);
    final boolean[] open = { true };
    database.newDocument("Node").set("id", "1").save();
    own.setRollbackOnly("issue 11046: refused commit");

    assertThatThrownBy(() -> ImporterTransactions.commitOwnTransaction(database, open, own))
        .isInstanceOf(TransactionException.class);

    assertThat(open[0]).as("the refused transaction is still the loop's to roll back").isTrue();
    assertThat(ImporterTransactions.isOwnTransactionLive(database, open[0], own)).isTrue();
    database.rollback();
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countOf("Node")).isZero();
  }

  private Neo4jImporter importer() throws Exception {
    final Neo4jImporter importer = new Neo4jImporter(database, new ImporterContext());
    final Field field = Neo4jImporter.class.getDeclaredField("inputStream");
    field.setAccessible(true);
    field.set(importer, new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
    return importer;
  }

  /** Writes a row and marks the import's transaction rollback-only: the engine then refuses its commit before it starts. */
  private Callable<Void, JSONObject> refuseTheCommit() {
    return json -> {
      database.newDocument("Node").set("id", json.getString("id")).save();
      ((DatabaseInternal) database).getTransaction().setRollbackOnly("issue 11046: refused commit");
      return null;
    };
  }

  private static void invokeReadFile(final Neo4jImporter importer, final Callable<Void, JSONObject> callback) throws Throwable {
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
}
