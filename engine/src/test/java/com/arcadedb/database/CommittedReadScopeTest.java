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
package com.arcadedb.database;

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract of {@link CommittedReadScope} itself, independently of the vector index that motivated it
 * (issue #7974): what a read inside the scope sees, what the calling thread gets back afterwards, and what happens
 * to work the block leaves open on the scope's own context.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CommittedReadScopeTest {
  private static final String DB_PATH = "./target/databases/CommittedReadScopeTest";

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aThreadWithNoActiveTransactionHasNothingSuspended() {
    withDatabase(db -> {
      final RID rid = insertRow(db, "committed");

      final List<TransactionContext> before = List.copyOf(transactionsOf(db));
      try (final CommittedReadScope ignored = CommittedReadScope.open(db)) {
        assertThat(transactionsOf(db))
            .as("an idle thread answers every read from committed state already, so nothing is replaced")
            .isEqualTo(before);
      }
      assertThat(transactionsOf(db)).isEqualTo(before);
      assertThat(db.lookupByRID(rid, true).asDocument().getString("name")).isEqualTo("committed");
    });
  }

  @Test
  void aReadInsideTheScopeDoesNotSeeTheCallersUncommittedWrite() {
    withDatabase(db -> {
      final RID rid = insertRow(db, "committed");

      db.begin();
      db.lookupByRID(rid, true).asDocument().modify().set("name", "uncommitted").save();
      final TransactionContext callersTransaction = db.getTransaction();

      try (final CommittedReadScope ignored = CommittedReadScope.open(db)) {
        assertThat(db.getTransaction())
            .as("the scope runs on a context of its own")
            .isNotSameAs(callersTransaction);
        assertThat(db.isTransactionActive())
            .as("that context is never begun, so the thread reads exactly as one that opened no transaction")
            .isFalse();
        assertThat(db.lookupByRID(rid, true).asDocument().getString("name")).isEqualTo("committed");
      }

      assertThat(db.getTransaction())
          .as("the caller's transaction comes back untouched")
          .isSameAs(callersTransaction);
      assertThat(db.isTransactionActive()).isTrue();
      assertThat(db.lookupByRID(rid, true).asDocument().getString("name"))
          .as("and still sees its own write")
          .isEqualTo("uncommitted");

      db.rollback();
      assertThat(db.lookupByRID(rid, true).asDocument().getString("name")).isEqualTo("committed");
    });
  }

  @Test
  void everyFrameLeftOpenOnTheScopesOwnContextIsRolledBack() {
    withDatabase(db -> {
      insertRow(db, "committed");

      db.begin();
      final TransactionContext callersTransaction = db.getTransaction();
      final List<TransactionContext> callersStack = List.copyOf(transactionsOf(db));

      final TransactionContext[] opened = new TransactionContext[2];
      try (final CommittedReadScope ignored = CommittedReadScope.open(db)) {
        // The first begin() re-begins the scope's own inactive context in place; the second, finding it active,
        // pushes a genuinely nested one. Both are left open on purpose - the sweep in close() is what this pins.
        db.begin();
        opened[0] = db.getTransaction();
        db.begin();
        opened[1] = db.getTransaction();

        assertThat(opened[1]).isNotSameAs(opened[0]);
        assertThat(transactionsOf(db)).hasSize(2);
      }

      assertThat(opened[0].isActive()).as("the outer frame the block left open must not survive").isFalse();
      assertThat(opened[1].isActive()).as("nor the inner one").isFalse();
      assertThat(transactionsOf(db)).isEqualTo(callersStack);
      assertThat(db.getTransaction()).isSameAs(callersTransaction);
      assertThat(db.isTransactionActive()).isTrue();

      db.rollback();
    });
  }

  private static List<TransactionContext> transactionsOf(final DatabaseInternal db) {
    return DatabaseContext.INSTANCE.getContext(db.getDatabasePath()).transactions;
  }

  private static RID insertRow(final DatabaseInternal db, final String name) {
    final RID[] rid = new RID[1];
    db.transaction(() -> rid[0] = db.newDocument("Doc").set("name", name).save().getIdentity());
    return rid[0];
  }

  private interface DatabaseTest {
    void run(DatabaseInternal db);
  }

  private static void withDatabase(final DatabaseTest test) {
    FileUtils.deleteRecursively(new File(DB_PATH));
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      try {
        db.transaction(() -> {
          final DocumentType t = db.getSchema().createDocumentType("Doc");
          t.createProperty("name", Type.STRING);
        });
        test.run(db);
      } finally {
        // A test that fails while a transaction is still open must still be the failure that gets reported: an
        // open transaction makes drop() throw from this finally, which would replace the assertion error with a
        // "Cannot drop the database in transaction" nobody can act on.
        db.rollbackAllNested();
        db.drop();
      }
    }
  }
}
