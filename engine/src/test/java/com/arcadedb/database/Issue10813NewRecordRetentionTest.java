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

import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10813: a document saved in a transaction kept both its property map and its serialized buffer alive until the commit,
 * because the transaction held the document itself. With read-your-writes off (the importer, {@code GraphBatch}) nothing in the
 * transaction reads it back, so it holds the RID and only a weak reference, for the identity reset of a rollback (#4562): a document
 * the caller dropped is collectable before the commit, one the caller holds is still reset.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10813NewRecordRetentionTest extends TestHelper {
  private static final String TYPE = "Doc";

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("id", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "id");
  }

  @AfterEach
  void restoreReadYourWrites() {
    database.setReadYourWrites(true);
  }

  @Test
  void withReadYourWritesOffADroppedNewDocumentIsNotHeldUntilTheCommit() {
    database.setReadYourWrites(false);
    database.begin();
    final WeakReference<MutableDocument> saved = saveAndDrop(1);
    final RID rid = identityOf(saved);
    assertThat(rid).isNotNull();

    assertThat(collected(saved)).as("nothing in the transaction holds the document").isTrue();
    assertThat(((DatabaseInternal) database).getTransaction().isCreatedInTransaction(rid))
        .as("the transaction still knows it created the record").isTrue();
    database.commit();

    assertThat(database.countType(TYPE, false)).isEqualTo(1);
    assertThat(database.lookupByRID(rid, true).asDocument().getInteger("id")).isEqualTo(1);
  }

  /** The control: with read-your-writes on, the transaction's record cache holds the document, as a lookup in it returns it. */
  @Test
  void withReadYourWritesOnTheTransactionStillHoldsTheDocument() {
    database.begin();
    final WeakReference<MutableDocument> saved = saveAndDrop(1);
    assertThat(collected(saved)).isFalse();
    assertThat(database.lookupByRID(saved.get().getIdentity(), false)).isSameAs(saved.get());
    database.commit();
  }

  @Test
  void withReadYourWritesOffARollbackStillResetsADocumentTheCallerHolds() {
    database.setReadYourWrites(false);
    database.begin();
    final MutableDocument held = database.newDocument(TYPE).set("id", 1);
    held.save();
    saveAndDrop(2);
    assertThat(held.getIdentity()).isNotNull();
    database.rollback();

    assertThat(held.getIdentity()).as("identity reset to provisional by the rollback").isNull();
    database.transaction(held::save);
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  /** A statement that fails inside the transaction takes back its own records, and leaves the others, held or collected. */
  @Test
  void withReadYourWritesOffAFailedStatementTakesBackOnlyItsOwnRecords() {
    database.setReadYourWrites(false);
    database.begin();
    final WeakReference<MutableDocument> dropped = saveAndDrop(1);
    final MutableDocument held = database.newDocument(TYPE).set("id", 2);
    held.save();
    assertThat(collected(dropped)).isTrue();

    // the second row violates the unique index: the statement is undone, the transaction goes on (#10346)
    final Throwable failure = catchThrowable(
        () -> database.command("sqlscript", "INSERT INTO " + TYPE + " SET id = 3; INSERT INTO " + TYPE + " SET id = 2;"));
    assertThat(failure).isNotNull();
    assertThat(held.getIdentity()).as("a record of an earlier statement is left alone").isNotNull();
    database.commit();

    assertThat(database.countType(TYPE, false)).isEqualTo(2);
    assertThat(database.query("sql", "SELECT FROM " + TYPE + " WHERE id = 3").hasNext()).isFalse();
  }

  /** Saves a new document and returns only a weak reference to it, so this frame does not keep it reachable. */
  private WeakReference<MutableDocument> saveAndDrop(final int id) {
    final MutableDocument document = database.newDocument(TYPE).set("id", id);
    document.save();
    return new WeakReference<>(document);
  }

  /** Reads the reference once, in a frame of its own, so the caller holds nothing that keeps the document reachable. */
  private static RID identityOf(final WeakReference<MutableDocument> reference) {
    final MutableDocument document = reference.get();
    return document != null ? document.getIdentity() : null;
  }

  private static boolean collected(final WeakReference<?> reference) {
    for (int i = 0; i < 50 && reference.get() != null; i++) {
      System.gc();
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return reference.get() == null;
  }
}
