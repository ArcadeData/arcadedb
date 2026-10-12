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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.BeforeRecordCreateListener;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9851: {@code LocalSchema.copyType()} copied the records in a transaction of its own and closed it with
 * {@code if (db.isTransactionActive()) db.rollback()}. An ArcadeDB nested transaction is an independent transaction rather than a
 * savepoint, and its {@code commit()} pops the context, so on a thread that already had a transaction open the question was answered
 * for the CALLER's transaction and the rollback discarded the caller's uncommitted work - on the successful path as well as the
 * failing one. The method now holds an {@code OwnTransaction} and rolls back only that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9851CopyTypeKeepsTheCallerTransactionTest extends TestHelper {

  private static final int RECORDS = 25;

  private DatabaseInternal internal() {
    return (DatabaseInternal) database;
  }

  private void createSource() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Source").createProperty("n", Type.INTEGER);
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Source").set("n", i).save();
    });
  }

  /** The caller's own pending write survives a copy that succeeds, and is still its to commit. */
  @Test
  void copyTypeSucceedingInsideACallerTransactionKeepsTheCallersPendingWork() {
    createSource();
    database.getSchema().createDocumentType("Witness");

    database.begin();
    try {
      database.newDocument("Witness").set("w", "pending").save();
      final TransactionContext callers = internal().getTransaction();

      database.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65_536, 10);

      assertThat(database.isTransactionActive()).as("the caller's transaction must still be open").isTrue();
      assertThat(internal().getTransaction()).as("and be the same one").isSameAs(callers);
      assertThat(database.countType("Witness", false)).as("the caller's pending write is still visible to it").isEqualTo(1L);

      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.countType("Witness", false)).as("the caller's work reached the database").isEqualTo(1L);
    assertThat(database.countType("Target", false)).as("and so did the copy").isEqualTo((long) RECORDS);
  }

  /** A single batch (batch size 0 commits everything once, at the end) is the same case with one commit instead of several. */
  @Test
  void copyTypeInOneBatchInsideACallerTransactionKeepsTheCallersPendingWork() {
    createSource();
    database.getSchema().createDocumentType("Witness");

    database.begin();
    try {
      database.newDocument("Witness").set("w", "pending").save();

      database.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65_536, 0);

      assertThat(database.isTransactionActive()).isTrue();
      assertThat(database.countType("Witness", false)).isEqualTo(1L);
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.countType("Witness", false)).isEqualTo(1L);
    assertThat(database.countType("Target", false)).isEqualTo((long) RECORDS);
  }

  /**
   * The failing arm: a record that cannot be created makes the copy throw mid-way, after one batch already committed. The failure
   * rolls back the batch in flight and nothing else: the copy is dropped by the method's own safety net, and the caller's
   * transaction, with its pending write, is untouched.
   */
  @Test
  void copyTypeFailingInsideACallerTransactionKeepsTheCallersPendingWork() {
    createSource();
    database.getSchema().createDocumentType("Witness");

    final AtomicInteger created = new AtomicInteger();
    final BeforeRecordCreateListener failOnTheFifteenthCopiedRecord = record -> {
      if ("Target".equals(((Document) record).getTypeName()) && created.incrementAndGet() == 15)
        throw new IllegalStateException("simulated failure while copying");
      return true;
    };
    database.getEvents().registerListener(failOnTheFifteenthCopiedRecord);
    try {
      database.begin();
      try {
        database.newDocument("Witness").set("w", "pending").save();
        final TransactionContext callers = internal().getTransaction();

        assertThatThrownBy(() -> database.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65_536, 10))
            .isInstanceOf(IllegalStateException.class);

        assertThat(database.isTransactionActive()).as("the caller's transaction must still be open").isTrue();
        assertThat(internal().getTransaction()).as("and be the same one").isSameAs(callers);
        assertThat(database.countType("Witness", false)).as("the caller's pending write is still visible to it").isEqualTo(1L);

        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    } finally {
      database.getEvents().unregisterListener(failOnTheFifteenthCopiedRecord);
    }

    assertThat(database.countType("Witness", false)).as("the caller's work reached the database").isEqualTo(1L);
    assertThat(database.getSchema().existsType("Target")).as("the half-built copy is dropped").isFalse();
    assertThat(database.countType("Source", false)).as("the source is untouched").isEqualTo((long) RECORDS);
  }

  /** The schema must not be asked to leave a half-built copy behind, and the engine must not leak a transaction of its own. */
  @Test
  void copyTypeLeavesNoTransactionOfItsOwnOnTheThread() {
    createSource();

    database.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65_536, 10);

    assertThat(database.isTransactionActive()).as("no transaction is left open on a thread that had none").isFalse();
    assertThat(database.countType("Target", false)).isEqualTo((long) RECORDS);
  }
}
