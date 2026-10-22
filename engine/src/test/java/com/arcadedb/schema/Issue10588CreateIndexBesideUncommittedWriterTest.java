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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10588: {@code CREATE INDEX} silently skipped a record another session had written and not yet committed. The build scans
 * the committed state, and the writer's pages are invisible to it; the writer's commit staged no entry either, because the index
 * did not exist when the record was saved. The record stayed invisible to every index-backed query, and a UNIQUE index accepted a
 * duplicate of its key - with {@code CREATE INDEX} reporting success. The same hole opened for an OUTER frame of the caller's own
 * stack (an empty nested transaction over an outer one that wrote a record, which is what a {@code sqlscript} with its own
 * {@code BEGIN} looks like inside an open transaction).
 * <p>
 * The rule: every record that is in the type when a build ends has an entry in the index that build created. A transaction that
 * began before an index was registered on a bucket it wrote makes up the entries of that index at its own commit, under the same
 * read lock that keeps a {@code CREATE INDEX} out, whatever the build did.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10588CreateIndexBesideUncommittedWriterTest extends TestHelper {

  private static final int COMMITTED = 20;

  // ---- shape A: another session holds the write

  @Test
  void aUniqueIndexBuiltBesideAnotherSessionsUncommittedWriteStillEnforcesItsKey() throws Exception {
    createPopulatedType("T", "p");

    runWriterAcrossIndexCreation(() -> database.newDocument("T").set("p", 999).save(), () -> database.command("sql", "CREATE INDEX ON T (p) UNIQUE"));

    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 999)).isEqualTo(1L);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T").set("p", 999).save())).isInstanceOf(
        DuplicatedKeyException.class);
    assertThat(database.countType("T", true)).isEqualTo(COMMITTED + 1);
  }

  @Test
  void aNotUniqueIndexBuiltBesideAnotherSessionsUncommittedWriteFindsTheRecord() throws Exception {
    createPopulatedType("T", "p");

    runWriterAcrossIndexCreation(() -> database.newDocument("T").set("p", 999).save(), () -> database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE"));

    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 999)).as("through the index").isEqualTo(1L);
  }

  @Test
  void anUpdateAndADeleteMadeBesideTheBuildAreReflectedInTheIndex() throws Exception {
    createPopulatedType("T", "p");

    runWriterAcrossIndexCreation(() -> {
      // an update that moves a key, a delete, a record created and deleted, and a record created and updated
      final Document moved = database.query("sql", "SELECT FROM T WHERE p = 5").next().toElement();
      moved.modify().set("p", 500).save();
      database.query("sql", "SELECT FROM T WHERE p = 7").next().toElement().delete();
      database.newDocument("T").set("p", 777).save().delete();
      final Document created = database.newDocument("T").set("p", 888).save();
      created.modify().set("p", 889).save();
    }, () -> database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE"));

    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 5)).isZero();
    assertThat(lookup("T", "p", 500)).isEqualTo(1L);
    assertThat(lookup("T", "p", 7)).isZero();
    assertThat(lookup("T", "p", 777)).isZero();
    assertThat(lookup("T", "p", 888)).isZero();
    assertThat(lookup("T", "p", 889)).isEqualTo(1L);
    assertThat(database.countType("T", true)).isEqualTo(COMMITTED - 1 + 1);
  }

  @Test
  void aWriterThatRollsBackLeavesNoEntry() throws Exception {
    createPopulatedType("T", "p");

    final CountDownLatch written = new CountDownLatch(1);
    final CountDownLatch indexed = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.begin();
        database.newDocument("T").set("p", 999).save();
        written.countDown();
        indexed.await(30, TimeUnit.SECONDS);
        database.rollback();
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    writer.start();
    assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
    database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
    indexed.countDown();
    writer.join(30_000);
    assertThat(failure.get()).isNull();

    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 999)).isZero();
    database.transaction(() -> database.newDocument("T").set("p", 999).save());
    assertThat(lookup("T", "p", 999)).isEqualTo(1L);
  }

  @Test
  void aWriterThatCommitsADuplicateOfAKeyTakenAfterTheBuildIsRefusedAtItsCommit() throws Exception {
    createPopulatedType("T", "p");

    final CountDownLatch written = new CountDownLatch(1);
    final CountDownLatch indexed = new CountDownLatch(1);
    final AtomicReference<Throwable> commitFailure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.begin();
        database.newDocument("T").set("p", 999).save();
        written.countDown();
        indexed.await(30, TimeUnit.SECONDS);
        try {
          database.commit();
        } catch (final Throwable t) {
          commitFailure.set(t);
          if (database.isTransactionActive())
            database.rollback();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    writer.start();
    assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
    database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
    // another session takes the key while the writer's record is still pending: the index is live, so this commit wins the key
    database.transaction(() -> database.newDocument("T").set("p", 999).save());
    indexed.countDown();
    writer.join(30_000);

    assertThat(commitFailure.get()).as("the pending record would be a second holder of the key").isInstanceOf(DuplicatedKeyException.class);
    assertThat(lookup("T", "p", 999)).isEqualTo(1L);
    assertThat(database.countType("T", true)).isEqualTo(COMMITTED + 1);
    assertIndexCompleteFor("T", "p");
  }

  @Test
  void aWriterThatSavedBothBeforeAndAfterTheIndexExistedIsRefusedRetryablyAndTheRetryIsIndexedOnce() throws Exception {
    savedBeforeAndAfterTheIndexExisted("UNIQUE");
  }

  @Test
  void aWriterThatSavedBothBeforeAndAfterANotUniqueIndexExistedIsRefusedRetryablyAndTheRetryIsIndexedOnce() throws Exception {
    savedBeforeAndAfterTheIndexExisted("NOTUNIQUE");
  }

  /**
   * The entries of what the writer saved AFTER the index existed are already queued on its transaction, and an entry it owes for what
   * it saved BEFORE cannot be told from them: queueing the owed ones from the committed state would queue the others twice, which an
   * index that counts what it is given (the corpus counters of a full-text index) takes for two documents. So the commit is refused
   * with a retryable exception, and the retry finds the index there from its first write.
   */
  private void savedBeforeAndAfterTheIndexExisted(final String kind) throws Exception {
    // one bucket, so the writes before and after the index land in the same one: in a type of several buckets each bucket is
    // judged by what the transaction wrote to it, and writes to two different buckets are both exact
    createPopulatedType("T", "p", 1);

    final CountDownLatch written = new CountDownLatch(1);
    final CountDownLatch indexed = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicReference<Throwable> commitFailure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.begin();
        database.newDocument("T").set("p", 900).save();
        written.countDown();
        indexed.await(30, TimeUnit.SECONDS);
        database.newDocument("T").set("p", 901).save();
        database.query("sql", "SELECT FROM T WHERE p = 3").next().toElement().modify().set("p", 303).save();
        try {
          database.commit();
        } catch (final Throwable t) {
          commitFailure.set(t);
          if (database.isTransactionActive())
            database.rollback();
        }
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    writer.start();
    assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
    database.command("sql", "CREATE INDEX ON T (p) " + kind);
    indexed.countDown();
    writer.join(30_000);
    assertThat(failure.get()).isNull();

    assertThat(commitFailure.get()).isInstanceOf(NeedRetryException.class).hasMessageContaining("was created on type 'T'");
    // nothing of the refused transaction is there, and the index describes the committed state
    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 900)).isZero();
    assertThat(lookup("T", "p", 901)).isZero();
    assertThat(lookup("T", "p", 3)).isEqualTo(1L);

    // the retry
    database.transaction(() -> {
      database.newDocument("T").set("p", 900).save();
      database.newDocument("T").set("p", 901).save();
      database.query("sql", "SELECT FROM T WHERE p = 3").next().toElement().modify().set("p", 303).save();
    });
    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 900)).isEqualTo(1L);
    assertThat(lookup("T", "p", 901)).isEqualTo(1L);
    assertThat(lookup("T", "p", 3)).isZero();
    assertThat(lookup("T", "p", 303)).isEqualTo(1L);
  }

  /** The refusal of a writer on both sides of the index is retried by the retry loop of {@code Database.transaction()}, and succeeds. */
  @Test
  void theRetryLoopOfTransactionAbsorbsTheRefusalOfAWriterOnBothSidesOfTheIndex() throws Exception {
    createPopulatedType("T", "p", 1);

    final CountDownLatch written = new CountDownLatch(1);
    final CountDownLatch indexed = new CountDownLatch(1);
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.transaction(() -> {
          final boolean firstAttempt = attempts.incrementAndGet() == 1;
          database.newDocument("T").set("p", 900).save();
          if (firstAttempt) {
            written.countDown();
            try {
              indexed.await(30, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          database.newDocument("T").set("p", 901).save();
        });
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    writer.start();
    assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
    database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
    indexed.countDown();
    writer.join(30_000);

    assertThat(failure.get()).isNull();
    assertThat(attempts.get()).as("the first attempt was refused, the second committed").isEqualTo(2);
    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 900)).isEqualTo(1L);
    assertThat(lookup("T", "p", 901)).isEqualTo(1L);
  }

  @Test
  void aFullTextIndexBuiltBesideAnotherSessionsWriteCountsItsDocumentsOnce() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2").close();
    database.command("sql", "CREATE PROPERTY Doc.content STRING").close();
    database.transaction(() -> {
      for (int i = 0; i < COMMITTED; i++)
        database.newDocument("Doc").set("content", "alpha beta " + i).save();
    });

    runWriterAcrossIndexCreation(() -> database.newDocument("Doc").set("content", "alpha gamma pending").save(),
        () -> database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT"));

    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'pending') = true")) {
      assertThat(rs.stream().count()).as("the pending record, found through the full-text index").isEqualTo(1L);
    }
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).as("the corpus counters hold each document once").contains("\"totalDocs\":" + (COMMITTED + 1) + ",");
    }
  }

  @Test
  void aFullTextIndexBuiltInTheSameTransactionCountsItsDocumentsOnce() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2").close();
    database.command("sql", "CREATE PROPERTY Doc.content STRING").close();
    database.transaction(() -> {
      for (int i = 0; i < COMMITTED; i++)
        database.newDocument("Doc").set("content", "alpha beta " + i).save();
    });

    database.begin();
    database.newDocument("Doc").set("content", "alpha gamma pending").save();
    database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
    database.commit();

    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).as("the build queued the caller's record, and the commit did not queue it again")
          .contains("\"totalDocs\":" + (COMMITTED + 1) + ",");
    }
  }

  @Test
  void aWriterThatBeganAfterTheIndexExistedIsNotTouched() throws Exception {
    createPopulatedType("T", "p");
    database.command("sql", "CREATE INDEX ON T (p) UNIQUE");

    database.transaction(() -> database.newDocument("T").set("p", 999).save());

    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 999)).isEqualTo(1L);
  }

  @Test
  void aWriterThatOnlyWroteToTheTypeAfterTheIndexExistedIsNotRefusedNorQueuedTwice() throws Exception {
    createPopulatedType("T", "p");
    createPopulatedType("U", "p");

    final CountDownLatch written = new CountDownLatch(1);
    final CountDownLatch indexed = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        // the transaction began before the index existed and wrote to ANOTHER type, so it owes the index nothing: what it writes to T
        // from here on is queued as it is written
        database.begin();
        database.newDocument("U").set("p", 777).save();
        written.countDown();
        indexed.await(30, TimeUnit.SECONDS);
        database.newDocument("T").set("p", 999).save();
        database.commit();
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    writer.start();
    assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
    database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
    indexed.countDown();
    writer.join(30_000);
    assertThat(failure.get()).isNull();

    assertIndexCompleteFor("T", "p");
    assertThat(lookup("T", "p", 999)).isEqualTo(1L);
    assertThat(database.countType("U", true)).isEqualTo(COMMITTED + 1);
  }

  @Test
  void aTypeRenamedBetweenTheIndexAndTheCommitStillGetsTheEntriesTheWriterOwes() throws Exception {
    createPopulatedType("T", "p");

    runWriterAcrossIndexCreation(() -> database.newDocument("T").set("p", 999).save(), () -> {
      database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
      // the sub-indexes are renamed with their type, and the registration of the index has to follow
      database.command("sql", "ALTER TYPE T NAME T2");
    });

    assertIndexCompleteFor("T2", "p");
    assertThat(lookup("T2", "p", 999)).isEqualTo(1L);
  }

  @Test
  void aTransactionCommittingUnderneathAnotherOneThatOwesEntriesIsRefusedNotSkipped() {
    createPopulatedType("V", "id");

    database.begin();
    database.newDocument("V").set("id", 999).save();
    database.begin();
    try {
      database.command("sql", "CREATE INDEX ON V (id) UNIQUE");

      final TransactionContext outer = DatabaseContext.INSTANCE.getContext(((DatabaseInternal) database).getDatabasePath()).transactions.get(0);
      assertThatThrownBy(outer::commit).isInstanceOf(TransactionException.class).hasMessageContaining("innermost");
    } finally {
      if (database.isTransactionActive())
        database.rollbackAllNested();
    }

    assertThat(database.isTransactionActive()).isFalse();
    assertIndexCompleteFor("V", "id");
    assertThat(lookup("V", "id", 999)).isZero();
    assertThat(database.countType("V", true)).isEqualTo(COMMITTED);
  }

  @Test
  void aWriterOfAnotherTypeIsNotTouched() throws Exception {
    createPopulatedType("T", "p");
    createPopulatedType("U", "p");

    runWriterAcrossIndexCreation(() -> database.newDocument("U").set("p", 999).save(), () -> database.command("sql", "CREATE INDEX ON T (p) UNIQUE"));

    assertIndexCompleteFor("T", "p");
    assertThat(database.countType("U", true)).isEqualTo(COMMITTED + 1);
  }

  // ---- shape B: an outer frame of the caller's own stack holds the write

  @Test
  void anOuterFrameOfTheCallersOwnStackThatWroteIsIndexedAtItsCommit() {
    createPopulatedType("V", "id");

    database.begin();
    database.newDocument("V").set("id", 999).save();
    database.command("sqlscript", "BEGIN;\nCREATE INDEX ON V (id) UNIQUE;\nCOMMIT;");
    database.commit();

    assertIndexCompleteFor("V", "id");
    assertThat(lookup("V", "id", 999)).isEqualTo(1L);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("V").set("id", 999).save())).isInstanceOf(
        DuplicatedKeyException.class);
  }

  @Test
  void aNotUniqueIndexBuiltFromAnEmptyInnerFrameFindsTheOuterFramesRecord() {
    createPopulatedType("V", "id");

    database.begin();
    database.newDocument("V").set("id", 999).save();
    database.command("sqlscript", "BEGIN;\nCREATE INDEX ON V (id) NOTUNIQUE;\nCOMMIT;");
    database.commit();

    assertIndexCompleteFor("V", "id");
    assertThat(lookup("V", "id", 999)).isEqualTo(1L);
  }

  @Test
  void aBucketIndexBuiltFromAnEmptyInnerFrameFindsTheOuterFramesRecord() {
    createPopulatedType("V", "id");
    final DocumentType type = database.getSchema().getType("V");

    database.begin();
    final RID rid = database.newDocument("V").set("id", 999).save().getIdentity();
    database.begin();
    for (final var bucket : type.getBuckets(false))
      database.getSchema().buildBucketIndex("V", bucket.getName(), new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false).create();
    database.commit();
    database.commit();

    assertThat(rid).isNotNull();
    assertIndexCompleteFor("V", "id");
    assertThat(lookup("V", "id", 999)).isEqualTo(1L);
  }

  @Test
  void anInheritedIndexPropagatedFromAnEmptyInnerFrameFindsTheOuterFramesRecord() {
    database.command("sql", "CREATE DOCUMENT TYPE P").close();
    database.command("sql", "CREATE PROPERTY P.id INTEGER").close();
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE").close();
    database.command("sql", "CREATE DOCUMENT TYPE C").close();
    database.transaction(() -> {
      for (int i = 0; i < COMMITTED; i++)
        database.newDocument("C").set("id", 10_000 + i).save();
    });

    database.begin();
    database.newDocument("C").set("id", 999).save();
    database.begin();
    database.getSchema().getType("C").addSuperType("P");
    database.commit();
    database.commit();

    // the index of P now covers the buckets of C, and the record the outer frame wrote is in one of them
    assertThat(database.getSchema().getType("P").getAllIndexes(true)).isNotEmpty();
    assertThat(lookup("P", "id", 999)).isEqualTo(1L);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("C").set("id", 999).save())).isInstanceOf(
        DuplicatedKeyException.class);
  }

  @Test
  void oneFrameOnlyStaysCleanAsItWas() {
    createPopulatedType("V", "id");

    database.begin();
    database.newDocument("V").set("id", 999).save();
    database.command("sql", "CREATE INDEX ON V (id) UNIQUE");
    database.commit();

    assertIndexCompleteFor("V", "id");
    assertThat(lookup("V", "id", 999)).isEqualTo(1L);
  }

  // ---- helpers

  private void createPopulatedType(final String type, final String property) {
    createPopulatedType(type, property, 4);
  }

  private void createPopulatedType(final String type, final String property, final int buckets) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets).close();
    database.command("sql", "CREATE PROPERTY " + type + "." + property + " INTEGER").close();
    database.transaction(() -> {
      for (int i = 0; i < COMMITTED; i++)
        database.newDocument(type).set(property, i).save();
    });
  }

  /**
   * The writer thread does its writes and holds the transaction open; this thread runs the DDL; then the writer commits. The write
   * is staged before the index exists and committed after it was built.
   */
  private void runWriterAcrossIndexCreation(final Runnable writes, final Runnable ddl) throws Exception {
    final CountDownLatch written = new CountDownLatch(1);
    final CountDownLatch indexed = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.begin();
        writes.run();
        written.countDown();
        indexed.await(30, TimeUnit.SECONDS);
        database.commit();
      } catch (final Throwable t) {
        failure.set(t);
        written.countDown();
      }
    }, "writer-beside-create-index");
    writer.start();
    assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
    try {
      ddl.run();
    } finally {
      indexed.countDown();
    }
    writer.join(30_000);
    assertThat(writer.isAlive()).isFalse();
    assertThat(failure.get()).isNull();
  }

  /** Every record of the type has an entry in the index, and a lookup finds each of the records a scan finds. */
  private void assertIndexCompleteFor(final String type, final String property) {
    final TypeIndex index = database.getSchema().getType(type).getPolymorphicIndexByProperties(property);
    assertThat(index).as("an index on %s.%s", type, property).isNotNull();
    assertThat(index.countEntries()).as("entries of the index vs records of the type").isEqualTo(database.countType(type, true));

    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " FROM " + type)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(property));
    }
    for (final Integer value : values)
      assertThat(lookup(type, property, value)).as("%s.%s = %d through the index", type, property, value).isEqualTo(1L);
  }

  /** Records found by an index-backed equality, as opposed to a scan. */
  private long lookup(final String type, final String property, final int value) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE " + property + " = ?", value)) {
      return rs.stream().count();
    }
  }
}
