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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8610: under READ_COMMITTED, reading a record, letting a concurrent transaction commit a change to it,
 * then writing a value derived from the read through {@code modify()} lost the concurrent change silently, because
 * {@code modify()} reloads a vertex whose page moved on and the stale read disappeared with the reload. A document in
 * the same situation was already refused by the #6950 record-image check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue8610StaleReadModifyTest {
  private Database database;
  private RID      rid;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-8610-stale-read");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sqlscript", "CREATE VERTEX TYPE V; CREATE EDGE TYPE E; CREATE DOCUMENT TYPE D");
    final AtomicReference<RID> created = new AtomicReference<>();
    database.transaction(() -> created.set(database.newVertex("V").set("n", 0).save().getIdentity()));
    rid = created.get();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen()) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  /**
   * The issue: a read-modify-write on a vertex read in this transaction, over a concurrent commit, must be refused as a
   * retryable conflict, never commit a value computed from the stale read.
   */
  @Test
  void readModifyWriteOverAConcurrentCommitIsRefused() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    final int n = read.getInteger("n");
    commitConcurrently("n", 5);

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(readN()).as("the concurrent write survives").isEqualTo(5);
  }

  /**
   * The same for a vertex read through a scan.
   */
  @Test
  void readModifyWriteOfAScannedVertexOverAConcurrentCommitIsRefused() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Iterator<Record> it = database.iterateType("V", false);
    final Vertex read = it.next().asVertex();
    final int n = read.getInteger("n");
    commitConcurrently("n", 5);

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(readN()).isEqualTo(5);
  }

  /**
   * Retrying the transaction, as the retry helper does, reads the vertex again and succeeds: the refusal is retryable.
   */
  @Test
  void theRetryReadsAgainAndCommits() {
    final AtomicReference<Boolean> first = new AtomicReference<>(true);
    database.transaction(() -> {
      final Vertex read = rid.asVertex();
      final int n = read.getInteger("n");
      if (first.getAndSet(false))
        commitConcurrently("n", 5);
      read.modify().set("n", n + 1).save();
    }, false, 3);

    assertThat(readN()).as("the retry built on the concurrent 5").isEqualTo(6);
  }

  /**
   * A document read through a scan: its content is reloaded on modify() when its page moved on (issue #8312), which
   * hid the stale read from the #6950 record-image check the same way.
   */
  @Test
  void readModifyWriteOfAScannedDocumentOverAConcurrentCommitIsRefused() {
    final AtomicReference<RID> doc = new AtomicReference<>();
    database.transaction(() -> doc.set(database.newDocument("D").set("n", 0).save().getIdentity()));

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Document read = database.iterateType("D", false).next().asDocument();
    final int n = read.getInteger("n");
    commitConcurrently(() -> doc.get().asDocument().modify().set("n", 5).save());

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(doc.get().asDocument().getInteger("n")).isEqualTo(5);
  }

  /**
   * The same for an edge read through a scan.
   */
  @Test
  void readModifyWriteOfAScannedEdgeOverAConcurrentCommitIsRefused() {
    final AtomicReference<RID> edge = new AtomicReference<>();
    database.transaction(() -> {
      final MutableVertex other = database.newVertex("V").save();
      edge.set(rid.asVertex().newEdge("E", other, "n", 0).getIdentity());
    });

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Edge read = database.iterateType("E", false).next().asEdge();
    final int n = read.getInteger("n");
    commitConcurrently(() -> edge.get().asEdge().modify().set("n", 5).save());

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(edge.get().asEdge().getInteger("n")).isEqualTo(5);
  }

  /**
   * The mark survives a second modify() of the same record: that one finds the page pinned and does not reload, so a
   * value computed from the read before the first modify() must still be refused.
   */
  @Test
  void aSecondModifyOfTheSameStaleReadIsRefusedToo() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    final int n = read.getInteger("n");
    commitConcurrently("n", 5);
    read.modify(); // discarded

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(readN()).isEqualTo(5);
  }

  /**
   * The refusal is per record, as the #6950 check is for a document: a write to another property of a record changed
   * concurrently is refused too, since it may be computed from the property that changed (SET b = a + 1).
   */
  @Test
  void aWriteToAnotherPropertyOfAConcurrentlyChangedRecordIsRefused() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    final int n = read.getInteger("n");
    commitConcurrently("n", 5);

    assertThatThrownBy(() -> {
      read.modify().set("derived", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().has("derived")).isFalse();
  }

  /**
   * An UPDATE by RID loads the record lazily, so its first content read is the reload modify() performs after pinning
   * the page: a commit landing there is refused by the commit-time page check, as it must be. Committed or refused, the
   * concurrent write is never lost.
   */
  @Test
  void sqlUpdateByRidNeverLosesACommitLandingInsideTheStatement() {
    final boolean committed = runWithConcurrentCommitAfterFirstRead(
        () -> database.command("sql", "UPDATE V SET n = n + 1 WHERE @rid = ?", rid).close());

    assertThat(readN()).isEqualTo(committed ? 6 : 5);
  }

  /**
   * SQL UPDATE evaluates its SET against the record it converted with modify(), i.e. against the reloaded content: a
   * concurrent commit landing between the scan that read the record and the write is built upon, not refused.
   */
  @Test
  void sqlUpdateOverAScanBuildsOnACommitLandingInsideTheStatement() {
    final boolean committed = runWithConcurrentCommitAfterFirstRead(
        () -> database.command("sql", "UPDATE V SET n = n + 1").close());

    assertThat(committed).isTrue();
    assertThat(readN()).isEqualTo(6);
  }

  /**
   * And for an openCypher SET (the MERGE actions are covered by Issue8538MergeOnMatchLostUpdateTest).
   */
  @Test
  void cypherSetBuildsOnACommitLandingInsideTheStatement() {
    final boolean committed = runWithConcurrentCommitAfterFirstRead(
        () -> database.command("cypher", "MATCH (v:V) SET v.n = v.n + 1").close());

    assertThat(committed).isTrue();
    assertThat(readN()).isEqualTo(6);
  }

  /**
   * The read-transaction id stays non-negative however far the begin sequence goes: a negative id means "unknown" and
   * would switch the check off, and -2 marks a stale read.
   */
  @Test
  void theReadTransactionIdIsNeverNegative() {
    assertThat(ImmutableDocument.readTransactionId(-1)).isEqualTo(-1);
    assertThat(ImmutableDocument.readTransactionId(0)).isZero();
    assertThat(ImmutableDocument.readTransactionId(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    assertThat(ImmutableDocument.readTransactionId(1L << 31)).isZero();
    assertThat(ImmutableDocument.readTransactionId((1L << 32) - 2)).isNotNegative();
    assertThat(ImmutableDocument.readTransactionId(Long.MAX_VALUE)).isNotNegative();
  }

  /**
   * A transaction rolled back and begun again is another transaction: a record read in the first one is refreshed
   * silently when modified in the second, like any record held across transactions.
   */
  @Test
  void aRecordReadInARolledBackTransactionIsRefreshedInTheNextOne() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    database.rollback();

    commitConcurrently("n", 5);

    database.transaction(() -> read.modify().set("other", 1).save());
    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().getInteger("other")).isEqualTo(1);
  }

  /**
   * SQL UPDATE MERGE and CONTENT go through the same conversion as SET, so they build on a commit landing inside the
   * statement too.
   */
  @Test
  void sqlUpdateMergeBuildsOnACommitLandingInsideTheStatement() {
    final boolean committed = runWithConcurrentCommitAfterFirstRead(
        () -> database.command("sql", "UPDATE V MERGE {\"m\": 1}").close());

    assertThat(committed).isTrue();
    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().getInteger("m")).isEqualTo(1);
  }

  @Test
  void sqlUpdateContentBuildsOnACommitLandingInsideTheStatement() {
    final boolean committed = runWithConcurrentCommitAfterFirstRead(
        () -> database.command("sql", "UPDATE V CONTENT {\"n\": 7}").close());

    assertThat(committed).isTrue();
    assertThat(readN()).isEqualTo(7);
  }

  /**
   * An openCypher REMOVE writes no value computed from the read, but it is a property write on a record that changed
   * since the MATCH read it, so it is refused like any other; the concurrent write is never lost.
   */
  @Test
  void cypherRemoveNeverLosesACommitLandingInsideTheStatement() {
    database.transaction(() -> rid.asVertex().modify().set("tag", "x").save());

    final boolean committed = runWithConcurrentCommitAfterFirstRead(
        () -> database.command("cypher", "MATCH (v:V) REMOVE v.tag").close());

    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().has("tag")).isEqualTo(!committed);
  }

  /**
   * Creating an edge changes only the vertex edge lists, which is what the reload exists for: a concurrent change to the
   * vertex properties must not turn it into a conflict, nor be lost.
   */
  @Test
  void edgeCreationOverAConcurrentPropertyChangeCommits() {
    final AtomicReference<RID> other = new AtomicReference<>();
    database.transaction(() -> other.set(database.newVertex("V").set("n", 100).save().getIdentity()));

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    commitConcurrently("n", 5);
    read.newEdge("E", other.get().asVertex());
    database.commit();

    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
  }

  /**
   * A vertex read outside this transaction (held across transactions) keeps the refresh it always had: its content is
   * reloaded by modify() and the write commits. Only a read made inside the transaction is checked.
   */
  @Test
  void aVertexReadBeforeTheTransactionIsStillRefreshed() {
    final Vertex held = rid.asVertex();
    database.transaction(() -> held.modify().set("a", 1).save());
    database.transaction(() -> held.modify().set("b", 2).save());

    final Vertex reloaded = rid.asVertex();
    assertThat(reloaded.getInteger("a")).isEqualTo(1);
    assertThat(reloaded.getInteger("b")).isEqualTo(2);
  }

  /**
   * Nothing changed concurrently: no conflict, however the vertex was read.
   */
  @Test
  void aCurrentReadCommits() {
    database.transaction(() -> {
      final Vertex read = rid.asVertex();
      read.modify().set("n", read.getInteger("n") + 1).save();
    });
    assertThat(readN()).isEqualTo(1);
  }

  /**
   * The switch restores the previous behavior (the reload wins silently), for an application that relies on it.
   */
  @Test
  void theCheckCanBeDisabled() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_STALE_READ_CHECK, false);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    final int n = read.getInteger("n");
    commitConcurrently("n", 5);
    read.modify().set("n", n + 1).save();
    database.commit();

    assertThat(readN()).as("pre-#8610 behavior: the value computed from the stale read overwrites the concurrent 5")
        .isEqualTo(1);
  }

  private void commitConcurrently(final String property, final int value) {
    commitConcurrently(() -> rid.asVertex().modify().set(property, value).save());
  }

  private void commitConcurrently(final Runnable write) {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread concurrent = new Thread(() -> {
      try {
        database.transaction(write::run);
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    concurrent.start();
    try {
      concurrent.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(failure.get()).isNull();
  }

  /**
   * Runs {@code body} in a READ_COMMITTED transaction; right after the first V record it reads, another transaction
   * sets {@code n = 5} and commits.
   *
   * @return whether the transaction committed; {@code false} when it was refused with a retryable conflict
   */
  private boolean runWithConcurrentCommitAfterFirstRead(final Runnable body) {
    final Thread bodyThread = Thread.currentThread();
    final AtomicBoolean armed = new AtomicBoolean(true);
    final AfterRecordReadListener interleave = record -> {
      if (Thread.currentThread() == bodyThread && armed.compareAndSet(true, false))
        commitConcurrently("n", 5);
      return record;
    };
    database.getSchema().getType("V").getEvents().registerListener(interleave);
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      body.run();
      database.commit();
      return true;
    } catch (final RuntimeException e) {
      // A query engine may report the conflict wrapped in its own exception
      for (Throwable t = e; t != null; t = t.getCause())
        if (t instanceof ConcurrentModificationException)
          return false;
      throw e;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      database.getSchema().getType("V").getEvents().unregisterListener(interleave);
      assertThat(armed.get()).as("the concurrent commit must have been interleaved").isFalse();
    }
  }

  private int readN() {
    return rid.asVertex().getInteger("n");
  }
}
