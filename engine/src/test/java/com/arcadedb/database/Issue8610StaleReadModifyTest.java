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
import com.arcadedb.query.sql.executor.ResultSet;
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
    assertThat(ImmutableDocument.readTransactionId((1L << 30) - 1)).isEqualTo((1 << 30) - 1);
    assertThat(ImmutableDocument.readTransactionId(1L << 30)).isZero();
    assertThat(ImmutableDocument.readTransactionId(1L << 31)).isZero();
    assertThat(ImmutableDocument.readTransactionId((1L << 32) - 2)).isNotNegative();
    assertThat(ImmutableDocument.readTransactionId(Long.MAX_VALUE)).isNotNegative();
    // The stale marker, -2 - id, stays inside an int for every id
    assertThat(-2L - ImmutableDocument.readTransactionId(Long.MAX_VALUE)).isGreaterThanOrEqualTo(Integer.MIN_VALUE);
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
   * An openCypher REMOVE writes no value computed from what the MATCH read, so a commit landing in between leaves
   * nothing to lose: it commits, on top of the concurrent write.
   */
  @Test
  void cypherRemoveCommitsOverACommitLandingInsideTheStatement() {
    database.transaction(() -> rid.asVertex().modify().set("tag", "x").save());

    final boolean committed = runWithConcurrentCommitAfterFirstRead(
        () -> database.command("cypher", "MATCH (v:V) REMOVE v.tag").close());

    assertThat(committed).isTrue();
    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().has("tag")).isFalse();
  }

  /**
   * The same for the merge.node procedure, whose onMatch properties are constants.
   */
  @Test
  void mergeNodeProcedureCommitsOverACommitLandingInsideTheStatement() {
    database.command("sql", "CREATE PROPERTY V.key STRING");
    database.transaction(() -> rid.asVertex().modify().set("key", "k").save());

    final boolean committed = runWithConcurrentCommitAfterFirstRead(() -> database.command("cypher",
        "CALL merge.node(['V'], {key: 'k'}, {}, {flag: true}) YIELD node RETURN node").close());

    assertThat(committed).isTrue();
    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().getBoolean("flag")).isTrue();
  }

  /**
   * Moving an edge endpoint through set("@in") is a property write too: computed from a stale read, it is refused.
   */
  @Test
  void anEdgeEndpointMoveFromAStaleReadIsRefused() {
    final AtomicReference<RID> edge = new AtomicReference<>();
    final AtomicReference<RID> target = new AtomicReference<>();
    database.transaction(() -> {
      final MutableVertex other = database.newVertex("V").save();
      target.set(database.newVertex("V").save().getIdentity());
      edge.set(rid.asVertex().newEdge("E", other, "n", 0).getIdentity());
    });

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Edge read = database.iterateType("E", false).next().asEdge();
    commitConcurrently(() -> edge.get().asEdge().modify().set("n", 5).save());

    assertThatThrownBy(() -> {
      read.modify().set("@in", target.get()).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);
    // The move already rewired the edge inside the transaction before the save refused it: the transaction is doomed
    database.rollback();

    assertThat(edge.get().asEdge().getInteger("n")).isEqualTo(5);
  }

  /**
   * A stale mark belongs to the transaction that made it: a retry loop that reuses the record it read in the refused
   * attempt holds that record across transactions, and the next attempt refreshes it silently instead of being refused
   * until the retries run out.
   */
  @Test
  void aStaleMarkDoesNotOutliveItsTransaction() {
    final Vertex[] held = new Vertex[1];
    final AtomicReference<Boolean> first = new AtomicReference<>(true);
    database.transaction(() -> {
      if (held[0] == null)
        held[0] = rid.asVertex();
      final int n = held[0].getInteger("n");
      if (first.getAndSet(false))
        commitConcurrently("n", 5);
      held[0].modify().set("n", n + 1).save();
    }, false, 3);

    // The retry reused the instance reloaded by the refused attempt, so it read the concurrent 5 and wrote 6
    assertThat(readN()).isEqualTo(6);
  }

  /**
   * The merge.relationship procedure writes constant onMatch properties too.
   */
  @Test
  void mergeRelationshipProcedureCommitsOverACommitLandingInsideTheStatement() {
    database.transaction(() -> {
      final MutableVertex other = database.newVertex("V").set("name", "other").save();
      rid.asVertex().modify().set("name", "me").save();
      rid.asVertex().newEdge("E", other, "n", 0);
    });

    final boolean committed = runWithConcurrentCommitAfterFirstRead(() -> database.command("cypher",
        "MATCH (a:V {name: 'me'}), (b:V {name: 'other'}) "
            + "CALL merge.relationship(a, 'E', {}, {}, b, {flag: true}) YIELD rel RETURN rel").close());

    assertThat(committed).isTrue();
    assertThat(readN()).isEqualTo(5);
    try (final ResultSet rs = database.query("sql", "SELECT flag FROM E")) {
      assertThat(rs.next().<Boolean>getProperty("flag")).isTrue();
    }
  }

  /**
   * This transaction's own writes never count as a concurrent change: modifying the same record again after saving
   * it, while an unrelated record on the same page is committed by another transaction, is no conflict.
   */
  @Test
  void ownWritesAndAnUnrelatedCommitOnTheSamePageAreNoConflict() {
    final AtomicReference<RID> neighbour = new AtomicReference<>();
    database.transaction(() -> neighbour.set(database.newVertex("V").set("n", 0).save().getIdentity()));

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    read.modify().set("a", 1).save();
    commitConcurrently(() -> neighbour.get().asVertex().modify().set("n", 9).save());
    read.modify().set("b", 2).save();
    database.commit();

    final Vertex reloaded = rid.asVertex();
    assertThat(reloaded.getInteger("a")).isEqualTo(1);
    assertThat(reloaded.getInteger("b")).isEqualTo(2);
    assertThat(neighbour.get().asVertex().getInteger("n")).isEqualTo(9);
  }

  /**
   * A nested transaction is a transaction of its own: a record read in the outer one and modified in the nested one is
   * held across transactions, and refreshed silently.
   */
  @Test
  void aRecordReadInTheOuterTransactionIsRefreshedInANestedOne() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Vertex read = rid.asVertex();
    commitConcurrently("n", 5);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    read.modify().set("other", 1).save();
    database.commit();
    database.commit();

    assertThat(readN()).isEqualTo(5);
    assertThat(rid.asVertex().getInteger("other")).isEqualTo(1);
  }

  /**
   * A scan kept across commit() and begin() reads its next batch in the new transaction, so a record of that batch is
   * checked there: its stale read is refused, not refreshed silently.
   */
  @Test
  void aScanBatchReadAfterANewBeginIsCheckedInThatTransaction() {
    // More records than one prefetch batch (1024), so the scan reads a second batch
    database.transaction(() -> {
      for (int i = 0; i < 1_100; i++)
        database.newDocument("D").set("i", i, "n", 0).save();
    });

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final Iterator<Record> scan = database.iterateType("D", false);
    // The iterator refills as soon as a batch runs out: stop one short so the second batch is read after the new begin
    for (int i = 0; i < 1_023; i++)
      scan.next();
    database.commit();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    scan.next(); // the last record of the first batch, which reads the second batch now, in this transaction
    final Document read = scan.next().asDocument();
    final int n = read.getInteger("n");
    final RID readRid = read.getIdentity();
    commitConcurrently(() -> readRid.asDocument().modify().set("n", 5).save());

    assertThatThrownBy(() -> {
      read.modify().set("n", n + 1).save();
      database.commit();
    }).isInstanceOf(ConcurrentModificationException.class);

    assertThat(readRid.asDocument().getInteger("n")).isEqualTo(5);
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
