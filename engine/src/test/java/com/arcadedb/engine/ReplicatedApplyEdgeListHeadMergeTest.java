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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11592 on the replicated apply path: an entry carries page images, not the records it deleted, so a transaction that read a
 * vertex alive before an entry wrote that vertex's page cannot merge an edge-list head on it, whether its commit comes after the
 * apply or overlaps it. An entry that wrote only other pages leaves the merge alone.
 */
class ReplicatedApplyEdgeListHeadMergeTest extends TestHelper {
  private static final long WAIT_S = 30;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("name", Type.STRING);
      database.getSchema().createEdgeType("E", 1);
    });
  }

  @AfterEach
  void clearHook() {
    TransactionManager.applyChangesHookForTesting = null;
    TransactionContext.afterEdgeListHeadMergeForTesting = null;
  }

  @Test
  void aReplicatedApplyRefusesTheHeadMergeOfATransactionBegunBeforeIt() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    final long sequenceBefore = transactionManager.getVertexDeleteSequence();
    final AtomicReference<Vertex> readB = new AtomicReference<>();

    // a replicated entry carries page images, not the records it deleted. This one rewrites b's page as it is, which is what the
    // delete of b and the insert of an identical vertex at its RID can leave
    final Throwable failure = interleave(() -> readB.set(vertex(b)), () -> applyReplicatedRewriteOfPageOf(b),
        () -> vertex(a).newEdge("E", readB.get()));

    assertThat(transactionManager.wasVertexDeletedSince(b, sequenceBefore)).isTrue();
    assertThat(transactionManager.wasVertexDeletedSince(b, transactionManager.getVertexDeleteSequence())).isFalse();
    assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertNoRelationship(a, b);
  }

  @Test
  void aCommitOverlappingTheReplicatedApplyOfTheVertexPageIsRefused() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final AtomicReference<Vertex> readB = new AtomicReference<>();
    final AtomicBoolean committedDuringApply = new AtomicBoolean();

    // the entry has published b's page and has not finished: the transaction that read b before it commits right then
    final CountDownLatch commitNow = new CountDownLatch(1);
    final CountDownLatch commitDone = new CountDownLatch(1);
    TransactionManager.applyChangesHookForTesting = () -> {
      commitNow.countDown();
      try {
        committedDuringApply.set(commitDone.await(WAIT_S, TimeUnit.SECONDS));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch readDone = new CountDownLatch(1);
    final Thread thread = new Thread(() -> {
      try {
        database.begin();
        try {
          readB.set(vertex(b));
        } finally {
          readDone.countDown();
        }
        if (!commitNow.await(WAIT_S, TimeUnit.SECONDS))
          throw new IllegalStateException("the apply did not reach its hook within " + WAIT_S + "s");
        vertex(a).newEdge("E", readB.get());
        database.commit();
      } catch (final Throwable e) {
        failure.set(e);
        if (database.isTransactionActive())
          database.rollback();
      } finally {
        commitDone.countDown();
      }
    }, "first");
    thread.start();
    try {
      assertThat(readDone.await(WAIT_S, TimeUnit.SECONDS)).as("the first transaction read b").isTrue();
      applyReplicatedRewriteOfPageOf(b);
    } finally {
      commitNow.countDown();
      thread.join(TimeUnit.SECONDS.toMillis(WAIT_S));
      assertThat(thread.isAlive()).as("the first transaction finished").isFalse();
    }

    assertThat(committedDuringApply.get()).as("the commit ran while the entry was being applied").isTrue();
    assertThat(failure.get()).isInstanceOf(ConcurrentModificationException.class);
    assertNoRelationship(a, b);
  }

  @Test
  void aReplicatedApplyOfTheVertexPageStartedAfterTheCheckRefusesTheCommit() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final AtomicReference<Vertex> readB = new AtomicReference<>();
    final AtomicBoolean applied = new AtomicBoolean();

    // the commit checked b and merged the heads; only then the entry rewriting b's page is applied, whole, before the deferred writes
    // and the page-version checks: without a barrier the slot merge replays b onto the page the entry wrote
    TransactionContext.afterEdgeListHeadMergeForTesting = () -> {
      TransactionContext.afterEdgeListHeadMergeForTesting = null;
      final Thread apply = new Thread(() -> {
        applyReplicatedRewriteOfPageOf(b);
        applied.set(true);
      }, "apply");
      apply.start();
      try {
        apply.join(TimeUnit.SECONDS.toMillis(WAIT_S));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final Throwable failure = interleave(() -> readB.set(vertex(b)), () -> {
    }, () -> vertex(a).newEdge("E", readB.get()));

    assertThat(applied.get()).as("the entry was applied between the check and the page-version checks").isTrue();
    assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    assertNoRelationship(a, b);
  }

  @Test
  void aReplicatedApplyOfAnotherVertexPageLeavesTheHeadMergeAlone() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final RID far = compute(() -> {
      MutableVertex vertex;
      do
        vertex = database.newVertex("V").set("name", "far").save();
      while (pageOf(vertex.getIdentity()) == pageOf(b));
      return vertex.getIdentity();
    });
    assertThat(pageOf(a)).isEqualTo(pageOf(b));
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    final long sequenceBefore = transactionManager.getVertexDeleteSequence();
    final AtomicReference<Vertex> readB = new AtomicReference<>();

    // the entry writes the page of another vertex of the same bucket: neither a nor b can be one it deleted
    final Throwable failure = interleave(() -> readB.set(vertex(b)), () -> applyReplicatedRewriteOfPageOf(far),
        () -> vertex(a).newEdge("E", readB.get()));

    assertThat(transactionManager.wasVertexDeletedSince(far, sequenceBefore)).isTrue();
    assertThat(transactionManager.wasVertexDeletedSince(a, sequenceBefore)).isFalse();
    assertThat(transactionManager.wasVertexDeletedSince(b, sequenceBefore)).isFalse();
    assertThat(failure).isNull();
    database.transaction(() -> {
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).containsExactly(b);
      assertThat(neighbours(b, Vertex.DIRECTION.IN)).containsExactly(a);
    });
  }

  private void assertNoRelationship(final RID a, final RID b) {
    database.transaction(() -> {
      try (final ResultSet result = database.query("sql", "SELECT count(@rid) AS c FROM E")) {
        assertThat(result.next().<Number>getProperty("c").longValue()).isZero();
      }
      assertThat(neighbours(b, Vertex.DIRECTION.IN)).isEmpty();
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).isEmpty();
    });
  }

  /**
   * Runs {@code first} in a transaction on a thread of its own, then {@code second} on this thread, then {@code then} in the first
   * transaction, then commits it.
   *
   * @return what the first transaction failed with, or null when it committed
   */
  private Throwable interleave(final Runnable first, final Runnable second, final Runnable then) throws Exception {
    final CountDownLatch firstStarted = new CountDownLatch(1);
    final CountDownLatch secondDone = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread thread = new Thread(() -> {
      try {
        database.begin();
        try {
          first.run();
        } finally {
          firstStarted.countDown();
        }
        if (!secondDone.await(WAIT_S, TimeUnit.SECONDS))
          throw new IllegalStateException("the second step did not finish within " + WAIT_S + "s");
        then.run();
        database.commit();
      } catch (final Throwable e) {
        failure.set(e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "first");
    thread.start();
    assertThat(firstStarted.await(WAIT_S, TimeUnit.SECONDS)).as("the first transaction started").isTrue();
    try {
      second.run();
    } finally {
      secondDone.countDown();
      thread.join(TimeUnit.SECONDS.toMillis(WAIT_S));
      assertThat(thread.isAlive()).as("the first transaction finished").isFalse();
    }
    return failure.get();
  }

  /** What a follower does with an entry rewriting the page of {@code rid}: TransactionManager.applyChanges of its image. */
  private void applyReplicatedRewriteOfPageOf(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rid.getBucketId());
    final PageId pageId = new PageId(db, bucket.getFileId(), pageOf(rid));
    final ImmutablePage page;
    try {
      page = db.getPageManager().getImmutablePage(pageId, bucket.getPageSize(), false, true);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = pageId.getFileId();
    walPage.pageNumber = pageId.getPageNumber();
    walPage.currentPageVersion = (int) page.getVersion() + 1;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();
    final byte[] content = new byte[walPage.changesTo - walPage.changesFrom + 1];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTransaction = new WALFile.WALTransaction();
    walTransaction.txId = Long.MAX_VALUE;
    walTransaction.timestamp = System.currentTimeMillis();
    walTransaction.pages = new WALFile.WALPage[] { walPage };
    assertThat(db.getTransactionManager().applyChanges(walTransaction, Collections.emptyMap(), false)).isTrue();
  }

  private int pageOf(final RID rid) {
    final LocalBucket bucket = (LocalBucket) ((DatabaseInternal) database).getSchema().getBucketById(rid.getBucketId());
    return (int) (rid.getPosition() / bucket.getMaxRecordsInPage());
  }

  private RID newVertex(final String name) {
    return compute(() -> database.newVertex("V").set("name", name).save().getIdentity());
  }

  private <T> T compute(final Supplier<T> supplier) {
    final AtomicReference<T> result = new AtomicReference<>();
    database.transaction(() -> result.set(supplier.get()));
    return result.get();
  }

  private Vertex vertex(final RID rid) {
    return database.lookupByRID(rid, true).asVertex();
  }

  private List<RID> neighbours(final RID rid, final Vertex.DIRECTION direction) {
    final List<RID> result = new ArrayList<>();
    for (final Vertex v : vertex(rid).getVertices(direction, "E"))
      result.add(v.getIdentity());
    return result;
  }
}
