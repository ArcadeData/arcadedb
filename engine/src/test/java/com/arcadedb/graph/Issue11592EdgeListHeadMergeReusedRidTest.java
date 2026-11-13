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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11592: a transaction that created the first relationship to a vertex committed although a concurrent transaction deleted
 * that vertex, when a third one inserted a vertex that took the deleted one's RID before the commit. The edge-list head merge of
 * #11498 put the relationship on the new vertex, which did not exist when the relationship was created. The commit is refused
 * with a retryable conflict again, as it was before #11498 and as it still is with the slot merge off.
 */
class Issue11592EdgeListHeadMergeReusedRidTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("name", Type.STRING);
      database.getSchema().createEdgeType("E", 1);
    });
  }

  @Test
  void aRelationshipToAVertexReplacedConcurrentlyUnderItsRidConflicts() throws Exception {
    assertRelationshipToReplacedVertexConflicts("new");
  }

  @Test
  void aRelationshipToAVertexReplacedConcurrentlyByAnIdenticalOneConflicts() throws Exception {
    // the new vertex has the same bytes as the one the transaction read: no comparison of the two images can tell them apart
    assertRelationshipToReplacedVertexConflicts("b");
  }

  @Test
  void aVertexReplacedByAnIdenticalOneBeforeTheTransactionWritesItConflicts() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final AtomicReference<Vertex> readB = new AtomicReference<>();
    final AtomicReference<RID> replacement = new AtomicReference<>();

    // the transaction reads b alive and pins its page only after the identical vertex took the RID: the version it pins is the
    // latest one, and comparing the image it read with the pinned one cannot mark the read stale
    final Throwable failure = interleave(() -> readB.set(vertex(b)), () -> {
      database.transaction(() -> vertex(b).delete());
      replacement.set(newVertex("b"));
    }, () -> vertex(a).newEdge("E", readB.get()));

    assertThat(replacement.get()).as("the new vertex reuses the RID of the deleted one").isEqualTo(b);
    assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    database.transaction(() -> {
      assertThat(database.countType("E", false)).isZero();
      assertThat(neighbours(b, Vertex.DIRECTION.IN)).isEmpty();
    });
  }

  @Test
  void appendsOverflowingAChunkFilledUnderTheRidOfAVertexReplacedConcurrentlyConflict() throws Exception {
    // first chunks of the maximum size: a full one cannot grow, so appends that do not fit go in a new head chunk
    final int savedChunkSize = GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.getValueAsInteger();
    GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(LocalDatabase.MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE);
    try {
      final RID a = newVertex("a");
      final RID sink = newVertex("sink");
      final List<RID> mine = new ArrayList<>();
      for (int i = 0; i < 11; i++)
        mine.add(newVertex("t" + i));
      final RID chunk = edgeAndOutHead(a, sink);
      final AtomicReference<RID> replacement = new AtomicReference<>();
      final AtomicReference<RID> replacementChunk = new AtomicReference<>();
      final long overflowsBefore = stats().edgeAppendMerges;

      // the transaction appends to a's chunk without filling it, so a is not written. Meanwhile a is deleted, an identical vertex
      // takes its RID, and its first chunk takes the RID of the one the transaction appended to and is filled up
      final Throwable failure = interleave(() -> {
        for (final RID t : mine)
          vertex(a).newEdge("E", vertex(t));
      }, () -> {
        database.transaction(() -> vertex(a).delete());
        replacement.set(newVertex("a"));
        replacementChunk.set(edgeAndOutHead(a, sink));
        database.transaction(() -> {
          while (replacementChunk.get().equals(((VertexInternal) vertex(a)).getOutEdgesHeadChunk()))
            vertex(a).newEdge("E", vertex(sink));
        });
      });

      assertThat(replacement.get()).as("the new vertex reuses the RID of the deleted one").isEqualTo(a);
      assertThat(replacementChunk.get()).as("the new vertex's chunk reuses the RID of the one appended to").isEqualTo(chunk);
      assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
      assertThat(stats().edgeAppendMerges).as("no append was moved to a new head chunk").isEqualTo(overflowsBefore);
      database.transaction(() -> assertThat(neighbours(a, Vertex.DIRECTION.OUT)).doesNotContainAnyElementsOf(mine));
    } finally {
      GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(savedChunkSize);
    }
  }

  @Test
  void aVertexTheTransactionCreatedOnARidFreedSinceItBeganTakesItsRelationships() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final AtomicReference<RID> created = new AtomicReference<>();

    // the RID was freed by a commit after the transaction began, but the vertex holding it is the transaction's own
    final Throwable failure = interleave(() -> vertex(a), () -> database.transaction(() -> vertex(b).delete()), () -> {
      final MutableVertex vertex = database.newVertex("V").set("name", "new").save();
      created.set(vertex.getIdentity());
      vertex(a).newEdge("E", vertex);
    });

    assertThat(failure).isNull();
    assertThat(created.get()).isEqualTo(b);
    database.transaction(() -> assertThat(neighbours(b, Vertex.DIRECTION.IN)).containsExactly(a));
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
    database.transaction(() -> assertThat(database.countType("E", false)).isZero());
  }

  @Test
  void aVertexReplacedBeforeTheTransactionBeganStillMergesItsEdgeListHead() throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final RID c = newVertex("c");
    database.transaction(() -> vertex(b).delete());
    final RID replacement = newVertex("new");
    assertThat(replacement).isEqualTo(b);
    final long mergesBefore = stats().edgeListHeadMerges;

    // the transaction reads the vertex that holds the RID now, so its relationship is to that one and merges with a concurrent one
    final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(replacement)),
        () -> database.transaction(() -> vertex(c).newEdge("E", vertex(replacement))));

    assertThat(failure).isNull();
    assertThat(stats().edgeListHeadMerges).isGreaterThan(mergesBefore);
    database.transaction(() -> assertThat(neighbours(replacement, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(a, c));
  }

  private void assertRelationshipToReplacedVertexConflicts(final String replacementName) throws Exception {
    final RID a = newVertex("a");
    final RID b = newVertex("b");
    final AtomicReference<RID> replacement = new AtomicReference<>();

    // b gets its first incoming relationship in the first transaction; meanwhile b is deleted and a new vertex takes its RID
    final Throwable failure = interleave(() -> vertex(a).newEdge("E", vertex(b)), () -> {
      database.transaction(() -> vertex(b).delete());
      replacement.set(newVertex(replacementName));
    });

    assertThat(replacement.get()).as("the new vertex reuses the RID of the deleted one").isEqualTo(b);
    assertThat(failure).isInstanceOf(ConcurrentModificationException.class);
    database.transaction(() -> {
      assertThat(database.countType("E", false)).isZero();
      assertThat(neighbours(a, Vertex.DIRECTION.OUT)).isEmpty();
      assertThat(neighbours(b, Vertex.DIRECTION.IN)).isEmpty();
      assertThat(vertex(b).getString("name")).isEqualTo(replacementName);
    });
  }

  /**
   * Runs {@code first} in a transaction on a thread of its own, then {@code second} on this thread, which commits its own
   * transactions, then commits the first one.
   *
   * @return what the first transaction failed with, or null when it committed
   */
  private Throwable interleave(final Runnable first, final Runnable second) throws Exception {
    return interleave(first, second, () -> {
    });
  }

  /**
   * Same as {@link #interleave(Runnable, Runnable)}, with {@code then} run in the first transaction after {@code second}, before
   * its commit.
   */
  private Throwable interleave(final Runnable first, final Runnable second, final Runnable then) throws Exception {
    final CountDownLatch firstStarted = new CountDownLatch(1);
    final CountDownLatch secondCommitted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread thread = new Thread(() -> {
      try {
        database.begin();
        try {
          first.run();
        } finally {
          firstStarted.countDown();
        }
        secondCommitted.await();
        then.run();
        database.commit();
      } catch (final Throwable e) {
        failure.set(e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "first");
    thread.start();
    firstStarted.await();
    try {
      second.run();
    } finally {
      secondCommitted.countDown();
      thread.join();
    }
    return failure.get();
  }

  /** What a follower does with an entry rewriting the page of {@code rid}: TransactionManager.applyChanges of its image. */
  private void applyReplicatedRewriteOfPageOf(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rid.getBucketId());
    final PageId pageId = new PageId(db, bucket.getFileId(), (int) (rid.getPosition() / bucket.getMaxRecordsInPage()));
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

  /** Creates the relationship {@code from} to {@code to} in a transaction of its own, then answers {@code from}'s out head. */
  private RID edgeAndOutHead(final RID from, final RID to) {
    return compute(() -> {
      vertex(from).newEdge("E", vertex(to));
      return ((VertexInternal) vertex(from)).getOutEdgesHeadChunk();
    });
  }

  private List<RID> neighbours(final RID rid, final Vertex.DIRECTION direction) {
    final List<RID> result = new ArrayList<>();
    for (final Vertex v : vertex(rid).getVertices(direction, "E"))
      result.add(v.getIdentity());
    return result;
  }

  private PageManager.PPageManagerStats stats() {
    return ((DatabaseInternal) database).getPageManager().getStats();
  }
}
