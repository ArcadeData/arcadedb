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
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12644: "Edge list head chunk ... not visible yet (concurrent commit in flight)" was raised for a head chunk that is
 * visible, and no wait made it so. The transaction had written the edges page the new head chunk is on (it appended to another
 * chunk there), its copy of that page predates the commit that created the chunk, and every read of the page goes through that
 * copy, while the vertex itself is read from the committed page, so its head points at a chunk the transaction's page cannot
 * contain. Nothing in flight was left to wait for. The relationship is created, and the transaction commits.
 * <p>
 * One edge bucket per direction, so every chunk of a direction is on one page.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12644EdgeListHeadOnPageHeldByTransactionTest extends TestHelper {
  private static final long WAIT_S = 30;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.getSchema().createEdgeType("E", 1);
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("id", i).save();
      create(0, 5);
      create(1, 4);
    });
  }

  @Test
  void aFirstChunkCreatedMeanwhileOnAPageTheTransactionHoldsIsReachedWithTheCypherStatementsOfTheReport() throws Exception {
    final Throwable failure = interleave(() -> cypher(0, 4), () -> cypherInATransactionOfItsOwn(2, 5), () -> cypher(2, 4));

    assertThat(failure).isNull();
    assertThat(neighbours(0, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(id(5), id(4));
    assertThat(neighbours(1, Vertex.DIRECTION.OUT)).containsExactly(id(4));
    assertThat(neighbours(2, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(id(5), id(4));
    assertThat(neighbours(4, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(id(1), id(0), id(2));
    assertThat(neighbours(5, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(id(0), id(2));
  }

  @Test
  void aFirstChunkCreatedMeanwhileOnAPageTheTransactionHoldsIsReachedWithTheJavaApi() throws Exception {
    final Throwable failure = interleave(() -> javaApi(0, 4), () -> database.transaction(() -> javaApi(2, 5)), () -> javaApi(2, 4));

    assertThat(failure).isNull();
    assertThat(neighbours(2, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(id(5), id(4));
    assertThat(neighbours(4, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(id(1), id(0), id(2));
    assertThat(edgeCount()).isEqualTo(5);
  }

  @Test
  void theIncomingListIsReachedToo() throws Exception {
    // the transaction holds the page of the incoming lists through an append to vertex 4's list, and a commit gives vertex 3 its
    // first incoming chunk on it
    final Throwable failure = interleave(() -> javaApi(0, 4), () -> database.transaction(() -> javaApi(5, 3)), () -> javaApi(1, 3));

    assertThat(failure).isNull();
    assertThat(neighbours(3, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(id(5), id(1));
    assertThat(neighbours(4, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(id(1), id(0));
    assertThat(edgeCount()).isEqualTo(5);
  }

  @Test
  void aTransactionThatRetriesFindsTheSameResultAsOneThatDoesNot() throws Exception {
    // several relationships from the transaction to vertices whose first chunk a commit created meanwhile
    final Throwable failure = interleave(() -> javaApi(0, 4), () -> {
      database.transaction(() -> javaApi(2, 5));
      database.transaction(() -> javaApi(3, 5));
    }, () -> {
      javaApi(2, 4);
      javaApi(3, 4);
    });

    assertThat(failure).isNull();
    assertThat(neighbours(2, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(id(5), id(4));
    assertThat(neighbours(3, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(id(5), id(4));
    assertThat(neighbours(4, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(id(1), id(0), id(2), id(3));
    assertThat(edgeCount()).isEqualTo(7);
  }

  @Test
  void aListCreatedMeanwhileOnAPageTheTransactionHoldsIsReadByTheTransaction() throws Exception {
    // no relationship is created by the transaction after the commit: the read alone went through the image of the page, and found
    // no record behind the head the vertex points at
    final AtomicReference<List<RID>> readInTransaction = new AtomicReference<>();
    final AtomicReference<List<Integer>> readByCypher = new AtomicReference<>();

    final Throwable failure = interleave(() -> javaApi(0, 4), () -> database.transaction(() -> javaApi(2, 5)), () -> {
      final List<RID> read = new ArrayList<>();
      for (final Vertex v : vertex(2).getVertices(Vertex.DIRECTION.OUT, "E"))
        read.add(v.getIdentity());
      readInTransaction.set(read);

      final List<Integer> ids = new ArrayList<>();
      try (final ResultSet result = database.query("opencypher", "MATCH (a:V {id: 2})-[:E]->(x) RETURN x.id AS id")) {
        while (result.hasNext())
          ids.add(result.next().<Number>getProperty("id").intValue());
      }
      readByCypher.set(ids);
    });

    assertThat(failure).isNull();
    assertThat(readInTransaction.get()).containsExactly(id(5));
    assertThat(readByCypher.get()).containsExactly(5);
  }

  @Test
  void theListIsReadInTheTransactionThatExtendedItBehindAChunkItsPageImageCannotHold() throws Exception {
    final AtomicReference<List<RID>> readInTransaction = new AtomicReference<>();
    final AtomicReference<Long> countInTransaction = new AtomicReference<>();

    final Throwable failure = interleave(() -> javaApi(0, 4), () -> database.transaction(() -> javaApi(2, 5)), () -> {
      javaApi(2, 4);
      // the chunk the commit created is behind the one this transaction put in front of it
      final List<RID> read = new ArrayList<>();
      for (final Vertex v : vertex(2).getVertices(Vertex.DIRECTION.OUT, "E"))
        read.add(v.getIdentity());
      readInTransaction.set(read);
      countInTransaction.set(vertex(2).countEdges(Vertex.DIRECTION.OUT, "E"));
    });

    assertThat(failure).isNull();
    assertThat(readInTransaction.get()).containsExactlyInAnyOrder(id(5), id(4));
    assertThat(countInTransaction.get()).isEqualTo(2L);
  }

  @Test
  void aSuperNodeDirectoryCreatedMeanwhileOnAPageTheTransactionHoldsIsAppendedTo() throws Exception {
    final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    final int savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(4);
    try {
      // the commit promotes vertex 2 to a super node: the head it leaves is a directory, on the page the transaction holds
      final Throwable failure = interleave(() -> javaApi(0, 4), () -> database.transaction(() -> {
        for (int i = 0; i < 300; i++)
          javaApi(2, 5);
      }), () -> javaApi(2, 4));

      assertThat(failure).isNull();
      database.transaction(() -> {
        assertThat(database.lookupByRID(((VertexInternal) vertex(2)).getOutEdgesHeadChunk(), true)).isInstanceOf(StripeDirectory.class);
        assertThat(vertex(2).countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(301L);
        assertThat(vertex(4).countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(3L);
      });
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    }
  }

  @Test
  void aTransactionUnderRepeatableReadStillExtendsTheList() throws Exception {
    // a guard, not a reproduction: the read of a record the image cannot hold is READ_COMMITTED only, and a snapshot must keep
    // appending relationships the same way
    final Throwable failure = interleave(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ, () -> javaApi(0, 4),
        () -> database.transaction(() -> javaApi(2, 5)), () -> javaApi(2, 4));

    assertThat(failure).isNull();
    assertThat(neighbours(2, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(id(5), id(4));
    assertThat(edgeCount()).isEqualTo(5);
  }

  @Test
  void aRecordCommittedMeanwhileOnAPageTheTransactionHoldsIsReadUnderReadCommittedOnly() throws Exception {
    final AtomicReference<RID> created = new AtomicReference<>();
    final AtomicReference<Boolean> hiddenFromReadCommitted = new AtomicReference<>();
    final AtomicReference<Boolean> hiddenOfARecordInTheImage = new AtomicReference<>();
    final AtomicReference<String> readUnderReadCommitted = new AtomicReference<>();
    final AtomicReference<Boolean> existsUnderReadCommitted = new AtomicReference<>();

    // the transaction writes vertex 0, so it holds the page of the vertices as it was then
    final Throwable failure = interleave(() -> vertex(0).modify().set("name", "zero").save(),
        () -> created.set(compute(() -> database.newVertex("V").set("id", 6).set("name", "six").save().getIdentity())), () -> {
          final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(created.get().getBucketId());
          hiddenFromReadCommitted.set(bucket.isRecordHiddenByOwnPageImage(created.get()));
          hiddenOfARecordInTheImage.set(bucket.isRecordHiddenByOwnPageImage(id(0)));
          readUnderReadCommitted.set(database.lookupByRID(created.get(), true).asVertex().getString("name"));
          existsUnderReadCommitted.set(database.existsRecord(created.get()));
        });

    assertThat(failure).isNull();
    assertThat(hiddenFromReadCommitted.get()).isTrue();
    assertThat(hiddenOfARecordInTheImage.get()).isFalse();
    assertThat(readUnderReadCommitted.get()).isEqualTo("six");
    assertThat(existsUnderReadCommitted.get()).as("existsRecord agrees with the read").isTrue();

    // a snapshot is what REPEATABLE_READ asks for: the record committed after it was taken is not in it
    final AtomicReference<Throwable> underRepeatableRead = new AtomicReference<>();
    final AtomicReference<Boolean> existsUnderRepeatableRead = new AtomicReference<>();
    final Throwable repeatableReadFailure = interleave(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ,
        () -> vertex(1).modify().set("name", "one").save(),
        () -> compute(() -> database.newVertex("V").set("id", 7).set("name", "seven").save().getIdentity()), () -> {
          final RID seven = new RID(created.get().getBucketId(), created.get().getPosition() + 1);
          existsUnderRepeatableRead.set(database.existsRecord(seven));
          try {
            database.lookupByRID(seven, true);
          } catch (final Throwable e) {
            underRepeatableRead.set(e);
          }
        });

    assertThat(repeatableReadFailure).isNull();
    assertThat(underRepeatableRead.get()).isInstanceOf(RecordNotFoundException.class);
    assertThat(existsUnderRepeatableRead.get()).isFalse();
  }

  @Test
  void aRemovalFromAChunkTheImageOfThePageCannotHoldIsRefusedAtTheStatementWithTheReason() throws Exception {
    final AtomicReference<RID> edge = new AtomicReference<>();

    // the chunk can be read, and an entry removed from it would be written through the image of the page that has no slot for it
    final Throwable failure = interleave(() -> javaApi(0, 4), () -> edge.set(compute(() -> vertex(2).newEdge("E", vertex(5)).getIdentity())),
        () -> database.lookupByRID(edge.get(), true).asEdge().delete());

    assertThat(failure).isInstanceOf(ConcurrentModificationException.class).hasMessageContaining("predates the commit that created it");
    // the refused transaction left nothing behind
    assertThat(neighbours(2, Vertex.DIRECTION.OUT)).containsExactly(id(5));
    assertThat(edgeCount()).isEqualTo(3);
  }

  @Test
  void aContinuationChunkCommittedMeanwhileIsNotARecordHiddenByTheImage() throws Exception {
    final AtomicReference<RID> created = new AtomicReference<>();
    final List<RID> hidden = new ArrayList<>();

    // the continuation chunks of a record that outgrew its page are slots of the same bucket, and no record of their own. The
    // commit of the first transaction is not the subject here, the commit that rewrote its page may well refuse it
    interleave(() -> vertex(0).modify().set("name", "zero").save(),
        () -> created.set(compute(() -> database.newVertex("V").set("id", 6).set("blob", "x".repeat(200_000)).save().getIdentity())),
        () -> {
          final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(created.get().getBucketId());
          for (int page = 0; page < bucket.getTotalPages(); page++)
            for (int slot = 0; slot < bucket.getMaxRecordsInPage(); slot++) {
              final RID rid = new RID(created.get().getBucketId(), (long) page * bucket.getMaxRecordsInPage() + slot);
              if (bucket.isRecordHiddenByOwnPageImage(rid))
                hidden.add(rid);
            }
        });

    // the record is on a page the transaction does not hold, and the chunk that is on its page is not a record
    assertThat(hidden).isEmpty();
    assertThat(database.lookupByRID(created.get(), true).asVertex().getString("blob")).hasSize(200_000);
  }

  @Test
  void aRecordTheTransactionDeletedItselfIsNotBroughtBackFromTheCommittedPage() {
    database.transaction(() -> {
      final RID doomed = id(3);
      vertex(0).modify().set("name", "zero").save();
      vertex(3).delete();

      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(doomed.getBucketId());
      // the committed page still holds it: it is gone for the transaction, not hidden from it
      assertThat(bucket.existsRecordInCommittedPage(doomed)).isTrue();
      assertThat(bucket.isRecordHiddenByOwnPageImage(doomed)).isFalse();
      assertThatThrownBy(() -> database.lookupByRID(doomed, true)).isInstanceOf(RecordNotFoundException.class);
    });
  }

  @Test
  void aSlotNobodyHasCommittedIsNotHidden() {
    // the committed page has nothing there either: a head like that is a publication in flight, which stays a retryable conflict
    database.transaction(() -> {
      final RID head = ((VertexInternal) vertex(0)).getOutEdgesHeadChunk();
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(head.getBucketId());
      final RID absent = new RID(head.getBucketId(), head.getPosition() + 50);

      assertThat(bucket.isRecordHiddenByOwnPageImage(absent)).isFalse();
    });
  }

  private void create(final int from, final int to) {
    javaApi(from, to);
  }

  private void javaApi(final int from, final int to) {
    vertex(from).newEdge("E", vertex(to));
  }

  private Vertex vertex(final int id) {
    try (final ResultSet result = database.query("sql", "SELECT FROM V WHERE id = ?", id)) {
      return result.next().getVertex().get();
    }
  }

  private RID id(final int id) {
    return vertex(id).getIdentity();
  }

  private void cypher(final int from, final int to) {
    database.command("opencypher", "MATCH (a:V {id: " + from + "}), (x:V {id: " + to + "}) CREATE (a)-[:E]->(x)").close();
  }

  private void cypherInATransactionOfItsOwn(final int from, final int to) {
    database.transaction(() -> cypher(from, to));
  }

  private List<RID> neighbours(final int id, final Vertex.DIRECTION direction) {
    final List<RID> result = new ArrayList<>();
    database.transaction(() -> {
      for (final Vertex v : vertex(id).getVertices(direction, "E"))
        result.add(v.getIdentity());
    });
    return result;
  }

  /** The relationships in the type, counted by a query rather than read from the bucket record counter. */
  private long edgeCount() {
    try (final ResultSet result = database.query("sql", "SELECT count(@rid) AS c FROM E")) {
      return result.next().<Number>getProperty("c").longValue();
    }
  }

  /**
   * Runs {@code first} in a transaction that stays open, then {@code second} (which commits by itself) from the calling thread,
   * then {@code then} in the first transaction, and commits it.
   *
   * @return what the first transaction failed with, or null when it committed
   */
  private Throwable interleave(final Runnable first, final Runnable second, final Runnable then) throws Exception {
    return interleave(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED, first, second, then);
  }

  private <T> T compute(final Supplier<T> supplier) {
    final AtomicReference<T> result = new AtomicReference<>();
    database.transaction(() -> result.set(supplier.get()));
    return result.get();
  }

  private Throwable interleave(final Database.TRANSACTION_ISOLATION_LEVEL isolation, final Runnable first, final Runnable second,
      final Runnable then) throws Exception {
    final CountDownLatch firstStarted = new CountDownLatch(1);
    final CountDownLatch secondCommitted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread thread = new Thread(() -> {
      try {
        database.begin(isolation);
        try {
          first.run();
        } finally {
          firstStarted.countDown();
        }
        if (!secondCommitted.await(WAIT_S, TimeUnit.SECONDS))
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
      secondCommitted.countDown();
      thread.join(TimeUnit.SECONDS.toMillis(WAIT_S));
      assertThat(thread.isAlive()).as("the first transaction finished").isFalse();
    }
    return failure.get();
  }
}
