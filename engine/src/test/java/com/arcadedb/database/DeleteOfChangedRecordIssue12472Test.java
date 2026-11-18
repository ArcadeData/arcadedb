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
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A delete through a record read before a concurrent commit changed it is refused with the retryable
 * {@link ConcurrentModificationException} an update gets in the same position, instead of removing whatever now sits at the RID
 * (issue #12472): a RID reused by an insert lost the unrelated new record, and a {@code DELETE ... WHERE} removed a record a
 * concurrent update had made stop matching the predicate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeleteOfChangedRecordIssue12472Test extends TestHelper {

  /** The reproduction of the issue: the record read was deleted, and its RID handed to an unrelated insert. */
  @Test
  void aReusedRidIsNotDeletedThroughTheRecordReadBefore() throws Exception {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("D").withTotalBuckets(1).create();
      database.command("sql", "CREATE PROPERTY D.v INTEGER");
    });
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D").set("v", 1).save().getIdentity());

    database.begin();
    try {
      final Document readBefore = rid[0].asDocument(true);
      assertThat(readBefore.getInteger("v")).isEqualTo(1);

      final RID reusedRid = runConcurrently(() -> {
        database.transaction(() -> rid[0].asDocument().delete());
        final RID[] reused = new RID[1];
        for (int i = 0; i < 50 && !rid[0].equals(reused[0]); i++)
          database.transaction(() -> reused[0] = database.newDocument("D").set("v", 77).save().getIdentity());
        return reused[0];
      });
      assertThat(reusedRid).as("the RID must have been reused by the record with v = 77").isEqualTo(rid[0]);

      assertThatThrownBy(readBefore::delete).isInstanceOf(ConcurrentModificationException.class)
          .hasMessageContaining(rid[0].toString());
    } finally {
      database.rollback();
    }

    assertThat(database.existsRecord(rid[0])).isTrue();
    database.transaction(() -> assertThat(rid[0].asDocument().getInteger("v")).isEqualTo(77));
  }

  /** The record read was changed and committed by another transaction: refused, and the retry, which reads again, deletes it. */
  @Test
  void aRecordChangedAfterItsReadIsRefusedAndTheRetryDeletesIt() throws Exception {
    final RID rid = newDocument();

    database.begin();
    try {
      final Document readBefore = rid.asDocument(true);
      runConcurrently(() -> {
        database.transaction(() -> rid.asDocument().modify().set("v", 2).save());
        return null;
      });
      assertThatThrownBy(readBefore::delete).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
    database.transaction(() -> assertThat(rid.asDocument().getInteger("v")).isEqualTo(2));

    database.transaction(() -> rid.asDocument(true).delete());
    assertThat(database.existsRecord(rid)).isFalse();
  }

  /**
   * A {@code DELETE ... WHERE} whose candidate stops matching because a concurrent update committed between its read and its
   * delete: the statement is refused, and the retry re-evaluates the predicate and deletes nothing.
   */
  @Test
  void aSqlDeleteRacingAnUpdateIsRetriedAndDeletesNothing() {
    final RID rid = newDocument();

    final AtomicBoolean raced = new AtomicBoolean();
    final AtomicInteger attempts = new AtomicInteger();
    final BeforeRecordDeleteListener updateFirst = record -> {
      if (raced.compareAndSet(false, true))
        // THE CONCURRENT UPDATE LANDS AFTER THE STATEMENT READ THE RECORD AND BEFORE IT DELETES IT
        try {
          runConcurrently(() -> {
            database.transaction(() -> database.command("sql", "UPDATE D SET v = 2 WHERE v = 1"));
            return null;
          });
        } catch (final Exception e) {
          throw new IllegalStateException("the concurrent update failed", e);
        }
      return true;
    };
    database.getEvents().registerListener(updateFirst);
    final long[] deleted = new long[1];
    try {
      database.transaction(() -> {
        attempts.incrementAndGet();
        try (final ResultSet rs = database.command("sql", "DELETE FROM D WHERE v = 1")) {
          deleted[0] = rs.next().<Number>getProperty("count").longValue();
        }
      }, false, 3);
    } finally {
      database.getEvents().unregisterListener(updateFirst);
    }

    assertThat(raced.get()).isTrue();
    assertThat(attempts.get()).as("the first attempt must have been refused and retried").isEqualTo(2);
    assertThat(deleted[0]).as("the retry finds no record with v = 1").isZero();
    assertThat(database.existsRecord(rid)).isTrue();
    database.transaction(() -> assertThat(rid.asDocument().getInteger("v")).isEqualTo(2));
  }

  /** The transaction's own update, saved through another object of the record, is not a concurrent change. */
  @Test
  void anOwnUpdateThroughAnotherObjectIsNotAConflict() {
    final RID rid = newDocument();

    database.transaction(() -> {
      final Document readBefore = rid.asDocument(true);
      rid.asDocument(true).modify().set("v", 2).save();
      readBefore.delete();
    });
    assertThat(database.existsRecord(rid)).isFalse();
  }

  /**
   * The asynchronous executor writes an update to the page at once, inside the worker's transaction, so the slot differs from the
   * object read before that update without any other transaction involved: the delete that follows in the same worker transaction
   * is the transaction's own business, and refusing it would fail every retry the same way.
   */
  @Test
  void anOwnWriteOfTheAsynchronousExecutorIsNotAConflict() {
    ownWriteOfTheAsynchronousExecutorThenDelete();
  }

  /** As above, with read-your-writes off: no record cache of the transaction remembers the write. */
  @Test
  void anOwnWriteOfTheAsynchronousExecutorIsNotAConflictWithReadYourWritesOff() {
    database.setReadYourWrites(false);
    try {
      ownWriteOfTheAsynchronousExecutorThenDelete();
    } finally {
      database.setReadYourWrites(true);
    }
  }

  /**
   * A vertex taken for modification, changed by a concurrent transaction, then given an edge through that object: the edge-list
   * write is queued over the concurrent change for the commit-time merge of the heads (#11498), and being deferred to the commit it
   * is not what makes the slot differ from the image read. Deleting the vertex through that object is refused, or it would discard
   * the concurrent change unseen.
   */
  @Test
  void anEdgeListWriteQueuedOverAConcurrentChangeDoesNotExemptTheDelete() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("V").set("v", 1).save().getIdentity();
      rids[1] = database.newVertex("V").set("v", 9).save().getIdentity();
    });

    // OUTSIDE ANY TRANSACTION, SO modify() PINS NO PAGE THE CONCURRENT COMMIT COULD BE CHECKED AGAINST
    final MutableVertex readBefore = rids[0].asVertex(true).modify();
    assertThat(readBefore.getInteger("v")).isEqualTo(1);
    database.begin();
    try {
      runConcurrently(() -> {
        database.transaction(() -> rids[0].asVertex().modify().set("v", 2).save());
        return null;
      });
      readBefore.newEdge("E", rids[1].asVertex());
      assertThat(((DatabaseInternal) database).getTransaction().isUpdateQueued(rids[0])).as("the edge-list write must be queued").isTrue();

      assertThatThrownBy(readBefore::delete).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
    assertThat(database.existsRecord(rids[0])).isTrue();
    database.transaction(() -> assertThat(rids[0].asVertex().getInteger("v")).isEqualTo(2));
  }

  private void ownWriteOfTheAsynchronousExecutorThenDelete() {
    final RID rid = newDocument();
    final Document readBefore = rid.asDocument(true);
    assertThat(readBefore.getInteger("v")).isEqualTo(1);

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicInteger deletes = new AtomicInteger();
    database.async().setCommitEvery(1_000);
    database.async().updateRecord(readBefore.modify().set("v", 2), updated -> {
    }, failure::set);
    database.async().deleteRecord(readBefore, deleted -> deletes.incrementAndGet(), failure::set);
    database.async().waitCompletion();

    assertThat(failure.get()).isNull();
    assertThat(deletes.get()).isEqualTo(1);
    assertThat(database.existsRecord(rid)).isFalse();
  }

  /** A record created in this transaction and deleted through the object that created it, after another update of it. */
  @Test
  void aRecordCreatedAndUpdatedInTheTransactionIsDeleted() {
    database.transaction(() -> database.getSchema().createDocumentType("D"));
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument created = database.newDocument("D").set("v", 1).save();
      rid[0] = created.getIdentity();
      rid[0].asDocument(true).modify().set("v", 2).save();
      created.delete();
    });
    assertThat(database.existsRecord(rid[0])).isFalse();
  }

  /** A property of a vertex changed concurrently: refused, as for a document. */
  @Test
  void aVertexChangedAfterItsReadIsRefused() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("V"));
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex("V").set("v", 1).save().getIdentity());

    database.begin();
    try {
      final Vertex readBefore = rid[0].asVertex(true);
      runConcurrently(() -> {
        database.transaction(() -> rid[0].asVertex().modify().set("v", 2).save());
        return null;
      });
      assertThatThrownBy(readBefore::delete).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
    assertThat(database.existsRecord(rid[0])).isTrue();
  }

  /**
   * An edge appended concurrently to a chunk the vertex already has leaves the vertex record as it was: the delete goes through and
   * takes the new edge with it, as before.
   */
  @Test
  void aVertexWhoseEdgeListGrewInPlaceIsDeletedWithTheNewEdge() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("v", 1).save();
      final MutableVertex b = database.newVertex("V").set("v", 2).save();
      a.newEdge("E", b);
      rids[0] = a.getIdentity();
      rids[1] = b.getIdentity();
    });

    final RID[] newEdge = new RID[1];
    database.begin();
    try {
      final Vertex readBefore = rids[0].asVertex(true);
      readBefore.getInteger("v");
      newEdge[0] = runConcurrently(() -> {
        final RID[] created = new RID[1];
        database.transaction(() -> created[0] = rids[0].asVertex().newEdge("E", rids[1].asVertex()).getIdentity());
        return created[0];
      });
      readBefore.delete();
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.existsRecord(rids[0])).isFalse();
    assertThat(database.existsRecord(newEdge[0])).as("the edge appended concurrently goes with the vertex").isFalse();
    database.transaction(() -> assertThat(rids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isZero());
  }

  /** A property of an edge changed concurrently: refused, as for a document. */
  @Test
  void anEdgeChangedAfterItsReadIsRefused() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      rid[0] = a.newEdge("E", database.newVertex("V").save(), "w", 1).getIdentity();
    });

    database.begin();
    try {
      final Edge readBefore = rid[0].asEdge(true);
      runConcurrently(() -> {
        database.transaction(() -> rid[0].asEdge().modify().set("w", 2).save());
        return null;
      });
      assertThatThrownBy(readBefore::delete).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
    assertThat(database.existsRecord(rid[0])).isTrue();
    database.transaction(() -> assertThat(rid[0].asEdge().getInteger("w")).isEqualTo(2));
  }

  private RID newDocument() {
    database.transaction(() -> database.getSchema().createDocumentType("D"));
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D").set("v", 1).save().getIdentity());
    return rid[0];
  }

  /** Runs {@code task} in another thread, so in another transaction, and waits for it: get() rethrows its failure. */
  private static <T> T runConcurrently(final Callable<T> task) throws Exception {
    final FutureTask<T> future = new FutureTask<>(task);
    new Thread(future).start();
    return future.get();
  }
}
