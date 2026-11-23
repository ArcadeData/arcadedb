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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A delete removes the index entries of the record as it is when the delete reaches it, not as it was when the record was
 * read. A record read, then changed by a concurrent transaction that committed, then deleted, used to keep the entry the
 * concurrent change added, pointing at a record that no longer exists: once the RID was reused, an indexed lookup returned
 * a record holding another value (issue #11733).
 * <p>
 * The same holds when the object deleted differs from the indexed state because of this transaction alone: a property assigned
 * and never saved, or an update saved through another object of the same record.
 * <p>
 * Since issue #12472 the delete of a record read before a concurrent change is refused as a concurrent modification: the tests of
 * that shape check that the refusal leaves the record and its entries as committed, and that the retry cleans them up.
 */
class DeleteStaleImageIndexCleanupIssue11733Test extends TestHelper {

  @Test
  void aDocumentChangedAfterItsReadLeavesNoIndexEntry() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D");
      database.command("sql", "CREATE PROPERTY D.v INTEGER");
      database.command("sql", "CREATE INDEX ON D (v) NOTUNIQUE");
    });
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D").set("v", 1).save().getIdentity());

    deleteAfterAConcurrentUpdate(rid[0]);

    assertThat(entries("D[v]", 1)).isEmpty();
    assertThat(entries("D[v]", 2)).isEmpty();
  }

  @Test
  void aVertexAndAnEdgeChangedAfterTheirReadLeaveNoIndexEntry() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.command("sql", "CREATE PROPERTY V.v INTEGER");
      database.command("sql", "CREATE INDEX ON V (v) NOTUNIQUE");
      database.getSchema().createEdgeType("E");
      database.command("sql", "CREATE PROPERTY E.v INTEGER");
      database.command("sql", "CREATE INDEX ON E (v) NOTUNIQUE");
    });
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("v", 1).save();
      final MutableVertex b = database.newVertex("V").set("v", 9).save();
      rids[0] = a.newEdge("E", b, "v", 1).getIdentity();
      rids[1] = a.getIdentity();
    });

    deleteAfterAConcurrentUpdate(rids[0]);
    assertThat(entries("E[v]", 1)).isEmpty();
    assertThat(entries("E[v]", 2)).isEmpty();

    deleteAfterAConcurrentUpdate(rids[1]);
    assertThat(entries("V[v]", 1)).isEmpty();
    assertThat(entries("V[v]", 2)).isEmpty();
    assertThat(entries("V[v]", 9)).hasSize(1);
  }

  /**
   * The cleanup reads the record without firing the before-read events: a listener hiding the record would otherwise send the
   * cleanup back to the object the caller holds, and the entry the concurrent change added would stay. Since issue #12472 a record
   * read before the change is refused instead, so the slot is read for a record handed out without loading it.
   */
  @Test
  void aBeforeReadListenerHidingTheRecordDoesNotSendTheCleanupBackToTheImageRead() throws Exception {
    final RID rid = newIndexedDocument();

    database.begin();
    final Document unloaded = (Document) database.lookupByRID(rid, false);
    final FutureTask<Integer> concurrent = new FutureTask<>(() -> {
      database.transaction(() -> rid.asDocument().modify().set("v", 2).save());
      final int[] committed = new int[1];
      database.transaction(() -> committed[0] = rid.asDocument().getInteger("v"));
      return committed[0];
    });
    new Thread(concurrent).start();
    assertThat(concurrent.get()).as("the value the concurrent transaction committed").isEqualTo(2);

    final BeforeRecordReadListener hide = record -> !record.equals(rid);
    database.getEvents().registerListener(hide);
    try {
      unloaded.delete();
      database.commit();
    } finally {
      database.getEvents().unregisterListener(hide);
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(entries("D[v]", 1)).isEmpty();
    assertThat(entries("D[v]", 2)).isEmpty();
  }

  /**
   * The EXTERNAL values go with the record as it is now too: one the concurrent change added is not named by the image read, and
   * stayed in the paired bucket, referenced by nothing.
   */
  @Test
  void anExternalValueAddedAfterTheReadIsDeletedWithTheRecord() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("X");
    type.createProperty("v", Type.INTEGER);
    type.createProperty("blob", Type.STRING).setExternal(true);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("X").set("v", 1).save().getIdentity());
    assertThat(externalRecords(type)).isZero();

    deleteAfterAConcurrentUpdate(rid[0], () -> {
    }, changed -> changed.set("blob", "x".repeat(200)));

    assertThat(externalRecords(type)).isZero();
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Object orphans = rs.next().getProperty("orphanedExternalRecords");
      assertThat(orphans == null ? 0L : ((Number) orphans).longValue()).isZero();
    }
  }

  /**
   * A placeholder keeps its content on another page, which an update absorbed by the content record changes without touching
   * the pointer's page. The delete pins that page too before it compares, so an update of the content committed after the
   * comparison is refused at commit instead of being deleted with entries nobody removed.
   */
  @Test
  void aPlaceholderContentChangedAfterTheDeletePinnedItIsAConflict() throws Exception {
    final DocumentType type = database.getSchema().buildDocumentType().withName("P").withTotalBuckets(1).create();
    type.createProperty("v", Type.STRING);
    type.createProperty("k", Type.INTEGER);
    database.command("sql", "CREATE INDEX ON P (k) NOTUNIQUE NULL_STRATEGY SKIP");

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      // WRITTEN FIRST, SO IT IS NEVER THE LAST RECORD OF THE PAGE (A LAST RECORD GROWS INTO THE FREE TAIL), AND TINY, SO ITS SLOT
      // CANNOT HOST THE HEADER OF A CHUNK
      rid[0] = database.newDocument("P").set("v", "p").save().getIdentity();
      database.newDocument("P").set("v", "n".repeat(19)).save();
    });
    BucketPageLayoutTestSupport.sealFirstPage(database, "P");
    database.transaction(() -> rid[0].asDocument(true).modify().set("k", 1).set("v", "b".repeat(30 * 1024)).save());
    assertThat((Long) BucketPageLayoutTestSupport.bucketStats(database, "P").get("totalPlaceholderRecords"))
        .as("the record must be a placeholder pointer").isEqualTo(1L);

    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();

    database.begin();
    try {
      final Document readBefore = rid[0].asDocument(true);
      assertThat(readBefore.getInteger("k")).isEqualTo(1);
      // THE FIRST THING THE DELETE DOES WITH THE RECORD, DONE HERE TO PUT THE CONCURRENT COMMIT RIGHT AFTER IT
      bucket.pinRecordForDelete(rid[0]);

      final FutureTask<Integer> concurrent = new FutureTask<>(() -> {
        database.transaction(() -> rid[0].asDocument().modify().set("k", 2).save());
        final int[] committed = new int[1];
        database.transaction(() -> committed[0] = rid[0].asDocument().getInteger("k"));
        return committed[0];
      });
      new Thread(concurrent).start();
      assertThat(concurrent.get()).as("the value the concurrent transaction committed").isEqualTo(2);
      assertThat((Long) placeholderRecords(bucket)).as("the update was absorbed by the content record").isEqualTo(1L);

      readBefore.delete();
      assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.existsRecord(rid[0])).isTrue();
    assertThat(entries("P[k]", 1)).isEmpty();
    assertThat(entries("P[k]", 2)).containsExactly(rid[0]);
  }

  /** Read by another thread: the calling one may be inside a transaction that holds pages of the bucket. */
  private Object placeholderRecords(final LocalBucket bucket) throws Exception {
    final FutureTask<Object> check = new FutureTask<>(() -> {
      final Object[] found = new Object[1];
      database.transaction(() -> found[0] = bucket.check(0, false).get("totalPlaceholderRecords"));
      return found[0];
    });
    new Thread(check).start();
    return check.get();
  }

  /**
   * A record handed out without loading it has no image yet: the delete reads it from the slot it pinned, not by letting the
   * record load itself while the cleanup asks its properties.
   */
  @Test
  void aDocumentNeverLoadedIsCleanedUpFromItsSlot() throws Exception {
    final RID rid = newIndexedDocument();

    database.begin();
    final Document unloaded = (Document) database.lookupByRID(rid, false);
    assertThat(((BaseRecord) unloaded).getBuffer()).as("the record must not be loaded yet").isNull();

    final FutureTask<Integer> concurrent = new FutureTask<>(() -> {
      database.transaction(() -> rid.asDocument().modify().set("v", 2).save());
      final int[] committed = new int[1];
      database.transaction(() -> committed[0] = rid.asDocument().getInteger("v"));
      return committed[0];
    });
    new Thread(concurrent).start();
    assertThat(concurrent.get()).as("the value the concurrent transaction committed").isEqualTo(2);

    unloaded.delete();
    assertThat(((BaseRecord) unloaded).getBuffer()).as("the cleanup must not load the caller's record").isNull();
    database.commit();

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(entries("D[v]", 1)).isEmpty();
    assertThat(entries("D[v]", 2)).isEmpty();
  }

  /** A property assigned and never saved is not in the index: the delete removes the entry of the stored value. */
  @Test
  void aDocumentDeletedWithAnUnsavedChangeLeavesNoIndexEntry() {
    final RID rid = newIndexedDocument();

    database.transaction(() -> {
      final MutableDocument changed = rid.asDocument(true).modify();
      changed.set("v", 5);
      changed.delete();
    });

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(entries("D[v]", 1)).isEmpty();
    assertThat(entries("D[v]", 5)).isEmpty();
  }

  /** The change is the transaction's own, saved through one handle, and the delete goes through the one read before it. */
  @Test
  void aDocumentUpdatedInTheSameTransactionAndDeletedThroughAnOlderHandleLeavesNoIndexEntry() {
    final RID rid = newIndexedDocument();

    database.transaction(() -> {
      final Document readBefore = rid.asDocument(true);
      readBefore.modify().set("v", 2).save();
      readBefore.delete();
    });

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(entries("D[v]", 1)).isEmpty();
    assertThat(entries("D[v]", 2)).isEmpty();
  }

  /** As above, with the change saved and then another one assigned and left unsaved on the handle that is deleted. */
  @Test
  void aDocumentSavedThenChangedAgainAndDeletedLeavesNoIndexEntry() {
    final RID rid = newIndexedDocument();

    database.transaction(() -> {
      final MutableDocument changed = rid.asDocument(true).modify();
      changed.set("v", 2).save();
      changed.set("v", 7);
      changed.delete();
    });

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(entries("D[v]", 1)).isEmpty();
    assertThat(entries("D[v]", 2)).isEmpty();
    assertThat(entries("D[v]", 7)).isEmpty();
  }

  private RID newIndexedDocument() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D");
      database.command("sql", "CREATE PROPERTY D.v INTEGER");
      database.command("sql", "CREATE INDEX ON D (v) NOTUNIQUE");
    });
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D").set("v", 1).save().getIdentity());
    return rid[0];
  }

  /**
   * Reads the record, lets another transaction set {@code v = 2} and commit, then deletes the record as it was read. Since issue
   * #12472 that delete is refused, and leaves the record and its index entries as the concurrent transaction committed them; the
   * retry reads the record again and deletes it, entries and EXTERNAL values included.
   */
  private void deleteAfterAConcurrentUpdate(final RID rid) throws Exception {
    deleteAfterAConcurrentUpdate(rid, () -> {
    });
  }

  /** As {@link #deleteAfterAConcurrentUpdate(RID)}, running {@code beforeDelete} right before the delete. */
  private void deleteAfterAConcurrentUpdate(final RID rid, final Runnable beforeDelete) throws Exception {
    deleteAfterAConcurrentUpdate(rid, beforeDelete, changed -> {
    });
  }

  /** As {@link #deleteAfterAConcurrentUpdate(RID, Runnable)}, the concurrent transaction applying {@code alsoChange} too. */
  private void deleteAfterAConcurrentUpdate(final RID rid, final Runnable beforeDelete, final Consumer<MutableDocument> alsoChange)
      throws Exception {
    database.begin();
    final Document readBefore = rid.asDocument(true);
    assertThat(readBefore.getInteger("v")).isEqualTo(1);

    // get() rethrows a failure of the concurrent transaction, which would otherwise leave the record unchanged and this test
    // deleting it as it was read, proving nothing
    final FutureTask<Integer> concurrent = new FutureTask<>(() -> {
      database.transaction(() -> {
        final MutableDocument changed = rid.asDocument().modify().set("v", 2);
        alsoChange.accept(changed);
        changed.save();
      });
      final int[] committed = new int[1];
      database.transaction(() -> committed[0] = rid.asDocument().getInteger("v"));
      return committed[0];
    });
    new Thread(concurrent).start();
    assertThat(concurrent.get()).as("the value the concurrent transaction committed").isEqualTo(2);

    beforeDelete.run();
    try {
      assertThatThrownBy(readBefore::delete).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }
    assertThat(database.existsRecord(rid)).isTrue();
    final String indexName = rid.asDocument().getTypeName() + "[v]";
    if (database.getSchema().existsIndex(indexName)) {
      assertThat(entries(indexName, 1)).isEmpty();
      assertThat(entries(indexName, 2)).containsExactly(rid);
    }

    // THE RETRY, WHICH READS THE RECORD AGAIN
    database.transaction(() -> rid.asDocument(true).delete());
    assertThat(database.existsRecord(rid)).isFalse();
  }

  /** Scanned, not the cached counter: what the paired external buckets of {@code type} really hold. */
  private long externalRecords(final DocumentType type) {
    final long[] total = new long[1];
    database.transaction(() -> {
      for (final Bucket bucket : type.getBuckets(false)) {
        final Integer externalId = ((LocalDocumentType) type).getExternalBucketIdFor(bucket.getFileId());
        assertThat(externalId).as("the type pairs an external bucket with bucket %d", bucket.getFileId()).isNotNull();
        database.getSchema().getBucketById(externalId).scan((recordRid, view) -> {
          ++total[0];
          return true;
        }, null);
      }
    });
    return total[0];
  }

  private List<RID> entries(final String indexName, final int key) {
    final List<RID> found = new ArrayList<>();
    database.transaction(() -> {
      final IndexCursor cursor = ((TypeIndex) database.getSchema().getIndexByName(indexName)).get(new Object[] { key });
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
    });
    return found;
  }
}
