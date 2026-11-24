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
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A record an after-read listener handed back other content for (a decryption, for instance) is deleted, and updated, as any other
 * (issue #12579). Since #12472 a delete compares the record's slot with the image the caller read and refuses it, with a retryable
 * {@link ConcurrentModificationException}, when they differ: {@code modify()} built its copy from the content the listener
 * returned, so the slot always differed, with no concurrent transaction involved, and every retry was refused the same way. The
 * update guard (#6950) compared the same image and refused the same way.
 * <p>
 * The image to compare is the stored one, the one the record was read as before the listener ran. The same image is what the index
 * entries and the EXTERNAL values of a delete are cleaned up from, since the listener may have rewritten an indexed property.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class DeleteOfListenerRewrittenRecordIssue12579Test extends TestHelper {
  private static final AfterRecordReadListener REWRITE_NUMBER = record -> record.asDocument().modify().set("n", 42);
  private static final AfterRecordReadListener REWRITE_KEY    = record -> record.asDocument().modify().set("k", "plain");

  /** The reproduction of the issue: the mutable copy of a record the listener rewrote, deleted. */
  @Test
  void theModifiedCopyOfARewrittenRecordIsDeletedWithItsIndexEntries() {
    final RID rid = newDocumentOfTypeWithUniqueKey("i0");
    registerOnDocumentType(REWRITE_NUMBER);

    database.transaction(() -> {
      final MutableDocument modified = database.lookupByRID(rid, false).asDocument().modify();
      assertThat(modified.getInteger("n")).as("the copy is built from the content the listener returned").isEqualTo(42);
      modified.delete();
    });

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(indexedRids("DI", "i0")).as("the index entry of the deleted record").isEmpty();
    // THE UNIQUE KEY IS FREE AGAIN
    database.transaction(() -> database.newDocument("DI").set("k", "i0").save());
  }

  /** The immutable record that was read, touched and deleted: the content it holds is the listener's too. */
  @Test
  void theImmutableRecordAListenerRewroteIsDeleted() {
    final RID rid = newDocumentOfTypeWithUniqueKey("i0");
    registerOnDocumentType(REWRITE_NUMBER);

    database.transaction(() -> {
      final Document read = database.lookupByRID(rid, false).asDocument();
      assertThat(read.getInteger("n")).isEqualTo(42);
      read.delete();
    });

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(indexedRids("DI", "i0")).isEmpty();
  }

  /** The content reloaded through the listener, the other place where it replaces what the record holds. */
  @Test
  void aReloadedRecordAListenerRewroteIsDeleted() {
    final RID rid = newDocumentOfTypeWithUniqueKey("i0");
    registerOnDocumentType(REWRITE_NUMBER);

    database.transaction(() -> {
      final Document read = database.lookupByRID(rid, false).asDocument();
      read.reload();
      assertThat(read.getInteger("n")).isEqualTo(42);
      read.delete();
    });

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(indexedRids("DI", "i0")).isEmpty();
  }

  /**
   * The listener rewrote the INDEXED property (a decryption): the entries to remove are those the index holds, which were written
   * from the stored value, not from the value the record answers.
   */
  @Test
  void theIndexEntriesOfAnIndexedPropertyAListenerRewroteAreRemovedByTheirStoredKey() {
    final RID rid = newDocumentOfTypeWithUniqueKey("stored");
    registerOnDocumentType(REWRITE_KEY);

    database.transaction(() -> {
      final MutableDocument modified = database.lookupByRID(rid, false).asDocument().modify();
      assertThat(modified.getString("k")).isEqualTo("plain");
      modified.delete();
    });

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(indexedRids("DI", "stored")).as("the entry written from the stored value").isEmpty();
    assertThat(indexedRids("DI", "plain")).isEmpty();
    database.transaction(() -> database.newDocument("DI").set("k", "stored").save());
  }

  /**
   * The listener's content renders an EXTERNAL property inline, so the pointer to the paired external bucket is not in it: the
   * values to delete with the record are found in the stored image.
   */
  @Test
  void theExternalValuesOfARewrittenRecordAreDeletedWithIt() {
    final DocumentType type = database.getSchema().createDocumentType("DI");
    type.createProperty("blob", Type.STRING).setExternal(true);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("DI").set("blob", "to-be-deleted").set("n", 0).save().getIdentity());
    registerOnDocumentType(REWRITE_NUMBER);

    final LocalBucket externalBucket = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(
        ((LocalDocumentType) type).getExternalBucketIdFor(rid[0].getBucketId()));
    final long externalRecords = externalBucket.count();
    assertThat(externalRecords).isEqualTo(1L);

    database.transaction(() -> {
      final MutableDocument modified = database.lookupByRID(rid[0], false).asDocument().modify();
      assertThat(modified.getString("blob")).isEqualTo("to-be-deleted");
      modified.delete();
    });

    assertThat(database.existsRecord(rid[0])).isFalse();
    assertThat(externalBucket.count()).as("the external record goes with its owner").isZero();
  }

  /** The immutable record, not its copy, with the indexed property rewritten. */
  @Test
  void theImmutableRecordOfARewrittenIndexedPropertyIsDeletedByItsStoredKey() {
    final RID rid = newDocumentOfTypeWithUniqueKey("stored");
    registerOnDocumentType(REWRITE_KEY);

    database.transaction(() -> {
      final Document read = database.lookupByRID(rid, false).asDocument();
      assertThat(read.getString("k")).isEqualTo("plain");
      read.delete();
    });

    assertThat(indexedRids("DI", "stored")).isEmpty();
  }

  /**
   * A record the listener filtered away holds no content, and is deleted, with its index entries, as a record that was never loaded:
   * its caller decided on a RID, not on content.
   */
  @Test
  void aRecordAListenerFilteredAwayIsDeletedWithItsIndexEntries() {
    final RID rid = newDocumentOfTypeWithUniqueKey("i0");
    registerOnDocumentType(record -> null);

    database.transaction(() -> {
      final Document read = database.lookupByRID(rid, false).asDocument();
      assertThat(((ImmutableDocument) read).loadContent()).as("filtered away by the listener").isFalse();
      read.delete();
    });

    assertThat(database.existsRecord(rid)).isFalse();
    assertThat(indexedRids("DI", "i0")).isEmpty();
  }

  /** The update of the copy is refused by the same comparison (#6950), and it is the same defect. */
  @Test
  void theModifiedCopyOfARewrittenRecordIsUpdated() {
    final RID rid = newDocumentOfTypeWithUniqueKey("i0");
    registerOnDocumentType(REWRITE_NUMBER);

    database.transaction(() -> database.lookupByRID(rid, false).asDocument().modify().set("m", 1).save());

    unregisterFromDocumentType(REWRITE_NUMBER);
    database.transaction(() -> assertThat(rid.asDocument().getInteger("m")).isEqualTo(1));
  }

  /**
   * The index of an update is diffed against the stored image too: the record now holds the value the listener returned, and the
   * entry of the value it held before must go.
   */
  @Test
  void theIndexOfAnUpdatedRecordIsDiffedAgainstTheStoredImage() {
    final RID rid = newDocumentOfTypeWithUniqueKey("stored");
    registerOnDocumentType(REWRITE_KEY);

    database.transaction(() -> database.lookupByRID(rid, false).asDocument().modify().set("m", 1).save());

    unregisterFromDocumentType(REWRITE_KEY);
    database.transaction(() -> assertThat(rid.asDocument().getString("k")).isEqualTo("plain"));
    assertThat(indexedRids("DI", "stored")).as("the entry of the value the record held before").isEmpty();
    assertThat(indexedRids("DI", "plain")).containsExactly(rid);
  }

  /** The record changed by a concurrent commit after its read is still refused, with the retryable conflict. */
  @Test
  void aRewrittenRecordChangedByAConcurrentCommitIsStillRefused() throws Exception {
    final RID rid = newDocumentOfTypeWithUniqueKey("i0");
    registerOnDocumentType(REWRITE_NUMBER);

    database.begin();
    try {
      final MutableDocument modified = database.lookupByRID(rid, false).asDocument().modify();
      assertThat(modified.getInteger("n")).isEqualTo(42);

      runConcurrently(() -> {
        database.transaction(() -> database.command("sql", "UPDATE DI SET m = 5 WHERE k = 'i0'").close());
        return null;
      });

      assertThatThrownBy(modified::delete).isInstanceOf(ConcurrentModificationException.class).hasMessageContaining(rid.toString());
    } finally {
      database.rollback();
    }

    assertThat(database.existsRecord(rid)).isTrue();
    unregisterFromDocumentType(REWRITE_NUMBER);
    database.transaction(() -> assertThat(rid.asDocument().getInteger("m")).isEqualTo(5));
  }

  /** The same for the update of the copy: refused, not lost over the concurrent change. */
  @Test
  void anUpdateOfARewrittenRecordChangedByAConcurrentCommitIsStillRefused() throws Exception {
    final RID rid = newDocumentOfTypeWithUniqueKey("i0");
    registerOnDocumentType(REWRITE_NUMBER);

    database.begin();
    try {
      final MutableDocument modified = database.lookupByRID(rid, false).asDocument().modify();
      assertThat(modified.getInteger("n")).isEqualTo(42);

      runConcurrently(() -> {
        database.transaction(() -> database.command("sql", "UPDATE DI SET m = 5 WHERE k = 'i0'").close());
        return null;
      });

      assertThatThrownBy(() -> modified.set("m", 1).save()).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }

    unregisterFromDocumentType(REWRITE_NUMBER);
    database.transaction(() -> assertThat(rid.asDocument().getInteger("m")).isEqualTo(5));
  }

  /** The record deleted by a concurrent transaction, its RID reused by an unrelated insert: the record that sits there now stays. */
  @Test
  void aReusedRidIsNotDeletedThroughARewrittenRecord() throws Exception {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("DI").withTotalBuckets(1).create();
      database.command("sqlscript", "CREATE PROPERTY DI.k STRING; CREATE INDEX ON DI (k) UNIQUE").close();
    });
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("DI").set("k", "i0").set("n", 0).save().getIdentity());
    registerOnDocumentType(REWRITE_NUMBER);

    database.begin();
    try {
      final MutableDocument modified = database.lookupByRID(rid[0], false).asDocument().modify();
      assertThat(modified.getInteger("n")).isEqualTo(42);

      final RID reusedRid = runConcurrently(() -> {
        database.transaction(() -> database.command("sql", "DELETE FROM DI WHERE k = 'i0'").close());
        final RID[] reused = new RID[1];
        for (int i = 0; i < 50 && !rid[0].equals(reused[0]); i++) {
          final int key = i;
          database.transaction(() -> reused[0] = database.newDocument("DI").set("k", "r" + key).set("n", 0).save().getIdentity());
        }
        return reused[0];
      });
      assertThat(reusedRid).as("the RID must have been reused by an unrelated record").isEqualTo(rid[0]);

      assertThatThrownBy(modified::delete).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }

    assertThat(database.existsRecord(rid[0])).isTrue();
    unregisterFromDocumentType(REWRITE_NUMBER);
    database.transaction(() -> assertThat(rid[0].asDocument().getString("k")).startsWith("r"));
  }

  /** A vertex: its content is reloaded through the listener when it is modified, and its delete takes its edges with it. */
  @Test
  void aVertexAListenerRewroteIsDeletedWithItsEdges() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.command("sqlscript", "CREATE PROPERTY V.k STRING; CREATE INDEX ON V (k) UNIQUE").close();
    });
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("k", "a").set("n", 0).save();
      final MutableVertex b = database.newVertex("V").set("k", "b").set("n", 0).save();
      rids[0] = a.getIdentity();
      rids[1] = b.getIdentity();
      rids[2] = a.newEdge("E", b).getIdentity();
    });
    database.getSchema().getType("V").getEvents().registerListener(
        (AfterRecordReadListener) record -> record.asVertex().modify().set("n", 42));

    database.transaction(() -> {
      final MutableVertex modified = database.lookupByRID(rids[0], false).asVertex().modify();
      assertThat(modified.getInteger("n")).isEqualTo(42);
      modified.delete();
    });

    assertThat(database.existsRecord(rids[0])).isFalse();
    assertThat(database.existsRecord(rids[2])).as("the edge goes with its vertex").isFalse();
    assertThat(indexedRids("V", "a")).isEmpty();
    database.transaction(() -> {
      assertThat(rids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isZero();
      // AND THE ONE THAT IS NOT MODIFIED FIRST
      database.lookupByRID(rids[1], false).asVertex().delete();
    });
    assertThat(database.existsRecord(rids[1])).isFalse();
  }

  /**
   * An edge created through a vertex a listener rewrote, while a concurrent transaction gave the same vertex another edge: the
   * edge-list write has no property of the transaction, so it is merged with the concurrent one at the commit (#11498) as for any
   * vertex, and both edges are there.
   */
  @Test
  void anEdgeCreatedThroughARewrittenVertexIsMergedWithAConcurrentOne() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      rids[0] = database.newVertex("V").set("n", 0).save().getIdentity();
      rids[1] = database.newVertex("V").set("n", 0).save().getIdentity();
      rids[2] = database.newVertex("V").set("n", 0).save().getIdentity();
    });
    database.getSchema().getType("V").getEvents().registerListener(
        (AfterRecordReadListener) record -> record.asVertex().modify().set("n", 42));

    database.begin();
    try {
      final MutableVertex modified = database.lookupByRID(rids[0], false).asVertex().modify();
      assertThat(modified.getInteger("n")).isEqualTo(42);

      runConcurrently(() -> {
        database.transaction(() -> database.command("sql", "CREATE EDGE E FROM " + rids[0] + " TO " + rids[1]).close());
        return null;
      });

      modified.newEdge("E", rids[2].asVertex());
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    database.transaction(() -> assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(2));
  }

  /** An edge, modified and deleted the same way. */
  @Test
  void anEdgeAListenerRewroteIsDeleted() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      final MutableVertex b = database.newVertex("V").save();
      rids[0] = a.getIdentity();
      rids[1] = b.getIdentity();
      rids[2] = a.newEdge("E", b, "n", 0).getIdentity();
    });
    database.getSchema().getType("E").getEvents().registerListener(
        (AfterRecordReadListener) record -> record.asEdge().modify().set("n", 42));

    database.transaction(() -> {
      final MutableEdge modified = database.lookupByRID(rids[2], false).asEdge().modify();
      assertThat(modified.getInteger("n")).isEqualTo(42);
      modified.delete();
    });

    assertThat(database.existsRecord(rids[2])).isFalse();
    database.transaction(() -> assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isZero());
  }

  /** The edge read and deleted without being modified. */
  @Test
  void theImmutableEdgeAListenerRewroteIsDeleted() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
    final RID[] rids = new RID[1];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      final MutableVertex b = database.newVertex("V").save();
      rids[0] = a.newEdge("E", b, "n", 0).getIdentity();
    });
    database.getSchema().getType("E").getEvents().registerListener(
        (AfterRecordReadListener) record -> record.asEdge().modify().set("n", 42));

    database.transaction(() -> {
      final Edge read = database.lookupByRID(rids[0], false).asEdge();
      assertThat(read.getInteger("n")).isEqualTo(42);
      read.delete();
    });

    assertThat(database.existsRecord(rids[0])).isFalse();
  }

  private RID newDocumentOfTypeWithUniqueKey(final String key) {
    database.transaction(() -> {
      database.getSchema().createDocumentType("DI");
      database.command("sqlscript", "CREATE PROPERTY DI.k STRING; CREATE INDEX ON DI (k) UNIQUE").close();
    });
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.command("sql", "INSERT INTO DI SET k = '" + key + "', n = 0").next().getIdentity()
        .orElseThrow());
    return rid[0];
  }

  private void registerOnDocumentType(final AfterRecordReadListener listener) {
    database.getSchema().getType("DI").getEvents().registerListener(listener);
  }

  private void unregisterFromDocumentType(final AfterRecordReadListener listener) {
    database.getSchema().getType("DI").getEvents().unregisterListener(listener);
  }

  private List<RID> indexedRids(final String type, final String key) {
    final List<RID> rids = new ArrayList<>();
    final IndexCursor cursor = database.getSchema().getType(type).getPolymorphicIndexByProperties("k").get(new Object[] { key });
    while (cursor.hasNext())
      rids.add(cursor.next().getIdentity());
    return rids;
  }

  /** Runs {@code task} in another thread, so in another transaction, and waits for it: get() rethrows its failure. */
  private static <T> T runConcurrently(final Callable<T> task) throws Exception {
    final FutureTask<T> future = new FutureTask<>(task);
    new Thread(future).start();
    return future.get();
  }
}
