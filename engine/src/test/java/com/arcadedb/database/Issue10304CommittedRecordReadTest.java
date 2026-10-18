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

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10304, a follow-up of #10263: {@link LocalBucket#getRecordFromCommittedPage} answered a placeholder pointer
 * and a multi-page record through the transaction's own view, so the committed edge-list check of an edge delete read
 * a vertex that outgrew its page as the transaction saw it, head pointers included.
 * <p>
 * The bucket-level tests pin the record's pages with a {@code REPEATABLE_READ} read, which is what makes the
 * transaction's view differ from the committed one, and hold the answer against what a fresh transaction reads. The
 * graph-level tests drive the #10263 window - the transaction holds its own copy of the vertex from before the vertex
 * had any OUT list - on a vertex whose properties do not fit a page.
 */
class Issue10304CommittedRecordReadTest extends BucketPageLayoutTestSupport {
  /** 20 KB of content: big enough to spill out of a sealed page, small enough to fit a page of its own. */
  private static final int PLACEHOLDER_CONTENT_SIZE = 20 * 1024;
  /** Fits no page: the record is a chunk chain. */
  private static final int MULTI_PAGE_SIZE          = 200 * 1024;

  @Test
  void aMultiPageRecordIsReadAsCommittedNotAsTheTransactionHoldsIt() {
    database.transaction(() -> database.getSchema().createDocumentType("Big", 1).createProperty("v", Type.STRING));
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Big").set("v", "a".repeat(MULTI_PAGE_SIZE)).save().getIdentity());
    assertThat((Long) bucketStats("Big").get("totalMultiPageRecords")).isEqualTo(1L);

    assertCommittedReadSeesTheConcurrentUpdate("Big", rid[0], "b".repeat(MULTI_PAGE_SIZE));
  }

  @Test
  void aPlaceholderBackedRecordIsReadAsCommittedNotAsTheTransactionHoldsIt() {
    final RID placeholder = placeholderBackedRecord("Placeholder");

    assertCommittedReadSeesTheConcurrentUpdate("Placeholder", placeholder, "c".repeat(PLACEHOLDER_CONTENT_SIZE));
  }

  @Test
  void aDeletedMultiPageRecordReadsAsNoRecord() {
    database.transaction(() -> database.getSchema().createDocumentType("Gone", 1).createProperty("v", Type.STRING));
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Gone").set("v", "a".repeat(MULTI_PAGE_SIZE)).save().getIdentity());
    database.transaction(() -> rid[0].asDocument().delete());

    database.transaction(() -> assertThat(bucketOf("Gone").getRecordFromCommittedPage(rid[0])).isNull());
  }

  /**
   * The #10263 window on a vertex whose properties outgrow its page: the transaction rewrote the vertex while it had no
   * OUT list, a concurrent commit added the edge (and the head pointer to its list), and the delete of that edge finds
   * no entry in the list it reads. The committed check must read the new head pointer, not the transaction's.
   */
  @Test
  void deletingAnEdgeOfAMultiPageVertexHeldWithoutAnyListIsAConflict() {
    assertDeleteOfConcurrentEdgeIsAConflict(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
  }

  @Test
  void deletingAnEdgeOfAMultiPageVertexHeldWithoutAnyListIsAConflictUnderRepeatableRead() {
    assertDeleteOfConcurrentEdgeIsAConflict(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
  }

  private void assertDeleteOfConcurrentEdgeIsAConflict(final Database.TRANSACTION_ISOLATION_LEVEL isolation) {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1);
      database.getSchema().createEdgeType("E", 1);
    });
    final RID[] v = new RID[2];
    database.transaction(() -> {
      v[0] = database.newVertex("V").set("id", 0).set("blob", "x".repeat(MULTI_PAGE_SIZE)).save().getIdentity();
      v[1] = database.newVertex("V").set("id", 1).set("blob", "y".repeat(MULTI_PAGE_SIZE)).save().getIdentity();
    });
    assertThat((Long) bucketStats("V").get("totalMultiPageRecords")).as("both vertices must be chunk chains").isEqualTo(2L);

    database.begin(isolation);
    try {
      v[0].asVertex().modify().set("value", 1).save();
      v[1].asVertex().modify().set("value", 1).save();

      final RID[] added = new RID[1];
      inAnotherThread(() -> database.transaction(() -> added[0] = v[0].asVertex().newEdge("E", v[1].asVertex()).getIdentity()));

      assertThatThrownBy(() -> added[0].asEdge().delete()).isInstanceOf(ConcurrentModificationException.class)
          .as("refused by the committed edge-list check, not by a read in motion").hasMessageContaining("edge list of vertex");
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    database.transaction(() -> {
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
      final Set<RID> records = new HashSet<>();
      for (var it = database.iterateType("E", true); it.hasNext(); )
        records.add(it.next().getIdentity());
      for (final Edge e : v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "E"))
        assertThat(records).as("every OUT entry is an existing edge").contains(e.getIdentity());
    });
    checkDatabase();
  }

  /**
   * Reads {@code rid} under {@code REPEATABLE_READ}, which pins the pages it lives on, lets another thread commit
   * {@code newValue}, and holds {@link LocalBucket#getRecordFromCommittedPage} against what a fresh transaction reads.
   */
  private void assertCommittedReadSeesTheConcurrentUpdate(final String typeName, final RID rid, final String newValue) {
    final LocalBucket bucket = bucketOf(typeName);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final byte[] before = bucket.getRecord(rid).toByteArray();

      final byte[][] committed = new byte[1][];
      inAnotherThread(() -> {
        database.transaction(() -> rid.asDocument(true).modify().set("v", newValue).save());
        database.transaction(() -> committed[0] = bucket.getRecord(rid).toByteArray());
      });
      assertThat(Arrays.equals(committed[0], before)).as("the concurrent commit changed the record").isFalse();
      assertThat(Arrays.equals(bucket.getRecord(rid).toByteArray(), before)).as("the transaction still sees its pinned image").isTrue();

      final Binary read = bucket.getRecordFromCommittedPage(rid);
      assertThat(read).isNotNull();
      assertThat(Arrays.equals(read.toByteArray(), committed[0])).as("the committed read must see the concurrent commit").isTrue();
    } finally {
      database.rollback();
    }
    checkDatabase();
  }

  private RID placeholderBackedRecord(final String typeName) {
    final RID[] tiny = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING);
      tiny[0] = database.newDocument(typeName).set("v", "p").save().getIdentity();
    });
    sealFirstPage(typeName);

    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(PLACEHOLDER_CONTENT_SIZE)).save());

    final Map<String, Object> layout = bucketStats(typeName);
    assertThat((Long) layout.get("totalPlaceholderRecords"))
        .as("this test is about a placeholder-backed record, and " + typeName + " has none: " + layout).isEqualTo(1L);
    return tiny[0];
  }
}
