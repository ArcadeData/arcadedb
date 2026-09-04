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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for GitHub issue #7149.
 * <p>
 * Writing to a record that the SAME transaction has already deleted used to be queued for the deferred
 * commit-time write like any other update. {@code commit1stPhase()} then found the record missing and reported a
 * {@link ConcurrentModificationException} saying it "was deleted by a concurrent transaction" - naming a
 * concurrent transaction that never existed. Because that is a {@code NeedRetryException}, the caller re-ran the
 * whole command {@code arcadedb.txRetries} times against a state that could never change, and a single Bolt
 * client was finally told {@code Neo.TransientError.Transaction.DeadlockDetected}.
 * <p>
 * The delete wins instead: the record cannot exist at commit, so the write is dropped, exactly as the symmetric
 * order (an update queued BEFORE the delete, which {@code removeRecordFromCache} drops) already behaved. The
 * commit-time arm stays for what it was written for (#4959): a delete by a genuinely concurrent transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7149UpdateAfterDeleteInSameTxTest {
  private static final String DB_PATH = "./target/databases/testissue7149-update-after-delete";

  private DatabaseInternal database;

  @BeforeEach
  void setUp() {
    database = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
    final VertexType type = database.getSchema().createVertexType("Doc");
    type.createProperty("id", Integer.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "id");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private RID createRecord(final int id) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Doc");
      v.set("id", id);
      v.set("value", "initial");
      v.save();
      rid[0] = v.getIdentity();
    });
    return rid[0];
  }

  private long countRecords() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM Doc")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private List<RID> indexEntriesFor(final int id) {
    final IndexCursor cursor = database.getSchema().getIndexByName("Doc[id]").get(new Object[] { id });
    final List<RID> found = new ArrayList<>();
    while (cursor.hasNext())
      found.add(cursor.next().getIdentity());
    return found;
  }

  @Test
  void savingAStaleInstanceAfterAnInTransactionDeleteDoesNotReportAPhantomConflict() {
    final RID rid = createRecord(1);

    assertThatCode(() -> database.transaction(() -> {
      final MutableVertex stale = database.lookupByRID(rid, true).asVertex().modify();
      stale.set("value", "updated");
      stale.save();

      database.deleteRecord(database.lookupByRID(rid, true));
      assertThat(database.getTransaction().isDeletedInTransaction(rid)).isTrue();

      // The stale instance is still live in the caller's hands - this is what an OpenCypher row fanout, or any
      // application holding the record across the delete, does. Pre-fix this queued the write and the COMMIT blew
      // up with "deleted by a concurrent transaction".
      stale.set("value", "after-delete");
      stale.save();
    })).doesNotThrowAnyException();

    assertThat(countRecords()).as("the delete wins: the record must be gone").isZero();
  }

  @Test
  void theDroppedWriteLeavesNoPhantomIndexEntry() {
    final RID rid = createRecord(7);

    database.transaction(() -> {
      final MutableVertex stale = database.lookupByRID(rid, true).asVertex().modify();
      database.deleteRecord(database.lookupByRID(rid, true));
      // A write that changes the INDEXED property: the index maintenance must be skipped along with the write,
      // otherwise the delete's own index cleanup is undone and the key resurrects pointing at a dead RID.
      stale.set("id", 8);
      stale.save();
    });

    assertThat(countRecords()).isZero();
    assertThat(indexEntriesFor(7)).as("the delete removed the original key").isEmpty();
    assertThat(indexEntriesFor(8)).as("the dropped write must not have added a key").isEmpty();
  }

  @Test
  void aRecordDeletedInThisTransactionDoesNotBlockUpdatesToOtherRecords() {
    final RID deleted = createRecord(2);
    final RID survivor = createRecord(3);

    database.transaction(() -> {
      database.deleteRecord(database.lookupByRID(deleted, true));

      final MutableVertex v = database.lookupByRID(survivor, true).asVertex().modify();
      v.set("value", "still-written");
      v.save();
    });

    assertThat(countRecords()).isEqualTo(1);
    assertThat(database.lookupByRID(survivor, true).asVertex().getString("value")).isEqualTo("still-written");
  }

  @Test
  void aDroppedWriteIsNotCountedAsAnUpdate() {
    final RID rid = createRecord(5);

    final long before = ((Number) database.getStats().get("updateRecord")).longValue();
    database.transaction(() -> {
      final MutableVertex stale = database.lookupByRID(rid, true).asVertex().modify();
      database.deleteRecord(database.lookupByRID(rid, true));
      stale.set("value", "dropped");
      stale.save();
    });

    assertThat(((Number) database.getStats().get("updateRecord")).longValue())
        .as("a write that was never applied is not an update").isEqualTo(before);
  }

  @Test
  void anOrdinaryUpdateIsStillCounted() {
    final RID rid = createRecord(6);

    final long before = ((Number) database.getStats().get("updateRecord")).longValue();
    database.transaction(() -> {
      final MutableVertex v = database.lookupByRID(rid, true).asVertex().modify();
      v.set("value", "counted");
      v.save();
    });

    assertThat(((Number) database.getStats().get("updateRecord")).longValue()).isEqualTo(before + 1);
  }

  @Test
  void anUpdateQueuedBeforeTheDeleteIsStillDropped() {
    // The symmetric order, which removeRecordFromCache() has always handled: pinned here so the two halves of the
    // invariant the commit-time arm relies on stay together.
    final RID rid = createRecord(4);

    database.transaction(() -> {
      final MutableVertex v = database.lookupByRID(rid, true).asVertex().modify();
      v.set("value", "updated");
      v.save();
      database.deleteRecord(database.lookupByRID(rid, true));
    });

    assertThat(countRecords()).isZero();
  }
}
