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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6927: an index RANGE scan run inside a transaction ignored the transaction overlay's REMOVE entries, so it
 * emitted rows that the same transaction had already deleted or re-keyed. The point-lookup path
 * ({@link com.arcadedb.index.lsm.LSMTreeIndex#get(Object[], int)}) subtracts the pending removals from the disk
 * result via its {@code removedRids} filter; {@code LSMTreeIndexCursor} collected only the overlay's ADD entries
 * and dropped the REMOVEs on the floor, so the still-present disk entry was emitted unchanged.
 * <p>
 * The user-visible symptom is a row that does not satisfy the WHERE clause at all: {@code UPDATE ... SET age = 99
 * WHERE age = 20} becomes {@code REMOVE(20, rid) + ADD(99, rid)} in the overlay, and the following
 * {@code SELECT ... WHERE age &lt; 30} resolved the stale disk entry for key 20 back to the (now age=99) record.
 * Nothing re-checks the predicate downstream, because the index consumed it entirely.
 * <p>
 * The pure-delete variant was wrong in a noisier way: the dead RIDs reached
 * {@code GetValueFromIndexEntryStep}, which swallows the {@code RecordNotFoundException} with a WARNING per row,
 * so SQL looked right by accident. Any direct {@link RangeIndex#range} / {@link RangeIndex#iterator} caller had
 * no such net and received the dead RIDs.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6927RangeScanTxRemovesTest extends TestHelper {

  private static final String TYPE_NAME = "Person";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("name", Type.STRING);
      type.createProperty("age", Type.INTEGER);
      type.createProperty("code", Type.INTEGER);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "age" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "code" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
    });

    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("name", "a").set("age", 20).set("code", 1).save();
      database.newDocument(TYPE_NAME).set("name", "b").set("age", 25).set("code", 2).save();
    });
  }

  /**
   * The reporter's exact shape: an in-transaction UPDATE that re-keys an indexed property, followed by a range
   * scan whose predicate the index consumes entirely. Before the fix the scan returned the re-keyed row too,
   * with {@code age=99}, i.e. a row violating the WHERE clause it was selected by.
   */
  @Test
  void rangeScanDoesNotReturnRowReKeyedInTheSameTransaction() {
    database.begin();
    try {
      database.command("sql", "UPDATE " + TYPE_NAME + " SET age = 99 WHERE age = 20");

      final List<String> names = new ArrayList<>();
      final ResultSet rs = database.query("sql", "SELECT name, age FROM " + TYPE_NAME + " WHERE age < 30");
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.<Integer>getProperty("age")).isLessThan(30);
        names.add(r.getProperty("name"));
      }

      assertThat(names).containsExactly("b");

      // the point-lookup path has always been right: keep both paths pinned to the same answer
      assertThat(database.lookupByKey(TYPE_NAME, "age", 20).hasNext()).isFalse();

      // ...and the new key must be visible to the range scan
      final List<String> reKeyed = new ArrayList<>();
      final ResultSet rs2 = database.query("sql", "SELECT name FROM " + TYPE_NAME + " WHERE age > 30");
      while (rs2.hasNext())
        reKeyed.add(rs2.next().getProperty("name"));
      assertThat(reKeyed).containsExactly("a");
    } finally {
      database.rollback();
    }
  }

  /**
   * The same asymmetry seen from the raw {@link RangeIndex} API, which has no {@code RecordNotFoundException}
   * net downstream: a record deleted in this transaction must not be handed back by an ordered scan.
   */
  @Test
  void rangeCursorSkipsRecordDeletedInTheSameTransaction() {
    database.begin();
    try {
      final RID deleted = lookupRid("age", 20);
      database.lookupByRID(deleted, true).asDocument().delete();

      assertThat(collectRange("age", 0, 30, true)).doesNotContain(deleted).hasSize(1);
      assertThat(collectRange("age", 0, 30, false)).doesNotContain(deleted).hasSize(1);
      assertThat(collectIterator("age", true)).doesNotContain(deleted).hasSize(1);
      assertThat(collectIterator("age", false)).doesNotContain(deleted).hasSize(1);
    } finally {
      database.rollback();
    }
  }

  /**
   * A non-unique key holding two RIDs where only one is deleted in the transaction: the per-RID REMOVE must
   * suppress exactly that RID and leave the sibling alone. This is the case a "the whole key is gone" shortcut
   * would get wrong in the opposite direction.
   */
  @Test
  void rangeCursorSuppressesOnlyTheRemovedRidOfASharedKey() {
    database.transaction(() -> database.newDocument(TYPE_NAME).set("name", "c").set("age", 20).set("code", 3).save());

    database.begin();
    try {
      final List<RID> sharing = collectRange("age", 20, 20, true);
      assertThat(sharing).hasSize(2);

      final RID victim = sharing.get(0);
      final RID survivor = sharing.get(1);
      database.lookupByRID(victim, true).asDocument().delete();

      assertThat(collectRange("age", 0, 30, true)).contains(survivor).doesNotContain(victim);
      assertThat(collectRange("age", 0, 30, false)).contains(survivor).doesNotContain(victim);
    } finally {
      database.rollback();
    }
  }

  /**
   * On a UNIQUE index a pending REMOVE means the key is gone, exactly as {@code LSMTreeIndex.get()} already
   * decided by returning an empty cursor; the range scan has to agree.
   */
  @Test
  void rangeCursorHonoursPendingRemoveOnAUniqueIndex() {
    database.begin();
    try {
      final RID deleted = lookupRid("code", 1);
      database.lookupByRID(deleted, true).asDocument().delete();

      assertThat(database.lookupByKey(TYPE_NAME, "code", 1).hasNext()).isFalse();
      assertThat(collectRange("code", 0, 10, true)).doesNotContain(deleted).hasSize(1);
      assertThat(collectRange("code", 0, 10, false)).doesNotContain(deleted).hasSize(1);
    } finally {
      database.rollback();
    }
  }

  /**
   * A record deleted AND a fresh one inserted at the same key in the same transaction: the ADD has to resurrect
   * the key even though a REMOVE is pending on it, so the scan sees the new RID and only the new RID.
   */
  @Test
  void rangeCursorResurrectsAKeyReInsertedAfterTheRemove() {
    database.begin();
    try {
      final RID deleted = lookupRid("age", 20);
      database.lookupByRID(deleted, true).asDocument().delete();
      final RID inserted = database.newDocument(TYPE_NAME).set("name", "a2").set("age", 20).set("code", 4).save()
          .getIdentity();

      final List<RID> rids = collectRange("age", 0, 30, true);
      assertThat(rids).contains(inserted).doesNotContain(deleted).hasSize(2);
    } finally {
      database.rollback();
    }
  }

  /**
   * Every entry of the scanned range removed in the transaction: the cursor must report exhaustion instead of
   * emitting dead RIDs, and must not resurrect them on a descending pass either.
   */
  @Test
  void rangeCursorIsEmptyWhenEveryEntryWasRemovedInTheTransaction() {
    database.begin();
    try {
      for (final RID rid : collectRange("age", 0, 100, true))
        database.lookupByRID(rid, true).asDocument().delete();

      assertThat(collectRange("age", 0, 100, true)).isEmpty();
      assertThat(collectRange("age", 0, 100, false)).isEmpty();
      assertThat(collectIterator("age", true)).isEmpty();

      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE age < 100");
      assertThat(rs.hasNext()).isFalse();
    } finally {
      database.rollback();
    }
  }

  /**
   * The {@code REPLACE} shape, which only a UNIQUE index can produce: within one transaction the record holding key
   * {@code K} is deleted and a DIFFERENT record is inserted at the same {@code K}. Because a unique index's overlay
   * map is keyed by key value alone (not by RID), {@code TransactionIndexContext.addIndexKeyLock} collapses the pair
   * into a single {@code REPLACE(rid=new, oldRid=old)} entry - so the pending removal of the old RID survives ONLY
   * as {@code oldRid}, which commit does replay ({@code index.removeReplay(keyValues, oldRid)}). A scan that reads
   * only {@code rid} therefore re-emits the old RID straight off the page.
   * <p>
   * The type is created with a single bucket on purpose: each bucket owns its own sub-index and therefore its own
   * transaction lane, so a delete and an insert landing in different buckets stay two independent entries and never
   * merge into a REPLACE.
   */
  @Test
  void rangeScanAndLookupAgreeOnAUniqueKeyReplacedInTheSameTransaction() {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType("Registration", 1);
      type.createProperty("code", Type.INTEGER);
      database.getSchema().buildTypeIndex("Registration", new String[] { "code" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
    });
    database.transaction(() -> database.newDocument("Registration").set("code", 7).save());

    database.begin();
    try {
      final IndexCursor before = database.lookupByKey("Registration", "code", 7);
      assertThat(before.hasNext()).isTrue();
      final RID replaced = before.next().getIdentity();

      database.lookupByRID(replaced, true).asDocument().delete();
      final RID replacement = database.newDocument("Registration").set("code", 7).save().getIdentity();

      final RangeIndex index = (RangeIndex) database.getSchema().getType("Registration")
          .getPolymorphicIndexByProperties("code");

      final List<RID> ranged = drain(index.range(true, new Object[] { 0 }, true, new Object[] { 10 }, true));
      assertThat(ranged).containsExactly(replacement);
      assertThat(drain(index.range(false, new Object[] { 10 }, true, new Object[] { 0 }, true)))
          .containsExactly(replacement);

      // the point lookup on a UNIQUE key must agree: exactly one RID, the replacement. Asserted on the RAW
      // sub-index too: TypeIndex.get() asks a unique index for limit=1 and returns whichever entry comes out
      // first, so it would MASK a two-RID answer behind HashSet iteration order rather than fail on it.
      for (final IndexInternal sub : ((TypeIndex) database.getSchema().getType("Registration")
          .getPolymorphicIndexByProperties("code")).getSubIndexes())
        assertThat(drain(sub.get(new Object[] { 7 }))).containsExactly(replacement);

      assertThat(drain(database.lookupByKey("Registration", "code", 7))).containsExactly(replacement);
    } finally {
      database.rollback();
    }
  }

  /**
   * A key-wide {@code remove(keys)} - the manual-index API form that carries no RID - on a NON-unique index. Both the
   * range scan and the point lookup have to drop every disk RID at that key. {@code get()} used to allocate an EMPTY
   * {@code removedRids} set for this shape and then filter nothing with it, so the removal was silently a no-op.
   */
  @Test
  void bothPathsHonourAKeyWideRemoveOnANonUniqueIndex() {
    database.transaction(() -> database.newDocument(TYPE_NAME).set("name", "c").set("age", 20).set("code", 3).save());

    database.begin();
    try {
      final RangeIndex index = rangeIndex("age");
      assertThat(drain(index.get(new Object[] { 20 }))).hasSize(2);

      // no RID: the whole key goes, both RIDs with it
      ((IndexInternal) index).remove(new Object[] { 20 });

      assertThat(index.get(new Object[] { 20 }).hasNext()).isFalse();
      assertThat(collectRange("age", 0, 30, true)).hasSize(1);
      assertThat(collectRange("age", 0, 30, false)).hasSize(1);
    } finally {
      database.rollback();
    }
  }

  /**
   * The same REPLACE shape as {@link #rangeScanAndLookupAgreeOnAUniqueKeyReplacedInTheSameTransaction} on a HASH
   * index, whose {@code get()} carries a copy of the very same overlay-merge code. A hash index has no range scan,
   * so the point lookup is the only path there - and it has to answer with one RID, not two.
   */
  @Test
  void hashIndexLookupHonoursAUniqueKeyReplacedInTheSameTransaction() {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType("Badge", 1);
      type.createProperty("serial", Type.INTEGER);
      database.getSchema().buildTypeIndex("Badge", new String[] { "serial" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
    });
    database.transaction(() -> database.newDocument("Badge").set("serial", 11).save());

    database.begin();
    try {
      final RID replaced = database.lookupByKey("Badge", "serial", 11).next().getIdentity();
      database.lookupByRID(replaced, true).asDocument().delete();
      final RID replacement = database.newDocument("Badge").set("serial", 11).save().getIdentity();

      for (final IndexInternal sub : ((TypeIndex) database.getSchema().getType("Badge")
          .getPolymorphicIndexByProperties("serial")).getSubIndexes())
        assertThat(drain(sub.get(new Object[] { 11 }))).containsExactly(replacement);

      assertThat(drain(database.lookupByKey("Badge", "serial", 11))).containsExactly(replacement);
    } finally {
      database.rollback();
    }
  }

  private RID lookupRid(final String property, final Object key) {
    final IndexCursor cursor = database.lookupByKey(TYPE_NAME, property, key);
    assertThat(cursor.hasNext()).isTrue();
    return cursor.next().getIdentity();
  }

  private RangeIndex rangeIndex(final String property) {
    return (RangeIndex) database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties(property);
  }

  private List<RID> collectRange(final String property, final int from, final int to, final boolean ascending) {
    final IndexCursor cursor = rangeIndex(property).range(ascending, new Object[] { ascending ? from : to }, true,
        new Object[] { ascending ? to : from }, true);
    return drain(cursor);
  }

  private List<RID> collectIterator(final String property, final boolean ascending) {
    return drain(rangeIndex(property).iterator(ascending));
  }

  private List<RID> drain(final IndexCursor cursor) {
    final List<RID> rids = new ArrayList<>();
    while (cursor.hasNext()) {
      final Identifiable next = cursor.next();
      assertThat(next).isNotNull();
      rids.add(next.getIdentity());
      // a RID handed out by an in-tx scan must resolve: a dead one is exactly the defect this test guards
      final Document doc = database.lookupByRID(next.getIdentity(), true).asDocument();
      assertThat(doc).isNotNull();
    }
    return rids;
  }
}
