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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * LSM index cursor tests (#5683, #5932, #6592, #6944, #9461).
 */
class LSMTreeIndexCursorTest extends TestHelper {
  // ===== Issue #5683 =====
  /*
   * Regression test for #5683 item 1: a latent minor-key-grouping defect in {@link LSMTreeIndexCursor#fetchNext()},
   * found while working on #5635 / #5662 / #5673 and re-confirmed there, but never reachable through the public API
   * because the constructor's own invariants keep a live {@code pageCursors[p]} paired with a non-null
   * {@code cursorKeys[p]}.
   * <p>
   * In the "FIND THE MINOR KEY" loop, the first live cursor whose cached key is {@code null} is added to
   * {@code minorKeyIndexes} while {@code minorKey} stays {@code null}. Before the fix, a second live cursor that also
   * reached the {@code minorKey == null} branch was appended to that SAME list instead of starting a fresh one, so the
   * merge treated two cursors positioned at two genuinely different keys as though they shared one: the first cursor's
   * RIDs were folded into a group labelled with the second cursor's key.
   * <p>
   * The test forces the otherwise-unreachable state directly via reflection on {@link LSMTreeIndexCursor}'s private
   * {@code cursorKeys} cache - the only way to exercise code that is defensive-by-construction today - and checks only
   * the FIRST merge round: once a slot's cached key is wiped, nothing in the class ever refreshes it for a cursor that
   * is not chosen into a group, so a full drain would run into the separate (and, for the same reason, equally
   * unreachable) question of what happens to a cursor whose key can never again compare equal to anything. That is out
   * of scope for the grouping defect under test here.
   */
  private static final String TYPE_NAME = "Widget";
  private static final int    PAGE_SIZE = 4096;

  @Test
  @DisplayName("#5683 null cached cursor key does not merge unrelated keys together")
  void nullCachedCursorKeyDoesNotMergeUnrelatedKeysTogether() throws Exception {
    createSchema();
    final LSMTreeIndexMutable mutable = bucketMutableIndex();
    final int bucketId = database.getSchema().getType(TYPE_NAME).getFirstBucketId();

    final int firstBatch = 300;
    final int secondBatch = 300;
    final int totalKeys = firstBatch + secondBatch;

    // Two batches, each committed in its own transaction, so they land on separate (older/newer) mutable pages.
    database.transaction(() -> {
      for (int i = 0; i < firstBatch; ++i)
        mutable.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });
    database.transaction(() -> {
      for (int i = firstBatch; i < totalKeys; ++i)
        mutable.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });

    database.transaction(() -> {
      try {
        assertThat(mutable.getTotalPages()).as("precondition: must span multiple pages to open multiple cursors")
            .isGreaterThan(1);

        final LSMTreeIndexCursor cursor = (LSMTreeIndexCursor) mutable.range(true, null, true, null, true);
        try {
          final Object[] pageCursors = readArrayField(cursor, "pageCursors");
          final Object[][] cursorKeys = readArrayField(cursor, "cursorKeys");
          assertThat(pageCursors[0]).as("precondition: slot 0 (the newest page) must be a live cursor").isNotNull();
          assertThat(pageCursors[1]).as("precondition: slot 1 (an older page) must be a live cursor").isNotNull();

          // Capture the REAL keys the two live cursors are positioned at before corrupting the cache, so the
          // assertions below can tell "correctly processed p1's key" apart from "wrongly labelled with p0's key".
          final int p0RealKey = (Integer) cursorKeys[0][0];
          final int p1RealKey = (Integer) cursorKeys[1][0];
          assertThat(p0RealKey).as("precondition: the two live cursors must start on different keys").isNotEqualTo(p1RealKey);

          // Corrupt ONLY the cache: pageCursors[0] stays live and pointed at its real, correct data - it is the
          // cached key the merge loop reads for grouping decisions that goes missing, exactly the "live cursor,
          // unknown key" shape the issue describes as unreachable through the constructor alone.
          cursorKeys[0] = null;

          assertThat(cursor.hasNext()).as("a live cursor must still produce a first entry").isTrue();
          final RID firstRid = cursor.next();
          final int firstKey = (Integer) cursor.getKeys()[0];

          // The fix: this round's group must be exactly p1's real key with p1's single RID - p0's null-keyed
          // cursor must NOT have been folded in under either label.
          assertThat(firstKey).as("the emitted key must be the cursor genuinely positioned there (p1), not p0's")
              .isEqualTo(p1RealKey);
          assertThat(firstRid).as("the RID must be p1's own, not merged with p0's unrelated entry")
              .isEqualTo(new RID(bucketId, p1RealKey));
        } finally {
          cursor.close();
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void createSchema() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();
    });
  }

  private LSMTreeIndexMutable bucketMutableIndex() {
    final com.arcadedb.index.TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("id").getFirst();
    for (final com.arcadedb.index.Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof LSMTreeIndex lsmIndex)
        return lsmIndex.getMutableIndex();
    throw new IllegalStateException("No LSM bucket index found for type " + TYPE_NAME);
  }

  @SuppressWarnings("unchecked")
  private static <T> T readArrayField(final LSMTreeIndexCursor cursor, final String fieldName) throws Exception {
    final Field field = LSMTreeIndexCursor.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (T) field.get(cursor);
  }

  // ===== Issue #5932 =====
  /*
   * Code review follow-up on PR #5961 (issue #5932). {@link LSMTreeIndexMutable#range(Object[], boolean, Object[],
   * boolean)} - the auto-detecting-direction overload, unreachable through the SQL/Cypher engines (every caller goes
   * through the explicit-direction overload via {@link com.arcadedb.index.RangeIndex}) but still a public method on
   * a public class - determined ascending/descending order by comparing the RAW, caller-supplied bounds instead of
   * narrowing them to the index's declared key types first, the exact defect #5932 fixed inside
   * {@link LSMTreeIndexCursor}. A direct caller passing a numeric-string bound against an {@code INTEGER}-typed
   * index would hit the same {@code ClassCastException}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Test
  @DisplayName("#5932 auto detect direction overload accepts mismatched type bounds")
  void autoDetectDirectionOverloadAcceptsMismatchedTypeBounds() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932RangeAutoDetectOverload", db -> {
      db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
      db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      db.newDocument("V").set("n", 10).save();
      db.newDocument("V").set("n", 20).save();
      db.commit();
      db.begin();

      final TypeIndex typeIndex = db.getSchema().getType("V").getIndexesByProperties("n").getFirst();

      for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets()) {
        final LSMTreeIndexMutable mutable = ((LSMTreeIndex) bucketIndex).getMutableIndex();

        assertThatCode(() -> {
          try (final IndexCursor cursor = mutable.range(new Object[] { "5" }, true, new Object[] { "15" }, true)) {
            int count = 0;
            while (cursor.hasNext()) {
              cursor.next();
              ++count;
            }
            assertThat(count).isLessThanOrEqualTo(1); // at most n=10 falls in this bucket
          }
        }).doesNotThrowAnyException();
      }
    });
  }

  // ===== Issue #6592 =====
  /*
   * Follow-up regression for https://github.com/ArcadeData/arcadedb/issues/6592: once a composite-index prefix
   * scan is answered with a range() cursor, {@link LSMTreeIndexCursor#getClosestEntryInTx} finds the single
   * closest pending (uncommitted) key with one floor/ceiling/higher/lower lookup and used to give up entirely
   * when THAT key turned out to be fully removed (every pending change on it a REMOVE) - e.g. a record inserted
   * and deleted again within the same transaction. Instead of walking to the next candidate key in the same
   * direction, exactly like the on-page cursor skips a fully-tombstoned key via advanceCursor()+continue, the
   * overlay was left empty for the rest of the scan: a prefix range scan whose ORDER BY DESC starts from the
   * just-deleted top of the group came back with nothing, even though older rows of the same group were still
   * live and pending in the very same transaction.
   * <p>
   * A second, narrower defect lived in the same loop: on a non-unique index, a key can carry BOTH a live insert
   * (a different RID) and a REMOVE in the same transaction. The old code {@code break}-ed the whole per-key
   * values loop on the first REMOVE it saw (map iteration order is unspecified), silently dropping the sibling
   * live RID too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String SUPPLIER_TYPE_NAME   = "Supplier";
  private static final int    GROUP_SIZE  = 10;

  private TypeIndex createCompositeIndex() {
    final DocumentType type = database.getSchema().buildDocumentType().withName(SUPPLIER_TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("key1", String.class);
    type.createProperty("key2", String.class);
    type.createProperty("orderedAt", Long.class);
    database.getSchema().buildTypeIndex(SUPPLIER_TYPE_NAME, new String[] { "key1", "key2", "orderedAt" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    return database.getSchema().getType(SUPPLIER_TYPE_NAME).getPolymorphicIndexByProperties("key1", "key2", "orderedAt");
  }

  private static long countAndAssertOrder(final IndexCursor cursor, final boolean descending) {
    long previous = -1;
    long count = 0;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        // getKeys() describes the entry next() LAST RETURNED (see LSMTreeIndexCursor's class docs, #5635).
        final long orderedAt = (Long) cursor.getKeys()[2];
        if (count > 0)
          assertThat(descending ? orderedAt < previous : orderedAt > previous)
              .as("keys must come out strictly %s (got %d after %d)", descending ? "descending" : "ascending", orderedAt, previous)
              .isTrue();
        previous = orderedAt;
        ++count;
      }
    } finally {
      cursor.close();
    }
    return count;
  }

  @Test
  @DisplayName("#6592 descending prefix scan skips the just deleted top of the group while uncommitted")
  void descendingPrefixScanSkipsTheJustDeletedTopOfTheGroupWhileUncommitted() {
    final TypeIndex index = createCompositeIndex();

    database.begin();
    try {
      // INSERT THE WHOLE GROUP AND DELETE ITS TOP - THE ENTRY A DESCENDING SCAN STARTS FROM - ALL WITHIN THE SAME
      // STILL-OPEN TRANSACTION, SO THE ENTRY EXISTS ONLY AS A PENDING INSERT+REMOVE PAIR IN THE TX OVERLAY, NEVER
      // ON ANY PAGE
      RID top = null;
      for (int i = 0; i < GROUP_SIZE; ++i) {
        final RID rid = database.newDocument(SUPPLIER_TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", (long) i).save().getIdentity();
        if (i == GROUP_SIZE - 1)
          top = rid;
      }
      database.deleteRecord(top.getRecord());

      final IndexCursor cursor = index.range(false, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true);
      final long count = countAndAssertOrder(cursor, true);

      assertThat(count).as("the delete removed exactly one of the %d rows in the group", GROUP_SIZE).isEqualTo(GROUP_SIZE - 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  @DisplayName("#6592 ascending prefix scan skips the just deleted bottom of the group while uncommitted")
  void ascendingPrefixScanSkipsTheJustDeletedBottomOfTheGroupWhileUncommitted() {
    final TypeIndex index = createCompositeIndex();

    database.begin();
    try {
      // MIRROR CASE: DELETE THE BOTTOM OF THE GROUP, THE ENTRY AN ASCENDING SCAN STARTS FROM
      RID bottom = null;
      for (int i = 0; i < GROUP_SIZE; ++i) {
        final RID rid = database.newDocument(SUPPLIER_TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", (long) i).save().getIdentity();
        if (i == 0)
          bottom = rid;
      }
      database.deleteRecord(bottom.getRecord());

      final IndexCursor cursor = index.range(true, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true);
      final long count = countAndAssertOrder(cursor, false);

      assertThat(count).as("the delete removed exactly one of the %d rows in the group", GROUP_SIZE).isEqualTo(GROUP_SIZE - 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  @DisplayName("#6592 non unique key shared by a live insert and a remove keeps the live one")
  void nonUniqueKeySharedByALiveInsertAndARemoveKeepsTheLiveOne() {
    final TypeIndex index = createCompositeIndex();

    database.begin();
    try {
      // TWO DOCUMENTS SHARE THE EXACT SAME COMPOSITE KEY (NON-UNIQUE INDEX): INSERT BOTH, THEN DELETE ONLY ONE OF
      // THEM, ALL WITHIN THIS SAME UNCOMMITTED TRANSACTION - THE KEY'S PENDING CHANGES NOW MIX A REMOVE (FOR THE
      // DELETED RID) WITH A LIVE INSERT (FOR THE SURVIVING RID)
      final RID toDelete = database.newDocument(SUPPLIER_TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", 0L).save().getIdentity();
      database.newDocument(SUPPLIER_TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", 0L).save();
      database.deleteRecord(toDelete.getRecord());

      final IndexCursor cursor = index.range(false, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true);
      try {
        assertThat(cursor.hasNext()).as("the surviving sibling under the same key must still be found").isTrue();
        cursor.next();
        assertThat(cursor.hasNext()).as("only the deleted RID is gone, not the whole key").isFalse();
      } finally {
        cursor.close();
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  @DisplayName("#6592 descending prefix scan walks past two consecutive tombstoned keys to the survivor")
  void descendingPrefixScanWalksPastTwoConsecutiveTombstonedKeysToTheSurvivor() {
    final TypeIndex index = createCompositeIndex();

    database.begin();
    try {
      // DELETE THE TOP TWO OF A THREE-ROW GROUP: THE RETRY LOOP MUST HOP TWICE (orderedAt=2, THEN orderedAt=1)
      // BEFORE LANDING ON THE SOLE SURVIVOR (orderedAt=0), NOT JUST ONE
      final RID[] rids = new RID[3];
      for (int i = 0; i < rids.length; ++i)
        rids[i] = database.newDocument(SUPPLIER_TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", (long) i).save().getIdentity();
      database.deleteRecord(rids[2].getRecord());
      database.deleteRecord(rids[1].getRecord());

      final IndexCursor cursor = index.range(false, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true);
      try {
        assertThat(cursor.hasNext()).isTrue();
        cursor.next();
        assertThat((Long) cursor.getKeys()[2]).isEqualTo(0L);
        assertThat(cursor.hasNext()).as("only the survivor remains").isFalse();
      } finally {
        cursor.close();
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  @DisplayName("#6592 prefix scan returns empty rather than leaking into an adjacent key when the whole group is tombstoned")
  void prefixScanReturnsEmptyRatherThanLeakingIntoAnAdjacentKeyWhenTheWholeGroupIsTombstoned() {
    final TypeIndex index = createCompositeIndex();

    database.begin();
    try {
      // AN ADJACENT GROUP (key1=a, key2=c) SORTS RIGHT AFTER THE (a,b) PREFIX'S OWN RANGE. DELETING EVERY ROW OF
      // (a,b) MUST MAKE THE RETRY LOOP HIT typedToKeys' BOUND (pastBound) AND STOP, RATHER THAN WALK STRAIGHT
      // THROUGH INTO THE ADJACENT GROUP'S ENTRIES
      final RID[] rids = new RID[2];
      for (int i = 0; i < rids.length; ++i)
        rids[i] = database.newDocument(SUPPLIER_TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", (long) i).save().getIdentity();
      database.newDocument(SUPPLIER_TYPE_NAME).set("key1", "a", "key2", "c", "orderedAt", 0L).save();
      for (final RID rid : rids)
        database.deleteRecord(rid.getRecord());

      try (final IndexCursor descCursor = index.range(false, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true)) {
        assertThat(descCursor.hasNext()).as("the whole (a,b) group is tombstoned - must not leak into (a,c)").isFalse();
      }
      try (final IndexCursor ascCursor = index.range(true, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true)) {
        assertThat(ascCursor.hasNext()).as("same check ascending").isFalse();
      }
    } finally {
      database.rollback();
    }
  }

  // ===== Issue #6944 =====
  /*
   * Regression test for #6944: {@link LSMTreeIndexCursor#fetchNext()} allocated a fresh {@code ArrayList},
   * {@code HashMap} and {@code HashSet} for every key group it emitted, i.e. per row on a unique-index scan. The fix
   * hoists the three containers to instance fields and {@code clear()}s them at the start of every group instead.
   * <p>
   * That reuse is only safe if every group starts from a genuinely empty container - the risky case is a group with
   * NO live page cursor (every {@code pageCursors[p]} already exhausted and null), which is reached once a scan works
   * through all committed disk entries and only the in-transaction overlay tail remains: the "FIND THE MINOR KEY" loop
   * never touches {@code minorKeyIndexes} on that path, so a reused field would otherwise carry over the previous
   * group's (unrelated) cursor indices instead of the explicit {@code clear()} added at the top of the round. These
   * tests force exactly that page-exhausted-then-tx-only-tail transition, in both directions, plus a duplicate/tombstone
   * heavy scan that cycles the {@code ridState}/{@code mergedRIDs} fields across many groups.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Test
  @DisplayName("#6944 tx only tail after disk exhaustion ascending")
  void txOnlyTailAfterDiskExhaustionAscending() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc6944a").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    database.getSchema().buildTypeIndex("Doc6944a", new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    // Committed baseline: several disk-backed key groups.
    database.transaction(() -> {
      for (int i = 0; i < 20; ++i)
        database.newDocument("Doc6944a").set("a", i).save();
    });

    database.begin();
    try {
      // Uncommitted keys strictly ABOVE every committed key: once the scan exhausts the disk cursors it must
      // keep emitting these tx-only groups WITHOUT inheriting the last disk group's stale cursor indices.
      for (int i = 20; i < 30; ++i)
        database.newDocument("Doc6944a").set("a", i).save();

      final Set<Integer> seen = new HashSet<>();
      final ResultSet rs = database.query("sql", "SELECT a FROM Doc6944a ORDER BY a ASC");
      while (rs.hasNext())
        seen.add((Integer) rs.next().getProperty("a"));

      assertThat(seen).as("both the committed disk entries and the uncommitted tx-only tail must be visible")
          .containsExactlyInAnyOrderElementsOf(IntStream.range(0, 30).boxed().toList());
    } finally {
      database.rollback();
    }
  }

  @Test
  @DisplayName("#6944 tx only tail after disk exhaustion descending")
  void txOnlyTailAfterDiskExhaustionDescending() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc6944b").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    database.getSchema().buildTypeIndex("Doc6944b", new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    database.transaction(() -> {
      for (int i = 20; i < 40; ++i)
        database.newDocument("Doc6944b").set("a", i).save();
    });

    database.begin();
    try {
      // Uncommitted keys strictly BELOW every committed key: in DESC order these become the tx-only tail once
      // the disk cursors (the higher keys) are exhausted.
      for (int i = 0; i < 20; ++i)
        database.newDocument("Doc6944b").set("a", i).save();

      final Set<Integer> seen = new HashSet<>();
      final ResultSet rs = database.query("sql", "SELECT a FROM Doc6944b ORDER BY a DESC");
      while (rs.hasNext())
        seen.add((Integer) rs.next().getProperty("a"));

      assertThat(seen).as("both the committed disk entries and the uncommitted tx-only tail must be visible")
          .containsExactlyInAnyOrderElementsOf(IntStream.range(0, 40).boxed().toList());
    } finally {
      database.rollback();
    }
  }

  @Test
  @DisplayName("#6944 many groups with duplicates and tombstones survive field reuse")
  void manyGroupsWithDuplicatesAndTombstonesSurviveFieldReuse() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc6944c").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    database.getSchema().buildTypeIndex("Doc6944c", new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    // 3 committed batches (separate transactions -> separate mutable pages) so keys interleave across pages,
    // each key duplicated a few times, to exercise the ridState/mergedRIDs merge across many key groups.
    for (int batch = 0; batch < 3; ++batch) {
      final int b = batch;
      database.transaction(() -> {
        for (int i = 0; i < 100; ++i)
          for (int dup = 0; dup < 2; ++dup)
            database.newDocument("Doc6944c").set("a", i).set("batch", b).set("dup", dup).save();
      });
    }

    // Delete every RID belonging to duplicate slot 0 to force per-RID tombstones interleaved with the survivors.
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Doc6944c WHERE dup = 0");
      while (rs.hasNext())
        rs.next().getRecord().get().asDocument().delete();
    });

    int count = 0;
    final Set<String> distinctKeysSeen = new HashSet<>();
    final ResultSet rs = database.query("sql", "SELECT a FROM Doc6944c ORDER BY a ASC");
    while (rs.hasNext()) {
      distinctKeysSeen.add(rs.next().getProperty("a").toString());
      ++count;
    }

    // 3 batches * 1 surviving duplicate (dup=1) per key = 3 rows per key, 100 distinct keys.
    assertThat(count).as("only the non-tombstoned duplicates must survive the scan").isEqualTo(300);
    assertThat(distinctKeysSeen).hasSize(100);
  }

  /**
   * The actual perf claim: {@code minorKeyIndexes}/{@code ridState}/{@code mergedRIDs} must be the SAME container
   * instance across key groups, not a fresh {@code ArrayList}/{@code HashMap}/{@code HashSet} per group. Before the
   * fix, this identity check fails on the very first two rows.
   */
  @Test
  @DisplayName("#6944 scratch containers are reused across groups")
  void scratchContainersAreReusedAcrossGroups() throws Exception {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc6944d").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    database.getSchema().buildTypeIndex("Doc6944d", new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    database.transaction(() -> {
      for (int i = 0; i < 10; ++i)
        database.newDocument("Doc6944d").set("a", i).save();
    });

    database.transaction(() -> {
      try {
        final var typeIndex = database.getSchema().getType("Doc6944d").getIndexesByProperties("a").getFirst();
        LSMTreeIndexMutable mutable = null;
        for (final var bucketIndex : typeIndex.getIndexesOnBuckets())
          if (bucketIndex instanceof LSMTreeIndex lsmIndex)
            mutable = lsmIndex.getMutableIndex();

        final LSMTreeIndexCursor cursor = (LSMTreeIndexCursor) mutable.range(true, null, true, null, true);
        try {
          final Field minorKeyIndexesField = LSMTreeIndexCursor.class.getDeclaredField("minorKeyIndexes");
          final Field ridStateField = LSMTreeIndexCursor.class.getDeclaredField("ridState");
          final Field mergedRIDsField = LSMTreeIndexCursor.class.getDeclaredField("mergedRIDs");
          minorKeyIndexesField.setAccessible(true);
          ridStateField.setAccessible(true);
          mergedRIDsField.setAccessible(true);

          assertThat(cursor.hasNext()).isTrue();
          final RID first = cursor.next();
          final Object minorKeyIndexesAfterFirst = minorKeyIndexesField.get(cursor);
          final Object ridStateAfterFirst = ridStateField.get(cursor);
          final Object mergedRIDsAfterFirst = mergedRIDsField.get(cursor);

          assertThat(cursor.hasNext()).isTrue();
          final RID second = cursor.next();
          assertThat(second).isNotEqualTo(first);

          assertThat(minorKeyIndexesField.get(cursor)).as("minorKeyIndexes must be the SAME instance, not reallocated per group")
              .isSameAs(minorKeyIndexesAfterFirst);
          assertThat(ridStateField.get(cursor)).as("ridState must be the SAME instance, not reallocated per group")
              .isSameAs(ridStateAfterFirst);
          assertThat(mergedRIDsField.get(cursor)).as("mergedRIDs must be the SAME instance, not reallocated per group")
              .isSameAs(mergedRIDsAfterFirst);
        } finally {
          cursor.close();
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  // ===== Issue #9461 =====
  /*
   * Regression test for #9461: {@link LSMTreeIndexCursor} reuses its per-key-group scratch ({@code ridState},
   * {@code mergedRIDs}) across groups (#6944). A {@code HashMap} never shrinks its table, and {@code clear()}, iteration
   * and {@code toArray()} all cost O(capacity), so once the scan passed ONE key holding N RIDs every later key paid O(N)
   * as well: a scan over K keys cost O(K x N). A FULL geospatial index, where every record shares the top GeoHash cells,
   * took ~4 hours to count 11M entries in the benchmark lane.
   */
  private static final int HOT_KEY_RIDS  = 50_000;
  private static final int DISTINCT_KEYS = 50_000;

  @Test
  @DisplayName("#9461 scan after a hot key stays linear")
  void scanAfterAHotKeyStaysLinear() {
    final TypeIndex index = createIndexWithHotKey("Doc9461a");

    // Pre-fix this count takes tens of seconds (50k later keys x a 128k-slot table, three passes each); fixed, it is a
    // few hundred milliseconds. The bound is the complexity claim itself, so do not loosen it to silence a red run.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final long entries = index.countEntries();
    stopwatch.assertStayedUnder(5_000, "a full scan past one hot key must stay linear in the number of keys (#9461)");

    assertThat(entries).isEqualTo(HOT_KEY_RIDS + DISTINCT_KEYS);
  }

  @Test
  @DisplayName("#9461 scratch is replaced after a hot key group")
  void scratchIsReplacedAfterAHotKeyGroup() throws Exception {
    final TypeIndex index = createIndexWithHotKey("Doc9461b");

    final LSMTreeIndexCursor cursor = (LSMTreeIndexCursor) lsmOf(index).getMutableIndex().range(true, null, true, null, true);
    try {
      final Field ridStateField = LSMTreeIndexCursor.class.getDeclaredField("ridState");
      final Field mergedRIDsField = LSMTreeIndexCursor.class.getDeclaredField("mergedRIDs");
      ridStateField.setAccessible(true);
      mergedRIDsField.setAccessible(true);

      // key 0 is the hot key: drain its whole group, then step onto the first ordinary key
      for (int i = 0; i < HOT_KEY_RIDS; i++)
        cursor.next();
      final Object ridStateAfterHotKey = ridStateField.get(cursor);
      final Object mergedRIDsAfterHotKey = mergedRIDsField.get(cursor);

      final RID firstOrdinary = cursor.next();
      assertThat(cursor.getKeys()[0]).isEqualTo(1);
      assertThat(firstOrdinary).isNotNull();

      assertThat(ridStateField.get(cursor)).as("a ridState grown by the hot key must not be reused").isNotSameAs(ridStateAfterHotKey);
      assertThat(mergedRIDsField.get(cursor)).as("a mergedRIDs grown by the hot key must not be reused")
          .isNotSameAs(mergedRIDsAfterHotKey);

      // ordinary groups keep reusing the (fresh, small) containers, as #6944 intended
      final Object ridStateSmall = ridStateField.get(cursor);
      cursor.next();
      assertThat(ridStateField.get(cursor)).isSameAs(ridStateSmall);
    } finally {
      cursor.close();
    }
  }

  private TypeIndex createIndexWithHotKey(final String typeName) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    final TypeIndex index = database.getSchema().buildTypeIndex(typeName, new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      // the hot key sorts first, so every ordinary key is scanned after it
      for (int i = 0; i < HOT_KEY_RIDS; i++)
        database.newDocument(typeName).set("a", 0).save();
      for (int i = 1; i <= DISTINCT_KEYS; i++)
        database.newDocument(typeName).set("a", i).save();
    });
    return index;
  }

  private static LSMTreeIndex lsmOf(final TypeIndex index) {
    return (LSMTreeIndex) index.getIndexesOnBuckets()[0];
  }
}
