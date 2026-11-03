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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.IndexCursorCollection;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionIndexContext.ComparableKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexUnderlyingCompactedSeriesCursor;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * The contract of the one index-key equality policy, pinned directly rather than only through the three call sites
 * that read it ({@code DocumentIndexer}, {@code TransactionIndexContext.IndexKey} and its {@code ComparableKey}).
 * <p>
 * The point of the class is that array-valued key elements - a {@code BINARY} property's {@code byte[]}, a dense
 * vector's {@code float[]} - are compared by CONTENT, because they do not override {@code Object.equals()} and the
 * shallow {@code Arrays.equals} therefore compares them by identity. That is issue #7109 on one side of the index
 * path and issue #7881 on the other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class IndexKeyEqualityTest {

  @Test
  void anArrayValuedElementIsComparedByContent() {
    final Object[] a = { new byte[] { 1, 2, 3 } };
    final Object[] b = { new byte[] { 1, 2, 3 } };

    assertThat(IndexKeyEquality.sameTuple(a, b)).isTrue();
    assertThat(IndexKeyEquality.hashTuple(a)).isEqualTo(IndexKeyEquality.hashTuple(b));
    assertThat(IndexKeyEquality.sameValue(a[0], b[0])).isTrue();
  }

  @Test
  void contentThatDiffersIsStillToldApart() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new byte[] { 1, 2, 3 } },
        new Object[] { new byte[] { 1, 2, 4 } })).isFalse();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new float[] { 1f, 2f } },
        new Object[] { new float[] { 1f, 2f, 3f } })).isFalse();
  }

  @Test
  void everyPrimitiveArrayTypeAnIndexKeyCanHoldIsCoveredNotJustObjectArrays() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new float[] { 1f, 2f } },
        new Object[] { new float[] { 1f, 2f } })).isTrue();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new int[] { 7 } }, new Object[] { new int[] { 7 } })).isTrue();
    // A NESTED Object[] element goes one level deeper still, which is what distinguishes deepEquals from equals.
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new Object[] { new byte[] { 5 } } },
        new Object[] { new Object[] { new byte[] { 5 } } })).isTrue();
  }

  @Test
  void aScalarElementKeepsItsOwnEquality() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a", 1 }, new Object[] { "a", 1 })).isTrue();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a", 1 }, new Object[] { "a", 2 })).isFalse();
    // A List element is content-comparable on its own, which is why ComparableKey.compareTo's List arm never
    // disagreed with the shallow equality the way the array arm did.
    assertThat(IndexKeyEquality.sameTuple(new Object[] { List.of("x", "y") }, new Object[] { List.of("x", "y") }))
        .isTrue();
  }

  @Test
  void aLengthMismatchIsNotTheSameKey() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a" }, new Object[] { "a", "b" })).isFalse();
    assertThat(IndexKeyEquality.sameTuple(new Object[0], new Object[0])).isTrue();
  }

  @Test
  void aNullTupleEqualsOnlyAnotherNullTupleAndANullElementOnlyAnotherNullElement() {
    assertThat(IndexKeyEquality.sameTuple(null, null)).isTrue();
    assertThat(IndexKeyEquality.sameTuple(null, new Object[] { "a" })).isFalse();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a" }, null)).isFalse();
    assertThat(IndexKeyEquality.hashTuple(null)).isZero();

    assertThat(IndexKeyEquality.sameTuple(new Object[] { null }, new Object[] { null })).isTrue();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { null }, new Object[] { "a" })).isFalse();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a" }, new Object[] { null })).isFalse();
    assertThat(IndexKeyEquality.sameValue(null, null)).isTrue();
  }

  @Test
  void equalTuplesHashEqually() {
    // The half of the contract a HashMap depends on: two tuples that compare equal must never land in different
    // buckets. Checked on the array-valued case, which is the one the shallow form got wrong.
    for (final Object[][] pair : new Object[][][] {
        { { new byte[] { 1, 2 } }, { new byte[] { 1, 2 } } },
        { { "a", new float[] { 3f } }, { "a", new float[] { 3f } } },
        { { null, new int[] { 4 } }, { null, new int[] { 4 } } } }) {
      assertThat(IndexKeyEquality.sameTuple(pair[0], pair[1])).isTrue();
      assertThat(IndexKeyEquality.hashTuple(pair[0])).isEqualTo(IndexKeyEquality.hashTuple(pair[1]));
    }
  }

  /**
   * Regression test for issue #7881: the transaction-side index overlay compared array-valued key elements (a
   * {@code BINARY} property is a {@code byte[]}) with the SHALLOW {@code Arrays.equals}/{@code Arrays.hashCode} in
   * {@code equals()}/{@code hashCode()}, while {@code ComparableKey.compareTo} compared the very same elements BY
   * CONTENT through {@code BinaryComparator}. Both are used, on the same data, by the same code path: {@code compareTo}
   * orders the {@code TreeMap} of pending entries and {@code equals}/{@code hashCode} key the {@code HashMap} inside
   * each of its values, so the outer lookup found the key and the inner one missed it.
   * <p>
   * What followed from that one miss is the REMOVE-then-ADD merge: the ADD never found the REMOVE it was meant to
   * retire, so it was not turned into a REPLACE carrying the superseded {@code oldRid} - which is exactly what commit
   * keys the removal of the old RID on. In practice the very next check, the one that sweeps the OTHER sub-indexes,
   * reads its per-key map by iteration rather than by lookup, so it still saw the stale ADD and REFUSED a perfectly
   * legitimate delete-and-reinsert of the same BINARY key in one transaction with a {@code DuplicatedKeyException}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#7881 transaction index array key equality")
  class TransactionIndexArrayKeyEquality extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        // ONE bucket, so both records of a transaction land on the same sub-index and go through the per-key map that
        // the shallow equality broke - the cross-sub-index check a few lines further down never used it.
        database.getSchema().createDocumentType("Bin7881", 1).createProperty("hash", Type.BINARY);
        database.getSchema().getType("Bin7881").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "hash");
      });
    }

    @Test
    void equalsAgreesWithCompareToOnContentEqualByteArrayKeys() {
      final Object[] a = new Object[] { new byte[] { 1, 2, 3 } };
      final Object[] b = new Object[] { new byte[] { 1, 2, 3 } };

      assertThat(new ComparableKey(a).compareTo(new ComparableKey(b))).isZero();
      assertThat(new ComparableKey(a)).isEqualTo(new ComparableKey(b));
      assertThat(new ComparableKey(a).hashCode()).isEqualTo(new ComparableKey(b).hashCode());

      final RID rid = new RID(1, 1);
      final IndexKey k1 = new IndexKey(true, IndexKey.IndexKeyOperation.ADD, a, rid);
      final IndexKey k2 = new IndexKey(true, IndexKey.IndexKeyOperation.ADD, b, rid);
      assertThat(k1).isEqualTo(k2);
      assertThat(k1.hashCode()).isEqualTo(k2.hashCode());
    }

    @Test
    void theNonUniqueBranchOfTheKeyComparesItsArrayValuesByContentToo() {
      // The unique branch above keys on the tuple alone; a non-unique index keys on the tuple AND the RID, and it is
      // the SAME helper doing the tuple half. Pinned so a future edit to one branch cannot silently leave the other
      // on a shallow comparison (PR #8091 review).
      final Object[] a = { new byte[] { 1, 2, 3 } };
      final Object[] b = { new byte[] { 1, 2, 3 } };
      final RID rid = new RID(1, 1);
      final RID otherRid = new RID(1, 2);

      final IndexKey k1 = new IndexKey(false, IndexKey.IndexKeyOperation.ADD, a, rid);
      final IndexKey sameRid = new IndexKey(false, IndexKey.IndexKeyOperation.ADD, b, rid);
      final IndexKey otherRidSameKey = new IndexKey(false, IndexKey.IndexKeyOperation.ADD, b, otherRid);

      assertThat(k1).isEqualTo(sameRid);
      assertThat(k1.hashCode()).isEqualTo(sameRid.hashCode());
      assertThat(k1).as("a non-unique index keeps one entry per RID, so the RID still separates them")
          .isNotEqualTo(otherRidSameKey);
    }

    /** The invariant the per-key duplicate check exists for, kept under a content-aware equality. */
    @Test
    void twoRecordsWithTheSameBinaryKeyInOneTransactionAreRefused() {
      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument("Bin7881").set("hash", new byte[] { 1, 2, 3 }).save();
        database.newDocument("Bin7881").set("hash", new byte[] { 1, 2, 3 }).save();
      })).isInstanceOf(DuplicatedKeyException.class);

      assertThat(ridsFor(new byte[] { 1, 2, 3 })).isEmpty();
    }

    @Test
    void aDeleteAndReinsertOfTheSameBinaryKeyInOneTransactionLeavesNoStaleEntry() {
      final RID[] first = new RID[1];
      database.transaction(() -> first[0] = database.newDocument("Bin7881").set("hash", new byte[] { 9, 8, 7 }).save()
          .getIdentity());

      final RID[] second = new RID[1];
      database.transaction(() -> {
        database.lookupByRID(first[0], true).asDocument().modify().delete();
        second[0] = database.newDocument("Bin7881").set("hash", new byte[] { 9, 8, 7 }).save().getIdentity();
      });

      assertThat(second[0]).isNotEqualTo(first[0]);
      assertThat(ridsFor(new byte[] { 9, 8, 7 }))
          .as("the superseded RID must have been removed from the persisted index")
          .containsExactly(second[0]);
      assertThat(database.getSchema().getIndexByName("Bin7881[hash]").countEntries()).isEqualTo(1);
    }

    @Test
    void theUniqueConstraintStillHoldsAcrossTransactions() {
      database.transaction(() -> database.newDocument("Bin7881").set("hash", new byte[] { 4, 5, 6 }).save());
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Bin7881");
        doc.set("hash", new byte[] { 4, 5, 6 });
        doc.save();
      })).isInstanceOf(DuplicatedKeyException.class);
    }

    private List<RID> ridsFor(final byte[] key) {
      final List<RID> rids = new ArrayList<>();
      final IndexCursor cursor = database.lookupByKey("Bin7881", "hash", key);
      while (cursor.hasNext())
        rids.add(cursor.next().getIdentity());
      return rids;
    }
  }

  /**
   * Issue #10188: a NOTUNIQUE LSM-tree index returned entries its records no longer had once the engine had compacted it and the
   * records were updated. The compaction stores the values of a key in chunks, and a key with many values has its chunks in
   * consecutive entries that run over the end of a data page. The cursor over the compacted series handed the chunks out one page
   * at a time, so the merge of the series with the mutable pages applied the tombstones of the mutable pages to the first page of
   * the key only: the values of the following pages were returned as they were, deleted or not. Point lookups were not affected.
   * <p>
   * Here one key holds most of the records, so its values span several data pages, and every scan direction and range is checked
   * against the records after the key lost a third of its records to other keys and a tenth of them to deletion.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#10188 run of one key spanning several pages")
  class MultiPageKeyRun extends TestHelper {
    private static final String TYPE      = "T";
    private static final int    HOT_KEY   = 1;
    private static final int    HOT       = 2_400;
    private static final int    COLD      = 150;
    private static final int    PAGE_SIZE = 2_048;

    private Object savedMinPages;

    @BeforeEach
    void disableAutomaticCompaction() {
      // Compaction only when the test asks for it, so the series and the tombstones are where the test puts them
      savedMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValue();
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    }

    @AfterEach
    void restoreAutomaticCompaction() {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(savedMinPages);
    }

    @Test
    void everyScanOfACompactedKeyRunSeesTheTombstonesOfTheMutablePages() {
      final LSMTreeIndex bucketIndex = createTypeWithHotKey();
      compact(bucketIndex);

      // The values of the hot key must reach past a data page, or the test proves nothing
      assertThat(bucketIndex.getMutableIndex().getSubIndex().getTotalPages()).as("pages of the compacted index").isGreaterThanOrEqualTo(5);

      final List<RID> hot = ridsWithKey(HOT_KEY);
      database.transaction(() -> {
        for (int i = 0; i < hot.size(); i++) {
          final Document document = database.lookupByRID(hot.get(i), true).asDocument();
          if (i % 3 == 0)
            document.modify().set("k", 2).save();
          else if (i % 10 == 1)
            document.delete();
        }
      });

      assertTheIndexMatchesTheRecords();
    }

    @Test
    void aKeyThatWasMovedAwayAndBackIsReturnedOnce() {
      final LSMTreeIndex bucketIndex = createTypeWithHotKey();
      compact(bucketIndex);

      final List<RID> hot = ridsWithKey(HOT_KEY);
      // Away, and back: the last operation of each record on the key is an addition after a tombstone
      database.transaction(() -> {
        for (int i = 0; i < hot.size(); i += 2)
          database.lookupByRID(hot.get(i), true).asDocument().modify().set("k", 2).save();
      });
      database.transaction(() -> {
        for (int i = 0; i < hot.size(); i += 4)
          database.lookupByRID(hot.get(i), true).asDocument().modify().set("k", HOT_KEY).save();
      });

      assertTheIndexMatchesTheRecords();
    }

    @Test
    void aTombstoneInTheCompactedSeriesItselfIsAppliedToTheFollowingPages() {
      final LSMTreeIndex bucketIndex = createTypeWithHotKey();
      compact(bucketIndex);

      final List<RID> hot = ridsWithKey(HOT_KEY);
      database.transaction(() -> {
        for (int i = 0; i < hot.size(); i += 2)
          database.lookupByRID(hot.get(i), true).asDocument().modify().set("k", 2).save();
      });
      // A second series: it holds the tombstones, the first one the entries they delete
      compact(bucketIndex);

      assertTheIndexMatchesTheRecords();
    }

    @Test
    void thePositionOfASeriesCursorIsTheEndOfTheKeyWhateverIsAskedFirst() throws Exception {
      final LSMTreeIndex bucketIndex = createTypeWithHotKey();
      compact(bucketIndex);
      // The root page, the first page of the file and at least three data pages: the hot key reaches over several of them
      assertThat(bucketIndex.getMutableIndex().getSubIndex().getTotalPages()).as("pages of the compacted index").isGreaterThanOrEqualTo(5);

      for (final boolean ascending : new boolean[] { true, false }) {
        final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> byPosition = bucketIndex.getMutableIndex().getSubIndex()
            .newIterators(ascending, null, null);
        final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> byKey = bucketIndex.getMutableIndex().getSubIndex()
            .newIterators(ascending, null, null);
        assertThat(byPosition).hasSameSizeAs(byKey).isNotEmpty();

        int keys = 0;
        int hotKeys = 0;
        for (int series = 0; series < byPosition.size(); series++) {
          final LSMTreeIndexUnderlyingCompactedSeriesCursor asksPositionFirst = byPosition.get(series);
          final LSMTreeIndexUnderlyingCompactedSeriesCursor asksKeyFirst = byKey.get(series);
          while (asksPositionFirst.hasNext()) {
            assertThat(asksKeyFirst.hasNext()).isTrue();
            asksPositionFirst.next();
            asksKeyFirst.next();

            final int pageAsked = asksPositionFirst.getCurrentPageId().getPageNumber();
            final int positionAsked = asksPositionFirst.getCurrentPositionInPage();
            final Object[] key = asksKeyFirst.getKeys();
            final int firstPage = asksKeyFirst.getCurrentPageId().getPageNumber();

            assertThat(pageAsked).as("page of key " + key[0]).isEqualTo(firstPage);
            assertThat(positionAsked).as("position of key " + key[0]).isEqualTo(asksKeyFirst.getCurrentPositionInPage());
            assertThat(asksPositionFirst.getValue()).as("values of key " + key[0]).isEqualTo(asksKeyFirst.getValue());
            ++keys;
            if (asksKeyFirst.getValue().length >= HOT / 2)
              ++hotKeys;
          }
          assertThat(asksKeyFirst.hasNext()).isFalse();
          asksPositionFirst.close();
          asksKeyFirst.close();
        }
        // The last page of the index is still being filled and stays in the mutable index: the series holds the keys up to there
        assertThat(keys).as("keys of the series, " + (ascending ? "ascending" : "descending")).isGreaterThanOrEqualTo(2);
        assertThat(hotKeys).as("the hot key comes back whole").isEqualTo(1);
      }
    }

    private LSMTreeIndex createTypeWithHotKey() {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
      type.createProperty("k", Integer.class);
      database.getSchema().buildTypeIndex(TYPE, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
          .withPageSize(PAGE_SIZE).create();

      database.transaction(() -> {
        for (int i = 0; i < HOT; i++)
          database.newDocument(TYPE).set("k", HOT_KEY).save();
        for (int i = 0; i < COLD; i++) {
          database.newDocument(TYPE).set("k", 0).save();
          database.newDocument(TYPE).set("k", 2).save();
        }
      });
      return (LSMTreeIndex) database.getSchema().getType(TYPE).getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0];
    }

    private static void compact(final LSMTreeIndex index) {
      try {
        assertThat(index.scheduleCompaction()).isTrue();
        index.compact();
      } catch (final Exception e) {
        throw new AssertionError("The compaction failed", e);
      }
    }

    private List<RID> ridsWithKey(final int key) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet resultSet = database.query("sql", "SELECT @rid AS rid FROM " + TYPE + " WHERE k = ?", key)) {
        while (resultSet.hasNext())
          rids.add(resultSet.next().getProperty("rid"));
      }
      Collections.sort(rids);
      return rids;
    }

    private void assertTheIndexMatchesTheRecords() {
      // What the records say, by a scan that does not use the index
      final Map<Integer, Set<RID>> expected = new HashMap<>();
      final Iterator<Record> records = database.iterateType(TYPE, true);
      while (records.hasNext()) {
        final Document document = records.next().asDocument();
        expected.computeIfAbsent(document.getInteger("k"), k -> new HashSet<>()).add(document.getIdentity());
      }

      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(TYPE + "[k]");
      final int total = expected.values().stream().mapToInt(Set::size).sum();

      assertThat(scan(index.iterator(true), -1)).as("ascending").isEqualTo(expected);
      assertThat(scan(index.iterator(false), -1)).as("descending").isEqualTo(expected);
      assertThat(index.countEntries()).as("countEntries()").isEqualTo(total);

      // A range that starts in the middle of the key run: the first and the last key of the range
      for (final int key : new int[] { 0, HOT_KEY, 2 }) {
        final Map<Integer, Set<RID>> only = new HashMap<>();
        if (expected.containsKey(key))
          only.put(key, expected.get(key));
        assertThat(scan(index.range(true, new Object[] { key }, true, new Object[] { key }, true), key)).as("ascending range of " + key)
            .isEqualTo(only);
        assertThat(scan(index.range(false, new Object[] { key }, true, new Object[] { key }, true), key)).as("descending range of " + key)
            .isEqualTo(only);
      }

      final Map<Integer, Set<RID>> fromHot = new HashMap<>(expected);
      fromHot.remove(0);
      assertThat(scan(index.range(true, new Object[] { HOT_KEY }, true, new Object[] { 2 }, true), -1)).as("ascending range from the hot key")
          .isEqualTo(fromHot);
      final Map<Integer, Set<RID>> upToHot = new HashMap<>(expected);
      upToHot.remove(2);
      assertThat(scan(index.range(false, new Object[] { HOT_KEY }, true, new Object[] { 0 }, true), -1)).as("descending range from the hot key")
          .isEqualTo(upToHot);

      for (final int key : expected.keySet()) {
        final Set<RID> found = new HashSet<>();
        for (final IndexCursor cursor = index.get(new Object[] { key }); cursor.hasNext(); )
          found.add(cursor.next().getIdentity());
        assertThat(found).as("point lookup of " + key).isEqualTo(expected.get(key));
      }
    }

    /** The RIDs the cursor returns by key, asserting none is returned twice. {@code onlyKey} -1 for any key. */
    private static Map<Integer, Set<RID>> scan(final IndexCursor cursor, final int onlyKey) {
      final Map<Integer, Set<RID>> found = new HashMap<>();
      int entries = 0;
      while (cursor.hasNext()) {
        final Identifiable rid = cursor.next();
        final int key = ((Number) cursor.getKeys()[0]).intValue();
        if (onlyKey >= 0)
          assertThat(key).as("key of a range of one key").isEqualTo(onlyKey);
        ++entries;
        found.computeIfAbsent(key, k -> new HashSet<>()).add(rid.getIdentity());
      }
      assertThat(entries).as("entries returned").isEqualTo(found.values().stream().mapToInt(Set::size).sum());
      return found;
    }
  }

  /**
   * Regression test for issue #7765: once a key-wide tombstone (the {@code RID(-1,-1)} sentinel written by
   * {@code IndexInternal.remove(Object[] keys)} with no RID) is COMMITTED - as opposed to still pending in the
   * writing transaction, which #6927 already covers correctly - {@code LSMTreeIndexAbstract
   * .lookupInPageAndAddInResultset()} (the reader behind {@code Index.get(keys)} / the SQL equality predicate) decoded
   * it as an ordinary per-RID tombstone via {@code getOriginalRID()}, which maps {@code (-1,-1)} back to
   * {@code (-1,-1)} - matching no real record - and only fed {@code removedKeys} (the thing that actually suppresses
   * the key) when {@code mainIndex.isUnique()}. On a NON-UNIQUE index the point lookup therefore kept returning every
   * RID written before the tombstone, while {@code range()}/{@code countEntries()} (which walk the cursor) and
   * {@code LSMTreeIndexCompactor.compactFull()} already agreed the key was gone - so the SAME index answered
   * differently depending on which read path was used, and the wrong answer even flipped to the right one the first
   * time a full compaction ran.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#7765 key-wide tombstone point lookup")
  class KeyWideTombstonePointLookup extends TestHelper {

    private static final String TYPE_NAME = "Foo";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final var type = database.getSchema().createDocumentType(TYPE_NAME);
        type.createProperty("name", Type.STRING);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "name" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      });
    }

    @Test
    void committedKeyWideRemoveIsHonouredByThePointLookupOnANonUniqueIndex() {
      database.transaction(() -> {
        database.newDocument(TYPE_NAME).set("name", "a").save();
        database.newDocument(TYPE_NAME).set("name", "a").save();
        database.newDocument(TYPE_NAME).set("name", "a").save();
      });

      final RangeIndex index = rangeIndex();
      assertThat(drain(index.get(new Object[] { "a" }))).hasSize(3);

      // Key-wide remove(keys), committed in its OWN transaction - separate from the inserts, so the removal really
      // reaches disk instead of being collapsed by the in-transaction ADD+REMOVE folding (#6927's territory).
      database.transaction(() -> ((IndexInternal) index).remove(new Object[] { "a" }));

      database.transaction(() -> {
        assertThat(drain(index.get(new Object[] { "a" })))
            .as("Index.get() must honour a committed key-wide tombstone on a NON-UNIQUE index")
            .isEmpty();
        assertThat(index.countEntries()).isZero();

        final List<RID> ascending = drain(index.range(true, new Object[] { "a" }, true, new Object[] { "a" }, true));
        assertThat(ascending).isEmpty();

        try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE name = 'a'")) {
          assertThat(rs.hasNext()).as("the SQL equality predicate must agree with Index.get()").isFalse();
        }
      });
    }

    /**
     * The walk is newest-to-oldest, so a key-wide tombstone must suppress only what was written BEFORE it - a fresh
     * insert at the same key AFTER the tombstone has to stay visible to the point lookup.
     */
    @Test
    void anInsertAfterTheKeyWideRemoveStaysVisibleToThePointLookup() {
      database.transaction(() -> {
        database.newDocument(TYPE_NAME).set("name", "b").save();
        database.newDocument(TYPE_NAME).set("name", "b").save();
      });

      final RangeIndex index = rangeIndex();

      database.transaction(() -> ((IndexInternal) index).remove(new Object[] { "b" }));

      final RID[] resurrected = new RID[1];
      database.transaction(() -> resurrected[0] = database.newDocument(TYPE_NAME).set("name", "b").save().getIdentity());

      database.transaction(() -> {
        final var found = drain(index.get(new Object[] { "b" }));
        assertThat(found).containsExactly(resurrected[0]);
        assertThat(index.countEntries()).isEqualTo(1);
      });
    }

    private RangeIndex rangeIndex() {
      return (RangeIndex) database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties("name");
    }

    private List<RID> drain(final IndexCursor cursor) {
      final List<RID> rids = new ArrayList<>();
      while (cursor.hasNext())
        rids.add(cursor.next().getIdentity());
      return rids;
    }
  }

  /**
   * Issue #9811, engine side: {@link TypeIndex#get(Object[])} answers a key lookup over whatever sub-indexes the index has at
   * that instant, so a lookup during a concurrent CREATE, REBUILD or DROP INDEX came back short - empty, for a key that exists
   * - without any error. A protocol that answers a client from it (the Redis wrapper: "the key does not exist") had to remember
   * three separate guards. {@link TypeIndex#getForQuery} makes them in one place and raises the stale-index
   * {@link IndexException} instead of a short answer, and {@code Database.lookupByKey} reads through it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#9811 guarded key lookup")
  class GuardedKeyLookup extends TestHelper {
    private static final int RECORDS = 2_000;

    @Test
    void aLookupOnAnIndexWhoseSubIndexesAreGoneRaisesInsteadOfAnsweringEmpty() {
      final TypeIndex index = createIndex();
      assertThat(index.getForQuery(new Object[] { 7L }).hasNext()).isTrue();

      // the state a concurrent drop leaves for a moment: the wrapper still valid, its sub-indexes gone
      final IndexInternal[] snapshot = index.getIndexesOnBuckets();
      for (final IndexInternal subIndex : snapshot)
        index.removeIndexOnBucket(subIndex);
      try {
        // the index cannot answer, and says so rather than "no such key"
        assertThatThrownBy(() -> index.getForQuery(new Object[] { 7L })).isInstanceOf(IndexException.class);
      } finally {
        for (final IndexInternal subIndex : snapshot)
          index.addIndexOnBucket(subIndex);
      }
      assertThat(index.getForQuery(new Object[] { 7L }).hasNext()).isTrue();
    }

    /**
     * The invariant {@link TypeIndex#getForQuery} rests on: every arm of {@code get} has collected its entries by the time it
     * returns, so the change check after it covers the whole read. A lazy arm added later would make the check pass while the
     * cursor still reads, and must fail here instead.
     */
    @Test
    void everyArmOfGetReturnsACollectedCursor() {
      final DocumentType type = createType();
      type.createProperty("text", Type.STRING);
      database.transaction(() -> database.newDocument("Doc").set("k", 1L).set("text", "alpha beta").save());

      final TypeIndex notUnique = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
      assertCollected(notUnique.get(new Object[] { 1L }));
      assertCollected(notUnique.get(new Object[] { 1L }, 1));
      database.getSchema().dropIndex(notUnique.getName());

      final TypeIndex unique = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "text");
      assertCollected(unique.get(new Object[] { "alpha beta" }));
      database.getSchema().dropIndex(unique.getName());

      final TypeIndex fullText = type.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "text");
      assertCollected(fullText.get(new Object[] { "alpha" }));
      database.getSchema().dropIndex(fullText.getName());

      database.command("sql", "CREATE INDEX ON Doc (text) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      assertCollected(((TypeIndex) database.getSchema().getIndexByName("Doc[text]")).get(new Object[] { "alpha" }));
    }

    private static void assertCollected(final IndexCursor cursor) {
      assertThat((Object) cursor).isInstanceOfAny(TempIndexCursor.class, IndexCursorCollection.class);
    }

    @Test
    void aLookupOnADroppedIndexIsAStaleRead() {
      final TypeIndex index = createIndex();
      database.getSchema().dropIndex(index.getName());

      final Throwable thrown = catchThrowable(() -> index.getForQuery(new Object[] { 7L }));
      assertThat(thrown).isInstanceOf(IndexException.class);
      assertThat(TypeIndex.isStaleRead(index, (RuntimeException) thrown)).isTrue();
    }

    /**
     * The issue's repro: one thread dropping and re-creating the index, readers looking up a key that exists. The raw read
     * answers "not found" now and then; the guarded reads either find the record or say they could not read the index.
     */
    @Test
    @Tag("slow")
    void underConcurrentDropAndCreateAGuardedLookupNeverAnswersAKeyThatExistsAsMissing() throws Exception {
      final DocumentType type = createType();
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicLong found = new AtomicLong();
      final AtomicLong silentlyEmpty = new AtomicLong();

      final Thread ddl = new Thread(() -> {
        try {
          while (!stop.get()) {
            database.command("sql", "DROP INDEX `Doc[k]` IF EXISTS");
            database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (k) UNIQUE");
            // a moment with the index whole, so the readers have something to find
            Thread.sleep(2);
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });

      final List<Thread> readers = new ArrayList<>();
      for (int r = 0; r < 4; r++) {
        final long key = 100L + r * 300L;
        readers.add(new Thread(() -> {
          while (!stop.get()) {
            // through the schema, as a protocol resolving an index by name does
            final TypeIndex index;
            try {
              index = (TypeIndex) database.getSchema().getIndexByName("Doc[k]");
            } catch (final SchemaException e) {
              continue; // the index is not there at this instant: a visible answer
            }
            check(() -> index.getForQuery(new Object[] { key }), index, found, silentlyEmpty, failure);
            check(() -> ConsistentKeyLookup.getForQuery(database, index, new Object[] { key }), index, found, silentlyEmpty, failure);
            check(() -> database.lookupByKey("Doc", "k", key), index, found, silentlyEmpty, failure);
          }
        }));
      }

      ddl.start();
      readers.forEach(Thread::start);
      // at least 3 s of churn, and until the readers found the key a few times: a loaded runner may leave few windows open
      await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(60)).until(() -> found.get() >= 20 || failure.get() != null);
      stop.set(true);
      ddl.join(60_000);
      for (final Thread reader : readers)
        reader.join(60_000);

      assertThat(failure.get()).isNull();
      assertThat(found.get()).as("the readers did find the key between the DDLs").isPositive();
      assertThat(silentlyEmpty.get()).as("a key that exists answered as missing").isZero();
    }

    /**
     * {@code Database.lookupByKey} now raises instead of answering short, and the openCypher MERGE that seeks through it scans
     * the type then: a short answer made MERGE create a node that exists, an exception would fail a statement that can be
     * answered.
     */
    @Test
    @Tag("slow")
    void underConcurrentDropAndCreateMergeNeitherFailsNorDuplicates() throws Exception {
      database.getSchema().createVertexType("Node", 4).createProperty("k", Type.LONG);
      database.transaction(() -> {
        for (long i = 0; i < RECORDS; i++)
          database.newVertex("Node").set("k", i).save();
      });
      database.command("sql", "CREATE INDEX ON Node (k) NOTUNIQUE");

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicLong merges = new AtomicLong();

      final Thread ddl = new Thread(() -> {
        try {
          while (!stop.get()) {
            database.command("sql", "DROP INDEX `Node[k]` IF EXISTS");
            database.command("sql", "CREATE INDEX IF NOT EXISTS ON Node (k) NOTUNIQUE");
            Thread.sleep(2);
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });
      final Thread merger = new Thread(() -> {
        try {
          while (!stop.get()) {
            database.transaction(() -> database.command("opencypher", "MERGE (n:Node {k: 777}) RETURN n").close());
            merges.incrementAndGet();
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      });

      ddl.start();
      merger.start();
      await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(60)).until(() -> merges.get() >= 20 || failure.get() != null);
      stop.set(true);
      ddl.join(60_000);
      merger.join(60_000);

      assertThat(failure.get()).isNull();
      assertThat(merges.get()).isPositive();
      assertThat(database.query("sql", "SELECT count(@rid) AS c FROM Node WHERE k = 777").next().<Long>getProperty("c"))
          .as("MERGE matched the node that exists every time").isEqualTo(1L);
    }

    private interface Lookup {
      IndexCursor run();
    }

    private static void check(final Lookup lookup, final TypeIndex index, final AtomicLong found, final AtomicLong silentlyEmpty,
        final AtomicReference<Throwable> failure) {
      try (final IndexCursor cursor = lookup.run()) {
        if (cursor.hasNext())
          found.incrementAndGet();
        else
          silentlyEmpty.incrementAndGet();
      } catch (final IllegalArgumentException e) {
        // lookupByKey: "No index has been created", the index was not there or not ready
        if (!e.getMessage().startsWith("No index") && !TypeIndex.isStaleRead(index, e))
          failure.compareAndSet(null, e);
      } catch (final SchemaException e) {
        // the type's index is not registered at this instant: visible, the caller retries
      } catch (final RuntimeException e) {
        if (!TypeIndex.isStaleRead(index, e))
          failure.compareAndSet(null, e);
      }
    }

    private TypeIndex createIndex() {
      return createType().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
    }

    private DocumentType createType() {
      final DocumentType type = database.getSchema().createDocumentType("Doc", 4);
      type.createProperty("k", Type.LONG);
      database.transaction(() -> {
        for (long i = 0; i < RECORDS; i++)
          database.newDocument("Doc").set("k", i).save();
      });
      return type;
    }
  }
}
