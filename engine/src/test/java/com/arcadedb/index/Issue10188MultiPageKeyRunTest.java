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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexUnderlyingCompactedSeriesCursor;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue10188MultiPageKeyRunTest extends TestHelper {
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
