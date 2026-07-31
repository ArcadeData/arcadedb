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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #5635 (follow-up of #5601): {@link LSMTreeIndexCursor} must honour the {@link Iterator} contract.
 * <p>
 * {@code hasNext()} used to answer on how many underlying page cursors were still live, not on whether any of them still
 * held a surviving RID. A scan that ended on a run of tombstoned keys therefore answered {@code true} and then handed the
 * caller a {@code null} element out of {@code next()} - {@code IndexCursor extends Iterator<Identifiable>}, so
 * {@code for (final Identifiable r : cursor)} yielded null. Every consumer that noticed grew its own guard
 * ({@code FullTextQueryExecutor} had four, {@code GeoIndexCursor} one, {@code MultiIndexCursor} propagated it upward) and
 * the ones that did not silently counted the null as a row: that was the whole residual of #5601's
 * {@code countEntries()}, and it is what made {@code SELECT max(id)} report a deleted key.
 * <p>
 * The tests drive the BUCKET-level index, so the cursor under test is a bare {@link LSMTreeIndexCursor} rather than the
 * {@code MultiIndexCursor} a {@link TypeIndex} wraps it in - the wrapper happens to absorb the null, which is exactly why
 * the defect stayed invisible for so long. The fixture is the delete-heavy shape the defect needs: a contiguous run of
 * tombstones at BOTH ends of the key space, so an ascending scan starts inside one and ends inside the other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexCursorContractTest extends TestHelper {
  private static final String TYPE_NAME     = "Measurement";
  private static final int    TOTAL         = 4_000;
  // survivors are the middle slice: [SURVIVOR_FROM, SURVIVOR_TO)
  private static final int    SURVIVOR_FROM = 1_800;
  private static final int    SURVIVOR_TO   = 1_900;
  private static final int    SURVIVORS     = SURVIVOR_TO - SURVIVOR_FROM;

  @Override
  public void beforeTest() {
    // keep compaction manual so each test controls the index shape deterministically
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Test
  void iteratingNeverYieldsNull() {
    final RangeIndex index = createTypeWithTombstoneRunsAtBothEnds();
    database.transaction(() -> {
      assertNoNullElement(index.range(true, new Object[] { 0 }, true, new Object[] { TOTAL }, true));
      assertNoNullElement(index.range(false, new Object[] { TOTAL }, true, new Object[] { 0 }, true));
      assertNoNullElement(index.iterator(true));
      assertNoNullElement(index.iterator(false));
    });
  }

  @Test
  void iteratingNeverYieldsNullAfterCompaction() throws Exception {
    final RangeIndex index = createTypeWithTombstoneRunsAtBothEnds();
    assertThat(((LSMTreeIndex) index).scheduleCompaction()).isTrue();
    assertThat(((LSMTreeIndex) index).compact()).isTrue();
    database.transaction(() -> {
      assertNoNullElement(index.range(true, new Object[] { 0 }, true, new Object[] { TOTAL }, true));
      assertNoNullElement(index.range(false, new Object[] { TOTAL }, true, new Object[] { 0 }, true));
    });
  }

  /**
   * The naive count - one {@code ++} per {@code next()} call, with no null filtering - must agree with the number of
   * live records. This is exactly what #5601's {@code countEntries()} did, and the trailing null was its whole residual.
   */
  @Test
  void naiveCountMatchesTheNumberOfLiveEntries() {
    final RangeIndex index = createTypeWithTombstoneRunsAtBothEnds();
    database.transaction(() -> {
      assertThat(countEveryNextCall(index.range(true, new Object[] { 0 }, true, new Object[] { TOTAL }, true)))
          .isEqualTo(SURVIVORS);
      assertThat(countEveryNextCall(index.range(false, new Object[] { TOTAL }, true, new Object[] { 0 }, true)))
          .isEqualTo(SURVIVORS);
    });
  }

  /**
   * Per {@link Iterator}, an exhausted cursor throws instead of answering null - which is what every other
   * {@link IndexCursor} implementation already does ({@code EmptyIndexCursor}, {@code TempIndexCursor},
   * {@code MultiIndexCursor}, {@code GeoIndexCursor}).
   */
  @Test
  void exhaustedCursorThrowsNoSuchElement() {
    final RangeIndex index = createTypeWithTombstoneRunsAtBothEnds();
    database.transaction(() -> {
      final IndexCursor cursor = index.range(true, new Object[] { 0 }, true, new Object[] { TOTAL }, true);
      try {
        int found = 0;
        while (cursor.hasNext()) {
          assertThat(cursor.next()).isNotNull();
          ++found;
        }
        assertThat(found).as("precondition: the scan must have emitted the survivors").isEqualTo(SURVIVORS);
        assertThatThrownBy(cursor::next).isInstanceOf(NoSuchElementException.class);
      } finally {
        cursor.close();
      }
    });
  }

  /**
   * A range that covers nothing but tombstones must be empty on the very first {@code hasNext()}, not "one null long".
   */
  @Test
  void rangeCoveringOnlyTombstonesIsEmpty() {
    final RangeIndex index = createTypeWithTombstoneRunsAtBothEnds();
    database.transaction(() -> {
      final IndexCursor cursor = index.range(true, new Object[] { 0 }, true, new Object[] { SURVIVOR_FROM - 1 }, true);
      try {
        assertThat(cursor.hasNext()).isFalse();
        assertThatThrownBy(cursor::next).isInstanceOf(NoSuchElementException.class);
      } finally {
        cursor.close();
      }
    });
  }

  /**
   * {@code getRecord()} and {@code getKeys()} must describe the entry {@code next()} just returned. The old
   * implementation peeked at the NOT yet consumed value instead, so a caller reading {@code getRecord()} after
   * {@code next()} saw the following row - or null on the last one of a key group.
   */
  @Test
  void getRecordAndGetKeysDescribeTheLastReturnedEntry() {
    final RangeIndex index = createTypeWithTombstoneRunsAtBothEnds();
    database.transaction(() -> {
      final IndexCursor cursor = index.range(true, new Object[] { 0 }, true, new Object[] { TOTAL }, true);
      try {
        int expectedId = SURVIVOR_FROM;
        while (cursor.hasNext()) {
          final Identifiable returned = cursor.next();
          assertThat(cursor.getRecord()).isEqualTo(returned);
          assertThat(cursor.getKeys()).containsExactly(expectedId);
          assertThat(((Document) returned.getRecord()).getInteger("id")).isEqualTo(expectedId);
          ++expectedId;
        }
        assertThat(expectedId).isEqualTo(SURVIVOR_TO);
      } finally {
        cursor.close();
      }
    });
  }

  /**
   * MIN/MAX read the key off the cursor after {@code next()}. With the old contract the trailing null still moved the
   * cursor, so the key left behind was a deleted one: {@code SELECT max(id)} answered with a tombstoned id.
   */
  @Test
  void minAndMaxSkipTheTombstoneRuns() {
    createTypeWithTombstoneRunsAtBothEnds();
    database.transaction(() -> {
      assertThat(database.query("sql", "SELECT min(id) AS m FROM " + TYPE_NAME).next().<Integer>getProperty("m"))
          .isEqualTo(SURVIVOR_FROM);
      assertThat(database.query("sql", "SELECT max(id) AS m FROM " + TYPE_NAME).next().<Integer>getProperty("m"))
          .isEqualTo(SURVIVOR_TO - 1);
    });
  }

  /**
   * The same shape through the SQL pipeline: an indexed range whose scan ends inside a tombstone run must not produce a
   * phantom row. {@code FetchFromIndexStep} pairs {@code cursor.getKeys()} with whatever {@code next()} answered, so a
   * null became an index entry with a null {@code rid}.
   */
  @Test
  void indexedRangeQueryReturnsOnlyLiveRecords() {
    createTypeWithTombstoneRunsAtBothEnds();
    database.transaction(() -> {
      final List<Integer> ids = new ArrayList<>();
      database.query("sql", "SELECT id FROM " + TYPE_NAME + " WHERE id >= 0 AND id <= " + TOTAL + " ORDER BY id")
          .forEachRemaining(r -> ids.add(r.getProperty("id")));
      assertThat(ids).hasSize(SURVIVORS).doesNotContainNull();
      assertThat(ids.getFirst()).isEqualTo(SURVIVOR_FROM);
      assertThat(ids.getLast()).isEqualTo(SURVIVOR_TO - 1);
    });
  }

  private RangeIndex createTypeWithTombstoneRunsAtBothEnds() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("id", Integer.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(64 * 1024).create();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL; ++i)
        database.newDocument(TYPE_NAME).set("id", i).save();
    });

    // collect the victims with a bucket scan (no index range scan) and delete them
    final List<RID> toDelete = new ArrayList<>(TOTAL);
    database.transaction(() -> {
      for (final Iterator<Record> it = database.iterateType(TYPE_NAME, false); it.hasNext(); ) {
        final Document doc = (Document) it.next();
        final int id = doc.getInteger("id");
        if (id < SURVIVOR_FROM || id >= SURVIVOR_TO)
          toDelete.add(doc.getIdentity());
      }
    });
    database.transaction(() -> {
      for (final RID rid : toDelete)
        database.deleteRecord(rid.getRecord());
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties("id");
    // the single bucket-level index: its cursors are bare LSMTreeIndexCursor, not wrapped in a MultiIndexCursor
    return (RangeIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static void assertNoNullElement(final IndexCursor cursor) {
    try {
      int found = 0;
      // the for-each is the point: IndexCursor is an Iterable<Identifiable>, so this is the contract under test
      for (final Identifiable entry : cursor) {
        assertThat(entry).as("iterating an index cursor must never yield a null element").isNotNull();
        ++found;
      }
      assertThat(found).isEqualTo(SURVIVORS);
    } finally {
      cursor.close();
    }
  }

  private static int countEveryNextCall(final IndexCursor cursor) {
    try {
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ++count;
      }
      return count;
    } finally {
      cursor.close();
    }
  }
}
