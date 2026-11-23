/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LSMTreeIndexPostings} answers what {@link LSMTreeIndex#get(Object[])} answers for a key, as primitive arrays sorted by RID
 * (issue #12458).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexPostingsTest {

  /** The resolution rules, on values appended as a walk meets them: newest first, a page's values oldest first. */
  @Nested
  class Resolution {
    @Test
    void aDeletionMarkerRemovesOnlyTheOlderValuesOfItsRid() {
      final LSMTreeIndexPostings postings = new LSMTreeIndexPostings();
      // newest page: the re-added value; older page: its deletion marker, then the original value
      page(postings, new long[][] { { 3, 7, 2, 40 } });
      page(postings, new long[][] { { 3, 7, 1, 30 }, { marker(3), 7, 0, 0 } });
      // RID #3:9 deleted after it was added
      page(postings, new long[][] { { 3, 9, 1, 10 } });
      postings.resolve();
      assertThat(postings.size()).isEqualTo(2);
      assertThat(postings.positions()[0]).isEqualTo(7L);
      assertThat(postings.termFrequencies()[0]).as("the newest value of the RID is the one kept").isEqualTo(2);
      assertThat(postings.documentLengths()[0]).isEqualTo(40);
      assertThat(postings.positions()[1]).isEqualTo(9L);

      final LSMTreeIndexPostings deleted = new LSMTreeIndexPostings();
      page(deleted, new long[][] { { 3, 9, 1, 10 }, { marker(3), 9, 0, 0 } });
      deleted.resolve();
      assertThat(deleted.size()).as("a marker newer than the value removes it").isZero();
    }

    @Test
    void ofSeveralLiveValuesOfOneRidTheNewestIsKept() {
      final LSMTreeIndexPostings postings = new LSMTreeIndexPostings();
      page(postings, new long[][] { { 2, 5, 1, 11 }, { 2, 5, 4, 44 } });
      postings.resolve();
      assertThat(postings.size()).isEqualTo(1);
      assertThat(postings.termFrequencies()[0]).isEqualTo(4);
    }

    @Test
    void aKeyWideMarkerEndsTheWalk() {
      final LSMTreeIndexPostings postings = new LSMTreeIndexPostings();
      // on the page, oldest first: a value older than the marker, the marker, a newer value
      final boolean goOn = page(postings, new long[][] { { 1, 4, 1, 1 }, { -1, -1, 0, 0 }, { 1, 6, 1, 1 } });
      assertThat(goOn).as("the walk stops at a key-wide marker").isFalse();
      postings.resolve();
      assertThat(postings.size()).isEqualTo(1);
      assertThat(postings.positions()[0]).isEqualTo(6L);
    }

    @Test
    void anyOrderIsSortedStablyByRid() {
      final Random random = new Random(7);
      for (final int size : new int[] { 1, 2, 5, 50, 3_000, 40_000 }) {
        for (final boolean mixedBuckets : new boolean[] { false, true }) {
          final LSMTreeIndexPostings postings = new LSMTreeIndexPostings();
          final List<long[]> walk = new ArrayList<>();
          for (int i = 0; i < size; ++i) {
            final int bucket = mixedBuckets ? random.nextInt(3) : 4;
            final long position = random.nextInt(Math.max(1, size / 2)) * 3L + (size > 1_000 ? 1L << 33 : 0L);
            // each live value carries its walk index as tf: the first one met of a RID is the one kept
            final boolean isMarker = random.nextInt(10) == 0;
            walk.add(new long[] { isMarker ? marker(bucket) : bucket, position, i + 1, 0 });
            postings.addRaw(isMarker ? marker(bucket) : bucket, position, i + 1, 0);
          }
          postings.resolve();

          // the reference: the first value met of each RID decides, sorted by RID
          final TreeSet<long[]> expected = new TreeSet<>(Comparator.<long[]>comparingLong(v -> v[0]).thenComparingLong(v -> v[1]));
          final TreeSet<String> decided = new TreeSet<>();
          for (final long[] v : walk) {
            final long bucket = v[0] >= 0 ? v[0] : -v[0] - 2;
            if (decided.add(bucket + ":" + v[1]) && v[0] >= 0)
              expected.add(new long[] { bucket, v[1], v[2] });
          }
          assertThat(postings.size()).as("size %d, mixed %s", size, mixedBuckets).isEqualTo(expected.size());
          int i = 0;
          for (final long[] v : expected) {
            assertThat(postings.bucketIds()[i]).isEqualTo((int) v[0]);
            assertThat(postings.positions()[i]).isEqualTo(v[1]);
            assertThat(postings.termFrequencies()[i]).isEqualTo((int) v[2]);
            ++i;
          }
        }
      }
    }

    @Test
    void duplicatesOnSeveralPagesFollowTheWalkOrder() {
      final Random random = new Random(9);
      for (final int pages : new int[] { 1, 3, 40 }) {
        final LSMTreeIndexPostings postings = new LSMTreeIndexPostings();
        // the walk: pages newest first, each page's values newest first, which is its stored order reversed
        final List<long[]> walk = new ArrayList<>();
        int sequence = 0;
        for (int p = 0; p < pages; ++p) {
          final int count = 1 + random.nextInt(30);
          final long[][] values = new long[count][];
          for (int v = 0; v < count; ++v) {
            final boolean isMarker = random.nextInt(6) == 0;
            values[v] = new long[] { isMarker ? marker(2) : 2, random.nextInt(25), ++sequence, 0 };
          }
          for (int v = count - 1; v >= 0; --v)
            walk.add(values[v]);
          page(postings, values);
        }
        postings.resolve();

        final TreeSet<Long> decided = new TreeSet<>();
        final TreeSet<long[]> expected = new TreeSet<>(Comparator.comparingLong(v -> v[1]));
        for (final long[] v : walk)
          if (decided.add(v[1]) && v[0] >= 0)
            expected.add(v);
        assertThat(postings.size()).as("%d pages", pages).isEqualTo(expected.size());
        int i = 0;
        for (final long[] v : expected) {
          assertThat(postings.positions()[i]).isEqualTo(v[1]);
          assertThat(postings.termFrequencies()[i]).as("the value met first in walk order").isEqualTo((int) v[2]);
          ++i;
        }
      }
    }

    /** Appends one page's values, given oldest first as a page holds them. */
    private boolean page(final LSMTreeIndexPostings postings, final long[][] values) {
      postings.startPage();
      for (final long[] v : values)
        postings.addRaw((int) v[0], v[1], (int) v[2], (int) v[3]);
      return postings.endPage();
    }

    private int marker(final int bucketId) {
      return -(bucketId + 2);
    }
  }

  /** The same index read both ways, across transactions, deletions, updates and a compaction. */
  @Nested
  class AgainstGet extends TestHelper {
    private static final String TYPE = "Item";
    private static final int    KEYS = 12;

    @Override
    protected void beginTest() {
      // the test compacts the index itself
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    }

    @Test
    void getPostingsAnswersWhatGetAnswers() throws Exception {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
      type.createProperty("key", String.class);
      // small pages, so a key spans many of them
      final TypeIndex typeIndex = database.getSchema().buildTypeIndex(TYPE, new String[] { "key" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(2048).create();
      final LSMTreeIndex index = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

      final Random random = new Random(3);
      for (int round = 0; round < 8; ++round) {
        database.transaction(() -> {
          for (int i = 0; i < 300; ++i)
            database.newDocument(TYPE).set("key", "k" + random.nextInt(KEYS)).save();
        });
        // deletions and key changes in a later transaction leave deletion markers in front of older values
        database.transaction(() -> {
          for (final Iterator<Record> it = database.iterateType(TYPE, false); it.hasNext(); ) {
            final Document doc = (Document) it.next();
            final int pick = random.nextInt(8);
            if (pick == 0)
              doc.delete();
            else if (pick == 1)
              doc.modify().set("key", "k" + random.nextInt(KEYS)).save();
          }
        });
        if (round == 4) {
          assertThat(typeIndex.scheduleCompaction()).isTrue();
          assertThat(typeIndex.compact()).isTrue();
        }
        assertSameValues(index);
      }

      // uncommitted changes of the transaction are seen too
      database.transaction(() -> {
        for (int i = 0; i < 20; ++i)
          database.newDocument(TYPE).set("key", "k" + random.nextInt(KEYS)).save();
        final Iterator<Record> it = database.iterateType(TYPE, false);
        it.next().delete();
        assertSameValues(index);
      });
    }

    private void assertSameValues(final Index index) {
      for (int k = 0; k < KEYS; ++k) {
        final Object[] key = { "k" + k };
        final TreeSet<RID> expected = new TreeSet<>();
        try (final IndexCursor cursor = index.get(key)) {
          while (cursor.hasNext())
            expected.add(cursor.next().getIdentity());
        }
        final LSMTreeIndexPostings postings = ((LSMTreeIndex) index).getPostings(key);
        final List<RID> actual = new ArrayList<>();
        for (int i = 0; i < postings.size(); ++i)
          actual.add(new RID(postings.bucketIds()[i], postings.positions()[i]));
        assertThat(actual).as("values of key %s", key[0]).containsExactlyElementsOf(expected);
      }
    }
  }
}
