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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8333: the records an index range matched are loaded through their bucket, by the sorted positions, instead of
 * one {@code lookupByRID()} each: every page is read once however many of the positions it holds, and the records are
 * built in batches exactly as a scan builds them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketPositionsIteratorTest extends TestHelper {
  private static final int RECORDS = 3_000;

  private final List<RID> rids = new ArrayList<>();
  private       LocalBucket bucket;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Item");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids.add(database.newDocument("Item").set("id", i).save().getIdentity());
    });
    bucket = (LocalBucket) database.getSchema().getType("Item").getBuckets(false).getFirst();
  }

  @Test
  void answersTheRecordsAtThePositionsInTheirOrder() {
    final long[] positions = positionsOf(0, RECORDS, 3);
    assertThat(ids(bucket.iterator(positions, 0, positions.length))).isEqualTo(idsAt(positions));

    // A slice of the array, as a unit of a parallel load reads it
    assertThat(ids(bucket.iterator(positions, 100, 200))).isEqualTo(idsAt(Arrays.copyOfRange(positions, 100, 200)));
    assertThat(ids(bucket.iterator(positions, 5, 5))).isEmpty();
  }

  @Test
  void readsEveryPageOnce() {
    final long[] positions = positionsOf(0, RECORDS, 1);
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    // WARM-UP PASS: LOADS EVERY PAGE INTO THE CACHE, SO THE MEASURED PASS BELOW COUNTS ONLY CACHE HITS
    assertThat(ids(bucket.iterator(positions, 0, positions.length))).hasSize(RECORDS);

    final long hitsBefore = pageManager.getStats().cacheHits;
    assertThat(ids(bucket.iterator(positions, 0, positions.length))).hasSize(RECORDS);
    final long hits = pageManager.getStats().cacheHits - hitsBefore;

    // A FEW PAGES HOLD 3,000 SMALL RECORDS: ONE LOOKUP PER RECORD WOULD BE >= RECORDS
    assertThat(hits).as("page-cache lookups to load %d records by position", RECORDS).isLessThan(RECORDS / 10);
  }

  @Test
  void duplicatesAreAnsweredOnceEach() {
    // AN INDEX CAN RETURN ONE RECORD ONCE PER MATCHING KEY (A MULTI-VALUE INDEX): EVERY OCCURRENCE IS AN ANSWER
    final long p = rids.get(10).getPosition();
    final long[] positions = { p, p, rids.get(11).getPosition() };
    assertThat(ids(bucket.iterator(positions, 0, positions.length))).containsExactly(10, 10, 11);
  }

  @Test
  void deletedMovedAndMultiPageRecordsAreResolvedLikeAScanResolvesThem() {
    final List<Integer> expected = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 600; i++) {
        if (i % 7 == 0)
          rids.get(i).asDocument().delete();
        else {
          if (i % 11 == 0) {
            // GROWS PAST ITS SLOT: MOVES TO A PLACEHOLDER, AND THE 1 IN 3 THAT GETS 200KB SPANS MULTIPLE PAGES
            final MutableDocument doc = rids.get(i).asDocument().modify();
            doc.set("payload", "x".repeat(i % 3 == 0 ? 200_000 : 2_000));
            doc.save();
          }
          expected.add(i);
        }
      }
    });

    final long[] positions = positionsOf(0, 600, 1);
    final List<Record> records = new ArrayList<>();
    bucket.iterator(positions, 0, positions.length).forEachRemaining(records::add);
    assertThat(records.stream().map(r -> ((Document) r).getInteger("id")).toList()).isEqualTo(expected);
    // THE PLACEHOLDER CONTENT AND THE CHUNKS OF A MULTI-PAGE RECORD ARE NOT RECORDS OF THEIR OWN
    for (final Record record : records)
      assertThat(record.getIdentity().getPosition()).isIn(positionsList(positions));
    assertThat(((Document) records.get(expected.indexOf(33))).getString("payload")).hasSize(200_000);
  }

  @Test
  void positionsPastTheBucketOrItsPagesAreSkipped() {
    final long last = rids.getLast().getPosition();
    final long[] positions = { rids.getFirst().getPosition(), last + 1, last + 1_000_000_000L };
    assertThat(ids(bucket.iterator(positions, 0, positions.length))).containsExactly(0);
  }

  @Test
  void aTransactionSeesItsOwnChanges() {
    database.transaction(() -> {
      rids.get(1).asDocument().delete();
      final MutableDocument updated = rids.get(2).asDocument().modify();
      updated.set("id", -2).save();
      final RID created = database.newDocument("Item").set("id", -3).save().getIdentity();

      final long[] positions = { rids.get(0).getPosition(), rids.get(1).getPosition(), rids.get(2).getPosition(),
          created.getPosition() };
      assertThat(ids(bucket.iterator(positions, 0, positions.length))).containsExactly(0, -2, -3);
    });
  }

  private long[] positionsOf(final int from, final int to, final int step) {
    final List<Long> positions = new ArrayList<>();
    for (int i = from; i < to; i += step)
      positions.add(rids.get(i).getPosition());
    final long[] result = new long[positions.size()];
    for (int i = 0; i < result.length; i++)
      result[i] = positions.get(i);
    Arrays.sort(result);
    return result;
  }

  private List<Integer> idsAt(final long[] positions) {
    final List<Integer> ids = new ArrayList<>();
    for (final long position : positions)
      ids.add(new RID(bucket.getFileId(), position).asDocument().getInteger("id"));
    return ids;
  }

  private static List<Long> positionsList(final long[] positions) {
    final List<Long> list = new ArrayList<>();
    for (final long p : positions)
      list.add(p);
    return list;
  }

  private static List<Integer> ids(final Iterator<Record> iterator) {
    final List<Integer> ids = new ArrayList<>();
    while (iterator.hasNext())
      ids.add(((Document) iterator.next()).getInteger("id"));
    return ids;
  }
}
