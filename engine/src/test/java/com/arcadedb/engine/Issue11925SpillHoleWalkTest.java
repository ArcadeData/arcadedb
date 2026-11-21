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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11925: vertices written small and grown past their page in a later write of the same transaction (an insert then
 * an update, or an update of small committed vertices) spill into chunk chains, one after the other, out of the same full
 * page. The first spill wrote the head chunk, its next-chunk pointer and any continuation chunk landing on a page that holds
 * records without declaring them hole-free (#9483), so the page lost its "only packed writes" state and every later growth
 * on it walked the page's whole record table looking for holes to pack (#10700): about 1,550 slots per record for 50,000
 * small vertices, 77.5 million in all. The head chunk fills exactly the room its slot had, the pointer is rewritten in place
 * and a continuation chunk is appended at a page's content end, so all three are now declared, and a page that had no hole
 * is not walked again. The cost is measured by the slots walked ({@link LocalBucket#getGrowthHoleWalkSlotsForTesting()}),
 * never by the clock.
 *
 * @author Shaun Patterson (shaunpatterson@gmail.com)
 */
class Issue11925SpillHoleWalkTest extends TestHelper {
  private static final int    RECORDS = 5_000;
  private static final String GROWN   = "a property long enough to make a small record outgrow its full page: ";

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getType("T").getBuckets(false).getFirst();
  }

  private void assertGrown(final int count) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T WHERE name = '" + GROWN + "' + k AND score = k * 2")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(count);
    }
  }

  private long walkedWhile(final Runnable work) {
    final long before = bucket().getGrowthHoleWalkSlotsForTesting();
    work.run();
    return bucket().getGrowthHoleWalkSlotsForTesting() - before;
  }

  @Test
  void insertThenGrowInOneTransactionDoesNotWalkThePagePerRecord() {
    database.getSchema().createVertexType("T", 1);
    final long walked = walkedWhile(() -> database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>(RECORDS);
      for (int i = 0; i < RECORDS; i++)
        vertices.add(database.newVertex("T").set("k", i).save());
      for (final MutableVertex vertex : vertices)
        vertex.set("name", GROWN + vertex.getInteger("k")).set("score", vertex.getInteger("k") * 2).save();
    }));

    // Walking the page for each record that spills reads about RECORDS x the slots of a page
    assertThat(walked).as("record-table slots walked looking for holes").isLessThan(RECORDS);
    assertGrown(RECORDS);
  }

  @Test
  void sqlInsertThenUpdateInOneTransactionDoesNotWalkThePagePerRecord() {
    database.getSchema().createVertexType("T", 1);
    final long walked = walkedWhile(() -> database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.command("sql", "INSERT INTO T SET k = ?", i);
      database.command("sql", "UPDATE T SET name = '" + GROWN + "' + k, score = k * 2");
    }));

    assertThat(walked).as("record-table slots walked looking for holes").isLessThan(RECORDS);
    assertGrown(RECORDS);
  }

  @Test
  void updateOfSmallCommittedRecordsDoesNotWalkThePagePerRecord() {
    database.getSchema().createVertexType("T", 1);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex("T").set("k", i).save();
    });

    final long walked = walkedWhile(
        () -> database.transaction(() -> database.command("sql", "UPDATE T SET name = '" + GROWN + "' + k, score = k * 2")));

    assertThat(walked).as("record-table slots walked looking for holes").isLessThan(RECORDS);
    assertGrown(RECORDS);

    // and the chains read back after a reopen, from what the commit wrote
    reopenDatabase();
    assertGrown(RECORDS);
  }

  @Test
  void aRecordTooSmallForAChunkHeaderStillSpillsBehindAPointerThatLeavesAHole() {
    // A document holding one small integer takes a 12-byte slot, less than a chunk header: it spills behind a 9-byte
    // placeholder pointer, which leaves the other 3 bytes as a hole. That write stays undeclared, so the next growth on the
    // page packs it first (#10700), as before
    database.getSchema().createDocumentType("T", 1);
    final long walked = walkedWhile(() -> database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.command("sql", "INSERT INTO T SET k = ?", i);
      database.command("sql", "UPDATE T SET name = '" + GROWN + "' + k, score = k * 2");
    }));

    assertThat(walked).as("growths after a pointer spill looked for holes").isPositive();
    assertGrown(RECORDS);
  }

  @Test
  void aDeleteOnThePageStillMakesAGrowthLookForHoles() {
    // #10700: bytes the same transaction gave back are holes only a pack can reach, so a growth on that page still walks it
    database.getSchema().createVertexType("T", 1);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids.add(database.newVertex("T").set("k", i).save().getIdentity());
    });
    assertThat(rids.get(1).getPosition()).as("both records on the first, full page").isLessThan(bucket().getMaxRecordsInPage());

    final long walked = walkedWhile(() -> database.transaction(() -> {
      database.deleteRecord(database.lookupByRID(rids.get(0), true));
      database.lookupByRID(rids.get(1), true).asDocument().modify().set("name", GROWN.repeat(40)).save();
    }));

    assertThat(walked).as("the growth after a delete looked for holes").isPositive();
    assertThat(database.existsRecord(rids.get(0))).isFalse();
    assertThat(database.lookupByRID(rids.get(1), true).asDocument().getString("name")).isEqualTo(GROWN.repeat(40));
  }

  /**
   * Every caller of the spill, each in its own transaction, so each commit holds its pages to the declarations: under
   * assertions (surefire's default) {@code compressPageAtCommit} re-proves every page that skips the compression packed and
   * its free-space claim exact. The callers: a record inserted larger than a page (the multi-page branch of the insert), a
   * record inserted and grown past its page in one transaction (the insert spill), a committed record grown past its page
   * (the update spill), a slot too small for a chunk header on a page with room to shift what follows (#6149), and a
   * record grown across several pages.
   */
  @Test
  void everyCallerOfTheSpillLeavesItsPagesPacked() {
    database.getSchema().createDocumentType("T", 1);
    final int pageSize = bucket().getPageSize();
    final Map<RID, String> expected = new HashMap<>();

    // insert larger than a page, and larger than three
    database.transaction(() -> {
      for (final int size : new int[] { pageSize + 100, pageSize * 3 + 7 })
        put(expected, database.newDocument("T"), text(size, size));
    });
    assertRecords(expected);

    // insert, then grow past the page, in one transaction
    database.transaction(() -> {
      final List<MutableDocument> inserted = new ArrayList<>();
      for (int i = 0; i < 20; i++)
        inserted.add(put(expected, database.newDocument("T"), text(i, 20)));
      for (int i = 0; i < inserted.size(); i++)
        put(expected, inserted.get(i), text(i, pageSize / 4));
    });
    assertRecords(expected);

    // tiny committed records on a page with room: the first growth past the page shifts the records that follow to make
    // the slot reach the chunk-header minimum (#6149), the next ones grow committed records past the page and across pages
    final List<RID> tiny = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableDocument document = database.newDocument("T");
        put(expected, document, "t" + i);
        tiny.add(document.getIdentity());
      }
    });
    database.transaction(() -> put(expected, modify(tiny.get(3)), text(3, pageSize + 50)));
    database.transaction(() -> put(expected, modify(tiny.get(5)), text(5, pageSize / 2)));
    database.transaction(() -> put(expected, modify(tiny.get(7)), text(7, pageSize * 4)));
    assertRecords(expected);

    reopenDatabase();
    assertRecords(expected);
    checkDatabaseIntegrity();
  }

  /**
   * A seeded mix of inserts, growths that stay in the page, growths past the page and across pages, shrinks and deletes,
   * many to a transaction: every commit is held to the declarations under assertions, every record reads back what was last
   * written, after each commit and after a reopen, and the database checks out. Every other transaction only inserts and
   * grows, so its pages keep the declarations to the commit instead of losing them to a shrink or a delete.
   */
  @Test
  void aMixOfInsertsGrowthsShrinksAndDeletesKeepsEveryPageConsistent() {
    database.getSchema().createDocumentType("T", 1);
    final int pageSize = bucket().getPageSize();
    final Random random = new Random(11925);
    final Map<RID, String> expected = new HashMap<>();
    final List<RID> live = new ArrayList<>();

    for (int transaction = 0; transaction < 30; transaction++) {
      final int kinds = transaction % 2 == 0 ? 4 : 6;
      database.transaction(() -> {
        for (int operation = 0; operation < 60; operation++) {
          final int kind = live.isEmpty() ? 0 : random.nextInt(kinds);
          if (kind <= 1) {
            final MutableDocument document = database.newDocument("T");
            put(expected, document, text(random.nextInt(), 1 + random.nextInt(40)));
            live.add(document.getIdentity());
            continue;
          }
          final int index = random.nextInt(live.size());
          final RID rid = live.get(index);
          final MutableDocument document = modify(rid);
          switch (kind) {
          case 2 -> put(expected, document, text(random.nextInt(), 40 + random.nextInt(400)));
          case 3 -> put(expected, document, text(random.nextInt(), pageSize / 2 + random.nextInt(pageSize * 2)));
          case 4 -> put(expected, document, text(random.nextInt(), 1 + random.nextInt(8)));
          default -> {
            document.delete();
            expected.remove(rid);
            live.set(index, live.getLast());
            live.removeLast();
          }
          }
        }
      });
      assertRecords(expected);
    }

    reopenDatabase();
    assertRecords(expected);
    checkDatabaseIntegrity();
  }

  /**
   * Two transactions spill disjoint committed records of the same pages and commit side by side, so the later commit's pages
   * go through the disjoint-slot merge (#6129) or a retry. A rebased page is compressed in full by the rebase itself (#5608),
   * so a declaration never lets a replayed page skip the compression; either way every record reads back what its own
   * transaction wrote.
   */
  @Test
  void concurrentCommitsSpillingDisjointRecordsOfTheSamePagesKeepEveryRecord() throws Exception {
    database.getSchema().createDocumentType("T", 1);
    final int pageSize = bucket().getPageSize();
    final Map<RID, String> expected = new HashMap<>();
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 400; i++) {
        final MutableDocument document = database.newDocument("T");
        put(expected, document, text(i, 30));
        rids.add(document.getIdentity());
      }
    });

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      for (int r = 0; r < 4; r++) {
        final int round = r;
        final int size = pageSize / 8 + round * 100;

        // THIS THREAD GROWS THE EVEN RECORDS PAST THEIR PAGE, NOT COMMITTED YET
        database.begin();
        final Map<RID, String> mine = growEveryOther(rids, 0, size, round);

        // THE OTHER THREAD GROWS THE ODD ONES OF THE SAME PAGES AND COMMITS FIRST
        final Future<Map<RID, String>> theirs = other.submit(() -> {
          final Map<RID, String> written = new HashMap<>();
          database.transaction(() -> {
            written.clear();
            written.putAll(growEveryOther(rids, 1, size, round));
          }, false, 20);
          return written;
        });
        expected.putAll(theirs.get(120, TimeUnit.SECONDS));

        try {
          database.commit();
          expected.putAll(mine);
        } catch (final ConcurrentModificationException e) {
          if (database.isTransactionActive())
            database.rollback();
          final Map<RID, String> retried = new HashMap<>();
          database.transaction(() -> {
            retried.clear();
            retried.putAll(growEveryOther(rids, 0, size, round));
          }, false, 20);
          expected.putAll(retried);
        }
        assertRecords(expected);
      }
    } finally {
      other.shutdownNow();
    }

    reopenDatabase();
    assertRecords(expected);
    checkDatabaseIntegrity();
  }

  private Map<RID, String> growEveryOther(final List<RID> rids, final int first, final int size, final int round) {
    final Map<RID, String> written = new HashMap<>();
    for (int i = first; i < rids.size(); i += 2) {
      final String value = text(i * 31 + round, size);
      modify(rids.get(i)).set("v", value).save();
      written.put(rids.get(i), value);
    }
    return written;
  }

  private static MutableDocument put(final Map<RID, String> expected, final MutableDocument document, final String value) {
    document.set("v", value).save();
    expected.put(document.getIdentity(), value);
    return document;
  }

  /** {@code length} characters that do not repeat with a short period, derived from {@code seed}. */
  private static String text(final int seed, final int length) {
    final Random random = new Random(seed);
    final char[] chars = new char[length];
    for (int i = 0; i < length; i++)
      chars[i] = (char) ('a' + random.nextInt(26));
    return new String(chars);
  }

  /** Through the database open now: a RID keeps the instance it was created with, which a reopen closes. */
  private MutableDocument modify(final RID rid) {
    return database.lookupByRID(rid, true).asDocument().modify();
  }

  private void assertRecords(final Map<RID, String> expected) {
    database.transaction(() -> {
      assertThat(database.countType("T", false)).isEqualTo(expected.size());
      for (final Map.Entry<RID, String> entry : expected.entrySet()) {
        final Document document = database.lookupByRID(entry.getKey(), true).asDocument();
        assertThat(document.getString("v")).as("record %s", entry.getKey()).isEqualTo(entry.getValue());
      }
    });
  }
}
