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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
}
