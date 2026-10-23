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

import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10637: since #10617 a record whose slot is smaller than a placeholder pointer (an empty or near-empty
 * document, 6-8 bytes) was refused when it outgrew a page with no spare bytes, and a retry met the same page. The
 * bytes the pointer misses now come from another plain record of the page, which moves behind a pointer of its own:
 * both records keep their RIDs and their content, and the page stays consistent.
 */
class Issue10637SmallSlotBorrowsBytesFromANeighbourTest extends BucketPageLayoutTestSupport {
  private static final String BIG = "b".repeat(20 * 1024);

  /**
   * A sealed page holding, in this order, the small slot, a short neighbour and 8 KB fillers. The smallest record that
   * can release the bytes is the short neighbour, so it moves off the page and every filler after it slides left: all
   * of them must still read back, and the database must check out clean.
   */
  @Test
  void theSmallestRecordThatCanReleaseTheBytesMovesBehindAPointer() {
    final RID[] empty = new RID[1];
    final RID[] neighbour = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Donor10637", 1).createProperty("v", Type.STRING);
      empty[0] = database.newDocument("Donor10637").save().getIdentity();
      neighbour[0] = database.newDocument("Donor10637").set("v", "a short neighbour").save().getIdentity();
    });
    final RID sealer = sealFirstPage("Donor10637");
    final List<RID> fillers = fillersOfPageZero(neighbour[0], sealer);

    assertThat((Long) bucketStats("Donor10637").get("totalPlaceholderRecords")).as("the premise: no pointer yet")
        .isZero();
    final long countBefore = countRecordsFromCounter("Donor10637");

    database.transaction(() -> empty[0].asDocument(true).modify().set("v", BIG).save());

    assertThat((Long) bucketStats("Donor10637").get("totalPlaceholderRecords"))
        .as("the grown record and the donor both live behind a pointer now").isEqualTo(2L);
    assertThat(countRecordsFromCounter("Donor10637")).isEqualTo(countBefore);
    assertThat(countRecords("Donor10637")).as("a scan must return every record the counter counts, once")
        .isEqualTo(countBefore);
    checkDatabase();

    database.transaction(() -> {
      assertThat(empty[0].asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("a short neighbour");
      for (final RID filler : fillers)
        assertThat(filler.asDocument(true).getString("v")).isEqualTo("f".repeat(8 * 1024));
      assertThat(sealer.asDocument(true).getString("v")).isEqualTo("s".repeat(70 * 1024));
    });
  }

  /**
   * The relocated donor is an ordinary pointer record from then on: it survives a reopen, it can be updated and it
   * can be deleted, and none of that disturbs the record it gave the bytes to.
   */
  @Test
  void theRelocatedDonorStaysAnOrdinaryRecord() {
    final RID[] empty = new RID[1];
    final RID[] neighbour = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Ordinary10637", 1).createProperty("v", Type.STRING);
      empty[0] = database.newDocument("Ordinary10637").save().getIdentity();
      neighbour[0] = database.newDocument("Ordinary10637").set("v", "a short neighbour").save().getIdentity();
    });
    sealFirstPage("Ordinary10637");
    final long countBefore = countRecordsFromCounter("Ordinary10637");

    database.transaction(() -> empty[0].asDocument(true).modify().set("v", BIG).save());

    reopenDatabase();
    database.transaction(() -> {
      assertThat(database.lookupByRID(neighbour[0], true).asDocument().getString("v")).isEqualTo("a short neighbour");
      assertThat(database.lookupByRID(empty[0], true).asDocument().getString("v")).isEqualTo(BIG);
    });

    database.transaction(() -> database.lookupByRID(neighbour[0], true).asDocument().modify().set("v", "updated after the move").save());
    database.transaction(() -> assertThat(database.lookupByRID(neighbour[0], true).asDocument().getString("v")).isEqualTo("updated after the move"));
    checkDatabase();

    database.transaction(() -> database.deleteRecord(database.lookupByRID(neighbour[0], true)));
    assertThat(countRecordsFromCounter("Ordinary10637")).isEqualTo(countBefore - 1);
    assertThat(countRecords("Ordinary10637")).isEqualTo(countBefore - 1);
    checkDatabase();
    database.transaction(() -> assertThat(database.lookupByRID(empty[0], true).asDocument().getString("v")).isEqualTo(BIG));
  }

  /**
   * Several small slots on the same sealed page each borrow the bytes they miss, one donor apiece, inside ONE
   * transaction: the offsets every relocation moves have to be the ones the next one reads.
   */
  @Test
  void severalSmallSlotsOfOnePageGrowInOneTransaction() {
    final List<RID> empties = new ArrayList<>();
    final RID[] neighbour = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Several10637", 1).createProperty("v", Type.STRING);
      for (int i = 0; i < 3; i++)
        empties.add(database.newDocument("Several10637").save().getIdentity());
      neighbour[0] = database.newDocument("Several10637").set("v", "a short neighbour").save().getIdentity();
    });
    sealFirstPage("Several10637");
    final long countBefore = countRecordsFromCounter("Several10637");

    database.transaction(() -> {
      for (final RID empty : empties)
        empty.asDocument(true).modify().set("v", BIG + empty.getPosition()).save();
    });

    assertThat(countRecordsFromCounter("Several10637")).isEqualTo(countBefore);
    assertThat(countRecords("Several10637")).isEqualTo(countBefore);
    checkDatabase();
    database.transaction(() -> {
      for (final RID empty : empties)
        assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG + empty.getPosition());
      assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("a short neighbour");
    });
  }

  /** The 8 KB fillers {@code sealFirstPage} put on page 0 between {@code after} and the record that sealed it. */
  private static List<RID> fillersOfPageZero(final RID after, final RID sealer) {
    final List<RID> fillers = new ArrayList<>();
    for (long position = after.getPosition() + 1; position < sealer.getPosition(); position++)
      fillers.add(new RID(after.getBucketId(), position));
    return fillers;
  }
}
