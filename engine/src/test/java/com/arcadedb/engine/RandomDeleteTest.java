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
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class RandomDeleteTest extends TestHelper {
  private final static int    TOT_RECORDS = 100_000;
  private final static String TYPE        = "Product";
  private static final int    CYCLES      = 3;

  @Test
  @Tag("slow")
  void smallRecords() {
    final Database db = database;

    db.getSchema().createVertexType(TYPE, 1);

    final List<RID> rids = new ArrayList<>(TOT_RECORDS);
    db.transaction(() -> {
      insert(db, rids);
      assertThat(db.countType(TYPE, true)).isEqualTo(TOT_RECORDS);

      // DELETE FROM 1 TO N
      for (int i = 0; i < TOT_RECORDS; i++)
        db.deleteRecord(rids.get(i).asVertex());

      assertThat(db.countType(TYPE, true)).isEqualTo(0);
    });

    db.transaction(() -> {
      // DELETE RANDOMLY X TIMES
      for (int cycle = 0; cycle < CYCLES; cycle++) {
        insert(db, rids);
        checkRecords(db, rids);

        for (int deleted = 0; deleted < TOT_RECORDS; ) {
          final int i = ThreadLocalRandom.current().nextInt(TOT_RECORDS);
          final RID rid = rids.get(i);
          if (rid != null) {
            db.deleteRecord(rid.asVertex());
            rids.set(i, null);
            ++deleted;
          }
        }
      }

      assertThat(db.countType(TYPE, true)).isEqualTo(0);

    });
  }

  private void checkRecords(Database db, List<RID> rids) {
    for (int i = 0; i < rids.size(); i++)
      assertThat(rids.get(i).asVertex()).isNotNull();

    final List<RID> found = new ArrayList<>();
    for (Iterator<Record> it = db.iterateType(TYPE, true); it.hasNext(); )
      found.add(it.next().asVertex().getIdentity());

    // The scan must return exactly the records that were inserted - every one of them, once each, and nothing else.
    // Compared as a SET (both sides sorted by position, which also catches a record handed out twice) and not in the
    // order they were inserted, because that order is not a property of anything: a scan walks a bucket in PHYSICAL
    // order, a RID is a physical position, and a record inserted into space a delete gave back lands wherever that
    // space is rather than after the last record written.
    //
    // Until #6339 the two orders did coincide, and only because the allocator could not see the space this test's own
    // mass delete had freed: the free-space statistics were never told, so every re-insert appended to the end of the
    // bucket and the bucket grew by another 100k slots per cycle. Asserting insertion order here was therefore
    // asserting that nothing was reused - the defect - so the order goes and the identity of the records stays.
    final List<RID> foundByPosition = new ArrayList<>(found);
    final List<RID> insertedByPosition = new ArrayList<>(rids);
    foundByPosition.sort(Comparator.comparingLong(RID::getPosition));
    insertedByPosition.sort(Comparator.comparingLong(RID::getPosition));
    assertThat(foundByPosition).hasSameSizeAs(insertedByPosition);
    assertThat(foundByPosition).isEqualTo(insertedByPosition);

    assertThat(db.countType(TYPE, true)).isEqualTo(rids.size());
  }

  private static void insert(final Database db, final List<RID> rids) {
    rids.clear();
    for (int i = 0; i < TOT_RECORDS; i++) {
      final MutableVertex v = db.newVertex(TYPE)//
          .set("id", i)//
          .save();

      rids.add(v.getIdentity());
    }
  }
}
