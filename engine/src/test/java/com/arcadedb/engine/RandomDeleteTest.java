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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class RandomDeleteTest {
  private final static int    TOT_RECORDS = 100_000;
  private final static String TYPE        = "Product";
  private static final int    CYCLES      = 3;

  @Test
  @Tag("slow")
  void smallRecords() {
    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/randomDeleteTest")) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      try (Database db = databaseFactory.create()) {
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
      } finally {
        if (databaseFactory.exists())
          databaseFactory.open().drop();
      }
    }
  }

  private void checkRecords(Database db, List<RID> rids) {
    for (int i = 0; i < rids.size(); i++)
      assertThat(rids.get(i).asVertex()).isNotNull();

    final List<RID> found = new ArrayList<>();
    for (Iterator<Record> it = db.iterateType(TYPE, true); it.hasNext(); )
      found.add(it.next().asVertex().getIdentity());

    assertThat(found).isEqualTo(rids);

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
