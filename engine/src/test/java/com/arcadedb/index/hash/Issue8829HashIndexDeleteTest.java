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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8829: deleting records of a type with a NOTUNIQUE_HASH index failed at commit once a key held a
 * few dozen records, because removing one RID from a long entry rewrote the whole entry at the end of the bucket page
 * and, when it did not fit, compacted a page that still held the old entry, so the rewrite could run past the page.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8829HashIndexDeleteTest extends TestHelper {

  @Test
  void deleteFromLongEntriesCommits() {
    deleteAndVerify(30_000, 100, 10);
  }

  @Test
  void deleteFromShortEntriesCommits() {
    deleteAndVerify(30_000, 10_000, 10);
  }

  @Test
  void deleteEverythingLeavesAnEmptyIndex() {
    deleteAndVerify(6_000, 20, 100);
  }

  private void deleteAndVerify(final int records, final int keys, final int deletePercent) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE H");
      database.command("sql", "CREATE PROPERTY H.g LONG");
      database.command("sql", "CREATE INDEX ON H (g) NOTUNIQUE_HASH");
    });

    final Random rnd = new Random(71);
    final List<RID> rids = new ArrayList<>();
    final int[] expected = new int[keys];
    database.begin();
    for (int i = 0; i < records; i++) {
      final int g = rnd.nextInt(keys);
      final var doc = database.newDocument("H").set("g", (long) g);
      doc.save();
      rids.add(doc.getIdentity());
      expected[g]++;
      if ((i + 1) % 1000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();

    final List<RID> shuffled = new ArrayList<>(rids);
    Collections.shuffle(shuffled, rnd);
    final int toDelete = (int) ((long) records * deletePercent / 100);

    database.begin();
    for (int i = 0; i < toDelete; i++) {
      final RID rid = shuffled.get(i);
      final int g = ((Number) rid.asDocument().get("g")).intValue();
      rid.asDocument().delete();
      expected[g]--;
      if ((i + 1) % 1000 == 0 || i == toDelete - 1) {
        database.commit();
        database.begin();
      }
    }
    database.commit();

    for (int g = 0; g < Math.min(keys, 50); g++) {
      assertThat(count("SELECT count(*) AS c FROM H WHERE g = " + g)).as("index count for g=" + g).isEqualTo(expected[g]);
      assertThat(count("SELECT count(*) AS c FROM H WHERE g + 0 = " + g)).as("scan count for g=" + g).isEqualTo(expected[g]);
    }
    assertThat(count("SELECT count(*) AS c FROM H")).isEqualTo(records - toDelete);

    // the buckets now hold dead space: inserting into them again must reclaim it and stay consistent
    database.transaction(() -> {
      for (int i = 0; i < records / 2; i++)
        database.newDocument("H").set("g", (long) (i % keys)).save();
    });
    for (int g = 0; g < Math.min(keys, 50); g++) {
      final int extra = records / 2 / keys + (g < (records / 2) % keys ? 1 : 0);
      assertThat(count("SELECT count(*) AS c FROM H WHERE g = " + g)).as("index count after reinsert for g=" + g)
          .isEqualTo(expected[g] + extra);
      assertThat(count("SELECT count(*) AS c FROM H WHERE g + 0 = " + g)).as("scan count after reinsert for g=" + g)
          .isEqualTo(expected[g] + extra);
    }
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
