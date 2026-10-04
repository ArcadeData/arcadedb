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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9034: after hundreds of DELETE FROM and insert cycles an equality lookup on a NOTUNIQUE_HASH key threw
 * "newPosition > limit": a compressed RID that ends a few bytes before the end of its page was read through a fixed 20 byte
 * window.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue9034HashLookupAfterDeleteInsertCyclesTest extends TestHelper {

  @Test
  void lookupSurvivesDeleteInsertCycles() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE H").close();
      database.command("sql", "CREATE PROPERTY H.id INTEGER").close();
      database.command("sql", "CREATE PROPERTY H.b BOOLEAN").close();
      database.command("sql", "CREATE INDEX ON H (b) NOTUNIQUE_HASH").close();
    });

    // seeds 1 to 4 reached the failure at cycles 2154, 2616, 1806 and 1215 on the reporter's build
    for (int seed = 1; seed <= 4; seed++) {
      final Random random = new Random(seed);
      for (int cycle = 1; cycle <= 3000; cycle++) {
        database.transaction(() -> database.command("sql", "DELETE FROM H").close());
        final int rows = 5 + random.nextInt(56);
        final int[] trues = { 0 };
        database.transaction(() -> {
          for (int i = 0; i < rows; i++) {
            final boolean b = random.nextBoolean();
            if (b)
              trues[0]++;
            database.newDocument("H").set("id", i, "b", b).save();
          }
        });
        assertThat(count(" WHERE b = true")).as("seed " + seed + " cycle " + cycle).isEqualTo(trues[0]);
        assertThat(count(" WHERE b = false")).as("seed " + seed + " cycle " + cycle).isEqualTo(rows - trues[0]);
      }
    }
  }

  private long count(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM H" + where)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }
}
