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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guards for #8834 (a range on a parent type's index under a subtype label returned the parent's and siblings'
 * vertices) and #8835 (a range on a property with only a hash index threw instead of scanning).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8834Issue8835RangeIndexTest extends TestHelper {

  private List<Long> longs(final String query) {
    final List<Long> got = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("v").longValue());
    }
    return got;
  }

  @Test
  void subtypeRangeOnParentIndexOnlyReturnsTheSubtype() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q EXTENDS P");
    database.command("sql", "CREATE VERTEX TYPE R EXTENDS P");
    database.command("sql", "CREATE PROPERTY P.a LONG");
    database.command("sql", "CREATE INDEX ON P (a) NOTUNIQUE");
    database.transaction(() -> {
      database.newVertex("P").set("a", 1000L).save();
      database.newVertex("Q").set("a", -1L).save();
      database.newVertex("R").set("a", 2000L).save();
    });
    assertThat(longs("MATCH (n:Q) WHERE n.a > 500 RETURN n.a AS v")).isEmpty();
    assertThat(longs("MATCH (n:R) WHERE n.a > 500 RETURN n.a AS v")).containsExactly(2000L);
    assertThat(longs("MATCH (n:Q) WHERE n.a < 500 RETURN n.a AS v")).containsExactly(-1L);
    assertThat(longs("MATCH (n:P) WHERE n.a > 500 RETURN n.a AS v ORDER BY v")).containsExactly(1000L, 2000L);
  }

  @Test
  void rangeOnHashIndexFallsBackToTheLabelScan() {
    database.command("sql", "CREATE VERTEX TYPE H");
    database.command("sql", "CREATE PROPERTY H.h LONG");
    database.command("sql", "CREATE PROPERTY H.u LONG");
    database.command("sql", "CREATE INDEX ON H (h) NOTUNIQUE_HASH");
    database.command("sql", "CREATE INDEX ON H (u) UNIQUE_HASH");
    database.transaction(() -> {
      for (long i = 0; i < 10; i++)
        database.newVertex("H").set("h", i, "u", i).save();
    });
    assertThat(longs("MATCH (n:H) WHERE n.h > 7 RETURN n.h AS v")).containsExactlyInAnyOrder(8L, 9L);
    assertThat(longs("MATCH (n:H) WHERE n.u <= 1 RETURN n.u AS v")).containsExactlyInAnyOrder(0L, 1L);
    assertThat(longs("MATCH (n:H) WHERE n.h = 7 RETURN n.h AS v")).containsExactly(7L);
  }
}
