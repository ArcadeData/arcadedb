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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8972: keys that compare equal but serialize to different lengths (DECIMAL 5.00 and 5) made the LSM retrieve
 * read the values of the run of equal keys from inside the key bytes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8972DecimalNotUniqueIndexTest extends TestHelper {

  private void create(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".amount DECIMAL");
    database.command("sql", "CREATE INDEX ON " + type + " (amount) NOTUNIQUE");
  }

  private List<String> names(final String type) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM " + type + " WHERE amount = 5")) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  @Test
  void wideScaleThenNarrow() {
    create("A");
    database.transaction(() -> {
      database.newDocument("A").set("name", "apple", "amount", new BigDecimal("5.00")).save();
      database.newDocument("A").set("name", "pear", "amount", new BigDecimal("4.00")).save();
    });
    database.transaction(() -> {
      database.newDocument("A").set("name", "plum", "amount", new BigDecimal("5")).save();
      database.newDocument("A").set("name", "fig", "amount", new BigDecimal("6")).save();
    });

    assertThat(names("A")).containsExactlyInAnyOrder("apple", "plum");
    final IndexCursor cursor = database.getSchema().getIndexByName("A[amount]").get(new Object[] { new BigDecimal("5") });
    final List<RID> rids = new ArrayList<>();
    while (cursor.hasNext())
      rids.add(cursor.next().getIdentity());
    assertThat(rids).hasSize(2);
    for (final RID rid : rids)
      assertThat(new BigDecimal(database.lookupByRID(rid, true).asDocument().get("amount").toString())).isEqualByComparingTo("5");
  }

  @Test
  void narrowScaleThenWideAndDelete() {
    create("B");
    database.transaction(() -> database.newDocument("B").set("name", "kiwi", "amount", new BigDecimal("5")).save());
    database.transaction(() -> database.newDocument("B").set("name", "lime", "amount", new BigDecimal("5.00")).save());
    database.transaction(() -> database.newDocument("B").set("name", "nut", "amount", new BigDecimal("5.0")).save());

    assertThat(names("B")).containsExactlyInAnyOrder("kiwi", "lime", "nut");

    database.transaction(() -> database.command("sql", "DELETE FROM B WHERE name = 'lime'"));
    assertThat(names("B")).containsExactlyInAnyOrder("kiwi", "nut");
    assertThat(database.countType("B", false)).isEqualTo(2);
  }

  @Test
  void manyWidthsAcrossTransactionsThenCompacted() throws Exception {
    create("C");
    final String[] widths = { "5", "5.00", "5.0", "5.000", "5.0000000000" };
    for (int i = 0; i < 12; i++) {
      final String amount = widths[i % widths.length];
      final int n = i;
      database.transaction(() -> database.newDocument("C").set("name", "n" + n, "amount", new BigDecimal(amount)).save());
    }
    assertThat(names("C")).hasSize(12);

    final Index index = database.getSchema().getIndexByName("C[amount]");
    ((IndexInternal) index).compact();
    assertThat(names("C")).hasSize(12);

    final IndexCursor cursor = index.get(new Object[] { new BigDecimal("5") });
    int found = 0;
    while (cursor.hasNext()) {
      assertThat(database.lookupByRID(cursor.next().getIdentity(), true)).isNotNull();
      found++;
    }
    assertThat(found).isEqualTo(12);
  }
}
