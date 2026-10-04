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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9245: the source plan of an UPDATE or DELETE was cached under a text that prints every positional parameter as
 * {@code ?}, so statements reading the same WHERE from different parameter positions shared one plan.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9245DmlPositionalPlanCacheTest extends TestHelper {

  private void setup(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".sku STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".brand STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (sku) UNIQUE");
    database.transaction(() -> {
      database.newDocument(type).set("sku", "S1").set("brand", "b1").save();
      database.newDocument(type).set("sku", "S2").set("brand", "b2").save();
      database.newDocument(type).set("sku", "NEW").set("brand", "b3").save();
    });
  }

  private long command(final String sql, final Object... params) {
    final long[] count = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql, params)) {
        count[0] = ((Number) rs.next().getProperty("count")).longValue();
      }
    });
    return count[0];
  }

  private String select(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<String> out = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty("sku") + ":" + r.getProperty("brand"));
      }
      return String.join(" ", out);
    }
  }

  private String rows(final String type) {
    return select("SELECT sku, brand FROM " + type + " ORDER BY sku");
  }

  @Test
  void updateAfterSelectWithSameWhere() {
    setup("A");
    select("SELECT FROM A WHERE sku = ?", "S1");
    assertThat(command("UPDATE A SET brand = ? WHERE sku = ?", "NEW", "S2")).isEqualTo(1L);
    assertThat(rows("A")).isEqualTo("NEW:b3 S1:b1 S2:NEW");
  }

  @Test
  void updateAfterUpdateWithDifferentSetParameterCount() {
    setup("B");
    command("UPDATE B SET brand = ? WHERE sku = ?", "b1", "S1");
    assertThat(command("UPDATE B SET category = ?, brand = ? WHERE sku = ?", "c-new", "NEW", "S2")).isEqualTo(1L);
    assertThat(rows("B")).isEqualTo("NEW:b3 S1:b1 S2:NEW");
  }

  @Test
  void selectAfterUpdateWithSameWhere() {
    setup("C");
    command("UPDATE C SET brand = ? WHERE sku = ?", "b1", "S1");
    assertThat(select("SELECT FROM C WHERE sku = ?", "S2")).isEqualTo("S2:b2");
  }

  @Test
  void deleteAfterUpdateWithSameWhere() {
    setup("D");
    command("UPDATE D SET brand = ? WHERE sku = ?", "b1", "S1");
    assertThat(command("DELETE FROM D WHERE sku = ?", "S2")).isEqualTo(1L);
    assertThat(rows("D")).isEqualTo("NEW:b3 S1:b1");
  }

  @Test
  void updateAfterDeleteWithSameWhere() {
    setup("E");
    command("DELETE FROM E WHERE sku = ?", "S1");
    assertThat(command("UPDATE E SET brand = ? WHERE sku = ?", "NEW", "S2")).isEqualTo(1L);
    assertThat(rows("E")).isEqualTo("NEW:b3 S2:NEW");
  }

  @Test
  void twoPropertiesWithoutIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE I");
    database.transaction(() -> {
      database.newDocument("I").set("sku", "S1").set("brand", "b1").save();
      database.newDocument("I").set("sku", "S2").set("brand", "b2").save();
      database.newDocument("I").set("sku", "NEW").set("brand", "b3").save();
    });
    select("SELECT FROM I WHERE sku = ? AND brand = ?", "S1", "b1");
    assertThat(command("UPDATE I SET brand = ? WHERE sku = ? AND brand = ?", "NEW", "S2", "b2")).isEqualTo(1L);
    assertThat(rows("I")).isEqualTo("NEW:b3 S1:b1 S2:NEW");
  }
}
