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
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9274: a statement first run with a value keeps its index plan, and a later run of the same text with a
 * null parameter must still match no record (#9238), for SELECT and for DELETE.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9274CachedPlanNullEqualityTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE PROPERTY T.p INTEGER");
    database.command("sql", "CREATE PROPERTY T.q INTEGER");
    database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON T (q, p) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.command("sql", "CREATE INDEX ON T (s COLLATE ci) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.id INTEGER");
    database.command("sql", "CREATE PROPERTY U.p INTEGER");
    database.command("sql", "CREATE INDEX ON U (p) UNIQUE NULL_STRATEGY INDEX");
    reload();
  }

  /** Loads the fixed data set; the tests below do not modify it, as the DELETE statements they run match nothing. */
  private void reload() {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM T");
      database.newDocument("T").set("id", 1, "p", 1, "q", 7, "s", "Abc").save();
      database.newDocument("T").set("id", 2, "p", null, "q", 7).save();
      database.newDocument("T").set("id", 3, "q", 7).save();
    });
  }

  private List<Integer> ids(final String where, final Object... params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE " + where, params)) {
      rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
    }
    Collections.sort(ids);
    return ids;
  }

  @Test
  void selectValueThenNull() {
    final Object nul = null;
    assertThat(ids("p = ?", 1)).containsExactly(1);
    assertThat(ids("p = ?", nul)).isEmpty();
    assertThat(ids("p = ?", 1)).containsExactly(1);
    assertThat(ids("p = ?", nul)).isEmpty();
  }

  @Test
  void selectCompositeValueThenNull() {
    final Object nul = null;
    assertThat(ids("q = ? AND p = ?", 7, 1)).containsExactly(1);
    assertThat(ids("q = ? AND p = ?", 7, nul)).isEmpty();
    assertThat(ids("q = ? AND p = ?", nul, 1)).isEmpty();
    assertThat(ids("q = ? AND p = ?", 7, 1)).containsExactly(1);
  }

  @Test
  void selectReversedOperandsValueThenNull() {
    final Object nul = null;
    assertThat(ids("? = p", 1)).containsExactly(1);
    assertThat(ids("? = p", nul)).isEmpty();
  }

  @Test
  void deleteValueThenNull() {
    final Object nul = null;
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ?", 99));
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ?", nul));
    assertThat(database.countType("T", false)).isEqualTo(3);

    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ? AND id > 100", 99));
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ? AND id > 100", nul));
    assertThat(database.countType("T", false)).isEqualTo(3);
  }

  @Test
  void selectNullLiteralAfterValue() {
    assertThat(ids("p = ?", 1)).containsExactly(1);
    assertThat(ids("p = null")).isEmpty();
  }

  @Test
  void selectNullOnRangeSideOfCompositeKey() {
    final Object nul = null;
    assertThat(ids("q = ? AND p > ?", 7, 0)).containsExactly(1);
    assertThat(ids("q = ? AND p > ?", 7, nul)).isEmpty();
    assertThat(ids("q = ? AND p > ?", 7, 0)).containsExactly(1);
  }

  @Test
  void selectCaseInsensitiveIndexValueThenNull() {
    final Object nul = null;
    assertThat(ids("s.toLowerCase() = ?", "abc")).containsExactly(1);
    assertThat(ids("s.toLowerCase() = ?", nul)).isEmpty();
    assertThat(ids("s.toLowerCase() = ?", "abc")).containsExactly(1);
  }

  @Test
  void selectUniqueIndexValueThenNull() {
    final Object nul = null;
    database.transaction(() -> database.newDocument("U").set("id", 1, "p", 5).save());
    final String sql = "SELECT id FROM U WHERE p = ?";
    for (final Object value : new Object[] { 5, nul, 5, nul }) {
      final List<Integer> found = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql, value)) {
        rs.forEachRemaining(r -> found.add(r.getProperty("id")));
      }
      assertThat(found).isEqualTo(value == null ? List.<Integer>of() : List.of(1));
    }
  }

  @Test
  void deleteWithValueStillDeletes() {
    final Object nul = null;
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ?", nul));
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ?", 1));
    assertThat(database.countType("T", false)).isEqualTo(2);
    reload();
  }
}
