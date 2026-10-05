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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9238: an equality whose value is a parameter bound to null matches no record, through an index as in a
 * scan. The index must not answer it with the records whose key is null, and a {@code NULL_STRATEGY ERROR} index must not throw.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9238EqualsNullParameterThroughIndexTest extends TestHelper {

  private static final String[] ALL = { "Scan", "Lsm", "Composite", "CompositeP", "Hash", "Err" };

  @Override
  protected void beginTest() {
    for (final String t : ALL) {
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".p INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".q INTEGER");
    }
    database.command("sql", "CREATE INDEX ON Lsm (p) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON Composite (q, p) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON CompositeP (p, q) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Hash (p) NOTUNIQUE_HASH NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON Err (p) NOTUNIQUE NULL_STRATEGY ERROR");
    database.transaction(() -> {
      for (final String t : ALL) {
        database.newDocument(t).set("id", 1, "p", 1, "q", 7).save();
        if (!t.equals("Err")) {
          database.newDocument(t).set("id", 2, "p", null, "q", 7).save();
          database.newDocument(t).set("id", 3, "q", 7).save();
        }
      }
    });
  }

  private List<Integer> ids(final String type, final String where, final Object... params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " WHERE " + where + " ORDER BY id", params)) {
      rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
    }
    return ids;
  }

  @Test
  void equalsNullPositionalParameter() {
    for (final String t : ALL)
      assertThat(ids(t, "p = ?", (Object) null)).as(t).isEmpty();
  }

  @Test
  void equalsNullNamedParameter() {
    final Map<String, Object> params = new HashMap<>();
    params.put("x", null);
    for (final String t : ALL) {
      final List<Integer> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT id FROM " + t + " WHERE p = :x", params)) {
        rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
      }
      assertThat(ids).as(t).isEmpty();
    }
  }

  @Test
  void compositeEqualsNullParameter() {
    for (final String t : ALL)
      assertThat(ids(t, "q = 7 AND p = ?", (Object) null)).as(t).isEmpty();
  }

  @Test
  void deleteWithNullParameterDeletesNothing() {
    for (final String t : ALL) {
      database.transaction(() -> database.command("sql", "DELETE FROM " + t + " WHERE p = ?", (Object) null));
      assertThat(database.countType(t, false)).as(t).isEqualTo(t.equals("Err") ? 1 : 3);
    }
  }

  @Test
  void equalsNonNullStillUsesTheIndex() {
    for (final String t : ALL)
      assertThat(ids(t, "p = ?", 1)).as(t).containsExactly(1);
    assertThat(ids("Composite", "q = 7 AND p = ?", 1)).containsExactly(1);
  }

  @Test
  void nullSafeEqualsAndIsNullStillFindNullRecords() {
    for (final String t : new String[] { "Scan", "Lsm", "Composite", "CompositeP", "Hash" }) {
      assertThat(ids(t, "p <=> ?", (Object) null)).as(t).containsExactly(2, 3);
      assertThat(ids(t, "p IS NULL")).as(t).containsExactly(2, 3);
    }
  }

  @Test
  void caseInsensitiveIndexEqualsNullParameter() {
    database.command("sql", "CREATE DOCUMENT TYPE Ci");
    database.command("sql", "CREATE PROPERTY Ci.id INTEGER");
    database.command("sql", "CREATE PROPERTY Ci.p STRING");
    database.command("sql", "CREATE INDEX ON Ci (p COLLATE ci) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      database.newDocument("Ci").set("id", 1, "p", "A").save();
      database.newDocument("Ci").set("id", 2, "p", null).save();
    });
    assertThat(ids("Ci", "p.toLowerCase() = ?", (Object) null)).isEmpty();
    assertThat(ids("Ci", "p.toLowerCase() = ?", "a")).containsExactly(1);
  }
}
