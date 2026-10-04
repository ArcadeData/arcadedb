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
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9032: {@code x IN (..., null)} must not return the records whose {@code x} is null when an index that holds null
 * keys answers it, as a scan does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9032InWithNullThroughIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    for (final String t : new String[] { "I", "C", "S", "Src" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".x INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".z INTEGER");
    }
    database.command("sql", "CREATE INDEX ON I (x) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON C (x, z) NOTUNIQUE");
    database.transaction(() -> {
      for (final String t : new String[] { "I", "C", "S" }) {
        database.newDocument(t).set("id", 1, "x", 1, "z", 5).save();
        database.newDocument(t).set("id", 2, "x", 2, "z", 5).save();
        database.newDocument(t).set("id", 3, "x", null, "z", 5).save();
      }
      database.newDocument("Src").set("id", 1, "x", 1).save();
      database.newDocument("Src").set("id", 2, "x", null).save();
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
  void inParameterWithNullElement() {
    for (final String t : new String[] { "I", "C", "S" })
      assertThat(ids(t, "x IN ?", Arrays.asList(1, null))).as(t).containsExactly(1);
  }

  @Test
  void inSubqueryReturningNull() {
    for (final String t : new String[] { "I", "C", "S" })
      assertThat(ids(t, "x IN (SELECT x FROM Src)")).as(t).containsExactly(1);
  }

  @Test
  void isNullStillFindsTheNullRecord() {
    for (final String t : new String[] { "I", "C", "S" }) {
      assertThat(ids(t, "(x IN ?) IS NULL", Arrays.asList(1, null))).as(t).containsExactly(2, 3);
      assertThat(ids(t, "x IS NULL")).as(t).containsExactly(3);
    }
  }

  @Test
  void inWithoutNullUnchanged() {
    for (final String t : new String[] { "I", "C", "S" }) {
      assertThat(ids(t, "x IN ?", Arrays.asList(1, 2))).as(t).containsExactly(1, 2);
      assertThat(ids(t, "x IN [1, null]")).as(t).containsExactly(1);
    }
  }

  @Test
  void compositeInWithNullInSecondSlot() {
    assertThat(ids("C", "x = 1 AND z IN ?", Arrays.asList(5, null))).containsExactly(1);
  }

  @Test
  void notInWithNullIsNeverTrue() {
    for (final String t : new String[] { "I", "C", "S" })
      assertThat(ids(t, "x NOT IN ?", Arrays.asList(1, null))).as(t).isEmpty();
  }

  @Test
  void uniqueIndexInWithNull() {
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.id INTEGER");
    database.command("sql", "CREATE PROPERTY U.x INTEGER");
    database.command("sql", "CREATE INDEX ON U (x) UNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      database.newDocument("U").set("id", 1, "x", 1).save();
      database.newDocument("U").set("id", 2, "x", null).save();
    });
    assertThat(ids("U", "x IN ?", Arrays.asList(1, null))).containsExactly(1);
  }
}
