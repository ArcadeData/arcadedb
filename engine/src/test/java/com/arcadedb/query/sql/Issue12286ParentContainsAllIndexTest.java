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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The operand of a CONTAINSALL read from the outer row ({@code $parent}) is not an index key: an index chosen by the
 * first outer row's value would be kept for the next one, whose operand may have no entry in it (issue #12286
 * review).
 */
class Issue12286ParentContainsAllIndexTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Inner2");
    database.command("sql", "CREATE PROPERTY Inner2.nums LIST OF INTEGER");
    database.command("sql", "CREATE PROPERTY Inner2.name STRING");
    database.command("sql", "CREATE INDEX ON Inner2 (nums BY ITEM) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Outer2");
    database.command("sql", "CREATE PROPERTY Outer2.id INTEGER");
    database.command("sql", "CREATE PROPERTY Outer2.list LIST OF INTEGER");
    database.transaction(() -> {
      database.newDocument("Inner2").set("nums", List.of(1)).set("name", "one").save();
      database.newDocument("Inner2").set("nums", List.of(2)).set("name", "two").save();
      database.newDocument("Outer2").set("id", 1).set("list", List.of(1)).save();
      database.newDocument("Outer2").set("id", 2).set("list", List.of()).save();
    });
  }

  @Test
  void aContainsAllOverTheParentListAnswersEveryRow() {
    // the outer row 1 asks for the list [1], the outer row 2 for the empty list: an empty operand is satisfied by
    // every collection, so both inner records answer it
    final String sql = "SELECT id, $n.name AS names FROM Outer2 LET $n = (SELECT name FROM Inner2 WHERE nums CONTAINSALL $parent.$current.list) ORDER BY id";
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Object names = r.getProperty("names");
        final List<String> list = names == null ? List.of() : ((List<?>) names).stream().map(String::valueOf).sorted().toList();
        rows.add(r.getProperty("id") + "->" + String.join(",", list));
      }
    }
    assertThat(rows).as("the outer rows").containsExactly("1->one", "2->one,two");
  }
}
