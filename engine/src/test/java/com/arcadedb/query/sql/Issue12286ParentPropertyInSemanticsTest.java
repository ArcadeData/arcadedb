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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A property of the outer row read through {@code $parent} is a record property: it is compared as one, without
 * converting the items of the list it is tested against, like {@code =} and an index do (issue #12286 review).
 */
class Issue12286ParentPropertyInSemanticsTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE OuterRec");
    database.command("sql", "CREATE PROPERTY OuterRec.key STRING");
    database.command("sql", "CREATE PROPERTY OuterRec.num INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE InnerRec");
    database.command("sql", "CREATE PROPERTY InnerRec.key INTEGER");
    database.transaction(() -> {
      database.newDocument("OuterRec").set("key", "01").set("num", 1).save();
      database.newDocument("InnerRec").set("key", 1).save();
    });
  }

  @Test
  void aParentPropertyIsNotConvertedToTheItemTypeOfTheList() {
    // the string "01" is not the integer 1
    final String sql = "SELECT (SELECT key FROM InnerRec WHERE $parent.$current.key IN [1]) AS m FROM OuterRec";
    final List<List<String>> rows = rows(sql);
    assertThat(rows).as("one outer row").hasSize(1);
    assertThat(rows.getFirst()).as("the string '01' is not the integer 1").isEmpty();
  }

  @Test
  void aParentPropertyOfTheSameTypeStillMatches() {
    // the integer 1 is the integer 1
    final String sql = "SELECT (SELECT key FROM InnerRec WHERE $parent.$current.num IN [1]) AS m FROM OuterRec";
    final List<List<String>> rows = rows(sql);
    assertThat(rows).as("one outer row").hasSize(1);
    assertThat(rows.getFirst()).as("the integer 1 is the integer 1").containsExactly("1");
  }

  private List<List<String>> rows(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().map(r -> {
        final Object m = r.getProperty("m");
        if (m == null)
          return List.<String>of();
        return ((List<?>) m).stream().map(o -> o instanceof Result result ? String.valueOf((Object) result.getProperty("key")) : String.valueOf(o)).toList();
      }).toList();
    }
  }
}
