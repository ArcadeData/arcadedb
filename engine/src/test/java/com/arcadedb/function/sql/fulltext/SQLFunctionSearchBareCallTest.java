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
package com.arcadedb.function.sql.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4023: SEARCH_INDEX and SEARCH_FIELDS are not propagating
 * the boolean return value when used as a bare predicate in WHERE (without "= true").
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionSearchBareCallTest extends TestHelper {

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.txt STRING");
      database.command("sql", "CREATE INDEX ON Doc (txt) FULL_TEXT");

      database.command("sql", "INSERT INTO Doc SET txt = 'what a wonderful world'");
      database.command("sql", "INSERT INTO Doc SET txt = 'unrelated content here'");
      database.command("sql", "INSERT INTO Doc SET txt = 'what about that'");
    });
  }

  @Test
  void searchIndexBareCallInWhere() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[txt]', 'what')");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.<String>getProperty("txt")).contains("what");
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void searchFieldsBareCallInWhere() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'], 'what')");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.<String>getProperty("txt")).contains("what");
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void searchIndexBareCallEquivalentToEqTrue() {
    database.transaction(() -> {
      final ResultSet bare = database.query("sql",
          "SELECT @rid AS rid FROM Doc WHERE SEARCH_INDEX('Doc[txt]', 'what')");
      final ResultSet eqTrue = database.query("sql",
          "SELECT @rid AS rid FROM Doc WHERE SEARCH_INDEX('Doc[txt]', 'what') = true");

      assertThat(toRids(bare)).isEqualTo(toRids(eqTrue));
    });
  }

  @Test
  void searchIndexBareCallCombinedWithAnd() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[txt]', 'what') AND txt LIKE '%world%'");

      assertThat(result.hasNext()).isTrue();
      final Result r = result.next();
      assertThat(r.<String>getProperty("txt")).isEqualTo("what a wonderful world");
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void searchIndexBareCallNegated() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Doc WHERE NOT SEARCH_INDEX('Doc[txt]', 'what')");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.<String>getProperty("txt")).doesNotContain("what");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  private static java.util.Set<Object> toRids(final ResultSet rs) {
    final java.util.Set<Object> ids = new java.util.HashSet<>();
    while (rs.hasNext())
      ids.add(rs.next().getProperty("rid"));
    return ids;
  }
}
