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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3583: Invalid value for $current: null when SQL command is executed.
 * <p>
 * Using .replace() method calls in WHERE clause within LET subqueries combined with ILIKE
 * and UNIONALL throws "Invalid value for $current: null".
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3583LetReplaceIlikeTest extends TestHelper {

  @Test
  void replaceWithIlikeInLetSubquery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE NER IF NOT EXISTS");
      database.command("sql", "CREATE DOCUMENT TYPE THEME IF NOT EXISTS");

      // Insert data with newlines and tabs using parameterized inserts
      database.command("sql", "INSERT INTO NER SET identity = ?", "hello\nworld");
      database.command("sql", "INSERT INTO NER SET identity = ?", "foo\tbar");
      database.command("sql", "INSERT INTO THEME SET identity = ?", "hello\tworld");
      database.command("sql", "INSERT INTO THEME SET identity = ?", "baz qux");

      final Map<String, Object> params = new HashMap<>();
      params.put("keyWordIdentifier_0", "hello");
      params.put("keyWordIdentifier_1", "world");

      final ResultSet rs = database.query("sql",
          "SELECT expand($c) " +
              "LET " +
              "  $a = (SELECT identity, @rid as id FROM NER " +
              "    WHERE identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_0 + '%') " +
              "      AND identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_1 + '%')), " +
              "  $b = (SELECT identity, @rid as id FROM THEME " +
              "    WHERE identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_0 + '%') " +
              "      AND identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_1 + '%')), " +
              "  $c = UNIONALL($a, $b)",
          params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      // "hello\nworld" from NER should match after replace(\n -> space)
      // "hello\tworld" from THEME should match after replace(\t -> space)
      assertThat(results).hasSize(2);
    });
  }

  @Test
  void replaceMethodCallInWhereClause() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestReplace IF NOT EXISTS");
      database.command("sql", "INSERT INTO TestReplace SET identity = ?", "hello\nworld");
      database.command("sql", "INSERT INTO TestReplace SET identity = 'no match'");

      final ResultSet rs = database.query("sql",
          "SELECT identity FROM TestReplace WHERE identity.replace('\\n', ' ') ILIKE '%hello world%'");

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("identity")).isEqualTo("hello\nworld");
    });
  }

  @Test
  void replaceWithIlikeAndIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE IndexedType IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY IndexedType.identity IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON IndexedType (identity) NOTUNIQUE");

      database.command("sql", "INSERT INTO IndexedType SET identity = ?", "hello\nworld");
      database.command("sql", "INSERT INTO IndexedType SET identity = ?", "foo\tbar");
      database.command("sql", "INSERT INTO IndexedType SET identity = 'no match'");

      // This query might use the index, leading to FilterStep instead of ScanWithFilterStep
      final ResultSet rs = database.query("sql",
          "SELECT identity FROM IndexedType " +
              "WHERE identity.replace('\\n', ' ').replace('\\t', ' ') ILIKE '%hello world%'");

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("identity")).isEqualTo("hello\nworld");
    });
  }

  @Test
  void replaceWithIlikeInLetSubqueryWithIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE NER2 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY NER2.identity IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON NER2 (identity) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE THEME2 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY THEME2.identity IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON THEME2 (identity) NOTUNIQUE");

      database.command("sql", "INSERT INTO NER2 SET identity = ?", "hello\nworld");
      database.command("sql", "INSERT INTO NER2 SET identity = ?", "foo\tbar");
      database.command("sql", "INSERT INTO THEME2 SET identity = ?", "hello\tworld");
      database.command("sql", "INSERT INTO THEME2 SET identity = ?", "baz qux");

      final Map<String, Object> params = new HashMap<>();
      params.put("keyWordIdentifier_0", "hello");
      params.put("keyWordIdentifier_1", "world");

      final ResultSet rs = database.query("sql",
          "SELECT expand($c) " +
              "LET " +
              "  $a = (SELECT identity, @rid as id FROM NER2 " +
              "    WHERE identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_0 + '%') " +
              "      AND identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_1 + '%')), " +
              "  $b = (SELECT identity, @rid as id FROM THEME2 " +
              "    WHERE identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_0 + '%') " +
              "      AND identity.replace('\\n', ' ').replace('\\t', ' ').replace('  ', ' ') ILIKE ('%' + :keyWordIdentifier_1 + '%')), " +
              "  $c = UNIONALL($a, $b)",
          params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(2);
    });
  }
}
