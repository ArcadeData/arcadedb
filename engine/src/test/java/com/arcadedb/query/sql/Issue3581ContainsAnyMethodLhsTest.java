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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3581: SQL: Left-hand side of CONTAINSANY being method return value errors (Regression).
 * <p>
 * Using CONTAINSANY with a method call (e.g., split()) on the left-hand side was throwing
 * "Invalid value for $current: null" because the $current context variable was not set
 * before evaluating the WHERE clause filter in ScanWithFilterStep.
 * </p>
 */
class Issue3581ContainsAnyMethodLhsTest extends TestHelper {

  @Test
  void containsAnyWithSplitOnLhs() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581 IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581 SET txt = 'te st'");
      database.command("sql", "INSERT INTO doc3581 SET txt = 'hello world'");
      database.command("sql", "INSERT INTO doc3581 SET txt = 'no match here'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581 WHERE txt.split(' ') CONTAINSANY 'te'");
      assertThat(rs.hasNext()).isTrue();
      final var result = rs.next();
      assertThat(result.<String>getProperty("txt")).isEqualTo("te st");
      assertThat(rs.hasNext()).isFalse();
    });
  }

  @Test
  void containsAnyWithSplitOnLhsMultipleMatches() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581b IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581b SET txt = 'te st'");
      database.command("sql", "INSERT INTO doc3581b SET txt = 'te other'");
      database.command("sql", "INSERT INTO doc3581b SET txt = 'no match'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581b WHERE txt.split(' ') CONTAINSANY 'te'");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void containsAnyWithSplitOnLhsNoMatch() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581c IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581c SET txt = 'hello world'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581c WHERE txt.split(' ') CONTAINSANY 'te'");
      assertThat(rs.hasNext()).isFalse();
    });
  }

  @Test
  void containsAnyWithSplitOnLhsNullField() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581d IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581d SET txt = 'te st'");
      database.command("sql", "INSERT INTO doc3581d SET other = 'no txt field'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581d WHERE txt.split(' ') CONTAINSANY 'te'");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("txt")).isEqualTo("te st");
      assertThat(rs.hasNext()).isFalse();
    });
  }

  @Test
  void containsAllWithSplitOnLhs() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3581e IF NOT EXISTS");
      database.command("sql", "INSERT INTO doc3581e SET txt = 'te st'");
      database.command("sql", "INSERT INTO doc3581e SET txt = 'te other'");

      final ResultSet rs = database.query("sql", "SELECT FROM doc3581e WHERE txt.split(' ') CONTAINSALL 'te'");
      // CONTAINSALL with single value: both records have 'te' as a split token
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }
}
