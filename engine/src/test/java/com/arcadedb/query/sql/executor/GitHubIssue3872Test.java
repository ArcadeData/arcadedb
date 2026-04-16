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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3872: SQL: Trailing selector is not resolved in SELECT ... LET.
 * When a LET expression has a trailing selector (e.g., .a), it should be resolved
 * rather than ignored.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GitHubIssue3872Test extends TestHelper {

  public GitHubIssue3872Test() {
    autoStartTx = true;
  }

  /**
   * SELECT $x LET $x = {"a": 1}.a should return 1, not {"a": 1}.
   */
  @Test
  void letWithMapLiteralTrailingSelector() {
    try (final ResultSet rs = database.query("sql", "SELECT $x LET $x = {\"a\": 1}.a")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Object) result.getProperty("$x")).isEqualTo(1);
    }
  }

  /**
   * SELECT $x LET $x = (SELECT 1 AS a).a should return [1], not [{"a": 1}].
   * The .a selector extracts the 'a' field from each subquery result.
   */
  @Test
  void letWithSubqueryTrailingSelector() {
    try (final ResultSet rs = database.query("sql", "SELECT $x LET $x = (SELECT 1 AS a).a")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final Object val = result.getProperty("$x");
      // The .a selector should extract the 'a' field from the subquery result set
      assertThat(val).isInstanceOf(java.util.List.class);
      @SuppressWarnings("unchecked")
      final java.util.List<Object> list = (java.util.List<Object>) val;
      assertThat(list).containsExactly(1);
    }
  }

  /**
   * Verify that a LET without trailing selector still works (no regression).
   */
  @Test
  void letWithMapLiteralNoSelector() {
    try (final ResultSet rs = database.query("sql", "SELECT $x LET $x = {\"a\": 1}")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Object) result.getProperty("$x")).isNotNull();
    }
  }

  /**
   * Verify that a LET with subquery without trailing selector still works (no regression).
   */
  @Test
  void letWithSubqueryNoSelector() {
    try (final ResultSet rs = database.query("sql", "SELECT $x LET $x = (SELECT 1 AS a)")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Object) result.getProperty("$x")).isNotNull();
    }
  }
}
