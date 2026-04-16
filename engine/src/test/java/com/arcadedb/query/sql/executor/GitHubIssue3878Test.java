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
 * Test for GitHub issue #3878: SQL: CREATE MATERIALIZED VIEW does not allow symbols in query.
 * Equality and comparison operators in SELECT projections should work both in plain SELECT
 * and in CREATE MATERIALIZED VIEW ... AS SELECT statements.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GitHubIssue3878Test extends TestHelper {

  public GitHubIssue3878Test() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE TestDoc");
    database.command("sql", "INSERT INTO TestDoc SET name = 'Alice', active = true");
    database.command("sql", "INSERT INTO TestDoc SET name = 'Bob', active = false");
  }

  /**
   * SELECT (true = true) should work as a plain query.
   */
  @Test
  void selectWithEqualityInProjection() {
    try (final ResultSet rs = database.query("sql", "SELECT (true = true) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Boolean) result.getProperty("result")).isTrue();
    }
  }

  /**
   * SELECT if((true = true), 1, 2) should work as a plain query.
   */
  @Test
  void selectWithIfFunctionContainingEquality() {
    try (final ResultSet rs = database.query("sql", "SELECT if((true = true), 1, 2) as val")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Integer) result.getProperty("val")).isEqualTo(1);
    }
  }

  /**
   * CREATE MATERIALIZED VIEW with equality in projection should work.
   */
  @Test
  void createMaterializedViewWithEqualityInProjection() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW EqView AS SELECT (true = true) as result FROM TestDoc");

    assertThat(database.getSchema().existsMaterializedView("EqView")).isTrue();

    try (final ResultSet rs = database.query("sql", "SELECT FROM EqView")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Boolean) result.getProperty("result")).isTrue();
    }

    database.command("sql", "DROP MATERIALIZED VIEW EqView");
  }

  /**
   * CREATE MATERIALIZED VIEW with if() function containing equality should work.
   */
  @Test
  void createMaterializedViewWithIfFunction() {
    database.command("sql",
        "CREATE MATERIALIZED VIEW IfView AS SELECT if((active = true), 1, 2) as flag FROM TestDoc");

    assertThat(database.getSchema().existsMaterializedView("IfView")).isTrue();

    try (final ResultSet rs = database.query("sql", "SELECT FROM IfView ORDER BY flag")) {
      assertThat(rs.hasNext()).isTrue();
      final Result first = rs.next();
      assertThat(rs.hasNext()).isTrue();
      final Result second = rs.next();
      // Alice (active=true) -> flag=1, Bob (active=false) -> flag=2
      assertThat((Integer) first.getProperty("flag")).isEqualTo(1);
      assertThat((Integer) second.getProperty("flag")).isEqualTo(2);
    }

    database.command("sql", "DROP MATERIALIZED VIEW IfView");
  }
}
