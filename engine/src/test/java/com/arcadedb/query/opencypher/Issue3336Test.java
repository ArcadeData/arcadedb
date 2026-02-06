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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3336: Cypher ternary operators with null ALWAYS returns null.
 * Verifies that Cypher three-valued logic (ternary logic) with null works correctly:
 * - true AND null  → null
 * - false AND null → false
 * - true OR null   → true
 * - false OR null  → null
 * - NOT null       → null
 * - null AND null  → null
 * - null OR null   → null
 */
class Issue3336Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/test-issue3336").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testTernaryAndWithNull() {
    try (final ResultSet rs = database.query("opencypher",
        "RETURN (true AND null) AS r1, (false AND null) AS r2")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // true AND null → null (unknown)
      assertThat(row.<Object>getProperty("r1")).isNull();
      // false AND null → false (false dominates AND)
      assertThat(row.<Boolean>getProperty("r2")).isEqualTo(false);
    }
  }

  @Test
  void testTernaryOrWithNull() {
    try (final ResultSet rs = database.query("opencypher",
        "RETURN (true OR null) AS r3, (false OR null) AS r4")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // true OR null → true (true dominates OR)
      assertThat(row.<Boolean>getProperty("r3")).isEqualTo(true);
      // false OR null → null (unknown)
      assertThat(row.<Object>getProperty("r4")).isNull();
    }
  }

  @Test
  void testTernaryNotNull() {
    try (final ResultSet rs = database.query("opencypher",
        "RETURN (NOT null) AS r5")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // NOT null → null
      assertThat(row.<Object>getProperty("r5")).isNull();
    }
  }

  @Test
  void testTernaryNullAndNull() {
    try (final ResultSet rs = database.query("opencypher",
        "RETURN (null AND null) AS r6, (null OR null) AS r7")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // null AND null → null
      assertThat(row.<Object>getProperty("r6")).isNull();
      // null OR null → null
      assertThat(row.<Object>getProperty("r7")).isNull();
    }
  }

  @Test
  void testOriginalIssueQuery() {
    // Exact query from the issue
    try (final ResultSet rs = database.query("opencypher",
        "RETURN (true AND null) AS r1, (false AND null) AS r2, (true OR null) AS r3, (NOT null) AS r4")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("r1")).isNull();
      assertThat(row.<Boolean>getProperty("r2")).isEqualTo(false);
      assertThat(row.<Boolean>getProperty("r3")).isEqualTo(true);
      assertThat(row.<Object>getProperty("r4")).isNull();
    }
  }

  @Test
  void testNonNullLogicStillWorks() {
    // Ensure regular boolean logic still works
    try (final ResultSet rs = database.query("opencypher",
        "RETURN (true AND true) AS r1, (true AND false) AS r2, (true OR false) AS r3, (NOT true) AS r4, (NOT false) AS r5")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("r1")).isTrue();
      assertThat(row.<Boolean>getProperty("r2")).isFalse();
      assertThat(row.<Boolean>getProperty("r3")).isTrue();
      assertThat(row.<Boolean>getProperty("r4")).isFalse();
      assertThat(row.<Boolean>getProperty("r5")).isTrue();
    }
  }
}
