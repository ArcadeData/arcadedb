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
 * Test for GitHub issue #2692: ORDER BY skips NULL values when using indexed properties
 * with the default NULL_STRATEGY SKIP.
 * <p>
 * The issue is that when ORDER BY uses an index for sorting optimization,
 * records with NULL values for the indexed property are not returned because
 * they are not stored in the index (with NULL_STRATEGY SKIP).
 * <p>
 * Expected behavior: ORDER BY should return ALL records, including those with NULL values,
 * regardless of whether an index is used for sorting optimization.
 */
class OrderByWithNullsTest extends TestHelper {
  private static final String TYPE_NAME = "doc";

  @Test
  void orderByIndexedPropertyShouldIncludeNullValues() {
    // Setup: Create type with indexed property (default NULL_STRATEGY is SKIP)
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".num LONG");
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (num) NOTUNIQUE");

    // Insert records: 2 with values, 1 with NULL
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 1");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 2");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {}"); // no num value (NULL)
    });

    // Test 1: Without ORDER BY - should return 3 records
    int countWithoutOrder;
    try (ResultSet result = database.query("sql", "SELECT FROM " + TYPE_NAME)) {
      countWithoutOrder = 0;
      while (result.hasNext()) {
        result.next();
        countWithoutOrder++;
      }
    }
    assertThat(countWithoutOrder).as("SELECT without ORDER BY should return all 3 records").isEqualTo(3);

    // Test 2: With ORDER BY ASC - should also return 3 records (including the NULL)
    int countWithOrderAsc;
    List<Long> valuesAsc = new ArrayList<>();
    try (ResultSet result = database.query("sql", "SELECT FROM " + TYPE_NAME + " ORDER BY num ASC")) {
      countWithOrderAsc = 0;
      while (result.hasNext()) {
        Result r = result.next();
        valuesAsc.add(r.getProperty("num"));
        countWithOrderAsc++;
      }
    }
    assertThat(countWithOrderAsc).as("SELECT with ORDER BY ASC should return all 3 records").isEqualTo(3);
    // NULL values should be first in ASC order (NULLS FIRST is the default for ASC in ArcadeDB)
    assertThat(valuesAsc.get(0)).as("First value in ASC order should be NULL").isNull();

    // Test 3: With ORDER BY DESC - should also return 3 records
    int countWithOrderDesc;
    List<Long> valuesDesc = new ArrayList<>();
    try (ResultSet result = database.query("sql", "SELECT FROM " + TYPE_NAME + " ORDER BY num DESC")) {
      countWithOrderDesc = 0;
      while (result.hasNext()) {
        Result r = result.next();
        valuesDesc.add(r.getProperty("num"));
        countWithOrderDesc++;
      }
    }
    assertThat(countWithOrderDesc).as("SELECT with ORDER BY DESC should return all 3 records").isEqualTo(3);
    // NULL values should be last in DESC order
    assertThat(valuesDesc.get(valuesDesc.size() - 1)).as("Last value in DESC order should be NULL").isNull();
  }

  @Test
  void orderByIndexedPropertyMultipleNulls() {
    // Setup: Create type with indexed property (default NULL_STRATEGY is SKIP)
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".num LONG");
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (num) NOTUNIQUE");

    // Insert records: 3 with values, 3 with NULL
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 1");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 2");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 3");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {}"); // NULL
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {}"); // NULL
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {}"); // NULL
    });

    // Without ORDER BY
    int countWithoutOrder;
    try (ResultSet result = database.query("sql", "SELECT FROM " + TYPE_NAME)) {
      countWithoutOrder = 0;
      while (result.hasNext()) {
        result.next();
        countWithoutOrder++;
      }
    }
    assertThat(countWithoutOrder).as("SELECT without ORDER BY should return all 6 records").isEqualTo(6);

    // With ORDER BY
    int countWithOrder;
    List<Long> values = new ArrayList<>();
    try (ResultSet result = database.query("sql", "SELECT FROM " + TYPE_NAME + " ORDER BY num")) {
      countWithOrder = 0;
      while (result.hasNext()) {
        Result r = result.next();
        values.add(r.getProperty("num"));
        countWithOrder++;
      }
    }
    assertThat(countWithOrder).as("SELECT with ORDER BY should return all 6 records").isEqualTo(6);

    // First 3 should be NULL, last 3 should be 1, 2, 3
    assertThat(values.subList(0, 3)).as("First 3 values should be NULL").containsOnly((Long) null);
    assertThat(values.subList(3, 6)).as("Last 3 values should be 1, 2, 3").containsExactly(1L, 2L, 3L);
  }

  @Test
  void orderByIndexedPropertyWithLimit() {
    // Setup
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".num LONG");
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (num) NOTUNIQUE");

    // Insert records: 3 with values, 2 with NULL
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 1");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 2");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 3");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {}"); // NULL
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {}"); // NULL
    });

    // With ORDER BY and LIMIT - should respect the total record count
    int countWithOrderAndLimit;
    try (ResultSet result = database.query("sql", "SELECT FROM " + TYPE_NAME + " ORDER BY num LIMIT 10")) {
      countWithOrderAndLimit = 0;
      while (result.hasNext()) {
        result.next();
        countWithOrderAndLimit++;
      }
    }
    assertThat(countWithOrderAndLimit).as("SELECT with ORDER BY and LIMIT should return all 5 records").isEqualTo(5);
  }

  @Test
  void orderByIndexedPropertyOnlyNulls() {
    // Edge case: all records have NULL for the indexed property
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".num LONG");
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (num) NOTUNIQUE");

    // Insert records: all with NULL for num
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'a'");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'b'");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'c'");
    });

    // With ORDER BY - should return all 3 records
    int countWithOrder;
    try (ResultSet result = database.query("sql", "SELECT FROM " + TYPE_NAME + " ORDER BY num")) {
      countWithOrder = 0;
      while (result.hasNext()) {
        result.next();
        countWithOrder++;
      }
    }
    assertThat(countWithOrder).as("SELECT with ORDER BY on all-NULL column should return all 3 records").isEqualTo(3);
  }

  @Test
  void orderByNonIndexedPropertyWithNulls() {
    // Control test: ORDER BY on a non-indexed property should work correctly
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".num LONG");
    // No index on num

    // Insert records
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 1");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET num = 2");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " CONTENT {}"); // NULL
    });

    // Without index, ORDER BY uses in-memory sorting which includes all records
    int countWithOrder;
    List<Long> values = new ArrayList<>();
    try (ResultSet result = database.query("sql", "SELECT FROM " + TYPE_NAME + " ORDER BY num")) {
      countWithOrder = 0;
      while (result.hasNext()) {
        Result r = result.next();
        values.add(r.getProperty("num"));
        countWithOrder++;
      }
    }
    assertThat(countWithOrder).as("SELECT with ORDER BY on non-indexed property should return all 3 records").isEqualTo(3);
    assertThat(values.get(0)).as("First value should be NULL").isNull();
  }
}
