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

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for GitHub issue #3116: Inconsistent Date Handling in Queries
 * <p>
 * This test verifies that date comparisons work consistently regardless of whether
 * the .asDate() method is used or not.
 * <p>
 * The reported inconsistencies are:
 * 1. `birthDate = '1952-08-06'` requires different handling than `birthDate > '1952-08-06'.asDate()`
 * 2. `birthDate > '1952-08-05'.asDate() AND birthDate < '1952-08-07'.asDate()` fails
 *    but `birthDate > '1952-08-05' AND birthDate < '1952-08-07'` works
 */
public class DateQueryConsistencyTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      // Create schema as reported in the issue
      database.getSchema().createDocumentType("Person");
      database.command("sql", "CREATE PROPERTY Person.birthDate DATE");
      database.command("sql", "CREATE PROPERTY Person.nameFamily STRING");
      database.command("sql", "CREATE PROPERTY Person.nameGiven STRING");

      // Insert test data
      database.command("sql",
          "INSERT INTO Person SET nameFamily = 'Withington', nameGiven = 'Paul', birthDate = ?",
          LocalDate.of(1952, 8, 6));

      // Insert additional records for range queries
      database.command("sql",
          "INSERT INTO Person SET nameFamily = 'Smith', nameGiven = 'John', birthDate = ?",
          LocalDate.of(1952, 8, 5));

      database.command("sql",
          "INSERT INTO Person SET nameFamily = 'Jones', nameGiven = 'Jane', birthDate = ?",
          LocalDate.of(1952, 8, 7));
    });
  }

  /**
   * Test equality comparison with plain string (no .asDate())
   * Expected: Should work
   */
  @Test
  void equalityWithPlainString() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE nameFamily = 'Withington' AND nameGiven = 'Paul' AND birthDate = '1952-08-06'");

      // Query with plain string equality should find the record
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("nameGiven")).isEqualTo("Paul");
      assertThat(result.hasNext()).isFalse();
    });
  }

  /**
   * Test equality comparison with .asDate() method
   * According to the issue, this pattern should also work
   */
  @Test
  void equalityWithAsDate() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE nameFamily = 'Withington' AND nameGiven = 'Paul' AND birthDate = '1952-08-06'.asDate()");

      // Query with .asDate() equality should find the record
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("nameGiven")).isEqualTo("Paul");
      assertThat(result.hasNext()).isFalse();
    });
  }

  /**
   * Test greater-than comparison with .asDate() method
   * According to the issue, this works
   */
  @Test
  void greaterThanWithAsDate() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE birthDate > '1952-08-05'.asDate()");

      // Should find Paul (Aug 6) and Jane (Aug 7)
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      // Query with > .asDate() should find 2 records
      assertThat(count).isEqualTo(2);
    });
  }

  /**
   * Test greater-than comparison with plain string (no .asDate())
   */
  @Test
  void greaterThanWithPlainString() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE birthDate > '1952-08-05'");

      // Should find Paul (Aug 6) and Jane (Aug 7)
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      // Query with > plain string should find 2 records
      assertThat(count).isEqualTo(2);
    });
  }

  /**
   * Test range query with multiple .asDate() calls
   * According to the issue, this FAILS but shouldn't
   */
  @Test
  void rangeQueryWithAsDate() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE birthDate > '1952-08-05'.asDate() AND birthDate < '1952-08-07'.asDate()");

      // Should find only Paul (Aug 6) - Range query with .asDate() should find the record
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("nameGiven")).isEqualTo("Paul");
      assertThat(result.hasNext()).isFalse();
    });
  }

  /**
   * Test range query with plain strings (no .asDate())
   * According to the issue, this WORKS
   */
  @Test
  void rangeQueryWithPlainStrings() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE birthDate > '1952-08-05' AND birthDate < '1952-08-07'");

      // Should find only Paul (Aug 6) - Range query with plain strings should find the record
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("nameGiven")).isEqualTo("Paul");
      assertThat(result.hasNext()).isFalse();
    });
  }

  /**
   * Test mixed comparison: one with .asDate(), one without
   */
  @Test
  void mixedComparison() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE birthDate > '1952-08-05'.asDate() AND birthDate < '1952-08-07'");

      // Should find only Paul (Aug 6) - Mixed comparison should find the record
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("nameGiven")).isEqualTo("Paul");
      assertThat(result.hasNext()).isFalse();
    });
  }

  /**
   * Test less-than comparison with .asDate() method
   */
  @Test
  void lessThanWithAsDate() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE birthDate < '1952-08-07'.asDate()");

      // Should find John (Aug 5) and Paul (Aug 6)
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      // Query with < .asDate() should find 2 records
      assertThat(count).isEqualTo(2);
    });
  }

  /**
   * Test less-than-or-equal comparison with .asDate() method
   */
  @Test
  void lessThanOrEqualWithAsDate() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE birthDate <= '1952-08-06'.asDate()");

      // Should find John (Aug 5) and Paul (Aug 6)
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      // Query with <= .asDate() should find 2 records
      assertThat(count).isEqualTo(2);
    });
  }

  /**
   * Test greater-than-or-equal comparison with .asDate() method
   */
  @Test
  void greaterThanOrEqualWithAsDate() {
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT FROM Person WHERE birthDate >= '1952-08-06'.asDate()");

      // Should find Paul (Aug 6) and Jane (Aug 7)
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      // Query with >= .asDate() should find 2 records
      assertThat(count).isEqualTo(2);
    });
  }
}
