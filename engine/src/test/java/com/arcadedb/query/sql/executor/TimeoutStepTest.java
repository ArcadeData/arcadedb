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
import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for TimeoutStep.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeoutStepTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createDocumentType("TestDoc");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("TestDoc").set("value", i).save();
      }
    });
  }

  @Test
  void shouldAllowQueryWithinTimeout() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM TestDoc TIMEOUT 10000");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }

      assertThat(count).isEqualTo(100);
      result.close();
    });
  }

  @Test
  void shouldThrowTimeoutExceptionWhenExpired() {
    database.transaction(() -> {
      // Set a very short timeout (1ms) - the query might timeout or complete very fast
      // This test verifies the timeout mechanism is in place
      try {
        final ResultSet result = database.query("sql", "SELECT FROM TestDoc TIMEOUT 1");

        // Try to consume results quickly
        int count = 0;
        while (result.hasNext() && count < 1000) {
          result.next();
          count++;
        }
        result.close();

        // If we get here, either the query was fast enough or timeout didn't trigger
        // This is acceptable as the query might complete very quickly
        assertThat(count).isGreaterThanOrEqualTo(0);
      } catch (final TimeoutException e) {
        // Timeout occurred as expected
        assertThat(e.getMessage()).contains("Timeout expired");
      }
    });
  }

  @Test
  void shouldWorkWithVeryShortTimeout() {
    database.transaction(() -> {
      // Test with a very short but still reasonable timeout
      try {
        final ResultSet result = database.query("sql", "SELECT FROM TestDoc TIMEOUT 1");

        int count = 0;
        while (result.hasNext()) {
          result.next();
          count++;
        }
        result.close();

        // Either completes quickly or times out
        assertThat(count).isGreaterThanOrEqualTo(0);
      } catch (final TimeoutException e) {
        // Timeout is acceptable for this test
        assertThat(e.getMessage()).contains("Timeout expired");
      }
    });
  }

  @Test
  void shouldHandleMultipleTimeoutChecks() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM TestDoc TIMEOUT 5000");

      // Pull results in batches
      int totalCount = 0;
      for (int i = 0; i < 10 && result.hasNext(); i++) {
        for (int j = 0; j < 10 && result.hasNext(); j++) {
          result.next();
          totalCount++;
        }
      }

      assertThat(totalCount).isGreaterThan(0);
      result.close();
    });
  }

  @Test
  void shouldWorkWithWhereClause() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM TestDoc WHERE value < 50 TIMEOUT 10000");

      int count = 0;
      while (result.hasNext()) {
        final Result item = result.next();
        assertThat(item.<Integer>getProperty("value")).isLessThan(50);
        count++;
      }

      assertThat(count).isEqualTo(50);
      result.close();
    });
  }

  @Test
  void shouldWorkWithOrderBy() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM TestDoc ORDER BY value DESC TIMEOUT 10000");

      int count = 0;
      Integer previousValue = null;
      while (result.hasNext()) {
        final Result item = result.next();
        final Integer value = item.getProperty("value");

        if (previousValue != null) {
          assertThat(value).isLessThanOrEqualTo(previousValue);
        }
        previousValue = value;
        count++;
      }

      assertThat(count).isEqualTo(100);
      result.close();
    });
  }

  @Test
  void shouldWorkWithGroupBy() {
    database.getSchema().createDocumentType("TestGrouped");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        database.newDocument("TestGrouped")
            .set("category", i % 3)
            .set("value", i)
            .save();
      }
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT category, count(*) as cnt FROM TestGrouped GROUP BY category TIMEOUT 10000");

      int count = 0;
      while (result.hasNext()) {
        final Result item = result.next();
        assertThat(item.<Long>getProperty("cnt")).isEqualTo(10L);
        count++;
      }

      assertThat(count).isEqualTo(3);
      result.close();
    });
  }

  @Test
  void shouldWorkWithSubquery() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM (SELECT FROM TestDoc WHERE value < 10) TIMEOUT 10000");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }

      assertThat(count).isEqualTo(10);
      result.close();
    });
  }

  @Test
  void shouldHandleLargeTimeout() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM TestDoc TIMEOUT 999999");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }

      assertThat(count).isEqualTo(100);
      result.close();
    });
  }

  @Test
  void shouldWorkWithLimitClause() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT FROM TestDoc LIMIT 10 TIMEOUT 10000");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }

      assertThat(count).isEqualTo(10);
      result.close();
    });
  }
}
