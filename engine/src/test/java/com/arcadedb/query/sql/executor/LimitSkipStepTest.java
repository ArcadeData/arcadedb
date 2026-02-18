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

class LimitSkipStepTest extends TestHelper {

  @Test
  void shouldLimitResults() {
    database.getSchema().createDocumentType("TestLimit");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("TestLimit").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestLimit LIMIT 10");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(10);
    result.close();
  }

  @Test
  void shouldSkipResults() {
    database.getSchema().createDocumentType("TestSkip");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newDocument("TestSkip").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestSkip SKIP 15");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(5); // 20 - 15 = 5
    result.close();
  }

  @Test
  void shouldCombineLimitAndSkip() {
    database.getSchema().createDocumentType("TestPagination");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("TestPagination").set("value", i).save();
      }
    });

    // Second page of 10 records
    final ResultSet result = database.query("sql", "SELECT FROM TestPagination SKIP 10 LIMIT 10");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(10);
    result.close();
  }

  @Test
  void shouldHandleLimitZero() {
    database.getSchema().createDocumentType("TestLimitZero");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestLimitZero").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestLimitZero LIMIT 0");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldHandleSkipGreaterThanResultSize() {
    database.getSchema().createDocumentType("TestSkipLarge");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestSkipLarge").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestSkipLarge SKIP 20");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldHandleLimitGreaterThanResultSize() {
    database.getSchema().createDocumentType("TestLimitLarge");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.newDocument("TestLimitLarge").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestLimitLarge LIMIT 100");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(5); // Only 5 records exist
    result.close();
  }

  @Test
  void shouldWorkWithOrderBy() {
    database.getSchema().createDocumentType("TestOrderLimit");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newDocument("TestOrderLimit").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderLimit ORDER BY value DESC LIMIT 5");

    int count = 0;
    int lastValue = Integer.MAX_VALUE;
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      assertThat(value).isLessThan(lastValue);
      lastValue = value;
      count++;
    }

    assertThat(count).isEqualTo(5);
    assertThat(lastValue).isEqualTo(15); // Top 5 descending: 19, 18, 17, 16, 15
    result.close();
  }

  @Test
  void shouldWorkWithWhereClause() {
    database.getSchema().createDocumentType("TestWhereLimitSkip");

    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        database.newDocument("TestWhereLimitSkip").set("value", i).save();
      }
    });

    // Filter for even numbers, skip first 5, limit to 10
    final ResultSet result = database.query("sql", "SELECT FROM TestWhereLimitSkip WHERE value % 2 = 0 SKIP 5 LIMIT 10");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      assertThat(value % 2).isEqualTo(0); // Should be even
      count++;
    }

    assertThat(count).isEqualTo(10);
    result.close();
  }

  @Test
  void shouldHandleMultiplePagination() {
    database.getSchema().createDocumentType("TestMultiPage");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("TestMultiPage").set("id", i).save();
      }
    });

    // Test pagination: page 1, page 2, page 3
    final int pageSize = 10;

    for (int page = 0; page < 3; page++) {
      final int skip = page * pageSize;
      final ResultSet result = database.query("sql", "SELECT FROM TestMultiPage ORDER BY id SKIP " + skip + " LIMIT " + pageSize);

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }

      assertThat(count).isEqualTo(pageSize);
      result.close();
    }
  }

  @Test
  void shouldHandleLimitOne() {
    database.getSchema().createDocumentType("TestLimitOne");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestLimitOne").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestLimitOne LIMIT 1");

    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }
}
