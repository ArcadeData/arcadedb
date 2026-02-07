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

public class FilterStepTest extends TestHelper {

  @Test
  void shouldFilterRecordsBySimpleCondition() {
    database.getSchema().createDocumentType("TestPerson");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestPerson").set("age", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestPerson WHERE age > 5");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.<Integer>getProperty("age")).isGreaterThan(5);
      count++;
    }

    assertThat(count).isEqualTo(4); // ages 6, 7, 8, 9
    result.close();
  }

  @Test
  void shouldFilterRecordsWithMultipleConditions() {
    database.getSchema().createDocumentType("TestProduct");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestProduct").set("price", i * 10).set("active", i % 2 == 0).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestProduct WHERE price >= 30 AND active = true");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.<Integer>getProperty("price")).isGreaterThanOrEqualTo(30);
      assertThat(item.<Boolean>getProperty("active")).isTrue();
      count++;
    }

    assertThat(count).isEqualTo(3); // prices 40, 60, 80 (indexes 4, 6, 8 which are even and >= 3)
    result.close();
  }

  @Test
  void shouldHandleEmptyResultSet() {
    database.getSchema().createDocumentType("TestData");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.newDocument("TestData").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestData WHERE value > 100");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldHandleNullValues() {
    database.getSchema().createDocumentType("TestUser");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        final var doc = database.newDocument("TestUser");
        if (i < 3)
          doc.set("name", "user" + i);

        doc.save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestUser WHERE name IS NOT NULL");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String name = item.getProperty("name");
      assertThat(name).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldFilterWithLikeOperator() {
    database.getSchema().createDocumentType("TestEmployee");

    database.transaction(() -> {
      database.newDocument("TestEmployee").set("name", "John Smith").save();
      database.newDocument("TestEmployee").set("name", "Jane Smith").save();
      database.newDocument("TestEmployee").set("name", "Bob Johnson").save();
      database.newDocument("TestEmployee").set("name", "Alice Williams").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestEmployee WHERE name LIKE '%Smith'");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.<String>getProperty("name")).endsWith("Smith");
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldFilterWithInOperator() {
    database.getSchema().createDocumentType("TestItem");

    database.transaction(() -> {
      database.newDocument("TestItem").set("category", "electronics").save();
      database.newDocument("TestItem").set("category", "books").save();
      database.newDocument("TestItem").set("category", "clothing").save();
      database.newDocument("TestItem").set("category", "electronics").save();
      database.newDocument("TestItem").set("category", "toys").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestItem WHERE category IN ['electronics', 'books']");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String category = item.getProperty("category");
      assertThat(category).isIn("electronics", "books");
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldFilterWithBetweenOperator() {
    database.getSchema().createDocumentType("TestScore");

    database.transaction(() -> {
      for (int i = 0; i <= 100; i += 10) {
        database.newDocument("TestScore").set("points", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestScore WHERE points BETWEEN 30 AND 70");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int points = item.getProperty("points");
      assertThat(points).isBetween(30, 70);
      count++;
    }

    assertThat(count).isEqualTo(5); // 30, 40, 50, 60, 70
    result.close();
  }

  @Test
  void shouldFilterWithNotOperator() {
    database.getSchema().createDocumentType("TestAccount");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestAccount").set("status", i % 3 == 0 ? "active" : "inactive").save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestAccount WHERE NOT (status = 'active')");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.<String>getProperty("status")).isEqualTo("inactive");
      count++;
    }

    assertThat(count).isEqualTo(6);
    result.close();
  }

  @Test
  void shouldFilterWithOrOperator() {
    database.getSchema().createDocumentType("TestOrder");

    database.transaction(() -> {
      database.newDocument("TestOrder").set("status", "pending").set("priority", "high").save();
      database.newDocument("TestOrder").set("status", "completed").set("priority", "low").save();
      database.newDocument("TestOrder").set("status", "cancelled").set("priority", "medium").save();
      database.newDocument("TestOrder").set("status", "pending").set("priority", "low").save();
      database.newDocument("TestOrder").set("status", "shipped").set("priority", "high").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrder WHERE status = 'pending' OR priority = 'high'");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String status = item.getProperty("status");
      final String priority = item.getProperty("priority");
      assertThat(status.equals("pending") || priority.equals("high")).isTrue();
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }
}
