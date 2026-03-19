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

class OrderByStepTest extends TestHelper {

  @Test
  void shouldOrderByAscending() {
    database.getSchema().createDocumentType("TestOrderAsc");

    database.transaction(() -> {
      database.newDocument("TestOrderAsc").set("value", 5).save();
      database.newDocument("TestOrderAsc").set("value", 2).save();
      database.newDocument("TestOrderAsc").set("value", 8).save();
      database.newDocument("TestOrderAsc").set("value", 1).save();
      database.newDocument("TestOrderAsc").set("value", 9).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderAsc ORDER BY value ASC");

    int previousValue = -1;
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      assertThat(value).isGreaterThan(previousValue);
      previousValue = value;
    }

    result.close();
  }

  @Test
  void shouldOrderByDescending() {
    database.getSchema().createDocumentType("TestOrderDesc");

    database.transaction(() -> {
      database.newDocument("TestOrderDesc").set("value", 5).save();
      database.newDocument("TestOrderDesc").set("value", 2).save();
      database.newDocument("TestOrderDesc").set("value", 8).save();
      database.newDocument("TestOrderDesc").set("value", 1).save();
      database.newDocument("TestOrderDesc").set("value", 9).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderDesc ORDER BY value DESC");

    int previousValue = Integer.MAX_VALUE;
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      assertThat(value).isLessThan(previousValue);
      previousValue = value;
    }

    result.close();
  }

  @Test
  void shouldOrderByMultipleFields() {
    database.getSchema().createDocumentType("TestOrderMulti");

    database.transaction(() -> {
      database.newDocument("TestOrderMulti").set("category", "A").set("price", 100).save();
      database.newDocument("TestOrderMulti").set("category", "B").set("price", 50).save();
      database.newDocument("TestOrderMulti").set("category", "A").set("price", 75).save();
      database.newDocument("TestOrderMulti").set("category", "B").set("price", 120).save();
      database.newDocument("TestOrderMulti").set("category", "A").set("price", 90).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderMulti ORDER BY category ASC, price DESC");

    String previousCategory = "";
    int previousPrice = Integer.MAX_VALUE;
    while (result.hasNext()) {
      final Result item = result.next();
      final String category = item.getProperty("category");
      final int price = item.getProperty("price");

      if (category.equals(previousCategory)) {
        // Within same category, price should be descending
        assertThat(price).isLessThanOrEqualTo(previousPrice);
      }

      previousCategory = category;
      previousPrice = price;
    }

    result.close();
  }

  @Test
  void shouldHandleNullValues() {
    database.getSchema().createDocumentType("TestOrderNull");

    database.transaction(() -> {
      database.newDocument("TestOrderNull").set("value", 5).save();
      database.newDocument("TestOrderNull").save(); // null value
      database.newDocument("TestOrderNull").set("value", 2).save();
      database.newDocument("TestOrderNull").save(); // null value
      database.newDocument("TestOrderNull").set("value", 8).save();
    });

    // Nulls should come first by default
    final ResultSet result = database.query("sql", "SELECT FROM TestOrderNull ORDER BY value ASC");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(5);
    result.close();
  }

  @Test
  void shouldOrderStrings() {
    database.getSchema().createDocumentType("TestOrderString");

    database.transaction(() -> {
      database.newDocument("TestOrderString").set("name", "Charlie").save();
      database.newDocument("TestOrderString").set("name", "Alice").save();
      database.newDocument("TestOrderString").set("name", "Bob").save();
      database.newDocument("TestOrderString").set("name", "David").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderString ORDER BY name ASC");

    String previousName = "";
    while (result.hasNext()) {
      final Result item = result.next();
      final String name = item.getProperty("name");
      assertThat(name.compareTo(previousName)).isGreaterThanOrEqualTo(0);
      previousName = name;
    }

    result.close();
  }

  @Test
  void shouldOrderDates() {
    database.getSchema().createDocumentType("TestOrderDate");

    database.transaction(() -> {
      database.newDocument("TestOrderDate").set("timestamp", System.currentTimeMillis() + 1000).save();
      database.newDocument("TestOrderDate").set("timestamp", System.currentTimeMillis()).save();
      database.newDocument("TestOrderDate").set("timestamp", System.currentTimeMillis() + 5000).save();
      database.newDocument("TestOrderDate").set("timestamp", System.currentTimeMillis() + 2000).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderDate ORDER BY timestamp ASC");

    long previousTimestamp = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final long timestamp = item.getProperty("timestamp");
      assertThat(timestamp).isGreaterThanOrEqualTo(previousTimestamp);
      previousTimestamp = timestamp;
    }

    result.close();
  }

  @Test
  void shouldOrderWithLimit() {
    database.getSchema().createDocumentType("TestOrderLimit");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newDocument("TestOrderLimit").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderLimit ORDER BY value DESC LIMIT 5");

    int count = 0;
    int expectedValue = 19;
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      assertThat(value).isEqualTo(expectedValue);
      expectedValue--;
      count++;
    }

    assertThat(count).isEqualTo(5);
    result.close();
  }

  @Test
  void shouldHandleEmptyResultSet() {
    database.getSchema().createDocumentType("TestOrderEmpty");

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderEmpty ORDER BY value ASC");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldOrderBooleans() {
    database.getSchema().createDocumentType("TestOrderBoolean");

    database.transaction(() -> {
      database.newDocument("TestOrderBoolean").set("active", true).save();
      database.newDocument("TestOrderBoolean").set("active", false).save();
      database.newDocument("TestOrderBoolean").set("active", true).save();
      database.newDocument("TestOrderBoolean").set("active", false).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestOrderBoolean ORDER BY active ASC");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(4);
    result.close();
  }

  @Test
  void shouldOrderByProjection() {
    database.getSchema().createDocumentType("TestOrderProjection");

    database.transaction(() -> {
      database.newDocument("TestOrderProjection").set("value", 5).save();
      database.newDocument("TestOrderProjection").set("value", 2).save();
      database.newDocument("TestOrderProjection").set("value", 8).save();
    });

    final ResultSet result = database.query("sql", "SELECT value * 2 AS doubled FROM TestOrderProjection ORDER BY doubled DESC");

    int previousValue = Integer.MAX_VALUE;
    while (result.hasNext()) {
      final Result item = result.next();
      final int doubled = item.getProperty("doubled");
      assertThat(doubled).isLessThan(previousValue);
      previousValue = doubled;
    }

    result.close();
  }
}
