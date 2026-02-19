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

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class DistinctExecutionStepTest extends TestHelper {

  @Test
  void shouldReturnDistinctValues() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("category", "Electronics").save();
      database.newDocument("Product").set("category", "Electronics").save();
      database.newDocument("Product").set("category", "Books").save();
      database.newDocument("Product").set("category", "Books").save();
      database.newDocument("Product").set("category", "Clothing").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT category FROM Product");

    final Set<String> categories = new HashSet<>();
    while (result.hasNext()) {
      final Result item = result.next();
      final String category = item.getProperty("category");
      categories.add(category);
    }

    assertThat(categories).containsExactlyInAnyOrder("Electronics", "Books", "Clothing");
    result.close();
  }

  @Test
  void shouldReturnDistinctWithMultipleFields() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("customer", "A").set("status", "pending").save();
      database.newDocument("Order").set("customer", "A").set("status", "pending").save();
      database.newDocument("Order").set("customer", "A").set("status", "completed").save();
      database.newDocument("Order").set("customer", "B").set("status", "pending").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT customer, status FROM Order");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(3); // A-pending, A-completed, B-pending
    result.close();
  }

  @Test
  void shouldReturnDistinctNumbers() {
    database.getSchema().createDocumentType("Score");

    database.transaction(() -> {
      database.newDocument("Score").set("value", 10).save();
      database.newDocument("Score").set("value", 20).save();
      database.newDocument("Score").set("value", 10).save();
      database.newDocument("Score").set("value", 30).save();
      database.newDocument("Score").set("value", 20).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT value FROM Score");

    final Set<Integer> values = new HashSet<>();
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      values.add(value);
    }

    assertThat(values).containsExactlyInAnyOrder(10, 20, 30);
    result.close();
  }

  @Test
  void shouldReturnDistinctWithOrderBy() {
    database.getSchema().createDocumentType("Employee");

    database.transaction(() -> {
      database.newDocument("Employee").set("department", "Sales").save();
      database.newDocument("Employee").set("department", "Engineering").save();
      database.newDocument("Employee").set("department", "Sales").save();
      database.newDocument("Employee").set("department", "Marketing").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT department FROM Employee ORDER BY department ASC");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("department")).isEqualTo("Engineering");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("department")).isEqualTo("Marketing");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("department")).isEqualTo("Sales");

    result.close();
  }

  @Test
  void shouldReturnDistinctWithLimit() {
    database.getSchema().createDocumentType("Tag");

    database.transaction(() -> {
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "python").save();
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "javascript").save();
      database.newDocument("Tag").set("name", "python").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT name FROM Tag LIMIT 2");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldHandleDistinctWithAllSameValues() {
    database.getSchema().createDocumentType("Constant");

    database.transaction(() -> {
      database.newDocument("Constant").set("value", "same").save();
      database.newDocument("Constant").set("value", "same").save();
      database.newDocument("Constant").set("value", "same").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT value FROM Constant");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.<String>getProperty("value")).isEqualTo("same");
      count++;
    }

    assertThat(count).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldHandleDistinctWithAllUniqueValues() {
    database.getSchema().createDocumentType("UniqueData");

    database.transaction(() -> {
      database.newDocument("UniqueData").set("id", 1).save();
      database.newDocument("UniqueData").set("id", 2).save();
      database.newDocument("UniqueData").set("id", 3).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT id FROM UniqueData");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldReturnDistinctWithWhereClause() {
    database.getSchema().createDocumentType("Person");

    database.transaction(() -> {
      database.newDocument("Person").set("age", 25).set("city", "NYC").save();
      database.newDocument("Person").set("age", 30).set("city", "NYC").save();
      database.newDocument("Person").set("age", 25).set("city", "LA").save();
      database.newDocument("Person").set("age", 35).set("city", "NYC").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT city FROM Person WHERE age >= 30");

    final Set<String> cities = new HashSet<>();
    while (result.hasNext()) {
      final Result item = result.next();
      cities.add(item.getProperty("city"));
    }

    assertThat(cities).containsExactly("NYC");
    result.close();
  }

  @Test
  void shouldReturnDistinctCountCombination() {
    database.getSchema().createDocumentType("Event");

    database.transaction(() -> {
      database.newDocument("Event").set("type", "click").save();
      database.newDocument("Event").set("type", "view").save();
      database.newDocument("Event").set("type", "click").save();
      database.newDocument("Event").set("type", "click").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT type FROM Event");

    final Set<String> types = new HashSet<>();
    while (result.hasNext()) {
      final Result item = result.next();
      types.add(item.getProperty("type"));
    }

    assertThat(types).hasSize(2);
    assertThat(types).containsExactlyInAnyOrder("click", "view");
    result.close();
  }

  @Test
  void shouldHandleDistinctWithNullValues() {
    database.getSchema().createDocumentType("Data");

    database.transaction(() -> {
      database.newDocument("Data").set("value", "A").save();
      database.newDocument("Data").set("value", "B").save();
      database.newDocument("Data").set("value", "A").save();
      database.newDocument("Data").save(); // No value field
    });

    final ResultSet result = database.query("sql",
        "SELECT DISTINCT value FROM Data");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThanOrEqualTo(2); // At least A and B
    result.close();
  }
}
