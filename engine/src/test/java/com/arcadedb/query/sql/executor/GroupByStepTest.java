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

class GroupByStepTest extends TestHelper {

  @Test
  void shouldGroupBySingleField() {
    database.getSchema().createDocumentType("Sale");

    database.transaction(() -> {
      database.newDocument("Sale").set("region", "North").set("amount", 100).save();
      database.newDocument("Sale").set("region", "North").set("amount", 200).save();
      database.newDocument("Sale").set("region", "South").set("amount", 150).save();
      database.newDocument("Sale").set("region", "South").set("amount", 250).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT region, sum(amount) AS total FROM Sale GROUP BY region");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String region = item.getProperty("region");
      final long total = item.<Number>getProperty("total").longValue();
      assertThat(region).isIn("North", "South");
      assertThat(total).isGreaterThan(0);
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldGroupByMultipleFields() {
    database.getSchema().createDocumentType("Transaction");

    database.transaction(() -> {
      database.newDocument("Transaction").set("region", "East").set("category", "A").set("amount", 100).save();
      database.newDocument("Transaction").set("region", "East").set("category", "A").set("amount", 150).save();
      database.newDocument("Transaction").set("region", "East").set("category", "B").set("amount", 200).save();
      database.newDocument("Transaction").set("region", "West").set("category", "A").set("amount", 300).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT region, category, sum(amount) AS total FROM Transaction GROUP BY region, category");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String region = item.getProperty("region");
      final String category = item.getProperty("category");
      final long total = item.<Number>getProperty("total").longValue();
      assertThat(region).isNotNull();
      assertThat(category).isNotNull();
      assertThat(total).isGreaterThan(0);
      count++;
    }

    assertThat(count).isEqualTo(3); // East-A, East-B, West-A
    result.close();
  }

  @Test
  void shouldGroupByWithCount() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("status", "pending").save();
      database.newDocument("Order").set("status", "pending").save();
      database.newDocument("Order").set("status", "completed").save();
      database.newDocument("Order").set("status", "completed").save();
      database.newDocument("Order").set("status", "completed").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT status, count(*) AS orderCount FROM Order GROUP BY status");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String status = item.getProperty("status");
      final long orderCount = item.<Number>getProperty("orderCount").longValue();

      if ("pending".equals(status)) {
        assertThat(orderCount).isEqualTo(2);
      } else if ("completed".equals(status)) {
        assertThat(orderCount).isEqualTo(3);
      }
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldGroupByWithAverage() {
    database.getSchema().createDocumentType("Student");

    database.transaction(() -> {
      database.newDocument("Student").set("grade", "A").set("score", 90).save();
      database.newDocument("Student").set("grade", "A").set("score", 95).save();
      database.newDocument("Student").set("grade", "B").set("score", 80).save();
      database.newDocument("Student").set("grade", "B").set("score", 85).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT grade, avg(score) AS avgScore FROM Student GROUP BY grade");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String grade = item.getProperty("grade");
      final double avgScore = item.<Number>getProperty("avgScore").doubleValue();

      if ("A".equals(grade)) {
        assertThat(avgScore).isGreaterThanOrEqualTo(92.0).isLessThanOrEqualTo(93.0);
      } else if ("B".equals(grade)) {
        assertThat(avgScore).isGreaterThanOrEqualTo(82.0).isLessThanOrEqualTo(83.0);
      }
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldGroupByWithMinMax() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("category", "Electronics").set("price", 100).save();
      database.newDocument("Product").set("category", "Electronics").set("price", 500).save();
      database.newDocument("Product").set("category", "Books").set("price", 10).save();
      database.newDocument("Product").set("category", "Books").set("price", 50).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT category, min(price) AS minPrice, max(price) AS maxPrice FROM Product GROUP BY category");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String category = item.getProperty("category");
      final int minPrice = item.<Number>getProperty("minPrice").intValue();
      final int maxPrice = item.<Number>getProperty("maxPrice").intValue();

      assertThat(minPrice).isLessThan(maxPrice);
      assertThat(category).isIn("Electronics", "Books");
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldGroupByWithOrderBy() {
    database.getSchema().createDocumentType("Employee");

    database.transaction(() -> {
      database.newDocument("Employee").set("department", "Sales").set("salary", 50000).save();
      database.newDocument("Employee").set("department", "Sales").set("salary", 60000).save();
      database.newDocument("Employee").set("department", "Engineering").set("salary", 80000).save();
      database.newDocument("Employee").set("department", "Engineering").set("salary", 90000).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT department, avg(salary) AS avgSalary FROM Employee GROUP BY department ORDER BY avgSalary DESC");

    assertThat(result.hasNext()).isTrue();
    final Result first = result.next();
    assertThat(first.<String>getProperty("department")).isEqualTo("Engineering");

    assertThat(result.hasNext()).isTrue();
    final Result second = result.next();
    assertThat(second.<String>getProperty("department")).isEqualTo("Sales");

    result.close();
  }

  @Test
  void shouldGroupByWithMultipleAggregations() {
    database.getSchema().createDocumentType("Invoice");

    database.transaction(() -> {
      database.newDocument("Invoice").set("customer", "A").set("amount", 100).save();
      database.newDocument("Invoice").set("customer", "A").set("amount", 200).save();
      database.newDocument("Invoice").set("customer", "B").set("amount", 150).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT customer, count(*) AS invoiceCount, sum(amount) AS totalAmount, avg(amount) AS avgAmount FROM Invoice GROUP BY customer");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String customer = item.getProperty("customer");
      final long invoiceCount = item.<Number>getProperty("invoiceCount").longValue();
      final long totalAmount = item.<Number>getProperty("totalAmount").longValue();
      final Object avgAmount = item.getProperty("avgAmount");

      assertThat(customer).isIn("A", "B");
      assertThat(invoiceCount).isGreaterThan(0);
      assertThat(totalAmount).isGreaterThan(0);
      assertThat(avgAmount).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldGroupBySingleGroup() {
    database.getSchema().createDocumentType("Data");

    database.transaction(() -> {
      database.newDocument("Data").set("type", "test").set("value", 10).save();
      database.newDocument("Data").set("type", "test").set("value", 20).save();
      database.newDocument("Data").set("type", "test").set("value", 30).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT type, count(*) AS recordCount FROM Data GROUP BY type");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("type")).isEqualTo("test");
    assertThat(item.<Number>getProperty("recordCount").longValue()).isEqualTo(3);
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void shouldGroupByWithLimit() {
    database.getSchema().createDocumentType("Purchase");

    database.transaction(() -> {
      database.newDocument("Purchase").set("store", "A").set("amount", 100).save();
      database.newDocument("Purchase").set("store", "B").set("amount", 200).save();
      database.newDocument("Purchase").set("store", "C").set("amount", 300).save();
      database.newDocument("Purchase").set("store", "D").set("amount", 400).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT store, sum(amount) AS total FROM Purchase GROUP BY store ORDER BY total DESC LIMIT 2");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldGroupByNumericField() {
    database.getSchema().createDocumentType("Score");

    database.transaction(() -> {
      database.newDocument("Score").set("points", 10).set("player", "A").save();
      database.newDocument("Score").set("points", 10).set("player", "B").save();
      database.newDocument("Score").set("points", 20).set("player", "C").save();
      database.newDocument("Score").set("points", 20).set("player", "D").save();
      database.newDocument("Score").set("points", 20).set("player", "E").save();
    });

    final ResultSet result = database.query("sql",
        "SELECT points, count(*) AS playerCount FROM Score GROUP BY points");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int points = item.getProperty("points");
      final long playerCount = item.<Number>getProperty("playerCount").longValue();

      if (points == 10) {
        assertThat(playerCount).isEqualTo(2);
      } else if (points == 20) {
        assertThat(playerCount).isEqualTo(3);
      }
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }
}
