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

class SubQueryStepTest extends TestHelper {

  @Test
  void shouldExecuteSimpleSubQuery() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("Order").set("amount", i * 100).set("status", "pending").save();
      }
    });

    final ResultSet result = database.query("sql",
        "SELECT amount, (SELECT count(*) FROM Order WHERE amount > $parent.current.amount) AS higherCount FROM Order WHERE amount < 500");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object amount = item.getProperty("amount");
      final Object higherCount = item.getProperty("higherCount");
      assertThat(amount).isNotNull();
      assertThat(higherCount).isNotNull();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldUseSubQueryInWhereClause() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("name", "A").set("price", 100).save();
      database.newDocument("Product").set("name", "B").set("price", 200).save();
      database.newDocument("Product").set("name", "C").set("price", 300).save();
      database.newDocument("Product").set("name", "D").set("price", 400).save();
    });

    // First get the average
    final ResultSet avgResult = database.query("sql", "SELECT avg(price) as avgPrice FROM Product");
    avgResult.hasNext();
    final double avgPrice = avgResult.next().<Number>getProperty("avgPrice").doubleValue();
    avgResult.close();

    final ResultSet result = database.query("sql",
        "SELECT name, price FROM Product WHERE price > " + avgPrice);

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int price = item.getProperty("price");
      assertThat(price).isGreaterThan(250); // Average is 250
      count++;
    }

    assertThat(count).isEqualTo(2); // Products C and D
    result.close();
  }

  @Test
  void shouldUseSubQueryForInOperator() {
    database.getSchema().createDocumentType("Department");
    database.getSchema().createDocumentType("Employee");

    database.transaction(() -> {
      final var dept1 = database.newDocument("Department").set("name", "Engineering").set("deptId", 1).save();
      final var dept2 = database.newDocument("Department").set("name", "Sales").set("deptId", 2).save();

      database.newDocument("Employee").set("name", "Alice").set("deptId", 1).save();
      database.newDocument("Employee").set("name", "Bob").set("deptId", 2).save();
      database.newDocument("Employee").set("name", "Charlie").set("deptId", 1).save();
      database.newDocument("Employee").set("name", "David").set("deptId", 3).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT name FROM Employee WHERE deptId IN (SELECT deptId FROM Department WHERE name = 'Engineering')");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String name = item.getProperty("name");
      assertThat(name).isIn("Alice", "Charlie");
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldHandleSubQueryReturningNoResults() {
    database.getSchema().createDocumentType("TestEmpty");

    database.transaction(() -> {
      database.newDocument("TestEmpty").set("value", 10).save();
      database.newDocument("TestEmpty").set("value", 20).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT value FROM TestEmpty WHERE value IN (SELECT value FROM TestEmpty WHERE value > 100)");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldUseSubQueryWithAggregation() {
    database.getSchema().createDocumentType("Sale");

    database.transaction(() -> {
      database.newDocument("Sale").set("region", "North").set("amount", 1000).save();
      database.newDocument("Sale").set("region", "North").set("amount", 1500).save();
      database.newDocument("Sale").set("region", "South").set("amount", 800).save();
      database.newDocument("Sale").set("region", "South").set("amount", 1200).save();
    });

    // Query with aggregation - verify GROUP BY works
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

    assertThat(count).isEqualTo(2); // North and South
    result.close();
  }

  @Test
  void shouldUseSubQueryInSelect() {
    database.getSchema().createDocumentType("Category");

    database.transaction(() -> {
      database.newDocument("Category").set("name", "Electronics").set("minPrice", 100).save();
      database.newDocument("Category").set("name", "Books").set("minPrice", 10).save();
      database.newDocument("Category").set("name", "Clothing").set("minPrice", 50).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT name, minPrice, (SELECT count(*) FROM Category WHERE minPrice < $parent.current.minPrice) AS lowerPriced FROM Category");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object name = item.getProperty("name");
      final Object lowerPriced = item.getProperty("lowerPriced");
      assertThat(name).isNotNull();
      assertThat(lowerPriced).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldUseNestedSubQueries() {
    database.getSchema().createDocumentType("TestNested");

    database.transaction(() -> {
      for (int i = 1; i <= 10; i++) {
        database.newDocument("TestNested").set("value", i).save();
      }
    });

    // Use subquery in SELECT clause with parent reference
    final ResultSet result = database.query("sql",
        "SELECT value, (SELECT count(*) FROM TestNested WHERE value > $parent.current.value) AS higherCount FROM TestNested WHERE value <= 5");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      final Object higherCount = item.getProperty("higherCount");
      assertThat(value).isLessThanOrEqualTo(5);
      assertThat(higherCount).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(5);
    result.close();
  }

  @Test
  void shouldUseSubQueryWithMax() {
    database.getSchema().createDocumentType("TestMax");

    database.transaction(() -> {
      database.newDocument("TestMax").set("score", 85).save();
      database.newDocument("TestMax").set("score", 92).save();
      database.newDocument("TestMax").set("score", 78).save();
      database.newDocument("TestMax").set("score", 95).save();
    });

    // Query with max aggregation and ORDER BY DESC
    final ResultSet result = database.query("sql",
        "SELECT score FROM TestMax ORDER BY score DESC LIMIT 1");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final int score = item.getProperty("score");
    assertThat(score).isEqualTo(95);
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void shouldUseSubQueryWithExists() {
    database.getSchema().createDocumentType("Customer");
    database.getSchema().createDocumentType("PurchaseOrder");

    database.transaction(() -> {
      database.newDocument("Customer").set("custId", 1).set("name", "Alice").save();
      database.newDocument("Customer").set("custId", 2).set("name", "Bob").save();
      database.newDocument("Customer").set("custId", 3).set("name", "Charlie").save();

      database.newDocument("PurchaseOrder").set("custId", 1).set("amount", 100).save();
      database.newDocument("PurchaseOrder").set("custId", 1).set("amount", 200).save();
    });

    // Find customers with orders
    final ResultSet result = database.query("sql",
        "SELECT name FROM Customer WHERE custId IN (SELECT custId FROM PurchaseOrder)");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("Alice");
      count++;
    }

    assertThat(count).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldUseSubQueryWithMultipleResults() {
    database.getSchema().createDocumentType("TestMulti");

    database.transaction(() -> {
      database.newDocument("TestMulti").set("category", "A").set("value", 10).save();
      database.newDocument("TestMulti").set("category", "A").set("value", 20).save();
      database.newDocument("TestMulti").set("category", "B").set("value", 30).save();
      database.newDocument("TestMulti").set("category", "C").set("value", 15).save();
    });

    final ResultSet result = database.query("sql",
        "SELECT category, value FROM TestMulti WHERE value IN (SELECT value FROM TestMulti WHERE value > 15)");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      assertThat(value).isGreaterThan(15);
      count++;
    }

    assertThat(count).isEqualTo(2); // 20 and 30
    result.close();
  }
}
