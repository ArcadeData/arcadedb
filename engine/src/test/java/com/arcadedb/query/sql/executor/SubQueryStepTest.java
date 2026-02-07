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

public class SubQueryStepTest extends TestHelper {

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
      assertThat(item.getProperty("amount")).isNotNull();
      assertThat(item.getProperty("higherCount")).isNotNull();
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

    final ResultSet result = database.query("sql",
        "SELECT name, price FROM Product WHERE price > (SELECT avg(price) FROM Product)");

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

    final ResultSet result = database.query("sql",
        "SELECT region, sum(amount) AS total FROM Sale GROUP BY region HAVING sum(amount) > (SELECT avg(amount) FROM Sale)");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final long total = item.<Number>getProperty("total").longValue();
      assertThat(total).isGreaterThan(1100); // Avg is ~1125
      count++;
    }

    assertThat(count).isGreaterThan(0);
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
      assertThat(item.getProperty("name")).isNotNull();
      assertThat(item.getProperty("lowerPriced")).isNotNull();
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

    final ResultSet result = database.query("sql",
        "SELECT value FROM TestNested WHERE value > (SELECT avg(value) FROM TestNested WHERE value > (SELECT min(value) FROM TestNested))");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int value = item.getProperty("value");
      assertThat(value).isGreaterThan(5); // Should be values above average
      count++;
    }

    assertThat(count).isGreaterThan(0);
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

    final ResultSet result = database.query("sql",
        "SELECT score FROM TestMax WHERE score = (SELECT max(score) FROM TestMax)");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<Integer>getProperty("score")).isEqualTo(95);
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
