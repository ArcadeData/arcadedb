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

public class UpdateExecutionStepTest extends TestHelper {

  @Test
  void shouldUpdateSingleField() {
    database.getSchema().createDocumentType("Customer");

    database.transaction(() -> {
      database.newDocument("Customer").set("name", "Alice").set("age", 30).save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Customer SET age = 31 WHERE name = 'Alice'");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Customer WHERE name = 'Alice'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<Integer>getProperty("age")).isEqualTo(31);
    result.close();
  }

  @Test
  void shouldUpdateMultipleFields() {
    database.getSchema().createDocumentType("Employee");

    database.transaction(() -> {
      database.newDocument("Employee").set("name", "Bob").set("salary", 50000).set("department", "Sales").save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Employee SET salary = 60000, department = 'Engineering' WHERE name = 'Bob'");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Employee WHERE name = 'Bob'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<Integer>getProperty("salary")).isEqualTo(60000);
    assertThat(item.<String>getProperty("department")).isEqualTo("Engineering");
    result.close();
  }

  @Test
  void shouldUpdateMultipleRecords() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("category", "Electronics").set("price", 100).save();
      database.newDocument("Product").set("category", "Electronics").set("price", 200).save();
      database.newDocument("Product").set("category", "Books").set("price", 50).save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Product SET price = price * 1.1 WHERE category = 'Electronics'");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Product WHERE category = 'Electronics'");
    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int price = item.<Number>getProperty("price").intValue();
      assertThat(price).isGreaterThan(100);
      count++;
    }
    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldUpdateWithWhereClause() {
    database.getSchema().createDocumentType("Task");

    database.transaction(() -> {
      database.newDocument("Task").set("title", "Task1").set("status", "pending").save();
      database.newDocument("Task").set("title", "Task2").set("status", "pending").save();
      database.newDocument("Task").set("title", "Task3").set("status", "completed").save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Task SET status = 'in_progress' WHERE status = 'pending'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Task WHERE status = 'in_progress'");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldUpdateToNullValue() {
    database.getSchema().createDocumentType("Document");

    database.transaction(() -> {
      database.newDocument("Document").set("name", "Doc1").set("description", "Test description").save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Document SET description = null WHERE name = 'Doc1'");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Document WHERE name = 'Doc1'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final Object description = item.getProperty("description");
    assertThat(description).isNull();
    result.close();
  }

  @Test
  void shouldUpdateBooleanField() {
    database.getSchema().createDocumentType("Setting");

    database.transaction(() -> {
      database.newDocument("Setting").set("feature", "notifications").set("enabled", false).save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Setting SET enabled = true WHERE feature = 'notifications'");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Setting WHERE feature = 'notifications'");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Boolean>getProperty("enabled")).isTrue();
    result.close();
  }

  @Test
  void shouldUpdateNumericField() {
    database.getSchema().createDocumentType("Counter");

    database.transaction(() -> {
      database.newDocument("Counter").set("name", "visits").set("count", 10).save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Counter SET count = count + 5 WHERE name = 'visits'");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Counter WHERE name = 'visits'");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("count")).isEqualTo(15);
    result.close();
  }

  @Test
  void shouldUpdateStringField() {
    database.getSchema().createDocumentType("Message");

    database.transaction(() -> {
      database.newDocument("Message").set("text", "Hello").set("author", "Alice").save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Message SET text = 'Hello World' WHERE author = 'Alice'");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Message WHERE author = 'Alice'");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("text")).isEqualTo("Hello World");
    result.close();
  }

  @Test
  void shouldUpdateWithComplexCondition() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("customer", "A").set("amount", 100).set("paid", false).save();
      database.newDocument("Order").set("customer", "B").set("amount", 200).set("paid", false).save();
      database.newDocument("Order").set("customer", "C").set("amount", 150).set("paid", true).save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Order SET paid = true WHERE amount >= 150 AND paid = false");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Order WHERE paid = true");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldUpdateAllRecordsWhenNoWhere() {
    database.getSchema().createDocumentType("Status");

    database.transaction(() -> {
      database.newDocument("Status").set("active", false).save();
      database.newDocument("Status").set("active", false).save();
      database.newDocument("Status").set("active", false).save();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE Status SET active = true");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Status WHERE active = true");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(3);
    result.close();
  }
}
