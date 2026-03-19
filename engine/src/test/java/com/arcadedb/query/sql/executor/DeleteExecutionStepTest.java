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

class DeleteExecutionStepTest extends TestHelper {

  @Test
  void shouldDeleteSingleRecord() {
    database.getSchema().createDocumentType("Customer");

    database.transaction(() -> {
      database.newDocument("Customer").set("name", "Alice").set("age", 30).save();
      database.newDocument("Customer").set("name", "Bob").set("age", 25).save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Customer WHERE name = 'Alice'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Customer");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldDeleteMultipleRecords() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("category", "Electronics").set("price", 100).save();
      database.newDocument("Product").set("category", "Electronics").set("price", 200).save();
      database.newDocument("Product").set("category", "Books").set("price", 50).save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Product WHERE category = 'Electronics'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Product");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldDeleteWithWhereClause() {
    database.getSchema().createDocumentType("Task");

    database.transaction(() -> {
      database.newDocument("Task").set("title", "Task1").set("priority", 1).save();
      database.newDocument("Task").set("title", "Task2").set("priority", 2).save();
      database.newDocument("Task").set("title", "Task3").set("priority", 3).save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Task WHERE priority > 1");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Task");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldDeleteWithComplexCondition() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("customer", "A").set("amount", 100).set("status", "pending").save();
      database.newDocument("Order").set("customer", "B").set("amount", 200).set("status", "completed").save();
      database.newDocument("Order").set("customer", "C").set("amount", 150).set("status", "pending").save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Order WHERE amount < 180 AND status = 'pending'");
    });

    // Should delete orders with amount < 180 AND status = pending
    // This deletes: A (100, pending) and C (150, pending)
    // Remaining: B (200, completed)
    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Order");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldDeleteAllRecordsWhenNoWhere() {
    database.getSchema().createDocumentType("Temp");

    database.transaction(() -> {
      database.newDocument("Temp").set("data", "value1").save();
      database.newDocument("Temp").set("data", "value2").save();
      database.newDocument("Temp").set("data", "value3").save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Temp");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Temp");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(0);
    result.close();
  }

  @Test
  void shouldDeleteBasedOnNumericComparison() {
    database.getSchema().createDocumentType("Score");

    database.transaction(() -> {
      database.newDocument("Score").set("player", "Alice").set("points", 100).save();
      database.newDocument("Score").set("player", "Bob").set("points", 50).save();
      database.newDocument("Score").set("player", "Charlie").set("points", 75).save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Score WHERE points < 80");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Score");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldDeleteBasedOnStringMatch() {
    database.getSchema().createDocumentType("User");

    database.transaction(() -> {
      database.newDocument("User").set("name", "Alice").set("role", "admin").save();
      database.newDocument("User").set("name", "Bob").set("role", "user").save();
      database.newDocument("User").set("name", "Charlie").set("role", "user").save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM User WHERE role = 'user'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM User");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldDeleteBasedOnBooleanField() {
    database.getSchema().createDocumentType("Account");

    database.transaction(() -> {
      database.newDocument("Account").set("username", "user1").set("active", true).save();
      database.newDocument("Account").set("username", "user2").set("active", false).save();
      database.newDocument("Account").set("username", "user3").set("active", false).save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Account WHERE active = false");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Account");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldDeleteNoneWhenNoMatch() {
    database.getSchema().createDocumentType("Document");

    database.transaction(() -> {
      database.newDocument("Document").set("name", "Doc1").save();
      database.newDocument("Document").set("name", "Doc2").save();
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Document WHERE name = 'NonExistent'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Document");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldDeleteFromEmptyType() {
    database.getSchema().createDocumentType("Empty");

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Empty WHERE name = 'test'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Empty");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(0);
    result.close();
  }
}
