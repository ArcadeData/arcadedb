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

class FetchFromTypeExecutionStepTest extends TestHelper {

  @Test
  void shouldFetchAllRecordsFromType() {
    database.getSchema().createDocumentType("Customer");

    database.transaction(() -> {
      database.newDocument("Customer").set("name", "Alice").save();
      database.newDocument("Customer").set("name", "Bob").save();
      database.newDocument("Customer").set("name", "Charlie").save();
    });

    final ResultSet result = database.query("sql", "SELECT * FROM Customer");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object name = item.getProperty("name");
      assertThat(name).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldFetchFromEmptyType() {
    database.getSchema().createDocumentType("EmptyType");

    final ResultSet result = database.query("sql", "SELECT * FROM EmptyType");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldFetchSpecificFields() {
    database.getSchema().createDocumentType("User");

    database.transaction(() -> {
      database.newDocument("User").set("name", "Alice").set("age", 30).set("email", "alice@test.com").save();
      database.newDocument("User").set("name", "Bob").set("age", 25).set("email", "bob@test.com").save();
    });

    final ResultSet result = database.query("sql", "SELECT name, age FROM User");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object name = item.getProperty("name");
      final Object age = item.getProperty("age");
      assertThat(name).isNotNull();
      assertThat(age).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldFetchFromMultipleDocuments() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("Product").set("id", i).set("name", "Product" + i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT * FROM Product");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(100);
    result.close();
  }

  @Test
  void shouldFetchWithWhereCondition() {
    database.getSchema().createDocumentType("Employee");

    database.transaction(() -> {
      database.newDocument("Employee").set("name", "Alice").set("salary", 50000).save();
      database.newDocument("Employee").set("name", "Bob").set("salary", 60000).save();
      database.newDocument("Employee").set("name", "Charlie").set("salary", 70000).save();
    });

    final ResultSet result = database.query("sql", "SELECT * FROM Employee WHERE salary > 55000");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int salary = item.getProperty("salary");
      assertThat(salary).isGreaterThan(55000);
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldFetchWithCount() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("amount", 100).save();
      database.newDocument("Order").set("amount", 200).save();
      database.newDocument("Order").set("amount", 300).save();
      database.newDocument("Order").set("amount", 400).save();
    });

    final ResultSet result = database.query("sql", "SELECT count(*) AS total FROM Order");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final long total = item.<Number>getProperty("total").longValue();
    assertThat(total).isEqualTo(4);

    result.close();
  }

  @Test
  void shouldFetchWithOrderBy() {
    database.getSchema().createDocumentType("Student");

    database.transaction(() -> {
      database.newDocument("Student").set("name", "Charlie").set("score", 85).save();
      database.newDocument("Student").set("name", "Alice").set("score", 95).save();
      database.newDocument("Student").set("name", "Bob").set("score", 90).save();
    });

    final ResultSet result = database.query("sql", "SELECT * FROM Student ORDER BY score DESC");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("score")).isEqualTo(95);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("score")).isEqualTo(90);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("score")).isEqualTo(85);

    result.close();
  }

  @Test
  void shouldFetchWithLimitAndSkip() {
    database.getSchema().createDocumentType("RecordData");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("RecordData").set("id", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT * FROM RecordData SKIP 3 LIMIT 4");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(4);
    result.close();
  }

  @Test
  void shouldFetchOnlyFirstRecord() {
    database.getSchema().createDocumentType("Task");

    database.transaction(() -> {
      database.newDocument("Task").set("priority", 1).save();
      database.newDocument("Task").set("priority", 2).save();
      database.newDocument("Task").set("priority", 3).save();
    });

    final ResultSet result = database.query("sql", "SELECT * FROM Task LIMIT 1");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldFetchWithMultipleConditions() {
    database.getSchema().createDocumentType("Invoice");

    database.transaction(() -> {
      database.newDocument("Invoice").set("customer", "A").set("amount", 100).set("paid", true).save();
      database.newDocument("Invoice").set("customer", "B").set("amount", 200).set("paid", false).save();
      database.newDocument("Invoice").set("customer", "C").set("amount", 150).set("paid", true).save();
      database.newDocument("Invoice").set("customer", "D").set("amount", 300).set("paid", true).save();
    });

    final ResultSet result = database.query("sql", "SELECT * FROM Invoice WHERE amount > 120 AND paid = true");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int amount = item.getProperty("amount");
      final boolean paid = item.getProperty("paid");
      assertThat(amount).isGreaterThan(120);
      assertThat(paid).isTrue();
      count++;
    }

    assertThat(count).isEqualTo(2); // C and D
    result.close();
  }
}
