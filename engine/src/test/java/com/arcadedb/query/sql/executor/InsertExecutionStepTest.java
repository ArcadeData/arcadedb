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

public class InsertExecutionStepTest extends TestHelper {

  @Test
  void shouldInsertSingleDocument() {
    database.getSchema().createDocumentType("Customer");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Customer SET name = 'Alice', age = 30");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Customer WHERE name = 'Alice'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("Alice");
    assertThat(item.<Integer>getProperty("age")).isEqualTo(30);
    result.close();
  }

  @Test
  void shouldInsertMultipleDocuments() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Product SET name = 'Product1', price = 100");
      database.command("sql", "INSERT INTO Product SET name = 'Product2', price = 200");
      database.command("sql", "INSERT INTO Product SET name = 'Product3', price = 300");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Product");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldInsertWithMultipleFields() {
    database.getSchema().createDocumentType("Employee");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Employee SET name = 'Bob', department = 'Engineering', salary = 75000, active = true");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Employee");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("Bob");
    assertThat(item.<String>getProperty("department")).isEqualTo("Engineering");
    assertThat(item.<Integer>getProperty("salary")).isEqualTo(75000);
    assertThat(item.<Boolean>getProperty("active")).isTrue();
    result.close();
  }

  @Test
  void shouldInsertAndReturnRecord() {
    database.getSchema().createDocumentType("Task");

    final ResultSet[] resultHolder = new ResultSet[1];
    database.transaction(() -> {
      resultHolder[0] = database.command("sql", "INSERT INTO Task SET title = 'Test Task', priority = 1 RETURN @this");
    });

    final ResultSet result = resultHolder[0];
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("title")).isEqualTo("Test Task");
    assertThat(item.<Integer>getProperty("priority")).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldInsertWithNullValues() {
    database.getSchema().createDocumentType("Document");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Document SET name = 'Doc1', description = null");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Document WHERE name = 'Doc1'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("Doc1");
    final Object description = item.getProperty("description");
    assertThat(description).isNull();
    result.close();
  }

  @Test
  void shouldInsertWithStringValues() {
    database.getSchema().createDocumentType("Message");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Message SET text = 'Hello World', author = 'Alice'");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Message");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("text")).isEqualTo("Hello World");
    assertThat(item.<String>getProperty("author")).isEqualTo("Alice");
    result.close();
  }

  @Test
  void shouldInsertWithNumericValues() {
    database.getSchema().createDocumentType("Measurement");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Measurement SET temperature = 25.5, humidity = 60, pressure = 1013");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Measurement");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<Number>getProperty("temperature").doubleValue()).isEqualTo(25.5);
    assertThat(item.<Integer>getProperty("humidity")).isEqualTo(60);
    assertThat(item.<Integer>getProperty("pressure")).isEqualTo(1013);
    result.close();
  }

  @Test
  void shouldInsertWithBooleanValues() {
    database.getSchema().createDocumentType("Setting");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Setting SET feature = 'notifications', enabled = true");
    });

    final ResultSet result = database.query("sql", "SELECT FROM Setting WHERE feature = 'notifications'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<Boolean>getProperty("enabled")).isTrue();
    result.close();
  }

  @Test
  void shouldInsertIntoNewType() {
    database.getSchema().createDocumentType("NewType");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO NewType SET data = 'test'");
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM NewType");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldInsertMultipleRecordsSequentially() {
    database.getSchema().createDocumentType("Log");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO Log SET message = 'Log entry " + i + "', level = 'INFO'");
      }
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Log");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(5);
    result.close();
  }
}
