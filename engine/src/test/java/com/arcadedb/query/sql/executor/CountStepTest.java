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

class CountStepTest extends TestHelper {

  @Test
  void shouldCountAllRecords() {
    database.getSchema().createDocumentType("TestCount");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("TestCount").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM TestCount");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(100);
    result.close();
  }

  @Test
  void shouldCountWithWhereClause() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("price", 100).save();
      database.newDocument("Product").set("price", 200).save();
      database.newDocument("Product").set("price", 300).save();
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Product WHERE price > 150");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldCountEmptyType() {
    database.getSchema().createDocumentType("Empty");

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Empty");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(0);
    result.close();
  }

  @Test
  void shouldCountWithGroupBy() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("status", "pending").save();
      database.newDocument("Order").set("status", "pending").save();
      database.newDocument("Order").set("status", "completed").save();
    });

    final ResultSet result = database.query("sql", "SELECT status, count(*) as total FROM Order GROUP BY status");
    int totalRecords = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final long count = item.<Number>getProperty("total").longValue();
      totalRecords += count;
      assertThat(count).isGreaterThan(0);
    }
    assertThat(totalRecords).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldCountDistinct() {
    database.getSchema().createDocumentType("Tag");

    database.transaction(() -> {
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "python").save();
      database.newDocument("Tag").set("name", "java").save();
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM (SELECT DISTINCT name FROM Tag)");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldCountSpecificField() {
    database.getSchema().createDocumentType("Data");

    database.transaction(() -> {
      database.newDocument("Data").set("value", 10).save();
      database.newDocument("Data").set("value", 20).save();
      database.newDocument("Data").save(); // No value field
    });

    final ResultSet result = database.query("sql", "SELECT count(value) as total FROM Data");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldCountWithMultipleConditions() {
    database.getSchema().createDocumentType("User");

    database.transaction(() -> {
      database.newDocument("User").set("age", 25).set("active", true).save();
      database.newDocument("User").set("age", 30).set("active", true).save();
      database.newDocument("User").set("age", 35).set("active", false).save();
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM User WHERE age >= 30 AND active = true");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldCountWithOrderByAndLimit() {
    database.getSchema().createDocumentType("Score");

    database.transaction(() -> {
      database.newDocument("Score").set("player", "A").set("points", 100).save();
      database.newDocument("Score").set("player", "B").set("points", 200).save();
      database.newDocument("Score").set("player", "C").set("points", 150).save();
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Score");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldCountLargeDataset() {
    database.getSchema().createDocumentType("BigData");

    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        database.newDocument("BigData").set("id", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM BigData");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(1000);
    result.close();
  }

  @Test
  void shouldCountWithIN() {
    database.getSchema().createDocumentType("Category");

    database.transaction(() -> {
      database.newDocument("Category").set("name", "A").save();
      database.newDocument("Category").set("name", "B").save();
      database.newDocument("Category").set("name", "C").save();
      database.newDocument("Category").set("name", "D").save();
    });

    final ResultSet result = database.query("sql", "SELECT count(*) as total FROM Category WHERE name IN ['A', 'C']");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(2);
    result.close();
  }
}
