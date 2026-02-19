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

class SkipExecutionStepTest extends TestHelper {

  @Test
  void shouldSkipFirstRecords() {
    database.getSchema().createDocumentType("TestSkip");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestSkip").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestSkip SKIP 5");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(5);
    result.close();
  }

  @Test
  void shouldSkipZeroRecords() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("name", "A").save();
      database.newDocument("Product").set("name", "B").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Product SKIP 0");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldSkipAllRecords() {
    database.getSchema().createDocumentType("Data");

    database.transaction(() -> {
      database.newDocument("Data").set("value", 1).save();
      database.newDocument("Data").set("value", 2).save();
      database.newDocument("Data").set("value", 3).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Data SKIP 10");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldSkipWithOrderBy() {
    database.getSchema().createDocumentType("Score");

    database.transaction(() -> {
      database.newDocument("Score").set("points", 100).save();
      database.newDocument("Score").set("points", 200).save();
      database.newDocument("Score").set("points", 300).save();
      database.newDocument("Score").set("points", 400).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Score ORDER BY points ASC SKIP 2");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("points")).isEqualTo(300);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("points")).isEqualTo(400);

    result.close();
  }

  @Test
  void shouldSkipWithWhere() {
    database.getSchema().createDocumentType("User");

    database.transaction(() -> {
      for (int i = 1; i <= 10; i++) {
        database.newDocument("User").set("age", i * 10).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE age > 30 SKIP 2");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(5); // 7 records match WHERE, skip 2, get 5
    result.close();
  }

  @Test
  void shouldSkipOne() {
    database.getSchema().createDocumentType("Task");

    database.transaction(() -> {
      database.newDocument("Task").set("priority", 1).save();
      database.newDocument("Task").set("priority", 2).save();
      database.newDocument("Task").set("priority", 3).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Task SKIP 1");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldSkipFromEmptyResult() {
    database.getSchema().createDocumentType("Empty");

    final ResultSet result = database.query("sql", "SELECT FROM Empty SKIP 5");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldSkipWithDistinct() {
    database.getSchema().createDocumentType("Tag");

    database.transaction(() -> {
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "python").save();
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "go").save();
    });

    final ResultSet result = database.query("sql", "SELECT DISTINCT name FROM Tag SKIP 1");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2); // 3 distinct values, skip 1, get 2
    result.close();
  }

  @Test
  void shouldSkipLargeOffset() {
    database.getSchema().createDocumentType("BigData");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("BigData").set("id", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM BigData SKIP 90");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(10);
    result.close();
  }

  @Test
  void shouldSkipWithGroupBy() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("region", "North").set("amount", 100).save();
      database.newDocument("Order").set("region", "South").set("amount", 200).save();
      database.newDocument("Order").set("region", "East").set("amount", 150).save();
    });

    final ResultSet result = database.query("sql", "SELECT region, sum(amount) as total FROM Order GROUP BY region");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(3); // 3 groups
    result.close();
  }
}
