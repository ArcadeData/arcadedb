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

class LimitExecutionStepTest extends TestHelper {

  @Test
  void shouldLimitResults() {
    database.getSchema().createDocumentType("TestLimit");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("TestLimit").set("value", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM TestLimit LIMIT 10");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(10);
    result.close();
  }

  @Test
  void shouldLimitToOne() {
    database.getSchema().createDocumentType("Product");

    database.transaction(() -> {
      database.newDocument("Product").set("name", "A").save();
      database.newDocument("Product").set("name", "B").save();
      database.newDocument("Product").set("name", "C").save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Product LIMIT 1");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(1);
    result.close();
  }

  @Test
  void shouldLimitZero() {
    database.getSchema().createDocumentType("Data");

    database.transaction(() -> {
      database.newDocument("Data").set("value", 1).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Data LIMIT 0");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldLimitMoreThanAvailable() {
    database.getSchema().createDocumentType("Small");

    database.transaction(() -> {
      database.newDocument("Small").set("value", 1).save();
      database.newDocument("Small").set("value", 2).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Small LIMIT 100");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldLimitWithOrderBy() {
    database.getSchema().createDocumentType("Score");

    database.transaction(() -> {
      database.newDocument("Score").set("points", 100).save();
      database.newDocument("Score").set("points", 200).save();
      database.newDocument("Score").set("points", 300).save();
      database.newDocument("Score").set("points", 400).save();
    });

    final ResultSet result = database.query("sql", "SELECT FROM Score ORDER BY points DESC LIMIT 2");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("points")).isEqualTo(400);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("points")).isEqualTo(300);

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldLimitWithWhere() {
    database.getSchema().createDocumentType("User");

    database.transaction(() -> {
      for (int i = 1; i <= 20; i++) {
        database.newDocument("User").set("age", i * 5).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM User WHERE age > 50 LIMIT 3");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.<Integer>getProperty("age")).isGreaterThan(50);
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldLimitDistinct() {
    database.getSchema().createDocumentType("Tag");

    database.transaction(() -> {
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "python").save();
      database.newDocument("Tag").set("name", "java").save();
      database.newDocument("Tag").set("name", "go").save();
      database.newDocument("Tag").set("name", "rust").save();
    });

    final ResultSet result = database.query("sql", "SELECT DISTINCT name FROM Tag LIMIT 3");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldLimitGroupBy() {
    database.getSchema().createDocumentType("Order");

    database.transaction(() -> {
      database.newDocument("Order").set("region", "North").set("amount", 100).save();
      database.newDocument("Order").set("region", "South").set("amount", 200).save();
      database.newDocument("Order").set("region", "East").set("amount", 150).save();
      database.newDocument("Order").set("region", "West").set("amount", 175).save();
    });

    final ResultSet result = database.query("sql", "SELECT region, sum(amount) as total FROM Order GROUP BY region LIMIT 2");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldLimitOnEmptyResult() {
    database.getSchema().createDocumentType("Empty");

    final ResultSet result = database.query("sql", "SELECT FROM Empty LIMIT 10");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldLimitLargeDataset() {
    database.getSchema().createDocumentType("BigData");

    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        database.newDocument("BigData").set("id", i).save();
      }
    });

    final ResultSet result = database.query("sql", "SELECT FROM BigData LIMIT 50");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(50);
    result.close();
  }
}
