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

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class UnwindExpandStepTest extends TestHelper {

  @Test
  void shouldUnwindArray() {
    database.getSchema().createDocumentType("TestUnwind");

    database.transaction(() -> {
      database.newDocument("TestUnwind").set("id", 1).set("tags", Arrays.asList("java", "database", "graph")).save();
      database.newDocument("TestUnwind").set("id", 2).set("tags", Arrays.asList("nosql", "performance")).save();
    });

    final ResultSet result = database.query("sql", "SELECT id, tags FROM TestUnwind UNWIND tags");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object id = item.getProperty("id");
      assertThat(id).isNotNull();
      final String tag = item.getProperty("tags");
      assertThat(tag).isNotNull().isInstanceOf(String.class);
      count++;
    }

    assertThat(count).isEqualTo(5); // 3 + 2 tags
    result.close();
  }

  @Test
  void shouldUnwindEmptyArray() {
    database.getSchema().createDocumentType("TestUnwindEmpty");

    database.transaction(() -> {
      database.newDocument("TestUnwindEmpty").set("id", 1).set("tags", Arrays.asList()).save();
      database.newDocument("TestUnwindEmpty").set("id", 2).set("tags", Arrays.asList("test")).save();
    });

    final ResultSet result = database.query("sql", "SELECT id, tags FROM TestUnwindEmpty UNWIND tags");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(1); // Only the record with "test" tag
    result.close();
  }

  @Test
  void shouldUnwindNullValues() {
    database.getSchema().createDocumentType("TestUnwindNull");

    database.transaction(() -> {
      database.newDocument("TestUnwindNull").set("id", 1).save(); // No tags field
      database.newDocument("TestUnwindNull").set("id", 2).set("tags", Arrays.asList("test")).save();
    });

    final ResultSet result = database.query("sql", "SELECT id, tags FROM TestUnwindNull UNWIND tags");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(1); // Only the record with tags
    result.close();
  }

  @Test
  void shouldUnwindNestedArrays() {
    database.getSchema().createDocumentType("TestUnwindNested");

    database.transaction(() -> {
      database.newDocument("TestUnwindNested")
          .set("id", 1)
          .set("groups", Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c", "d")))
          .save();
    });

    final ResultSet result = database.query("sql", "SELECT id, groups FROM TestUnwindNested UNWIND groups");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object groups = item.getProperty("groups");
      assertThat(groups).isInstanceOf(List.class);
      count++;
    }

    assertThat(count).isEqualTo(2); // 2 nested arrays
    result.close();
  }

  @Test
  void shouldExpandCollection() {
    database.getSchema().createVertexType("Person");

    database.transaction(() -> {
      final var person1 = database.newVertex("Person").set("name", "Alice").save();
      final var person2 = database.newVertex("Person").set("name", "Bob").save();
      final var person3 = database.newVertex("Person").set("name", "Charlie").save();

      database.newDocument("Group")
          .set("name", "Team")
          .set("members", Arrays.asList(person1.getIdentity(), person2.getIdentity(), person3.getIdentity()))
          .save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(members) FROM Group");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String name = item.getProperty("name");
      assertThat(name).isIn("Alice", "Bob", "Charlie");
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void shouldExpandSingleValue() {
    database.getSchema().createDocumentType("TestExpandSingle");

    database.transaction(() -> {
      database.newDocument("TestExpandSingle").set("value", "test").save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(value) FROM TestExpandSingle");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final String value = item.getProperty("value");
    assertThat(value).isEqualTo("test");
    result.close();
  }

  @Test
  void shouldUnwindWithFilter() {
    database.getSchema().createDocumentType("TestUnwindFilter");

    database.transaction(() -> {
      database.newDocument("TestUnwindFilter").set("id", 1).set("numbers", Arrays.asList(1, 2, 3, 4, 5)).save();
      database.newDocument("TestUnwindFilter").set("id", 2).set("numbers", Arrays.asList(6, 7, 8, 9, 10)).save();
    });

    final ResultSet result = database.query("sql", "SELECT id, numbers FROM TestUnwindFilter UNWIND numbers WHERE numbers > 5");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int number = item.getProperty("numbers");
      assertThat(number).isGreaterThan(5);
      count++;
    }

    assertThat(count).isEqualTo(5); // 6, 7, 8, 9, 10
    result.close();
  }

  @Test
  void shouldUnwindMultipleFields() {
    database.getSchema().createDocumentType("TestUnwindMulti");

    database.transaction(() -> {
      database.newDocument("TestUnwindMulti")
          .set("id", 1)
          .set("tags", Arrays.asList("tag1", "tag2"))
          .set("categories", Arrays.asList("cat1", "cat2"))
          .save();
    });

    // Unwind tags first
    final ResultSet result = database.query("sql", "SELECT id, tags, categories FROM TestUnwindMulti UNWIND tags");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object tags = item.getProperty("tags");
      final Object categories = item.getProperty("categories");
      assertThat(tags).isInstanceOf(String.class);
      assertThat(categories).isInstanceOf(List.class);
      count++;
    }

    assertThat(count).isEqualTo(2); // 2 tags
    result.close();
  }

  @Test
  void shouldHandleUnwindWithGroupBy() {
    database.getSchema().createDocumentType("TestUnwindGroup");

    database.transaction(() -> {
      database.newDocument("TestUnwindGroup").set("category", "A").set("values", Arrays.asList(1, 2, 3)).save();
      database.newDocument("TestUnwindGroup").set("category", "B").set("values", Arrays.asList(4, 5)).save();
      database.newDocument("TestUnwindGroup").set("category", "A").set("values", Arrays.asList(6, 7)).save();
    });

    final ResultSet result = database.query("sql", "SELECT category, sum(values) as total FROM TestUnwindGroup UNWIND values GROUP BY category");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String category = item.getProperty("category");
      final Object total = item.getProperty("total");
      assertThat(category).isIn("A", "B");
      assertThat(total).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(2); // 2 categories
    result.close();
  }

  @Test
  void shouldExpandEmptyCollection() {
    database.getSchema().createDocumentType("TestExpandEmpty");

    database.transaction(() -> {
      database.newDocument("TestExpandEmpty").set("items", Arrays.asList()).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(items) FROM TestExpandEmpty");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }
}
