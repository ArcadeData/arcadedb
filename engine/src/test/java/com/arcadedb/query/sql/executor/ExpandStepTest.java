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
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ExpandStepTest extends TestHelper {

  @Test
  void shouldExpandCollection() {
    database.getSchema().createDocumentType("Container");

    database.transaction(() -> {
      final List<Integer> values = new ArrayList<>();
      values.add(1);
      values.add(2);
      values.add(3);
      database.newDocument("Container").set("name", "test").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Container");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandNestedDocuments() {
    database.getSchema().createDocumentType("Parent");
    database.getSchema().createDocumentType("Child");

    database.transaction(() -> {
      final MutableDocument child1 = database.newDocument("Child").set("name", "child1");
      final MutableDocument child2 = database.newDocument("Child").set("name", "child2");
      child1.save();
      child2.save();

      final List<Object> children = new ArrayList<>();
      children.add(child1);
      children.add(child2);

      database.newDocument("Parent").set("name", "parent").set("children", children).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(children) FROM Parent");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object name = item.getProperty("name");
      assertThat(name).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldExpandEmptyCollection() {
    database.getSchema().createDocumentType("EmptyContainer");

    database.transaction(() -> {
      final List<Integer> emptyList = new ArrayList<>();
      database.newDocument("EmptyContainer").set("values", emptyList).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM EmptyContainer");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldExpandMultipleCollections() {
    database.getSchema().createDocumentType("MultiContainer");

    database.transaction(() -> {
      final List<Integer> values1 = new ArrayList<>();
      values1.add(1);
      values1.add(2);
      database.newDocument("MultiContainer").set("values", values1).save();

      final List<Integer> values2 = new ArrayList<>();
      values2.add(3);
      values2.add(4);
      database.newDocument("MultiContainer").set("values", values2).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM MultiContainer");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandWithWhereClause() {
    database.getSchema().createDocumentType("FilteredContainer");

    database.transaction(() -> {
      final List<Integer> values1 = new ArrayList<>();
      values1.add(10);
      values1.add(20);
      database.newDocument("FilteredContainer").set("name", "A").set("values", values1).save();

      final List<Integer> values2 = new ArrayList<>();
      values2.add(30);
      database.newDocument("FilteredContainer").set("name", "B").set("values", values2).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM FilteredContainer WHERE name = 'A'");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandSingleValue() {
    database.getSchema().createDocumentType("Single");

    database.transaction(() -> {
      final List<String> values = new ArrayList<>();
      values.add("only");
      database.newDocument("Single").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Single");

    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void shouldExpandLargeCollection() {
    database.getSchema().createDocumentType("Large");

    database.transaction(() -> {
      final List<Integer> values = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        values.add(i);
      }
      database.newDocument("Large").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Large");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandStrings() {
    database.getSchema().createDocumentType("Strings");

    database.transaction(() -> {
      final List<String> values = new ArrayList<>();
      values.add("alpha");
      values.add("beta");
      values.add("gamma");
      database.newDocument("Strings").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Strings");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandWithLimit() {
    database.getSchema().createDocumentType("Limited");

    database.transaction(() -> {
      final List<Integer> values = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        values.add(i);
      }
      database.newDocument("Limited").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Limited LIMIT 5");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(5);
    result.close();
  }

  @Test
  void shouldExpandNonExistentField() {
    database.getSchema().createDocumentType("NoField");

    database.transaction(() -> {
      database.newDocument("NoField").set("name", "test").save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(nonexistent) FROM NoField");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }
}
