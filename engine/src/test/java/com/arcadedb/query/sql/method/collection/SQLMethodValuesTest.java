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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodValuesTest extends TestHelper {
  private SQLMethod function;

  @BeforeEach
  void setup() {
    function = new SQLMethodValues();
  }

  @Test
  void withResult() {
    final ResultInternal resultInternal = new ResultInternal();
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    final Object result = function.execute(resultInternal, null, null, null);
    assertThat(new ArrayList<>((Collection<String>) result)).isEqualTo(Arrays.asList("Foo", "Bar"));
  }

  @Test
  void withCollection() {
    List<Map<String, Object>> collection = List.of(Map.of("key1", "value1"), Map.of("key2", "value2"));

    Object result = function.execute(collection, null, null, null);
    assertThat(result).isEqualTo(List.of("value1", "value2"));
  }

  @Test
  void withNull() {
    Object result = function.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void withSQL() {
    ResultSet resultSet = database.query("sql", """
        SELECT [{"x":1,"y":2}].keys() AS keys, [{"x":1,"y":2}].values() as values
        """);

    Result result = resultSet.next();
    assertThat(result.<List<String>>getProperty("keys")).isEqualTo(List.of("x", "y"));
    assertThat(result.<List<Integer>>getProperty("values")).isEqualTo(List.of(1, 2));
  }

  // NEW TESTS - Document handling

  @Test
  void withDocument() {
    database.transaction(() ->
      database.getSchema().createDocumentType("Person"));

    database.transaction(() -> {
      MutableDocument doc = database.newDocument("Person");
      doc.set("name", "Alice");
      doc.set("age", 30);
      doc.save();

      Object result = function.execute(doc, null, null, null);

      // Flat collection of the document's property values (toMap() also carries the internal
      // fields RID, type marker "d", type name), not wrapped in an extra one-element list.
      assertThat(result).isInstanceOf(Collection.class);
      final Collection<Object> values = (Collection<Object>) result;
      assertThat(values).containsExactlyInAnyOrder("Alice", 30, doc.getIdentity().toString(), "d", "Person");
    });
  }

  @Test
  void withCollectionOfDocuments() {
    database.transaction(() ->
      database.getSchema().createDocumentType("Person"));

    List<Document> documents = new ArrayList<>();
    database.transaction(() -> {
      MutableDocument doc1 = database.newDocument("Person");
      doc1.set("name", "Alice");
      doc1.set("age", 30);
      doc1.save();
      documents.add(doc1);

      MutableDocument doc2 = database.newDocument("Person");
      doc2.set("name", "Bob");
      doc2.set("age", 25);
      doc2.save();
      documents.add(doc2);
    });

    Object result = function.execute(documents, null, null, null);

    assertThat(result).isInstanceOf(List.class);
    List<?> flattenedValues = (List<?>) result;
    // Each document contributes its own (unwrapped) toMap() values: name, age plus the 3
    // internal fields (RID, type marker, type name) = 5 values per document.
    assertThat(flattenedValues).hasSize(10);
    assertThat((List<Object>) flattenedValues).contains("Alice", 30, "Bob", 25);
  }

  // NEW TESTS - Result object handling

  @Test
  void withCollectionOfResults() {
    ResultInternal result1 = new ResultInternal();
    result1.setProperty("name", "Alice");
    result1.setProperty("age", 30);

    ResultInternal result2 = new ResultInternal();
    result2.setProperty("name", "Bob");
    result2.setProperty("age", 25);

    List<Result> results = List.of(result1, result2);

    Object result = function.execute(results, null, null, null);

    assertThat(result).isInstanceOf(List.class);
    List<?> flattenedValues = (List<?>) result;
    assertThat(flattenedValues).hasSize(4);
    List<Object> valueList = new ArrayList<>(flattenedValues);
    assertThat(valueList).containsAll(Arrays.asList("Alice", 30, "Bob", 25));
  }

  // NEW TESTS - Mixed and unsupported types

  @Test
  void withCollectionOfMixedMapAndString() {
    // Scalar elements (e.g. plain strings) have no values of their own: they are skipped rather
    // than crashing the whole collection.
    List<Object> mixed = List.of(
        Map.of("key1", "value1"),
        "some string",
        Map.of("key2", "value2")
    );

    final Object result = function.execute(mixed, null, null, null);
    assertThat(result).isEqualTo(List.of("value1", "value2"));
  }

  @Test
  void withCollectionOfUnsupportedTypes() {
    List<Object> unsupported = List.of(42, true, "text");

    // Every element is a scalar with no values: they are all skipped, yielding an empty list.
    final Object result = function.execute(unsupported, null, null, null);
    assertThat(result).isEqualTo(List.of());
  }

  @Test
  void withEmptyCollection() {
    List<Map<String, Object>> emptyList = List.of();

    Object result = function.execute(emptyList, null, null, null);
    assertThat(result).isEqualTo(List.of());
  }

  @Test
  void withNestedCollections() {
    // Collection of collections of maps
    List<List<Map<String, Object>>> nested = List.of(
        List.of(Map.of("a", 1), Map.of("b", 2)),
        List.of(Map.of("c", 3))
    );

    Object result = function.execute(nested, null, null, null);

    assertThat(result).isInstanceOf(List.class);
    List<?> flattenedValues = (List<?>) result;
    assertThat(flattenedValues).hasSize(3);
    List<Object> valueList = new ArrayList<>(flattenedValues);
    assertThat(valueList).containsAll(Arrays.asList(1, 2, 3));
  }

  @Test
  void withSQLQueryOnDocuments() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Product");

      MutableDocument product1 = database.newDocument("Product");
      product1.set("name", "Laptop");
      product1.set("price", 999);
      product1.save();

      MutableDocument product2 = database.newDocument("Product");
      product2.set("name", "Mouse");
      product2.set("price", 25);
      product2.save();
    });

    // list(name, price) builds a list of scalars (e.g. ["Laptop", 999]); .values() on a
    // collection of scalars has nothing to recurse into, so it no longer NPEs - it yields an
    // empty list per row instead.
    try (ResultSet resultSet = database.query("sql",
        "SELECT list(name, price).values() AS allValues FROM Product")) {
      while (resultSet.hasNext())
        assertThat(resultSet.next().<List<Object>>getProperty("allValues")).isEqualTo(List.of());
    }
  }

  // Regression tests for issue #6387

  @Test
  void withCollectionContainingScalarAndNullDoesNotThrow() {
    final List<Object> mixed = Arrays.asList(Map.of("a", 1), null, "scalar", 42, Map.of("b", 2));

    final Object result = function.execute(mixed, null, null, null);
    assertThat(result).isEqualTo(List.of(1, 2));
  }

  @Test
  void withSQLScalarCollectionDoesNotThrow() {
    // SELECT [1,2,3].values() -- a collection of scalars has no values, so this used to NPE
    try (final ResultSet resultSet = database.query("sql", "SELECT [1,2,3].values() AS vals")) {
      final Result record = resultSet.next();
      assertThat(record.<List<Object>>getProperty("vals")).isEqualTo(List.of());
    }
  }

  @Test
  void withMapOfMaps() {
    Map<String, Map<String, Object>> mapOfMaps = Map.of(
        "user1", Map.of("name", "Alice", "age", 30),
        "user2", Map.of("name", "Bob", "age", 25)
    );

    Object result = function.execute(mapOfMaps, null, null, null);

    assertThat(result).isInstanceOf(Collection.class);
    Collection<?> values = (Collection<?>) result;
    // Should contain the inner maps (not flattened further)
    assertThat(values).hasSize(2);
    assertThat(values).allMatch(v -> v instanceof Map);
  }
}
