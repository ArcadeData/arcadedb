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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8744: two selections of an introspection object that share a response key overwrote each other,
 * so only the last one's sub-selection survived. The GraphQL specification (CollectFields / MergeSelectionSets) merges them into
 * one field carrying the sub-selections of both.
 */
class Issue8744IntrospectionMergeSameKeyTest extends AbstractGraphQLTest {

  @Test
  void fieldsSelectedTwiceOnGraphQLTypeAreMerged() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __type(name: \"Book\") { fields { name } fields { type { name } } } }");
      assertThat(record.getPropertyNames()).containsExactly("fields");
      final List<Result> fields = record.getProperty("fields");
      assertThat(fields.stream().map(f -> f.<String>getProperty("name")).toList())
          .containsExactly("id", "name", "pageCount", "authors");
      for (final Result field : fields) {
        // FIRST-OCCURRENCE ORDER, AS THE SPECIFICATION'S CollectFields ORDERS THE RESPONSE KEYS
        assertThat(field.getPropertyNames()).containsExactly("name", "type");
        assertThat(field.<Result>getProperty("type").getPropertyNames()).containsExactly("name");
      }
      return null;
    });
  }

  @Test
  void fieldsSelectedTwiceOnDatabaseOnlyTypeAreMerged() {
    executeTest(database -> {
      database.transaction(() -> database.getSchema().createDocumentType("Shelf").createProperty("position", Type.INTEGER));

      final Result record = single(database, "{ __type(name: \"Shelf\") { fields { name } fields { type { name } } } }");
      final List<Result> fields = record.getProperty("fields");
      assertThat(fields).hasSize(1);
      final Result field = fields.getFirst();
      assertThat(field.getPropertyNames()).containsExactly("name", "type");
      assertThat(field.<String>getProperty("name")).isEqualTo("position");
      assertThat(field.<Result>getProperty("type").<String>getProperty("name")).isEqualTo("Int");
      return null;
    });
  }

  @Test
  void schemaTypesSelectedTwiceAreMerged() {
    executeTest(database -> {
      defineTypes(database);
      database.transaction(() -> database.getSchema().createDocumentType("Shelf"));

      final Result record = single(database, "{ __schema { types { name } types { kind } } }");
      assertThat(record.getPropertyNames()).containsExactly("types");
      final List<Result> types = record.getProperty("types");
      // GraphQL-defined (Book), database-only (Shelf) and built-in scalar (Boolean) types go through three different builders
      assertThat(types.stream().map(t -> t.<String>getProperty("name")).toList()).contains("Book", "Shelf", "Boolean");
      for (final Result type : types) {
        assertThat(type.getPropertyNames()).containsExactly("name", "kind");
        assertThat(type.<String>getProperty("kind")).isNotNull();
      }
      return null;
    });
  }

  @Test
  void typeSelectedTwiceInsideFieldsIsMerged() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __type(name: \"Book\") { fields { type { name } type { kind } } } }");
      for (final Result field : record.<List<Result>>getProperty("fields")) {
        assertThat(field.getPropertyNames()).containsExactly("type");
        assertThat(field.<Result>getProperty("type").getPropertyNames()).containsExactly("name", "kind");
      }
      return null;
    });
  }

  @Test
  void ofTypeSelectedTwiceIsMerged() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database,
          "{ __type(name: \"Book\") { fields { name type { ofType { name } ofType { kind } } } } }");
      final Result authors = record.<List<Result>>getProperty("fields").stream()
          .filter(f -> "authors".equals(f.getProperty("name"))).findFirst().orElseThrow();
      final Result ofType = authors.<Result>getProperty("type").getProperty("ofType");
      assertThat(ofType.getPropertyNames()).containsExactly("name", "kind");
      assertThat(ofType.<String>getProperty("name")).isEqualTo("Author");
      assertThat(ofType.<String>getProperty("kind")).isEqualTo("OBJECT");
      return null;
    });
  }

  @Test
  void queryTypeSelectedTwiceIsMerged() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __schema { queryType { name } queryType { kind } } }");
      final Result queryType = record.getProperty("queryType");
      assertThat(queryType.getPropertyNames()).containsExactly("name", "kind");
      assertThat(queryType.<String>getProperty("name")).isEqualTo("Query");
      assertThat(queryType.<String>getProperty("kind")).isEqualTo("OBJECT");
      return null;
    });
  }

  @Test
  void fragmentSpreadWithTheSameKeyAsADirectSelectionIsMerged() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database,
          "{ __type(name: \"Book\") { fields { name } ...F } } fragment F on __Type { fields { type { name } } }");
      for (final Result field : record.<List<Result>>getProperty("fields")) {
        assertThat(field.getPropertyNames()).containsExactly("name", "type");
        assertThat(field.<Result>getProperty("type").getPropertyNames()).containsExactly("name");
      }

      final Result inline = single(database,
          "{ __schema { types { name } ... on __Schema { types { kind } } } }");
      for (final Result type : inline.<List<Result>>getProperty("types"))
        assertThat(type.getPropertyNames()).containsExactly("name", "kind");
      return null;
    });
  }

  @Test
  void aliasedSelectionsWithTheSameKeyAreMerged() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __type(name: \"Book\") { f: fields { name } f: fields { t: type { kind } } } }");
      assertThat(record.getPropertyNames()).containsExactly("f");
      for (final Result field : record.<List<Result>>getProperty("f")) {
        assertThat(field.getPropertyNames()).containsExactly("name", "t");
        assertThat(field.<Result>getProperty("t").getPropertyNames()).containsExactly("kind");
      }
      return null;
    });
  }

  @Test
  void conflictingLeavesUnderOneKeyKeepTheFirstOneWritten() {
    executeTest(database -> {
      defineTypes(database);

      // INVALID PER THE SPECIFICATION (FieldsInSetCanMerge), WHICH THIS MODULE DOES NOT VALIDATE: THE FIRST SELECTION IS KEPT,
      // AS GraphQLResultSet DOES FOR DATA SELECTIONS
      final Result record = single(database, "{ __type(name: \"Book\") { x: name x: kind } }");
      assertThat(record.getPropertyNames()).containsExactly("x");
      assertThat(record.<String>getProperty("x")).isEqualTo("Book");
      return null;
    });
  }

  private static Result single(final Database database, final String query) {
    try (final ResultSet resultSet = database.query("graphql", query)) {
      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat(resultSet.hasNext()).isFalse();
      return record;
    }
  }
}
