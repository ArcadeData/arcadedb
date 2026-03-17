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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class GraphQLIntrospectionTest extends AbstractGraphQLTest {

  @Test
  void introspectionSchemaTypes() {
    executeTest((database) -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", "{ __schema { types { name } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        final List<Result> types = record.getProperty("types");
        assertThat(types).isNotNull();

        final Set<String> typeNames = types.stream().map(t -> t.<String>getProperty("name")).collect(Collectors.toSet());
        // Should contain the GraphQL-defined types
        assertThat(typeNames).contains("Query", "Book", "Author");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void introspectionSchemaQueryType() {
    executeTest((database) -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", "{ __schema { queryType { name } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        final Result queryType = record.getProperty("queryType");
        assertThat(queryType).isNotNull();
        assertThat(queryType.<String>getProperty("name")).isEqualTo("Query");

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void introspectionTypeByName() {
    executeTest((database) -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", "{ __type(name: \"Book\") { name fields { name } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.<String>getProperty("name")).isEqualTo("Book");

        final List<Result> fields = record.getProperty("fields");
        assertThat(fields).isNotNull();

        final Set<String> fieldNames = fields.stream().map(f -> f.<String>getProperty("name")).collect(Collectors.toSet());
        assertThat(fieldNames).contains("id", "name", "pageCount", "authors");

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void introspectionTypename() {
    executeTest((database) -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", "{ __typename }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.<String>getProperty("__typename")).isEqualTo("Query");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void introspectionTypeFields() {
    executeTest((database) -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Author\") { name fields { name type { name kind } } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.<String>getProperty("name")).isEqualTo("Author");

        final List<Result> fields = record.getProperty("fields");
        assertThat(fields).isNotNull();
        assertThat(fields.size()).isGreaterThanOrEqualTo(3);

        // Check that field type info is present
        for (final Result field : fields) {
          assertThat(field.<String>getProperty("name")).isNotNull();
          final Result type = field.getProperty("type");
          assertThat(type).isNotNull();
          assertThat(type.<String>getProperty("name")).isNotNull();
          assertThat(type.<String>getProperty("kind")).isNotNull();
        }

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void introspectionSchemaWithDatabaseTypes() {
    executeTest((database) -> {
      // Don't define GraphQL types - just check that database types are exposed
      try (final ResultSet resultSet = database.query("graphql", "{ __schema { types { name } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        final List<Result> types = record.getProperty("types");
        assertThat(types).isNotNull();

        final Set<String> typeNames = types.stream().map(t -> t.<String>getProperty("name")).collect(Collectors.toSet());
        // Should contain database types even without GraphQL type definitions
        assertThat(typeNames).contains("Book", "Author");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }
}
