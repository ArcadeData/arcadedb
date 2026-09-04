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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7036 (follow-up to #6384/#6453): the introspection dispatch read {@code selection.getField()} only,
 * never falling back to the aliased field, so {@code t: __type(name: "Book")} failed with "requires a 'name' argument",
 * {@code s: __schema { ... }} answered an empty object and {@code f: fields { name }} was silently omitted. An aliased
 * sub-selection is now served under the alias, which is the response key the GraphQL spec prescribes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7036IntrospectionAliasTest extends AbstractGraphQLTest {

  @Test
  void aliasedTypeIntrospectionResolvesNameArgumentAndSelection() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", "{ t: __type(name: \"Book\") { name fields { name } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.<String>getProperty("name")).isEqualTo("Book");
        final List<Result> fields = record.getProperty("fields");
        assertThat(fields).isNotNull();
        assertThat(fieldNames(fields)).contains("id", "name", "pageCount", "authors");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void aliasedSchemaIntrospectionReturnsTheSchema() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", "{ s: __schema { queryType { name } types { name } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        final Result queryType = record.getProperty("queryType");
        assertThat(queryType).isNotNull();
        assertThat(queryType.<String>getProperty("name")).isEqualTo("Query");

        final List<Result> types = record.getProperty("types");
        assertThat(types).isNotNull();
        assertThat(fieldNames(types)).contains("Query", "Book", "Author");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void aliasedSubSelectionsAreServedUnderTheAlias() {
    executeTest(database -> {
      defineTypes(database);

      // GraphQL-defined type
      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Author\") { name f: fields { name t: type { name kind } } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.<String>getProperty("name")).isEqualTo("Author");
        assertThat(record.getPropertyNames()).contains("f").doesNotContain("fields");
        final List<Result> fields = record.getProperty("f");
        assertThat(fields).isNotEmpty();
        for (final Result field : fields) {
          assertThat(field.getPropertyNames()).contains("t").doesNotContain("type");
          final Result type = field.getProperty("t");
          assertThat(type.<String>getProperty("name")).isNotNull();
          assertThat(type.<String>getProperty("kind")).isNotNull();
        }
        assertThat(resultSet.hasNext()).isFalse();
      }

      // Database-only type, served by the other builder
      database.transaction(() -> {
        database.getSchema().createDocumentType("Shelf").createProperty("position", Type.INTEGER);
      });
      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Shelf\") { name f: fields { name t: type { name kind } } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.<String>getProperty("name")).isEqualTo("Shelf");
        final List<Result> fields = record.getProperty("f");
        assertThat(fields).hasSize(1);
        assertThat(fields.get(0).<String>getProperty("name")).isEqualTo("position");
        final Result type = fields.get(0).getProperty("t");
        assertThat(type.<String>getProperty("name")).isEqualTo("Int");
        assertThat(type.<String>getProperty("kind")).isEqualTo("SCALAR");
        assertThat(resultSet.hasNext()).isFalse();
      }

      // __schema sub-selections
      try (final ResultSet resultSet = database.query("graphql", "{ __schema { q: queryType { name } all: types { name } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.getPropertyNames()).contains("q", "all").doesNotContain("queryType", "types");
        assertThat(record.<Result>getProperty("q").<String>getProperty("name")).isEqualTo("Query");
        assertThat(fieldNames(record.getProperty("all"))).contains("Query", "Book", "Author");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  private static Set<String> fieldNames(final List<Result> results) {
    return results.stream().map(r -> r.<String>getProperty("name")).collect(Collectors.toSet());
  }
}
