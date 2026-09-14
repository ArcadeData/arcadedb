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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #7116: introspection used to report a list field as {@code kind: "LIST"}
 * with the ELEMENT type's name in {@code name} and no {@code ofType} at all, and never reported
 * {@code NON_NULL} for a non-null modifier. Both diverge from the introspection schema every
 * GraphQL client (GraphiQL, Apollo codegen, schema-to-types generators) relies on to walk
 * list/non-null wrappers via {@code ofType}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7116IntrospectionListNonNullTest extends AbstractGraphQLTest {

  private void defineTypesWithModifiers(final Database database) {
    final String types = """
        type Query {
          bookById(id: String): Book
        }

        type Book {
          id: String!
          name: String
          tags: [String]
          authors: [Author!]!
        }

        type Author {
          id: String
        }""";
    database.command("graphql", types);
  }

  @Test
  void listFieldReportsWrapperWithNoNameAndOfTypeElement() {
    executeTest(database -> {
      defineTypesWithModifiers(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Book\") { fields { name type { kind name ofType { kind name } } } } }")) {
        final Result record = resultSet.next();
        final Result tags = fieldNamed(record, "tags");

        final Result type = tags.getProperty("type");
        assertThat(type.<String>getProperty("kind")).isEqualTo("LIST");
        assertThat(type.<String>getProperty("name")).isNull();

        final Result ofType = type.getProperty("ofType");
        assertThat(ofType).isNotNull();
        assertThat(ofType.<String>getProperty("kind")).isEqualTo("SCALAR");
        assertThat(ofType.<String>getProperty("name")).isEqualTo("String");
      }

      return null;
    });
  }

  @Test
  void nonNullFieldReportsWrapperWithNoNameAndOfTypeInner() {
    executeTest(database -> {
      defineTypesWithModifiers(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Book\") { fields { name type { kind name ofType { kind name } } } } }")) {
        final Result record = resultSet.next();
        final Result id = fieldNamed(record, "id");

        final Result type = id.getProperty("type");
        assertThat(type.<String>getProperty("kind")).isEqualTo("NON_NULL");
        assertThat(type.<String>getProperty("name")).isNull();

        final Result ofType = type.getProperty("ofType");
        assertThat(ofType).isNotNull();
        assertThat(ofType.<String>getProperty("kind")).isEqualTo("SCALAR");
        assertThat(ofType.<String>getProperty("name")).isEqualTo("String");
      }

      return null;
    });
  }

  @Test
  void plainNamedFieldStillReportsItsOwnName() {
    executeTest(database -> {
      defineTypesWithModifiers(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Book\") { fields { name type { kind name } } } }")) {
        final Result record = resultSet.next();
        final Result name = fieldNamed(record, "name");

        final Result type = name.getProperty("type");
        assertThat(type.<String>getProperty("kind")).isEqualTo("SCALAR");
        assertThat(type.<String>getProperty("name")).isEqualTo("String");
      }

      return null;
    });
  }

  @Test
  void nonNullListOfNonNullObjectsChainsAllFourWrappers() {
    // [Author!]! -> NON_NULL -> LIST -> NON_NULL -> OBJECT, exactly the chain the issue calls out.
    executeTest(database -> {
      defineTypesWithModifiers(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ __type(name: \"Book\") { fields { name type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } }")) {
        final Result record = resultSet.next();
        final Result authors = fieldNamed(record, "authors");

        final Result outerNonNull = authors.getProperty("type");
        assertThat(outerNonNull.<String>getProperty("kind")).isEqualTo("NON_NULL");
        assertThat(outerNonNull.<String>getProperty("name")).isNull();

        final Result list = outerNonNull.getProperty("ofType");
        assertThat(list.<String>getProperty("kind")).isEqualTo("LIST");
        assertThat(list.<String>getProperty("name")).isNull();

        final Result innerNonNull = list.getProperty("ofType");
        assertThat(innerNonNull.<String>getProperty("kind")).isEqualTo("NON_NULL");
        assertThat(innerNonNull.<String>getProperty("name")).isNull();

        final Result object = innerNonNull.getProperty("ofType");
        assertThat(object.<String>getProperty("kind")).isEqualTo("OBJECT");
        assertThat(object.<String>getProperty("name")).isEqualTo("Author");
      }

      return null;
    });
  }

  private static Result fieldNamed(final Result typeResult, final String fieldName) {
    for (final Result field : typeResult.<java.util.List<Result>>getProperty("fields"))
      if (fieldName.equals(field.<String>getProperty("name")))
        return field;
    throw new AssertionError("field not found: " + fieldName);
  }
}
