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
 * Regression test for issue #7888 (follow-up to #7036): the leaf fields of an introspection result ({@code name}, {@code kind},
 * {@code ofType}, {@code __typename}) were written under hardcoded keys and whether or not the client selected them, so an alias
 * on a leaf was ignored and unselected keys came back anyway. Every leaf is now written only when selected, under the response
 * key the selection carries.
 */
class Issue7888IntrospectionLeafAliasTest extends AbstractGraphQLTest {

  @Test
  void aliasedNameOnGraphQLTypeIsTheOnlyKey() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __type(name: \"Book\") { n: name } }");
      assertThat(record.getPropertyNames()).containsExactly("n");
      assertThat(record.<String>getProperty("n")).isEqualTo("Book");

      final Result kind = single(database, "{ __type(name: \"Book\") { k: kind } }");
      assertThat(kind.getPropertyNames()).containsExactly("k");
      assertThat(kind.<String>getProperty("k")).isEqualTo("OBJECT");
      return null;
    });
  }

  @Test
  void unselectedLeavesAreNotReturned() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __type(name: \"Book\") { fields { name } } }");
      assertThat(record.getPropertyNames()).containsExactly("fields");
      for (final Result field : record.<List<Result>>getProperty("fields"))
        assertThat(field.getPropertyNames()).containsExactly("name");

      final Result typeOnly = single(database, "{ __type(name: \"Book\") { fields { type { kind } } } }");
      for (final Result field : typeOnly.<List<Result>>getProperty("fields")) {
        assertThat(field.getPropertyNames()).containsExactly("type");
        assertThat(field.<Result>getProperty("type").getPropertyNames()).containsExactly("kind");
      }
      return null;
    });
  }

  @Test
  void aliasedLeavesOnDatabaseOnlyType() {
    executeTest(database -> {
      defineTypes(database);
      database.transaction(() -> database.getSchema().createDocumentType("Shelf").createProperty("position", Type.INTEGER));

      final Result record = single(database,
          "{ __type(name: \"Shelf\") { n: name k: kind fields { fn: name t: type { tn: name tk: kind } } } }");
      assertThat(record.getPropertyNames()).containsExactlyInAnyOrder("n", "k", "fields");
      assertThat(record.<String>getProperty("n")).isEqualTo("Shelf");
      assertThat(record.<String>getProperty("k")).isEqualTo("OBJECT");

      final List<Result> fields = record.getProperty("fields");
      assertThat(fields).hasSize(1);
      final Result field = fields.getFirst();
      assertThat(field.getPropertyNames()).containsExactlyInAnyOrder("fn", "t");
      assertThat(field.<String>getProperty("fn")).isEqualTo("position");

      final Result type = field.getProperty("t");
      assertThat(type.getPropertyNames()).containsExactlyInAnyOrder("tn", "tk");
      assertThat(type.<String>getProperty("tn")).isEqualTo("Int");
      assertThat(type.<String>getProperty("tk")).isEqualTo("SCALAR");
      return null;
    });
  }

  @Test
  void aliasedLeavesOnScalarType() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __type(name: \"String\") { n: name k: kind } }");
      assertThat(record.getPropertyNames()).containsExactlyInAnyOrder("n", "k");
      assertThat(record.<String>getProperty("n")).isEqualTo("String");
      assertThat(record.<String>getProperty("k")).isEqualTo("SCALAR");
      return null;
    });
  }

  @Test
  void aliasedNameInSchemaTypesCoversEveryKindOfType() {
    executeTest(database -> {
      defineTypes(database);
      database.transaction(() -> database.getSchema().createDocumentType("Shelf"));

      final Result record = single(database, "{ __schema { types { n: name } } }");
      final List<Result> types = record.getProperty("types");
      // GraphQL-defined (Book), database-only (Shelf) and built-in scalar (Boolean) types go through three different builders
      assertThat(types.stream().map(t -> t.<String>getProperty("n")).toList()).contains("Book", "Shelf", "Boolean");
      for (final Result type : types)
        assertThat(type.getPropertyNames()).containsExactly("n");
      return null;
    });
  }

  @Test
  void aliasedNameOnQueryType() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __schema { queryType { n: name } } }");
      final Result queryType = record.getProperty("queryType");
      assertThat(queryType.getPropertyNames()).containsExactly("n");
      assertThat(queryType.<String>getProperty("n")).isEqualTo("Query");
      return null;
    });
  }

  @Test
  void aliasedTopLevelTypename() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ t: __typename }");
      assertThat(record.getPropertyNames()).containsExactly("t");
      assertThat(record.<String>getProperty("t")).isEqualTo("Query");
      return null;
    });
  }

  @Test
  void aliasedLeavesAlongTheOfTypeChain() {
    executeTest(database -> {
      database.command("graphql", """
          type Query {
            bookById(id: String): Book
          }

          type Book {
            id: String!
            authors: [Author!]!
          }

          type Author {
            id: String
          }""");

      final Result record = single(database,
          "{ __type(name: \"Book\") { fields { fn: name t: type { k: kind n: name o: ofType { k: kind o: ofType { k: kind o: ofType { k: kind n: name o: ofType { k: kind } } } } } } } }");
      Result authors = null;
      for (final Result field : record.<List<Result>>getProperty("fields"))
        if ("authors".equals(field.getProperty("fn")))
          authors = field;
      assertThat(authors).isNotNull();

      // [Author!]! -> NON_NULL -> LIST -> NON_NULL -> OBJECT, every hop keyed by the alias only
      final Result outerNonNull = authors.getProperty("t");
      assertThat(outerNonNull.getPropertyNames()).containsExactlyInAnyOrder("k", "n", "o");
      assertThat(outerNonNull.<String>getProperty("k")).isEqualTo("NON_NULL");
      assertThat(outerNonNull.<String>getProperty("n")).isNull();

      final Result list = outerNonNull.getProperty("o");
      assertThat(list.getPropertyNames()).containsExactlyInAnyOrder("k", "o");
      assertThat(list.<String>getProperty("k")).isEqualTo("LIST");

      final Result innerNonNull = list.getProperty("o");
      assertThat(innerNonNull.getPropertyNames()).containsExactlyInAnyOrder("k", "o");
      assertThat(innerNonNull.<String>getProperty("k")).isEqualTo("NON_NULL");

      final Result object = innerNonNull.getProperty("o");
      assertThat(object.getPropertyNames()).containsExactlyInAnyOrder("k", "n", "o");
      assertThat(object.<String>getProperty("k")).isEqualTo("OBJECT");
      assertThat(object.<String>getProperty("n")).isEqualTo("Author");
      // a named type has no ofType: the selected key is present and null
      assertThat(object.<Result>getProperty("o")).isNull();
      return null;
    });
  }

  @Test
  void typenameInsideIntrospectionObjects() {
    executeTest(database -> {
      defineTypes(database);

      final Result type = single(database, "{ __type(name: \"Book\") { tn: __typename fields { __typename type { __typename } } } }");
      assertThat(type.<String>getProperty("tn")).isEqualTo("__Type");
      for (final Result field : type.<List<Result>>getProperty("fields")) {
        assertThat(field.<String>getProperty("__typename")).isEqualTo("__Field");
        assertThat(field.<Result>getProperty("type").<String>getProperty("__typename")).isEqualTo("__Type");
      }

      final Result schema = single(database, "{ __schema { __typename queryType { __typename } } }");
      assertThat(schema.<String>getProperty("__typename")).isEqualTo("__Schema");
      assertThat(schema.<Result>getProperty("queryType").<String>getProperty("__typename")).isEqualTo("__Type");
      return null;
    });
  }

  @Test
  void fragmentOnTypeInfoIsExpanded() {
    executeTest(database -> {
      defineTypes(database);

      final Result record = single(database, "{ __type(name: \"Book\") { fields { name type { ... on __Type { n: name k: kind } } } } }");
      for (final Result field : record.<List<Result>>getProperty("fields")) {
        final Result type = field.getProperty("type");
        assertThat(type.getPropertyNames()).containsExactlyInAnyOrder("n", "k");
        assertThat(type.<String>getProperty("k")).isNotNull();
      }
      return null;
    });
  }

  @Test
  void namedObjectInTheOfTypeChainServesItsSelectedFields() {
    executeTest(database -> {
      defineTypes(database);

      // Book.authors is [Author]: the LIST's ofType is the named Author type, a full __Type whose fields can be selected
      final Result record = single(database,
          "{ __type(name: \"Book\") { fields { name type { ofType { n: name af: fields { an: name } } } } } }");
      Result authors = null;
      for (final Result field : record.<List<Result>>getProperty("fields"))
        if ("authors".equals(field.getProperty("name")))
          authors = field;
      assertThat(authors).isNotNull();

      final Result author = authors.<Result>getProperty("type").getProperty("ofType");
      assertThat(author.getPropertyNames()).containsExactlyInAnyOrder("n", "af");
      assertThat(author.<String>getProperty("n")).isEqualTo("Author");
      assertThat(author.<List<Result>>getProperty("af").stream().map(f -> f.<String>getProperty("an")).toList())
          .contains("id", "firstName", "lastName", "wrote");
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
