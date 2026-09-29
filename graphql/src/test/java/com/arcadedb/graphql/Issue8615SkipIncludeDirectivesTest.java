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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8615: the built-in {@code @skip(if:)} and {@code @include(if:)} executable directives were not evaluated
 * anywhere, so a client toggling parts of a query through variables (a common Apollo/Relay pattern) got the fields it asked
 * to leave out. The GraphQL specification allows them on a field, a fragment spread and an inline fragment: a selection is
 * resolved only when no {@code @skip} condition is true and no {@code @include} condition is false.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8615SkipIncludeDirectivesTest extends AbstractGraphQLTest {

  @Test
  void skipOnAFieldWithALiteral() {
    assertSingleBook("{ bookById(id: \"book-1\") { id name @skip(if: true) pageCount @skip(if: false) } }", null, record -> {
      assertThat(record.getPropertyNames()).contains("id", "pageCount").doesNotContain("name");
      assertThat(record.<Integer>getProperty("pageCount")).isEqualTo(223);
    });
  }

  @Test
  void includeOnAFieldWithAVariable() {
    final String query = "query($withName: Boolean!) { bookById(id: \"book-1\") { id name @include(if: $withName) } }";
    assertSingleBook(query, Map.of("withName", true),
        record -> assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone"));
    assertSingleBook(query, Map.of("withName", false), record -> assertThat(record.getPropertyNames()).doesNotContain("name"));
  }

  @Test
  void aVariableDefaultValueDecidesWhenNoValueIsPassed() {
    assertSingleBook("query($skipName: Boolean = true) { bookById(id: \"book-1\") { id name @skip(if: $skipName) } }", null,
        record -> assertThat(record.getPropertyNames()).doesNotContain("name"));
  }

  /** Both on the same selection: resolved only when the skip condition is false AND the include condition is true. */
  @Test
  void skipAndIncludeTogetherMustBothLetTheFieldThrough() {
    final String query = "query($s: Boolean!, $i: Boolean!) { bookById(id: \"book-1\") { id name @skip(if: $s) @include(if: $i) } }";
    for (final boolean skip : new boolean[] { false, true })
      for (final boolean include : new boolean[] { false, true })
        assertSingleBook(query, Map.of("s", skip, "i", include), record -> assertThat(record.getPropertyNames().contains("name"))
            .as("@skip(if: %s) @include(if: %s)", skip, include).isEqualTo(!skip && include));
  }

  @Test
  void skipOnAFragmentSpread() {
    final String query = """
        fragment Details on Book { name pageCount }
        query($brief: Boolean!) { bookById(id: "book-1") { id ...Details @skip(if: $brief) } }""";
    assertSingleBook(query, Map.of("brief", true),
        record -> assertThat(record.getPropertyNames()).contains("id").doesNotContain("name", "pageCount"));
    assertSingleBook(query, Map.of("brief", false),
        record -> assertThat(record.getPropertyNames()).contains("id", "name", "pageCount"));
  }

  @Test
  void includeOnAnInlineFragment() {
    assertSingleBook("{ bookById(id: \"book-1\") { id ... on Book @include(if: false) { name } } }", null,
        record -> assertThat(record.getPropertyNames()).doesNotContain("name"));
    assertSingleBook("{ bookById(id: \"book-1\") { id ... @include(if: true) { name } } }", null,
        record -> assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone"));
  }

  /**
   * The specification's CollectFields evaluates the directives before it records a fragment as visited: a spread they
   * exclude does not stop another spread of the same fragment at that level from contributing.
   */
  @Test
  void aSkippedSpreadDoesNotHideAnotherSpreadOfTheSameFragment() {
    assertSingleBook("fragment F on Book { name }  { bookById(id: \"book-1\") { id ...F @skip(if: true) ...F } }", null,
        record -> assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone"));
  }

  /** The same key selected twice, once skipped: the other selection still resolves it. */
  @Test
  void aFieldSkippedOnceIsStillResolvedThroughItsOtherSelection() {
    assertSingleBook("{ bookById(id: \"book-1\") { id name @skip(if: true) name } }", null,
        record -> assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone"));
  }

  @Test
  void directivesAreEvaluatedAtEveryLevelAndInsideFragments() {
    final String query = """
        fragment AuthorName on Author { firstName lastName @skip(if: $brief) }
        query($brief: Boolean!) { bookById(id: "book-1") { id authors { ...AuthorName } } }""";
    assertSingleBook(query, Map.of("brief", true), record -> {
      final List<Result> authors = record.getProperty("authors");
      assertThat(authors).hasSize(1);
      assertThat(authors.getFirst().<String>getProperty("firstName")).isEqualTo("Joanne");
      assertThat(authors.getFirst().getPropertyNames()).doesNotContain("lastName");
    });

    assertSingleBook("{ bookById(id: \"book-1\") { id authors @skip(if: true) { firstName } } }", null,
        record -> assertThat(record.getPropertyNames()).contains("id").doesNotContain("authors"));
  }

  /** Every book is resolved with the same outcome: the directives do not depend on the record. */
  @Test
  void theOutcomeHoldsForEveryRecord() {
    executeTest(database -> {
      defineTypes(database);
      try (final ResultSet resultSet = database.query("graphql",
          "query($x: Boolean!) { bookByName { id name @include(if: $x) pageCount @skip(if: $x) } }", Map.of("x", true))) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result record = resultSet.next();
          assertThat(record.getPropertyNames()).contains("id", "name").doesNotContain("pageCount");
          count++;
        }
        assertThat(count).isEqualTo(2);
      }
      return null;
    });
  }

  /** A valid document can exclude the only field of its operation: the response has no field, which is not an error. */
  @Test
  void theOnlyFieldOfTheOperationCanBeSkipped() {
    executeTest(database -> {
      defineTypes(database);
      final String query = "query($x: Boolean!) { bookById(id: \"book-1\") @skip(if: $x) { id } }";
      try (final ResultSet resultSet = database.query("graphql", query, Map.of("x", true))) {
        assertThat(resultSet.hasNext()).isFalse();
      }
      try (final ResultSet resultSet = database.query("graphql", query, Map.of("x", false))) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-1");
      }
      return null;
    });
  }

  @Test
  void skippingOneOfTwoOperationFieldsLeavesASingleQuery() {
    assertSingleBook("{ bookById(id: \"book-1\") { id }  bookByName(name: \"Mr. brain\") @include(if: false) { id } }", null,
        record -> assertThat(record.<String>getProperty("id")).isEqualTo("book-1"));
  }

  @Test
  void introspectionHonorsTheDirectives() {
    executeTest(database -> {
      defineTypes(database);
      try (final ResultSet resultSet = database.query("graphql", "{ __type(name: \"Book\") { name fields @skip(if: true) { name } } }")) {
        final Result type = resultSet.next();
        assertThat(type.<String>getProperty("name")).isEqualTo("Book");
        assertThat(type.getPropertyNames()).doesNotContain("fields");
      }
      try (final ResultSet resultSet = database.query("graphql",
          "{ __schema { queryType @include(if: false) { name } types { name } } }")) {
        final Result schema = resultSet.next();
        assertThat(schema.getPropertyNames()).contains("types").doesNotContain("queryType");
      }
      return null;
    });
  }

  @Test
  void invalidDirectivesAreRejectedBeforeAnyRecordIsRead() {
    executeTest(database -> {
      defineTypes(database);
      final Map<String, Object> nullValue = new HashMap<>();
      nullValue.put("n", null);

      assertRejected(database, "{ bookById(id: \"book-1\") { name @skip } }", null, "requires the argument 'if'");
      assertRejected(database, "{ bookById(id: \"book-1\") { name @include(if: \"yes\") } }", null, "must be a Boolean");
      assertRejected(database, "{ bookById(id: \"book-1\") { name @skip(if: $undeclared) } }", null, "not declared");
      assertRejected(database, "query($n: Boolean) { bookById(id: \"book-1\") { name @include(if: $n) } }", nullValue,
          "must be a Boolean, but it is null");
      assertRejected(database, "{ bookById(id: \"book-1\") { name @skip(if: true, when: 1) } }", null, "no argument 'when'");
      assertRejected(database, "{ bookById(id: \"book-1\") { name @skip(if: true, if: false) } }", null, "more than once");
      assertRejected(database, "{ bookById(id: \"book-1\") { name @skip(if: true) @skip(if: false) } }", null, "more than once");
      assertRejected(database, "fragment F on Book @skip(if: true) { name }  { bookById(id: \"book-1\") { ...F } }", null,
          "cannot be used on the definition of fragment 'F'");
      // THE SAME RULES ON A FRAGMENT SPREAD AND ON AN INLINE FRAGMENT
      assertRejected(database, "fragment F on Book { name }  { bookById(id: \"book-1\") { ...F @include(if: $missing) } }", null,
          "not declared");
      assertRejected(database, "{ bookById(id: \"book-1\") { ... on Book @skip(if: 1) { name } } }", null, "must be a Boolean");
      // VALIDATION DOES NOT DEPEND ON THE VARIABLES: AN INVALID DIRECTIVE INSIDE A SKIPPED BRANCH IS STILL REPORTED
      assertRejected(database, "{ bookById(id: \"book-1\") { authors @skip(if: true) { firstName @include(if: 1) } } }", null,
          "must be a Boolean");
      return null;
    });
  }

  private static void assertRejected(final Database database, final String query, final Map<String, Object> parameters,
      final String message) {
    assertThatThrownBy(() -> (parameters != null ? database.query("graphql", query, parameters) : database.query("graphql", query)).close())
        .as(query)
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining(message);
  }

  private void assertSingleBook(final String query, final Map<String, Object> parameters, final Consumer<Result> assertions) {
    executeTest(database -> {
      defineTypes(database);
      try (final ResultSet resultSet = parameters != null ? database.query("graphql", query, parameters) :
          database.query("graphql", query)) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.getPropertyNames()).doesNotContainNull();
        assertions.accept(record);
        assertThat(resultSet.hasNext()).isFalse();
      }
      return null;
    });
  }
}
