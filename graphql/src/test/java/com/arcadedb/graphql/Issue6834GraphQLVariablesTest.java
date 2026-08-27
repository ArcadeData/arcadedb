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

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6834: the grammar accepted variables but nothing bound them, so every {@code $variable}
 * resolved to null and was interpolated into the generated SQL as the literal {@code null}, silently returning the
 * wrong rows instead of raising an error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6834GraphQLVariablesTest extends AbstractGraphQLTest {

  @Test
  void variableIsBoundFromTheQueryParameters() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", "query($n: String) { bookByName(name: $n) { id name } }",
          "n", "Harry Potter and the Philosopher's Stone")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
        assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
        assertThat(resultSet.hasNext()).isFalse();
      }

      try (final ResultSet resultSet = database.query("graphql", "query($n: String) { bookByName(name: $n) { id } }",
          "n", "Mr. brain")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-2");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void nonStringVariableKeepsItsType() {
    executeTest(database -> {
      final String types = """
          type Query {
            bookByPageCount(pageCount: Int): Book
          }

          type Book {
            id: String
            name: String
            pageCount: Int
          }""";
      database.command("graphql", types);

      try (final ResultSet resultSet = database.query("graphql",
          "query($p: Int) { bookByPageCount(pageCount: $p) { id } }", "p", 422)) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-2");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void declaredDefaultValueIsUsedWhenNoParameterIsPassed() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql",
          "query($n: String = \"Mr. brain\") { bookByName(name: $n) { id } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-2");
        assertThat(resultSet.hasNext()).isFalse();
      }

      // A passed value wins over the declared default.
      try (final ResultSet resultSet = database.query("graphql",
          "query($n: String = \"Mr. brain\") { bookByName(name: $n) { id } }", "n",
          "Harry Potter and the Philosopher's Stone")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-1");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void variableIsBoundOnTheNativeDirectivePath() {
    executeTest(database -> {
      final String types = """
          type Query {
            bookByName(bookNameParameter: String): Book @sql(statement: "select from Book where name = :bookNameParameter")
          }

          type Book {
            id: String
            name: String
          }""";
      database.command("graphql", types);

      try (final ResultSet resultSet = database.query("graphql",
          "query($n: String) { bookByName(bookNameParameter: $n) { id } }", "n", "Mr. brain")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-2");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void undeclaredVariableIsRejected() {
    executeTest(database -> {
      defineTypes(database);

      // No variable definitions at all: a wrong result set is much worse than an error.
      assertThatThrownBy(() -> database.query("graphql", "{ bookByName(name: $n) { id } }").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$n");

      // Declared variables, but not this one.
      assertThatThrownBy(
          () -> database.query("graphql", "query($other: String) { bookByName(name: $n) { id } }", "other", "x").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$n");

      return null;
    });
  }

  @Test
  void nonNullVariableWithoutValueIsRejected() {
    executeTest(database -> {
      defineTypes(database);

      assertThatThrownBy(() -> database.query("graphql", "query($n: String!) { bookByName(name: $n) { id } }").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$n");

      return null;
    });
  }

  @Test
  void nonNullVariableExplicitlySetToNullIsRejected() {
    executeTest(database -> {
      defineTypes(database);

      // The key is present, so an "is it missing?" check alone would let this through: the specification requires
      // a non-null variable to reject a null value, not just an absent one.
      final Map<String, Object> parameters = new HashMap<>();
      parameters.put("n", null);

      assertThatThrownBy(
          () -> database.query("graphql", "query($n: String!) { bookByName(name: $n) { id } }", parameters).close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$n");

      return null;
    });
  }

  @Test
  void nullableVariableExplicitlySetToNullIsAccepted() {
    executeTest(database -> {
      defineTypes(database);

      final Map<String, Object> parameters = new HashMap<>();
      parameters.put("n", null);

      try (final ResultSet resultSet = database.query("graphql", "query($n: String) { bookByName(name: $n) { id } }",
          parameters)) {
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void nullableVariableWithoutValueResolvesToNull() {
    executeTest(database -> {
      defineTypes(database);

      // A nullable variable left unset is null by the specification: `name = null` matches nothing, but it is the
      // documented outcome rather than the accidental one it used to be.
      try (final ResultSet resultSet = database.query("graphql", "query($n: String) { bookByName(name: $n) { id } }")) {
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }
}
