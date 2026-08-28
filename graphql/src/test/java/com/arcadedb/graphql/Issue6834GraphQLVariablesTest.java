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
import java.util.List;
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
  void valueThatTheDeclaredTypeCannotHoldIsRejected() {
    executeTest(database -> {
      final String types = """
          type Query {
            bookByPageCount(pageCount: Int): Book
            bookByName(name: String): Book
          }

          type Book {
            id: String
            name: String
            pageCount: Int
          }""";
      database.command("graphql", types);

      assertThatThrownBy(
          () -> database.query("graphql", "query($p: Int) { bookByPageCount(pageCount: $p) { id } }", "p", "422").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$p")
          .hasMessageContaining("Int");

      assertThatThrownBy(
          () -> database.query("graphql", "query($n: String) { bookByName(name: $n) { id } }", "n", 42).close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$n")
          .hasMessageContaining("String");

      // An integer input value is a valid Float by the specification, so this one must go through.
      try (final ResultSet resultSet = database.query("graphql",
          "query($p: Int) { bookByPageCount(pageCount: $p) { id } }", "p", 422)) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-2");
      }

      return null;
    });
  }

  @Test
  void idAcceptsAnIntegralValueOfAnyWidth() {
    executeTest(database -> {
      final String types = """
          type Query {
            bookByKey(key: ID): Book
          }

          type Book {
            id: String
            key: ID
            name: String
          }""";
      database.command("graphql", types);

      database.newVertex("Book").set("id", "book-3").set("key", 5_000_000_000L).set("name", "Big key").save();

      // ID carries no range of its own - it is serialised as a string - which is exactly why a schema reaches for
      // it when the key does not fit in the 32-bit Int. Routing it through the Int bound would reject the values
      // it exists to accept.
      try (final ResultSet resultSet = database.query("graphql", "query($k: ID) { bookByKey(key: $k) { id } }", "k",
          5_000_000_000L)) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-3");
        assertThat(resultSet.hasNext()).isFalse();
      }

      // A string is equally valid for ID, a floating-point value is not.
      assertThatThrownBy(
          () -> database.query("graphql", "query($k: ID) { bookByKey(key: $k) { id } }", "k", 1.5d).close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("ID");

      return null;
    });
  }

  @Test
  void defaultLiteralThatTheDeclaredTypeCannotHoldIsRejected() {
    executeTest(database -> {
      defineTypes(database);

      assertThatThrownBy(
          () -> database.query("graphql", "query($n: Int = \"Mr. brain\") { bookByName(name: $n) { id } }").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$n")
          .hasMessageContaining("Int");

      return null;
    });
  }

  @Test
  void customScalarTypeIsNotChecked() {
    executeTest(database -> {
      final String types = """
          type Query {
            bookByName(name: BookName): Book
          }

          type Book {
            id: String
            name: String
          }""";
      database.command("graphql", types);

      // BookName is not a type this module models: rejecting what it cannot describe would be worse than passing
      // it through, so a variable declared with a custom scalar keeps working whatever its value is.
      try (final ResultSet resultSet = database.query("graphql",
          "query($n: BookName) { bookByName(name: $n) { id } }", "n", "Mr. brain")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-2");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void reservedWhereArgumentCannotBeFilledFromAVariable() {
    executeTest(database -> {
      final String types = """
          type Query {
            books(where: WHERE): [Book!]!
          }

          type Book {
            id: String
            name: String
          }""";
      database.command("graphql", types);

      // `where` is interpolated verbatim, so filling it from a variable would hand raw SQL to whoever supplies the
      // parameters - typically the caller an application considers untrusted. It never resolved before #6834, so
      // refusing it costs nothing and closes the hole that making variables work would otherwise open.
      assertThatThrownBy(() -> database.query("graphql", "query($w: WHERE) { books(where: $w) { id } }", "w",
          "name = 'Mr. brain' or 1 = 1").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$w")
          .hasMessageContaining("@sql");

      // A predicate written in the document itself keeps working: that text is authored, not supplied.
      try (final ResultSet resultSet = database.query("graphql", "{ books( where: \"name = 'Mr. brain'\" ) { id } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("book-2");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void inlineRelationshipDirectiveResolvesVariables() {
    executeTest(database -> {
      defineTypes(database);

      // An inline directive is written in the query document, so its arguments are in the operation's scope and a
      // variable belongs there as much as in an ordinary argument.
      // No comma between the two variable definitions: this grammar makes the comma a real token and only the
      // argument list accepts one, so `query($t: String, $d: String)` does not parse. Tracked as issue #6860 -
      // fixing it means regenerating the parser, which drags in unrelated differences from a newer generator.
      try (final ResultSet resultSet = database.query("graphql", """
          query($t: String $d: String) { bookByName(name: "Mr. brain") {
              authors @relationship(type: $t, direction: $d) { lastName }
          } }""", "t", "IS_AUTHOR_OF", "d", "IN")) {
        assertThat(resultSet.hasNext()).isTrue();

        final List<Result> authors = resultSet.next().getProperty("authors");
        assertThat(authors).hasSize(1);
        assertThat(authors.getFirst().<String>getProperty("lastName")).isEqualTo("Rowling");

        assertThat(resultSet.hasNext()).isFalse();
      }

      // A variable no operation declares is reported here too, rather than resolving to null and quietly walking no
      // edge at all. The directives are evaluated while mapping a record, so the result set has to be consumed.
      assertThatThrownBy(() -> {
        try (final ResultSet resultSet = database.query("graphql",
            "{ bookByName(name: \"Mr. brain\") { authors @relationship(type: $t) { lastName } } }")) {
          while (resultSet.hasNext())
            resultSet.next();
        }
      })
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("$t");

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
