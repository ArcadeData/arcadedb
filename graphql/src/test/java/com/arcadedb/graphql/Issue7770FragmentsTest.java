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
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7770: a fragment spread ({@code ...F}) or an inline fragment ({@code ... on Book { }}) parsed
 * but was never expanded, so the fields it selected were dropped and the result carried a {@code null} property key that
 * both {@link Result#toJSON()} and {@link JsonSerializer#serializeResult} refused with "Property name is null".
 */
class Issue7770FragmentsTest extends AbstractGraphQLTest {

  @Test
  void fragmentSpreadIsExpanded() {
    assertSingleBook("fragment F on Book { id name }  query { bookById(id: \"book-1\") { ...F } }", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
    });
  }

  @Test
  void fragmentDefinedAfterTheOperationIsExpanded() {
    // THE OPERATION IS EXECUTED AS SOON AS IT IS MET WHILE WALKING THE DOCUMENT: A FRAGMENT DECLARED AFTER IT MUST STILL BE FOUND
    assertSingleBook("query { bookById(id: \"book-1\") { ...F } }  fragment F on Book { id name }", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
    });
  }

  @Test
  void inlineFragmentIsExpanded() {
    assertSingleBook("query { bookById(id: \"book-1\") { ... on Book { id name } } }", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
    });
  }

  @Test
  void inlineFragmentWithoutTypeConditionIsExpanded() {
    assertSingleBook("{ bookById(id: \"book-1\") { ... { id pageCount } } }", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.<Integer>getProperty("pageCount")).isEqualTo(223);
    });
  }

  @Test
  void fieldsWrittenInlineAndThroughAFragmentAreBothReturned() {
    assertSingleBook("fragment F on Book { name }  query { bookById(id: \"book-1\") { id ...F } }", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
    });
  }

  @Test
  void aliasInsideAFragmentIsTheResponseKey() {
    assertSingleBook("fragment F on Book { title: name }  { bookById(id: \"book-1\") { ...F } }", record -> {
      assertThat(record.<String>getProperty("title")).isEqualTo("Harry Potter and the Philosopher's Stone");
      assertThat(record.getPropertyNames()).doesNotContain("name");
    });
  }

  @Test
  void nestedFragmentsAreExpanded() {
    assertSingleBook("""
        fragment AuthorName on Author { firstName lastName }
        fragment BookWithAuthors on Book { id authors { ...AuthorName } }
        { bookById(id: "book-1") { ...BookWithAuthors } }""", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      final List<Result> authors = record.getProperty("authors");
      assertThat(authors).hasSize(1);
      assertThat(authors.getFirst().<String>getProperty("firstName")).isEqualTo("Joanne");
      assertThat(authors.getFirst().<String>getProperty("lastName")).isEqualTo("Rowling");
      assertThat(authors.getFirst().getPropertyNames()).doesNotContainNull();
    });
  }

  @Test
  void fragmentSpreadInsideAFragmentIsExpanded() {
    assertSingleBook("""
        fragment Id on Book { id }
        fragment Full on Book { ...Id name }
        { bookById(id: "book-1") { ...Full } }""", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
    });
  }

  @Test
  void subSelectionsOfTheSameFieldAreMerged() {
    // `authors` IS SELECTED TWICE, ONCE DIRECTLY AND ONCE THROUGH THE FRAGMENT: THE SPEC MERGES THE TWO SUB-SELECTIONS
    assertSingleBook("""
        fragment F on Book { authors { lastName } }
        { bookById(id: "book-1") { authors { firstName } ...F } }""", record -> {
      final List<Result> authors = record.getProperty("authors");
      assertThat(authors).hasSize(1);
      assertThat(authors.getFirst().<String>getProperty("firstName")).isEqualTo("Joanne");
      assertThat(authors.getFirst().<String>getProperty("lastName")).isEqualTo("Rowling");
    });
  }

  @Test
  void inlineFragmentOnAnotherTypeDoesNotApply() {
    assertSingleBook("{ bookById(id: \"book-1\") { id ... on Author { firstName } } }", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.getPropertyNames()).doesNotContain("firstName");
    });
  }

  @Test
  void fragmentSpreadOnAnotherTypeDoesNotApply() {
    assertSingleBook("fragment A on Author { firstName }  { bookById(id: \"book-1\") { id ...A } }", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.getPropertyNames()).doesNotContain("firstName");
    });
  }

  @Test
  void fragmentSpreadAtTheTopLevelOfTheOperationIsExpanded() {
    assertSingleBook("fragment Q on Query { bookById(id: \"book-1\") { id name } }  { ...Q }", record -> {
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
      assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
    });
  }

  @Test
  void fragmentThroughANativeQueryDirectiveIsExpanded() {
    executeTest(database -> {
      database.command("graphql", """
          type Query {
            sqlBooks(id: String): [Book] @sql(statement: "select from Book where id = 'book-2'")
          }

          type Book {
            id: String
            name: String
          }""");

      try (final ResultSet resultSet = database.query("graphql", "fragment F on Book { id name }  { sqlBooks { ...F } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertSerializable(database, record);
        assertThat(record.<String>getProperty("id")).isEqualTo("book-2");
        assertThat(record.<String>getProperty("name")).isEqualTo("Mr. brain");
      }
      return null;
    });
  }

  @Test
  void fragmentInTypeIntrospectionIsExpanded() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", """
          fragment FieldInfo on __Field { name type { name } }
          fragment TypeInfo on __Type { name fields { ...FieldInfo } }
          { __type(name: "Book") { ...TypeInfo } }""")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertSerializable(database, record);

        assertThat(record.<String>getProperty("name")).isEqualTo("Book");
        final List<Result> fields = record.getProperty("fields");
        assertThat(fields.stream().map(f -> f.<String>getProperty("name")).collect(Collectors.toSet()))
            .contains("id", "name", "pageCount", "authors");
        assertThat(fields.getFirst().<Object>getProperty("type")).isNotNull();
      }
      return null;
    });
  }

  @Test
  void fragmentInSchemaIntrospectionIsExpanded() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", """
          { __schema { ...SchemaInfo } }
          fragment SchemaInfo on __Schema { queryType { name } types { ... on __Type { name } } }""")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertSerializable(database, record);

        final Result queryType = record.getProperty("queryType");
        assertThat(queryType.<String>getProperty("name")).isEqualTo("Query");
        final List<Result> types = record.getProperty("types");
        assertThat(types.stream().map(t -> t.<String>getProperty("name")).collect(Collectors.toSet()))
            .contains("Query", "Book", "Author");
      }
      return null;
    });
  }

  @Test
  void undefinedFragmentIsRejectedAsAParsingError() {
    executeTest(database -> {
      defineTypes(database);

      assertThatThrownBy(() -> database.query("graphql", "{ bookById(id: \"book-1\") { ...Missing } }").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Missing");
      return null;
    });
  }

  @Test
  void cyclicFragmentsAreRejectedAsAParsingError() {
    executeTest(database -> {
      defineTypes(database);

      assertThatThrownBy(() -> database.query("graphql", """
          fragment A on Book { id ...B }
          fragment B on Book { name ...A }
          { bookById(id: "book-1") { ...A } }""").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("cycle");
      return null;
    });
  }

  @Test
  void duplicateFragmentNameIsRejectedAsAParsingError() {
    executeTest(database -> {
      defineTypes(database);

      assertThatThrownBy(() -> database.query("graphql", """
          fragment A on Book { id }
          fragment A on Book { name }
          { bookById(id: "book-1") { ...A } }""").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("A");
      return null;
    });
  }

  @Test
  void repeatedSpreadsOfTheSameFragmentDoNotMultiplyTheWork() {
    // EVERY LEVEL SPREADS THE NEXT ONE TWICE: EXPANDED TEXTUALLY THIS IS 2^20 SELECTIONS OF `id`
    final StringBuilder document = new StringBuilder();
    final int levels = 20;
    for (int i = 0; i < levels; i++)
      document.append("fragment F").append(i).append(" on Book { ...F").append(i + 1).append(" ...F").append(i + 1).append(" }\n");
    document.append("fragment F").append(levels).append(" on Book { id }\n");
    document.append("{ bookById(id: \"book-1\") { ...F0 } }");

    assertSingleBook(document.toString(), record -> assertThat(record.<String>getProperty("id")).isEqualTo("book-1"));
  }

  private void assertSingleBook(final String query, final Consumer<Result> assertions) {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", query)) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertSerializable(database, record);
        assertions.accept(record);
        assertThat(resultSet.hasNext()).isFalse();
      }
      return null;
    });
  }

  /**
   * The two serializers the HTTP query/command handlers go through: both threw on the {@code null} key.
   */
  private static void assertSerializable(final Database database, final Result record) {
    assertThat(record.getPropertyNames()).doesNotContainNull();
    final JSONObject json = record.toJSON();
    assertThat(json).isNotNull();
    assertThat(new JsonSerializer(database).serializeResult(database, record)).isNotNull();
  }
}
