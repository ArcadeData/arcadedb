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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8745: {@code __typename} selected on a data object was looked up as a record property and
 * came back {@code null}, instead of the name of the object type (spec 4.4.1, "Type Name Introspection"). Apollo Client
 * adds it to every selection set, so every object read as having no type.
 */
class Issue8745TypenameOnDataObjectTest extends AbstractGraphQLTest {

  @Test
  void typenameOnTopLevelObjectIsTheObjectType() {
    assertSingleBook("{ bookById(id: \"book-1\") { __typename name } }", record -> {
      assertThat(record.<String>getProperty("__typename")).isEqualTo("Book");
      assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
    });
  }

  @Test
  void aliasedTypenameIsWrittenUnderTheAlias() {
    assertSingleBook("{ bookById(id: \"book-1\") { kind: __typename name } }", record -> {
      assertThat(record.<String>getProperty("kind")).isEqualTo("Book");
      assertThat(record.getPropertyNames()).doesNotContain("__typename");
    });
  }

  @Test
  void typenameOnNestedRelationshipIsTheNestedObjectType() {
    assertSingleBook("{ bookById(id: \"book-1\") { __typename authors { __typename firstName wrote { __typename id } } } }",
        record -> {
          assertThat(record.<String>getProperty("__typename")).isEqualTo("Book");
          final List<Result> authors = record.getProperty("authors");
          assertThat(authors).hasSize(1);
          assertThat(authors.getFirst().<String>getProperty("__typename")).isEqualTo("Author");
          assertThat(authors.getFirst().<String>getProperty("firstName")).isEqualTo("Joanne");
          final List<Result> wrote = authors.getFirst().getProperty("wrote");
          assertThat(wrote).hasSize(2);
          for (final Result book : wrote)
            assertThat(book.<String>getProperty("__typename")).isEqualTo("Book");
        });
  }

  @Test
  void typenameThroughAFragmentIsTheObjectType() {
    assertSingleBook("fragment F on Book { __typename id }  { bookById(id: \"book-1\") { ...F } }", record -> {
      assertThat(record.<String>getProperty("__typename")).isEqualTo("Book");
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
    });
  }

  @Test
  void typenameInsideAnInlineFragmentIsTheObjectType() {
    assertSingleBook("{ bookById(id: \"book-1\") { ... on Book { __typename } id } }", record -> {
      assertThat(record.<String>getProperty("__typename")).isEqualTo("Book");
      assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
    });
  }

  @Test
  void typenameOnRelationshipTargetOfADeclaredSubTypeIsTheSubType() {
    executeTest(database -> {
      database.getSchema().createVertexType("Novel").addSuperType("Book");
      final MutableVertex novel = database.newVertex("Novel");
      novel.set("id", "novel-1");
      novel.save();
      database.query("sql", "select from Author where id = 'author-1'").next().getElement().get().asVertex()
          .newEdge("IS_AUTHOR_OF", novel);

      defineTypes(database);
      database.command("graphql", "type Novel { id: String }");
      assertBook(database, "{ bookById(id: \"book-1\") { authors { wrote { id __typename } } } }", record -> {
        final List<Result> authors = record.getProperty("authors");
        final List<String> typeNames = new ArrayList<>();
        for (final Result book : authors.getFirst().<List<Result>>getProperty("wrote"))
          typeNames.add(book.getProperty("id") + ":" + book.getProperty("__typename"));
        assertThat(typeNames).containsExactlyInAnyOrder("book-1:Book", "book-2:Book", "novel-1:Novel");
      });
      return null;
    });
  }

  @Test
  void typenameIsNotReadFromARecordPropertyOfTheSameName() {
    // NAMES STARTING WITH "__" ARE RESERVED BY THE SPECIFICATION FOR INTROSPECTION: A PROPERTY THAT HAPPENS TO CARRY THE
    // NAME MUST NOT SHADOW THE TYPE
    executeTest(database -> {
      defineTypes(database);
      database.command("sql", "update Book set __typename = 'Bogus' where id = 'book-1'");
      assertBook(database, "{ bookById(id: \"book-1\") { __typename } }",
          record -> assertThat(record.<String>getProperty("__typename")).isEqualTo("Book"));
      return null;
    });
  }

  @Test
  void typenameOfADatabaseSubTypeNotInTheSchemaIsTheSchemaType() {
    executeTest(database -> {
      database.getSchema().createVertexType("Novel").addSuperType("Book");
      final MutableVertex novel = database.newVertex("Novel");
      novel.set("id", "novel-1");
      novel.set("name", "Dune");
      novel.save();

      defineTypes(database);
      assertBook(database, "{ bookById(id: \"novel-1\") { __typename name } }", record -> {
        assertThat(record.<String>getProperty("__typename")).isEqualTo("Book");
        assertThat(record.<String>getProperty("name")).isEqualTo("Dune");
      });
      return null;
    });
  }

  @Test
  void typenameOfADatabaseSubTypeDeclaredInTheSchemaIsTheConcreteType() {
    executeTest(database -> {
      database.getSchema().createVertexType("Novel").addSuperType("Book");
      final MutableVertex novel = database.newVertex("Novel");
      novel.set("id", "novel-1");
      novel.set("name", "Dune");
      novel.save();

      defineTypes(database);
      database.command("graphql", "type Novel { id: String name: String }");
      assertBook(database, "{ bookById(id: \"novel-1\") { __typename name } }",
          record -> assertThat(record.<String>getProperty("__typename")).isEqualTo("Novel"));
      assertBook(database, "{ bookById(id: \"book-1\") { __typename } }",
          record -> assertThat(record.<String>getProperty("__typename")).isEqualTo("Book"));
      return null;
    });
  }

  @Test
  void typenameOfADatabaseSubTypeIsItsNearestTypeDeclaredInTheSchema() {
    executeTest(database -> {
      database.getSchema().createVertexType("Novel").addSuperType("Book");
      database.getSchema().createVertexType("ScienceFiction").addSuperType("Novel");
      final MutableVertex novel = database.newVertex("ScienceFiction");
      novel.set("id", "novel-1");
      novel.set("name", "Dune");
      novel.save();

      defineTypes(database);
      database.command("graphql", "type Novel { id: String name: String }");
      assertBook(database, "{ bookById(id: \"novel-1\") { __typename name } }",
          record -> assertThat(record.<String>getProperty("__typename")).isEqualTo("Novel"));
      return null;
    });
  }

  @Test
  void typenameWithMultipleInheritanceIsTheDeclaredDirectParentBeforeAGrandparent() {
    executeTest(database -> {
      // Hybrid -> [Paper, Novel], Paper -> Book: Book (A GRANDPARENT, FIRST IN SUPER TYPE ORDER) AND Novel (A DIRECT
      // PARENT) ARE BOTH DECLARED, AND THE NEAREST ONE WINS
      database.getSchema().createVertexType("Paper").addSuperType("Book");
      database.getSchema().createVertexType("Novel");
      database.getSchema().createVertexType("Hybrid").addSuperType("Paper").addSuperType("Novel");
      final MutableVertex hybrid = database.newVertex("Hybrid");
      hybrid.set("id", "hybrid-1");
      hybrid.save();

      defineTypes(database);
      database.command("graphql", "type Novel { id: String }");
      assertBook(database, "{ bookById(id: \"hybrid-1\") { __typename } }",
          record -> assertThat(record.<String>getProperty("__typename")).isEqualTo("Novel"));
      return null;
    });
  }

  @Test
  void typenameOfARecordOfAnUnrelatedTypeIsTheFieldType() {
    // A NATIVE QUERY CAN RETURN RECORDS OF A TYPE THE FIELD CANNOT RETURN: NAMING THAT TYPE WOULD BREAK CLIENTS THAT CHECK
    // __typename AGAINST THE POSSIBLE TYPES OF THE FIELD
    executeTest(database -> {
      defineTypes(database);
      database.command("graphql", """
          type Query {
            bookById(id: String): Book
            mislabelled: Book @sql(statement: "select from Author")
          }""");
      try (final ResultSet resultSet = database.query("graphql", "{ mislabelled { __typename } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("__typename")).isEqualTo("Book");
        assertThat(resultSet.hasNext()).isFalse();
      }
      return null;
    });
  }

  @Test
  void typenameOnAProjectionThatIsNotARecordIsTheFieldType() {
    executeTest(database -> {
      defineTypes(database);
      database.command("graphql", """
          type Query {
            bookById(id: String): Book
            bookNames: [Book] @sql(statement: "select name from Book order by name")
          }""");
      try (final ResultSet resultSet = database.query("graphql", "{ bookNames { __typename name } }")) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result record = resultSet.next();
          assertThat(record.<String>getProperty("__typename")).isEqualTo("Book");
          assertThat(record.<String>getProperty("name")).isNotNull();
          ++count;
        }
        assertThat(count).isEqualTo(2);
      }
      return null;
    });
  }

  @Test
  void typenameIsResolvedForEveryRecordOfAResultSet() {
    // THE TYPE NAME IS CACHED BY DATABASE TYPE: EVERY RECORD, OF EITHER TYPE, STILL GETS ITS OWN
    executeTest(database -> {
      database.getSchema().createVertexType("Novel").addSuperType("Book");
      final MutableVertex novel = database.newVertex("Novel");
      novel.set("id", "novel-1");
      novel.save();

      defineTypes(database);
      database.command("graphql", """
          type Query {
            bookById(id: String): Book
            allBooks: [Book] @sql(statement: "select from Book order by id")
          }
          type Novel { id: String }""");
      try (final ResultSet resultSet = database.query("graphql", "{ allBooks { id __typename } }")) {
        final List<String> typeNames = new ArrayList<>();
        while (resultSet.hasNext()) {
          final Result record = resultSet.next();
          typeNames.add(record.getProperty("id") + ":" + record.getProperty("__typename"));
        }
        assertThat(typeNames).containsExactly("book-1:Book", "book-2:Book", "novel-1:Novel");
      }
      return null;
    });
  }

  @Test
  void typenameOnEmbeddedDocumentOutsideTheSchemaIsTheDatabaseType() {
    executeTest(database -> {
      defineTypes(database);
      database.command("graphql", """
          type Query {
            authorById(id: String): Author
          }
          type Author {
            id: String
            address: Address
          }""");
      try (final ResultSet resultSet = database.query("graphql",
          "{ authorById(id: \"author-1\") { __typename address { __typename city } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.<String>getProperty("__typename")).isEqualTo("Author");
        final Result address = record.getProperty("address");
        assertThat(address.<String>getProperty("__typename")).isEqualTo("Address");
        assertThat(address.<String>getProperty("city")).isEqualTo("Rome");
      }
      return null;
    });
  }

  @Test
  void typenameIsReturnedOnlyWhenSelected() {
    assertSingleBook("{ bookById(id: \"book-1\") { name } }",
        record -> assertThat(record.getPropertyNames()).doesNotContain("__typename"));
  }

  private void assertSingleBook(final String query, final Consumer<Result> assertions) {
    executeTest(database -> {
      defineTypes(database);
      assertBook(database, query, assertions);
      return null;
    });
  }

  private static void assertBook(final Database database, final String query, final Consumer<Result> assertions) {
    try (final ResultSet resultSet = database.query("graphql", query)) {
      assertThat(resultSet.hasNext()).isTrue();
      assertions.accept(resultSet.next());
      assertThat(resultSet.hasNext()).isFalse();
    }
  }
}
