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
package com.arcadedb.graphql.schema;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graphql.AbstractGraphQLTest;
import com.arcadedb.graphql.parser.ParseException;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8623: {@link GraphQLResultSet} cached the projections of a selection level only when no fragment type condition
 * was evaluated while expanding it. A named fragment always carries one, so every level reached through a fragment spread
 * was expanded again for every record. The outcome of a condition depends only on the schema type of the level and on the
 * database type of the record, so the projections are now cached once for all records, or once per database type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8623FragmentProjectionCacheTest extends AbstractGraphQLTest {
  private static final int BOOKS = 50;

  /**
   * Both conditions name the schema type of their level ({@code F on Book} under a Book, {@code A on Author} under an
   * Author), so they are decided without looking at the record: each level is built once, however many records.
   */
  @Test
  void aLevelReachedThroughAFragmentIsBuiltOnceForAllRecords() {
    executeTest(database -> {
      final GraphQLSchema schema = schemaWithBooks(database);

      try (final GraphQLResultSet resultSet = execute(schema, """
          fragment A on Author { firstName }
          fragment F on Book { id name authors { ...A } }
          { bookByName { ...F } }""", null)) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result record = resultSet.next();
          assertThat(record.<String>getProperty("id")).isNotNull();
          assertThat(record.<String>getProperty("name")).isNotNull();
          count++;
        }
        assertThat(count).isEqualTo(BOOKS + 2);
        // ONE BUILD FOR THE BOOK LEVEL, ONE FOR THE AUTHORS LEVEL
        assertThat(resultSet.getProjectionBuilds()).isEqualTo(2);
      }
      return null;
    });
  }

  /**
   * A condition on a sub-type of the level's schema type is decided by the record: the projections are cached per
   * database type, and a Novel must never be served the projections of a plain Book, nor the other way round.
   */
  @Test
  void aConditionDecidedByTheRecordIsCachedPerDatabaseType() {
    executeTest(database -> {
      final GraphQLSchema schema = schemaWithBooks(database);
      database.getSchema().createVertexType("Novel").addSuperType("Book");
      for (int i = 0; i < BOOKS; i++)
        database.newVertex("Novel").set("id", "novel-" + i).set("name", "Novel " + i).set("pageCount", 100 + i).save();

      try (final GraphQLResultSet resultSet = execute(schema,
          "{ bookByName { id ... on Novel { pageCount } } }", null)) {
        int books = 0;
        int novels = 0;
        while (resultSet.hasNext()) {
          final Result record = resultSet.next();
          final String id = record.getProperty("id");
          if (id.startsWith("novel-")) {
            assertThat(record.<Integer>getProperty("pageCount")).as(id).isNotNull();
            novels++;
          } else {
            assertThat(record.getPropertyNames()).as(id).doesNotContain("pageCount");
            books++;
          }
        }
        assertThat(books).isEqualTo(BOOKS + 2);
        assertThat(novels).isEqualTo(BOOKS);
        // ONE BUILD PER DATABASE TYPE MET
        assertThat(resultSet.getProjectionBuilds()).isEqualTo(2);
      }
      return null;
    });
  }

  /** The directives read the operation's variables, the same for every record: they do not defeat the cache. */
  @Test
  void directivesDoNotDefeatTheCache() {
    executeTest(database -> {
      final GraphQLSchema schema = schemaWithBooks(database);

      try (final GraphQLResultSet resultSet = execute(schema,
          "fragment F on Book { name @include(if: $x) }  query($x: Boolean!) { bookByName { id ...F pageCount @skip(if: $x) } }",
          Map.of("x", true))) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result record = resultSet.next();
          assertThat(record.getPropertyNames()).contains("id", "name").doesNotContain("pageCount");
          count++;
        }
        assertThat(count).isEqualTo(BOOKS + 2);
        assertThat(resultSet.getProjectionBuilds()).isEqualTo(1);
      }
      return null;
    });
  }

  /**
   * A named fragment's selection lists are shared by all its spreads, so the same list is resolved against a different
   * schema type under each: here {@code related { id }} is read as a list of Authors under a Book and as a list of Books
   * under an Author. The two must not evict each other on every record.
   */
  @Test
  void aListResolvedAgainstTwoSchemaTypesKeepsBothEntries() {
    executeTest(database -> {
      final GraphQLSchema schema = schemaWithBooks(database);
      define(schema, """
          type Query {
            bookByName(name: String): [Book]
          }

          type Book {
            id: String
            authors: [Author] @relationship(type: "IS_AUTHOR_OF", direction: IN)
            related: [Author] @relationship(type: "IS_AUTHOR_OF", direction: IN)
          }

          type Author {
            id: String
            related: [Book] @relationship(type: "IS_AUTHOR_OF", direction: OUT)
          }""");

      try (final GraphQLResultSet resultSet = execute(schema, """
          fragment R on Anything { related { id } }
          { bookByName { id ...R authors { id ...R } } }""", null)) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result record = resultSet.next();
          final List<Result> authors = record.getProperty("authors");
          assertThat(record.<List<Result>>getProperty("related")).hasSize(1);
          assertThat(authors.get(0).<List<Result>>getProperty("related")).hasSize(BOOKS + 2);
          count++;
        }
        assertThat(count).isEqualTo(BOOKS + 2);
        // THE BOOK LEVEL, THE AUTHORS LEVEL, AND THE SHARED related LIST ONCE UNDER EACH OF THE TWO SCHEMA TYPES
        assertThat(resultSet.getProjectionBuilds()).isEqualTo(4);
      }
      return null;
    });
  }

  /** Rows that wrap no record (a native query's projection) share one entry too, with the same outcome for each. */
  @Test
  void rowsWithoutARecordShareOneEntry() {
    executeTest(database -> {
      final GraphQLSchema schema = schemaWithBooks(database);
      define(schema, """
          type Query {
            bookRows: [Book] @sql(statement: "select id, name, pageCount from Book")
          }""");

      try (final GraphQLResultSet resultSet = execute(schema,
          "{ bookRows { id ... on Book { name } ... on Author { firstName } } }", null)) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result record = resultSet.next();
          assertThat(record.<String>getProperty("name")).isNotNull();
          assertThat(record.getPropertyNames()).doesNotContain("firstName");
          count++;
        }
        assertThat(count).isEqualTo(BOOKS + 2);
        assertThat(resultSet.getProjectionBuilds()).isEqualTo(1);
      }
      return null;
    });
  }

  private GraphQLSchema schemaWithBooks(final Database database) {
    final GraphQLSchema schema = new GraphQLSchema(database);
    define(schema, """
        type Query {
          bookByName(name: String): [Book]
        }

        type Book {
          id: String
          name: String
          pageCount: Int
          authors: [Author] @relationship(type: "IS_AUTHOR_OF", direction: IN)
        }

        type Author {
          id: String
          firstName: String
          lastName: String
        }""");

    final MutableVertex author = database.iterateType("Author", false).next().asVertex().modify();
    for (int i = 0; i < BOOKS; i++) {
      final MutableVertex book = database.newVertex("Book").set("id", "extra-" + i).set("name", "Book " + i).save();
      author.newEdge("IS_AUTHOR_OF", book);
    }
    return schema;
  }

  /** Runs a document through the schema itself, so the result set is the {@link GraphQLResultSet} it built. */
  private static GraphQLResultSet execute(final GraphQLSchema schema, final String document,
      final Map<String, Object> parameters) {
    try {
      return (GraphQLResultSet) schema.execute(document, parameters);
    } catch (final ParseException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void define(final GraphQLSchema schema, final String sdl) {
    try {
      schema.execute(sdl).close();
    } catch (final ParseException e) {
      throw new IllegalStateException(e);
    }
  }
}
