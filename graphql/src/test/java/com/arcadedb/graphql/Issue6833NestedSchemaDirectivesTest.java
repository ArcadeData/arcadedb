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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6833: every nesting level resolved its schema directives against the top-level query
 * return type, so a {@code @relationship} declared two levels deep was looked up in the wrong
 * {@code ObjectTypeDefinition} and the key was silently dropped from the response.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6833NestedSchemaDirectivesTest extends AbstractGraphQLTest {

  @Test
  void depthTwoRelationshipIsTraversed() {
    executeTest(database -> {
      defineTypes(database);

      // Book.authors is @relationship(IS_AUTHOR_OF, IN), Author.wrote is @relationship(IS_AUTHOR_OF, OUT). Before
      // the fix `wrote` was looked up on Book - the top-level return type - which has no such field, so the Author
      // records came back without the key at all.
      try (final ResultSet resultSet = database.query("graphql", """
          { bookByName(name: "Harry Potter and the Philosopher's Stone") {
              name
              authors { lastName wrote { name } }
          } }""")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result book = resultSet.next();
        assertThat(book.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");

        final List<Result> authors = book.getProperty("authors");
        assertThat(authors).hasSize(1);

        final Result author = authors.get(0);
        assertThat(author.<String>getProperty("lastName")).isEqualTo("Rowling");
        assertThat(author.getPropertyNames()).contains("wrote");

        final List<Result> wrote = author.getProperty("wrote");
        assertThat(wrote).hasSize(2);
        assertThat(wrote.stream().map(r -> r.<String>getProperty("name")).toList())
            .containsExactlyInAnyOrder("Harry Potter and the Philosopher's Stone", "Mr. brain");

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void depthThreeRelationshipIsTraversed() {
    executeTest(database -> {
      defineTypes(database);

      // The cycle Book -> Author -> Book is walked as deep as the query asks for: the guard that stops the
      // automatic expansion of a cyclic schema must not limit an explicit selection set.
      try (final ResultSet resultSet = database.query("graphql", """
          { bookByName(name: "Mr. brain") {
              name
              authors { wrote { name authors { firstName } } }
          } }""")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result book = resultSet.next();

        final List<Result> authors = book.getProperty("authors");
        assertThat(authors).hasSize(1);

        final List<Result> wrote = authors.get(0).getProperty("wrote");
        assertThat(wrote).hasSize(2);
        for (final Result written : wrote) {
          final List<Result> writtenBy = written.getProperty("authors");
          assertThat(writtenBy).hasSize(1);
          assertThat(writtenBy.get(0).<String>getProperty("firstName")).isEqualTo("Joanne");
        }

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void nestedFieldWithoutSelectionSetIsExpandedFromItsOwnType() {
    executeTest(database -> {
      defineTypes(database);

      // `authors` has no sub-selection, so it is expanded from the Author type declared in the schema. The guard
      // tracks only the types the automatic expansion is walking, and an explicit selection set does not push onto
      // it - so the path is [Author] here, Book is not on it yet, and Author.wrote IS expanded. The cut comes one
      // level deeper: expanding those Books finds Author already on the path and drops their `authors`. Without
      // that, the cycle Book -> Author -> Book would recurse until the stack overflows.
      try (final ResultSet resultSet = database.query("graphql",
          "{ bookByName(name: \"Mr. brain\") { name authors } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result book = resultSet.next();

        final List<Result> authors = book.getProperty("authors");
        assertThat(authors).hasSize(1);

        final Result author = authors.get(0);
        assertThat(author.<String>getProperty("firstName")).isEqualTo("Joanne");
        assertThat(author.<String>getProperty("lastName")).isEqualTo("Rowling");

        final List<Result> wrote = author.getProperty("wrote");
        assertThat(wrote).hasSize(2);
        for (final Result written : wrote)
          assertThat(written.getPropertyNames()).doesNotContain("authors");

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }
}
