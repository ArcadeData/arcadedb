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

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6384: a GraphQL query using a field alias (standard GraphQL syntax)
 * threw an unhandled {@link NullPointerException} instead of resolving the aliased field.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6384FieldAliasTest extends AbstractGraphQLTest {

  @Test
  void topLevelFieldAliasIsResolvedUnderTheAliasKey() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ bookByName(name: \"Mr. brain\") { myTitle: name pageCount } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        // the alias is the output key, not the real field name
        assertThat(record.getPropertyNames()).contains("myTitle", "pageCount");
        assertThat(record.getPropertyNames()).doesNotContain("name");
        assertThat(record.<String>getProperty("myTitle")).isEqualTo("Mr. brain");
        assertThat(record.<Integer>getProperty("pageCount")).isEqualTo(422);
      }

      return null;
    });
  }

  @Test
  void nestedFieldAliasIsResolvedUnderTheAliasKey() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", """
          { bookById(id: "book-1") {
              writers: authors {
                first: firstName
                lastName
              }
            }
          }""")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.getPropertyNames()).contains("writers");
        final Collection<?> writers = record.getProperty("writers");
        assertThat(writers).hasSize(1);
        final Result author = (Result) writers.iterator().next();
        assertThat(author.getPropertyNames()).contains("first", "lastName");
        assertThat(author.<String>getProperty("first")).isEqualTo("Joanne");
        assertThat(author.<String>getProperty("lastName")).isEqualTo("Rowling");
      }

      return null;
    });
  }

  @Test
  void mixingAliasedAndPlainFieldsInSameSelectionSetWorks() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ bookByName(name: \"Harry Potter and the Philosopher's Stone\") { id title: name pageCount } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.getPropertyNames()).contains("id", "title", "pageCount");
        assertThat(record.<String>getProperty("id")).isEqualTo("book-1");
        assertThat(record.<String>getProperty("title")).isEqualTo("Harry Potter and the Philosopher's Stone");
      }

      return null;
    });
  }
}
