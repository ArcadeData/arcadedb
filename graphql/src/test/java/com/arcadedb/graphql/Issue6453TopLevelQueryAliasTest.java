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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6453: aliasing the top-level query field itself (e.g.
 * {@code myBook: bookByName(...)}) failed schema resolution, because the dispatch code looked up
 * the field definition on the {@code Query} type using the alias instead of the real field name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6453TopLevelQueryAliasTest extends AbstractGraphQLTest {

  @Test
  void topLevelQueryFieldAliasWithArgumentIsResolved() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql",
          "{ myBook: bookByName(name: \"Mr. brain\") { pageCount } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        // the argument on the aliased field filtered to the right book, and only the requested
        // field was projected (both come from the FieldWithAlias, not the missing plain Field)
        assertThat(record.getPropertyNames()).contains("pageCount");
        assertThat(record.getPropertyNames()).doesNotContain("id", "name");
        assertThat(record.<Integer>getProperty("pageCount")).isEqualTo(422);
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void topLevelQueryFieldAliasWithoutArgumentIsResolved() {
    executeTest(database -> {
      defineTypes(database);

      try (final ResultSet resultSet = database.query("graphql", "{ book: bookById(id: \"book-1\") { name } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        assertThat(record.getPropertyNames()).contains("name");
        assertThat(record.<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");
      }

      return null;
    });
  }

  @Test
  void aliasedTopLevelFieldStillRejectsUndefinedArgument() {
    executeTest(database -> {
      defineTypes(database);

      assertThatThrownBy(() -> database.query("graphql", "{ myBook: bookByName(bogus: \"Mr. brain\") { name } }"))
          .isInstanceOf(CommandParsingException.class);

      return null;
    });
  }

  @Test
  void aliasedTopLevelFieldBackedByNativeSqlDirectiveAppliesArgumentAndProjection() {
    executeTest(database -> {
      defineTypes(database);
      database.command("graphql", """
          type Query {
            bookById(id: String): Book
            bookByName(bookNameParameter: String): Book @sql(statement: "select from Book where name = :bookNameParameter")
          }""");

      try (final ResultSet resultSet = database.query("graphql",
          "{ myBook: bookByName(bookNameParameter: \"Mr. brain\") { pageCount } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();

        // the @sql-backed field is only reachable via selection.getField(), which is null for an
        // aliased selection - without the FieldWithAlias branch this returns every Book unfiltered
        assertThat(record.getPropertyNames()).contains("pageCount");
        assertThat(record.<Integer>getProperty("pageCount")).isEqualTo(422);
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }
}
