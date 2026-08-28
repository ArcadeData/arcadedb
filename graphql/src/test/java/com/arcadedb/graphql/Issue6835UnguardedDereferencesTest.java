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
 * Regression test for issue #6835, two unguarded dereferences in the GraphQL module: a {@code Query} field declared
 * without a parameter list threw a NullPointerException rewrapped as a parsing error, and {@code mapProjections}
 * handed a null selection set to {@code mapBySelections} when an embedded document was projected from the schema
 * type rather than from an explicit sub-selection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6835UnguardedDereferencesTest extends AbstractGraphQLTest {

  @Test
  void queryFieldWithoutArgumentsIsAccepted() {
    executeTest(database -> {
      // ArgumentsDefinition is optional in the grammar, so `allBooks` leaves it null: "list all", the most natural
      // argument-less query, could not be expressed at all before the fix.
      final String types = """
          type Query {
            allBooks: [Book]
          }

          type Book {
            id: String
            name: String
            pageCount: Int
          }""";
      database.command("graphql", types);

      try (final ResultSet resultSet = database.query("graphql", "{ allBooks { name } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("name")).isEqualTo("Harry Potter and the Philosopher's Stone");

        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("name")).isEqualTo("Mr. brain");

        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }

  @Test
  void argumentNotDeclaredByAnArgumentLessFieldIsStillRejected() {
    executeTest(database -> {
      final String types = """
          type Query {
            allBooks: [Book]
          }

          type Book {
            id: String
            name: String
          }""";
      database.command("graphql", types);

      // Guarding the null arguments definition must not turn the field into one that accepts anything.
      assertThatThrownBy(() -> database.query("graphql", "{ allBooks(id: \"book-1\") { name } }").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("'id' not defined");

      return null;
    });
  }

  @Test
  void embeddedDocumentIsExpandedFromTheSchemaTypeWithoutASelectionSet() {
    executeTest(database -> {
      final String types = """
          type Query {
            addresses(firstName: String): [Author]
          }

          type Address {
            city: String
          }

          type Author {
            id: String
            firstName: String
            lastName: String
            address: Address
          }""";
      database.command("graphql", types);

      // `address` carries no sub-selection, so it is projected from the Address type declared in the schema. That
      // arm used to call mapBySelections() with the null selection set of the branch it sits in: a guaranteed NPE.
      try (final ResultSet resultSet = database.query("graphql", "{ addresses(firstName: \"Joanne\") }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result author = resultSet.next();
        assertThat(author.<String>getProperty("lastName")).isEqualTo("Rowling");

        final Result address = author.getProperty("address");
        assertThat(address).isNotNull();
        assertThat(address.<String>getProperty("city")).isEqualTo("Rome");

        assertThat(resultSet.hasNext()).isFalse();
      }

      // The same value reached through an explicit sub-selection keeps working.
      try (final ResultSet resultSet = database.query("graphql", "{ addresses(firstName: \"Joanne\") { address { city } } }")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result address = resultSet.next().getProperty("address");
        assertThat(address.<String>getProperty("city")).isEqualTo("Rome");
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }
}
