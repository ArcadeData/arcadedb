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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11447: a field with no native-query directive builds its own {@code where} from the arguments, and it has to treat an
 * argument the operation does not provide the way the directive path of #11098 does: the default the field declares is applied,
 * and a non-null argument with no default is an error rather than a term silently left out, which returned the whole type.
 */
class Issue11447PlainFieldArgumentDefaultTest extends AbstractGraphQLTest {
  @Override
  protected void defineTypes(final Database database) {
    super.defineTypes(database);
    database.command("graphql", """
        type Query {
          bookById(id: String): Book
          booksByName(name: String = "Mr. brain"): [Book]
          booksByNameDirective(name: String = "Mr. brain"): [Book] @sql(statement: "select from Book where name = :name")
          booksByPageCount(pageCount: Int = 422): [Book]
          booksByAnyName(name: String): [Book]
          booksByPredicate(where: String!, name: String = "Mr. brain"): [Book]
          booksByRequiredName(name: String!): [Book]
          booksByRequiredNameDirective(name: String!): [Book] @sql(statement: "select from Book where name = :name")
          booksByNames(name: [String!]): [Book]
          booksByRequiredNames(name: [String]!): [Book]
          booksByFilter(filter: BookFilter = { name: "Mr. brain" }): [Book]
        }

        input BookFilter {
          name: String
        }""");
  }

  @Test
  void omittedArgumentTakesTheFieldDefault() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(names(database, "{ booksByName { name } }", Map.of())).isEqualTo("Mr. brain");
      assertThat(names(database, "{ booksByNameDirective { name } }", Map.of())).isEqualTo("Mr. brain");
      assertThat(names(database, "{ booksByPageCount { name } }", Map.of())).isEqualTo("Mr. brain");
      return null;
    });
  }

  @Test
  void writtenArgumentOverridesTheFieldDefault() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(names(database, "{ booksByName(name: \"Harry Potter and the Philosopher's Stone\") { name } }", Map.of())).isEqualTo(
          "Harry Potter and the Philosopher's Stone");
      assertThat(names(database, "{ booksByPageCount(pageCount: 223) { name } }", Map.of())).isEqualTo(
          "Harry Potter and the Philosopher's Stone");
      return null;
    });
  }

  @Test
  void argumentBoundToAnOmittedVariableTakesTheFieldDefault() {
    executeTest(database -> {
      defineTypes(database);
      final String query = "query q($n: String) { booksByName(name: $n) { name } }";
      assertThat(names(database, query, Map.of())).isEqualTo("Mr. brain");

      // an explicit null is a value: no book has a null name
      final Map<String, Object> explicitNull = new HashMap<>();
      explicitNull.put("n", null);
      assertThat(names(database, query, explicitNull)).isEmpty();
      return null;
    });
  }

  @Test
  void omittedNullableArgumentWithoutDefaultDoesNotFilter() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(names(database, "{ booksByAnyName { name } }", Map.of())).contains("Mr. brain")
          .contains("Harry Potter and the Philosopher's Stone");
      return null;
    });
  }

  @Test
  void reservedWhereArgumentCanStillBeOmitted() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(names(database, "{ booksByPredicate { name } }", Map.of())).isEqualTo("Mr. brain");
      assertThat(names(database, "{ booksByPredicate(where: \"pageCount > 0\") { name } }", Map.of())).isEqualTo("Mr. brain");
      return null;
    });
  }

  @Test
  void objectDefaultIsRefusedRatherThanIgnored() {
    executeTest(database -> {
      defineTypes(database);
      assertThatThrownBy(() -> names(database, "{ booksByFilter { name } }", Map.of()))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("'filter'");
      return null;
    });
  }

  @Test
  void omittedNonNullArgumentIsAnError() {
    executeTest(database -> {
      defineTypes(database);
      assertThatThrownBy(() -> names(database, "{ booksByRequiredName { name } }", Map.of()))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("'name'");
      assertThatThrownBy(() -> names(database, "{ booksByRequiredNameDirective { name } }", Map.of()))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("'name'");

      assertThat(names(database, "{ booksByRequiredName(name: \"Mr. brain\") { name } }", Map.of())).isEqualTo("Mr. brain");
      return null;
    });
  }

  @Test
  void omittedListArgumentIsAnErrorOnlyWhenTheListIsNonNull() {
    executeTest(database -> {
      defineTypes(database);
      // [String!] is a nullable list of non-null items: it may be omitted
      assertThat(names(database, "{ booksByNames { name } }", Map.of())).contains("Mr. brain")
          .contains("Harry Potter and the Philosopher's Stone");
      assertThatThrownBy(() -> names(database, "{ booksByRequiredNames { name } }", Map.of()))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("'name'");
      return null;
    });
  }

  private static String names(final Database database, final String query, final Map<String, Object> variables) {
    final StringBuilder names = new StringBuilder();
    try (final ResultSet resultSet = database.query("graphql", query, variables)) {
      while (resultSet.hasNext())
        names.append(resultSet.next().<String>getProperty("name"));
    }
    return names.toString();
  }
}
