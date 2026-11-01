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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11098: an argument of a native-query directive the operation does not provide - omitted, or bound to a variable the
 * caller gave no value - takes the default the field declares, while an explicit null stays null. The statement reads every
 * argument as a parameter, and a parameter with no value is refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11098DirectiveArgumentDefaultTest extends AbstractGraphQLTest {
  @Override
  protected void defineTypes(final Database database) {
    super.defineTypes(database);
    database.command("graphql", """
        type Query {
          bookById(id: String): Book
          bookByName(bookNameParameter: String = "Mr. brain"): Book @sql(statement: "select from Book where name = :bookNameParameter")
          booksByNames(names: [String] = ["Mr. brain", "Nobody"]): [Book] @sql(statement: "select from Book where name in :names")
        }""");
  }

  @Test
  void omittedListArgumentTakesTheListDefault() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(names(database, "{ booksByNames { name } }", Map.of())).isEqualTo("Mr. brain");
      return null;
    });
  }

  @Test
  void omittedArgumentTakesTheFieldDefault() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(names(database, "{ bookByName { name } }", Map.of())).isEqualTo("Mr. brain");
      return null;
    });
  }

  @Test
  void argumentBoundToAnOmittedVariableTakesTheFieldDefault() {
    executeTest(database -> {
      defineTypes(database);
      final String query = "query q($n: String) { bookByName(bookNameParameter: $n) { name } }";
      assertThat(names(database, query, Map.of())).isEqualTo("Mr. brain");

      // an explicit null is a value: no book has a null name
      final Map<String, Object> explicitNull = new HashMap<>();
      explicitNull.put("n", null);
      assertThat(names(database, query, explicitNull)).isEmpty();

      assertThat(names(database, query, Map.of("n", "Harry Potter and the Philosopher's Stone"))).isEqualTo(
          "Harry Potter and the Philosopher's Stone");
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
