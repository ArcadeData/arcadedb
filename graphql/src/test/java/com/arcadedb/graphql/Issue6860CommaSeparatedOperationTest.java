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
 */
package com.arcadedb.graphql;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end companion to {@code Issue6860CommaSeparatorTest}: a schema and an operation written the way every
 * GraphQL client emits them - commas between the variable definitions, between the field arguments and between the
 * schema field definitions - has to survive the whole path, not just the parser.
 */
class Issue6860CommaSeparatedOperationTest extends AbstractGraphQLTest {

  @Test
  void multiVariableOperationAgainstACommaSeparatedSchema() {
    executeTest(database -> {
      database.command("graphql", """
          type Query {
            bookBy(name: String, pageCount: Int): Book
          }

          type Book {
            id: String,
            name: String,
            pageCount: Int
          }""");

      final Map<String, Object> matching = new HashMap<>();
      matching.put("n", "Mr. brain");
      matching.put("p", 422);

      try (final ResultSet resultSet = database.query("graphql",
          "query($n: String, $p: Int) { bookBy(name: $n, pageCount: $p) { id, name } }", matching)) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result record = resultSet.next();
        assertThat(record.<String>getProperty("id")).isEqualTo("book-2");
        assertThat(record.<String>getProperty("name")).isEqualTo("Mr. brain");
        assertThat(resultSet.hasNext()).isFalse();
      }

      // The two arguments are ANDed, so a mismatching page count must select nothing: the commas must not have been
      // swallowed together with one of the arguments.
      final Map<String, Object> mismatching = new HashMap<>();
      mismatching.put("n", "Mr. brain");
      mismatching.put("p", 1);

      try (final ResultSet resultSet = database.query("graphql",
          "query($n: String, $p: Int) { bookBy(name: $n, pageCount: $p) { id } }", mismatching)) {
        assertThat(resultSet.hasNext()).isFalse();
      }

      return null;
    });
  }
}
