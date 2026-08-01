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
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * GraphQL rewrapped every failure raised by the statement a directive delegates to as a
 * {@link CommandParsingException}, so a runtime error reached the caller as invalid syntax and the wire layers lost
 * the classification they report a client-vs-server verdict from. See issue #5628.
 */
class GraphQLExecutionErrorClassificationTest extends AbstractGraphQLTest {

  private void defineDirectives(final Database database) {
    database.command("graphql", """
        type Query {
          workingBooks(id: String): Book @sql(statement: "select from Book")
          missingTypeBooks(id: String): Book @sql(statement: "select from DoesNotExistAnywhere")
          dividedBooks(id: String): Book @sql(statement: "select pageCount / 0 as broken from Book")
        }

        type Book {
          id: String
          name: String
          pageCount: Int
          broken: Int
        }""");
  }

  @Test
  void anExecutionFailureIsNotReportedAsASyntaxError() {
    executeTest(database -> {
      defineDirectives(database);

      final Throwable error = catchThrowable(() -> database.query("graphql", "{ missingTypeBooks { id } }").close());

      assertThat(error).isInstanceOf(SchemaException.class);
      assertThat(error).isNotInstanceOf(CommandParsingException.class);
      assertThat(ErrorCategory.of(error)).isNotEqualTo(ErrorCategory.PARSING);

      return null;
    });
  }

  @Test
  void anArithmeticErrorKeepsItsOwnTypeThroughADirective() {
    executeTest(database -> {
      defineDirectives(database);

      // The projection is evaluated as rows are pulled, so this one needs the caller to actually iterate.
      final Throwable error = catchThrowable(
          () -> database.query("graphql", "{ dividedBooks { broken } }").stream().toList());

      assertThat(error).isInstanceOf(ArithmeticErrorException.class);
      assertThat(error).isNotInstanceOf(CommandParsingException.class);
      assertThat(ErrorCategory.of(error)).isEqualTo(ErrorCategory.ARITHMETIC);

      return null;
    });
  }

  @Test
  void agenuineGraphqlSyntaxErrorIsStillAParsingError() {
    // The passthrough must not swallow the case it was carved out of: a query naming something the schema does not
    // define is still the caller's syntax problem, and still has to arrive as one.
    executeTest(database -> {
      defineDirectives(database);

      assertThatThrownBy(() -> database.query("graphql", "{ notInTheSchema { id } }").close())
          .isInstanceOf(CommandParsingException.class);

      return null;
    });
  }

  @Test
  void aDirectiveThatWorksIsUnaffected() {
    // The passthrough sits on the failure path only; the successful path has to keep returning rows.
    executeTest(database -> {
      defineDirectives(database);

      try (final var resultSet = database.query("graphql", "{ workingBooks { id name } }")) {
        assertThat(resultSet.stream().toList()).hasSize(2);
      }

      return null;
    });
  }
}
