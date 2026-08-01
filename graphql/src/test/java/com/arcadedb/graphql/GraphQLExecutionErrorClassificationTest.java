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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ErrorCategory;
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
          unknownFunctionBooks(id: String): Book @sql(statement: "select notAFunction(name) from Book")
          dividedBooks(id: String): Book @sql(statement: "select from Book where pageCount / 0 > 1")
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
    // An unknown function is a CommandExecutionException. Delegating the statement through a GraphQL directive
    // used to relabel it a parsing error, so the identical statement answered differently depending on how it was
    // invoked; it now matches what issuing the SQL directly reports.
    executeTest(database -> {
      defineDirectives(database);

      final Throwable error = catchThrowable(() -> database.query("graphql", "{ unknownFunctionBooks { id } }").close());

      assertThat(error).isInstanceOf(CommandExecutionException.class);
      assertThat(error).isNotInstanceOf(CommandParsingException.class);

      return null;
    });
  }

  @Test
  void anUnknownTypeKeepsTheStatusItAlwaysHad() {
    // SchemaException is deliberately NOT passed through. The HTTP handler has no arm for it, so propagating it
    // would turn this from 400 into 500; and the class is not purely a caller error - Dictionary and
    // TransactionManager raise it for genuine server faults - so it cannot just be mapped to 400 either. Pinned so
    // the deliberate narrowness is not "tidied up" into a regression. Tracked as a follow-up.
    executeTest(database -> {
      database.command("graphql", """
          type Query {
            missingTypeBooks(id: String): Book @sql(statement: "select from DoesNotExistAnywhere")
          }

          type Book {
            id: String
          }""");

      assertThatThrownBy(() -> database.query("graphql", "{ missingTypeBooks { id } }").close())
          .isInstanceOf(CommandParsingException.class);

      return null;
    });
  }

  @Test
  void anArithmeticErrorKeepsItsOwnTypeThroughADirective() {
    executeTest(database -> {
      defineDirectives(database);

      // In a WHERE clause the division is evaluated while the query is being set up, so it surfaces here rather
      // than only once the caller pulls rows - which is what makes it pass through the rewrapping block.
      final Throwable error = catchThrowable(() -> database.query("graphql", "{ dividedBooks { id } }").close());

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
