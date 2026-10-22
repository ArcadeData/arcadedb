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
package com.arcadedb.postgres;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.ErrorCategory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10533: a Postgres connection now meets the refusal of a database a restore holds or a {@code close database} closed
 * ({@link DatabaseNotAvailableException}). That database exists, so the connection is told it is not available, not that it does
 * not exist - the recovery advice is the opposite - while a name that is not a database keeps its answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10533PostgresHeldDatabaseTest {

  @Test
  void aDatabaseTheServerHoldsIsNotADatabaseThatDoesNotExist() {
    final DatabaseNotAvailableException held = new DatabaseNotAvailableException(
        "Database 'mydb' is not available: a restore of it is in progress");

    assertThat(ErrorCategory.of(held)).isEqualTo(ErrorCategory.UNAVAILABLE);
    assertThat(PostgresNetworkExecutor.sqlStateFor(held)).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW);
  }

  /** Issue #10553: the same verdict reaches a statement of an open session, not only the opening of the connection. */
  @Test
  void aDatabaseHeldUnderAStatementIsCannotConnectNowToo() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandExecutionException("Error on executing the query",
        new DatabaseNotAvailableException("Database 'mydb' is not available")))).isEqualTo("57P03");
  }

  @Test
  void aNameThatIsNotADatabaseKeepsItsAnswer() {
    assertThat(ErrorCategory.of(new DatabaseNotFoundException("Database 'nope' does not exist"))).isNotEqualTo(ErrorCategory.UNAVAILABLE);
    assertThat(ErrorCategory.of(new DatabaseOperationException("Error on opening the database"))).isNotEqualTo(ErrorCategory.UNAVAILABLE);
    assertThat(PostgresNetworkExecutor.sqlStateFor(new DatabaseNotFoundException("Database 'nope' does not exist"))).isEqualTo("XX000");
  }
}
