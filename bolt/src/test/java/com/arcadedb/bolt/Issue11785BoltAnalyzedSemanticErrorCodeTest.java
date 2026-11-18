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
package com.arcadedb.bolt;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11785 on Bolt: a statement whose route analyzes it before executing it (an HA follower's {@code command()},
 * {@code apoc.do.when}) was answered {@code Neo.ClientError.Statement.SyntaxError} for an undefined variable, because
 * {@code OpenCypherQueryEngine.analyze()} wrapped the {@code CommandSemanticException} in its own {@code CommandParsingException}
 * and the Bolt classifier reads the outermost one. Driven by the exceptions the real engine throws, not by hand-built chains.
 */
class Issue11785BoltAnalyzedSemanticErrorCodeTest {
  private static final String UNDEFINED_VARIABLE = "MATCH (n) RETURN m";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/bolt-analyzed-semantic-error-11785").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void directExecutionIsASemanticError() {
    assertThat(code(catchThrowable(() -> database.query("opencypher", UNDEFINED_VARIABLE).close())))
        .isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
  }

  /** The route of an HA follower: RaftReplicatedDatabase.command() calls analyze() before executing. */
  @Test
  void analyzedStatementIsASemanticError() {
    assertThat(code(catchThrowable(() -> database.getQueryEngine("opencypher").analyze(UNDEFINED_VARIABLE))))
        .isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
  }

  @Test
  void doWhenBranchIsASemanticError() {
    assertThat(code(catchThrowable(() -> database.command("opencypher",
        "CALL apoc.do.when(true, '" + UNDEFINED_VARIABLE + "', '') YIELD value RETURN value").close())))
        .isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
  }

  @Test
  void analyzedSyntaxErrorStaysASyntaxError() {
    assertThat(code(catchThrowable(() -> database.getQueryEngine("opencypher").analyze("MATCH (n RETURN n"))))
        .isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
  }

  private static String code(final Throwable error) {
    assertThat(error).isNotNull();
    return BoltNetworkExecutor.classifyExecutionError(error, BoltErrorCodes.DATABASE_ERROR);
  }
}
