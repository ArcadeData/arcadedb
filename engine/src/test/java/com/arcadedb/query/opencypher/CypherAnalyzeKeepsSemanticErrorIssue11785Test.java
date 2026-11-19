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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.QueryEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11785: {@code OpenCypherQueryEngine.analyze()} wrapped every failure of the parse in a new
 * {@code CommandParsingException("Error analyzing Cypher query: ...")}, so a {@link CommandSemanticException} (an undefined
 * variable) reached the caller behind a wrapper that named the query and not the reason. Every caller that analyzes a statement
 * before executing it (an HA follower's {@code command()}, {@code apoc.do.when}) then reported a semantic error as a syntax
 * error. A {@link CommandParsingException} the parser raised now leaves {@code analyze()} unchanged, as it leaves the execution.
 */
class CypherAnalyzeKeepsSemanticErrorIssue11785Test {
  private static final String UNDEFINED_VARIABLE = "MATCH (n) RETURN m";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-analyze-semantic-error-11785").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void analyzeThrowsTheSemanticErrorItself() {
    final Throwable analyzed = catchThrowable(() -> engine().analyze(UNDEFINED_VARIABLE));

    assertThat(analyzed).isInstanceOf(CommandSemanticException.class);
    assertThat(analyzed.getMessage()).contains("Variable 'm' not defined").doesNotStartWith("Error analyzing Cypher query");
  }

  /** The exception analyze() throws is the one the direct execution throws: same class, same reason. */
  @Test
  void analyzeAndQueryAgreeOnTheError() {
    final Throwable executed = catchThrowable(() -> database.query("opencypher", UNDEFINED_VARIABLE).close());
    final Throwable analyzed = catchThrowable(() -> engine().analyze(UNDEFINED_VARIABLE));

    assertThat(executed).isInstanceOf(CommandSemanticException.class);
    assertThat(analyzed).isExactlyInstanceOf(executed.getClass());
    assertThat(analyzed.getMessage()).isEqualTo(executed.getMessage());
  }

  /** EXPLAIN/PROFILE are stripped before the parse: the semantic error of the wrapped statement still comes out unwrapped. */
  @Test
  void analyzeOfAnExplainedStatementThrowsTheSemanticErrorItself() {
    final Throwable analyzed = catchThrowable(() -> engine().analyze("EXPLAIN " + UNDEFINED_VARIABLE));

    assertThat(analyzed).isInstanceOf(CommandSemanticException.class);
    assertThat(analyzed.getMessage()).contains("Variable 'm' not defined");
  }

  /** A genuine syntax error is still a CommandParsingException, and not a semantic one. */
  @Test
  void analyzeOfASyntaxErrorIsStillAParsingError() {
    final Throwable analyzed = catchThrowable(() -> engine().analyze("MATCH (n RETURN n"));

    assertThat(analyzed).isInstanceOf(CommandParsingException.class).isNotInstanceOf(CommandSemanticException.class);
  }

  /** apoc.do.when analyzes the branch it runs before running it (to pick read or write): the semantic error reaches the caller. */
  @Test
  void doWhenReportsTheSemanticErrorOfItsBranch() {
    final Throwable thrown = catchThrowable(() -> database.command("opencypher",
        "CALL apoc.do.when(true, '" + UNDEFINED_VARIABLE + "', '') YIELD value RETURN value").close());

    assertThat(thrown).isInstanceOf(CommandSemanticException.class);
    assertThat(thrown.getMessage()).contains("Variable 'm' not defined").doesNotStartWith("Error analyzing Cypher query");
  }

  private QueryEngine engine() {
    return database.getQueryEngine("opencypher");
  }
}
