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
 * Issue #11973: a Bolt FAILURE for a statement that failed while it ran said "Error executing Cypher command: &lt;statement&gt;",
 * the message of the query engine's wrapper, and nothing about why. It now carries the failure its code is classified from, as the
 * HTTP API's {@code detail} does. Issue #11785: a semantic error an analyzer wrapped in a parsing failure keeps its code and its
 * reason. Each failure is the one the engine really throws.
 */
class Issue11973BoltFailureMessageTest {
  private static final String WRAPPER = "Error executing Cypher";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/Issue11973BoltFailureMessageTest").create();
    run("CREATE CONSTRAINT c FOR (n:U) REQUIRE n.x IS UNIQUE");
    run("CREATE INDEX i1 FOR (n:A) ON (n.x)");
    database.transaction(() -> database.command("opencypher", "CREATE (:U {x: 1}), (:A {x: 1}), (:D {x: 1}), (:D {x: 1})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void aUniqueViolationNamesTheKeyAndTheIndex() {
    assertFailure("CREATE (:U {x: 1})", BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR, "Duplicated key", "[1]");
  }

  @Test
  void aConstraintThatExistsSaysSo() {
    assertFailure("CREATE CONSTRAINT c FOR (n:U) REQUIRE n.x IS UNIQUE", BoltErrorCodes.CONSTRAINT_ALREADY_EXISTS_ERROR, "'c'",
        "already exists");
  }

  @Test
  void anEquivalentIndexNamesTheIndexThatHasTheProperties() {
    assertFailure("CREATE INDEX i2 FOR (n:A) ON (n.x)", BoltErrorCodes.SCHEMA_RULE_ALREADY_EXISTS_ERROR, "'i1'");
  }

  @Test
  void aUniqueConstraintOverDuplicatesNamesTheDuplicatedKey() {
    assertFailure("CREATE CONSTRAINT d FOR (n:D) REQUIRE n.x IS UNIQUE", BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR, "Duplicated key");
  }

  @Test
  void droppingAnIndexOrConstraintThatDoesNotExistNamesIt() {
    assertFailure("DROP INDEX nope", BoltErrorCodes.SEMANTIC_ERROR, "'nope'", "does not exist");
    assertFailure("DROP CONSTRAINT nope", BoltErrorCodes.SEMANTIC_ERROR, "'nope'", "does not exist");
  }

  @Test
  void anArgumentErrorSaysWhatTheArgumentIs() {
    assertFailure("MATCH (n:A) SET n = 5", BoltErrorCodes.ARGUMENT_ERROR, "must be a map, a node or a relationship");
  }

  /** Issue #11785: the analyzer wraps the semantic error it hit in a parsing failure, which labelled it a SyntaxError. */
  @Test
  void aSemanticErrorTheAnalyzerWrapsKeepsItsCodeAndReason() {
    final Throwable error = catchThrowable(() -> database.getQueryEngine("opencypher").analyze("MATCH (n) RETURN m"));
    assertThat(error).isNotNull();
    assertThat(BoltNetworkExecutor.classifyExecutionError(error, BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
    assertThat(BoltNetworkExecutor.failureText(error, "fallback")).contains("Variable 'm' not defined").doesNotContain("Error analyzing");
  }

  @Test
  void aFailureNoRuleClassifiesIsTheWholeChain() {
    final Throwable error = new RuntimeException("Error executing Cypher command: X", new IllegalStateException("the reason"));
    assertThat(BoltNetworkExecutor.failureText(error, "fallback")).isEqualTo("Error executing Cypher command: X -> the reason");
    assertThat(BoltNetworkExecutor.failureText(null, "fallback")).isEqualTo("fallback");
  }

  private void assertFailure(final String statement, final String code, final String... reason) {
    final Throwable error = catchThrowable(() -> run(statement));
    assertThat(error).as(statement).isNotNull();
    assertThat(BoltNetworkExecutor.classifyExecutionError(error, BoltErrorCodes.DATABASE_ERROR)).as(statement).isEqualTo(code);
    final String text = BoltNetworkExecutor.failureText(error, "fallback");
    assertThat(text).as(statement).doesNotStartWith(WRAPPER).contains(reason);
  }

  private void run(final String statement) {
    database.transaction(() -> database.command("opencypher", statement).stream().count());
  }
}
