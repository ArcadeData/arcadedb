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
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A Cypher query that references a parameter the caller never bound is rejected, as Neo4j does
 * ({@code Expected parameter(s): ...}).
 * <p>
 * An unbound parameter used to evaluate to null, and null is a legal value everywhere, so the query ran to
 * completion against a value nobody supplied: a filter matched nothing, a predicate came out false, and each
 * caller absorbed that into its neutral answer. Silence is what made issue #5501 and its siblings invisible,
 * and it points the wrong way for a guard - a de-duplicating
 * {@code WHERE NOT EXISTS { ... $id ... } CREATE ...} degraded into an unconditional CREATE and then failed
 * against the unique index the guard existed to protect.
 * <p>
 * Bound to null is not unbound: a caller that explicitly passes null means it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherUnboundParameterTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-unbound-parameter").create();
    database.getSchema().createVertexType("Account");
    database.getSchema().createEdgeType("TRANSFER");

    database.transaction(() -> {
      database.command("cypher", "CREATE (:Account {accountNumber:'A'})");
      database.command("cypher", "CREATE (:Account {accountNumber:'B'})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void unboundParameterInAPatternIsRejected() {
    assertThatThrownBy(() -> database.query("cypher", "MATCH (a:Account {accountNumber: $acct}) RETURN a").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Expected parameter(s): acct");
  }

  @Test
  void unboundParameterInAProjectionIsRejected() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN $acct AS v", Map.of()).close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Expected parameter(s): acct");
  }

  /**
   * The reported failure: the parameter sits inside an {@code EXISTS { }} body, which the outer statement
   * still contains, so the check catches it before the subquery can absorb it into a false.
   */
  @Test
  void unboundParameterInsideAnExistsSubqueryIsRejected() {
    assertThatThrownBy(() -> database.query("cypher", """
        MATCH (src:Account {accountNumber:'A'})
        WHERE EXISTS { MATCH (src)-[t:TRANSFER {transactionId: $tranId}]->() }
        RETURN count(*) AS c""", Map.of()).close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Expected parameter(s): tranId");
  }

  @Test
  void everyMissingParameterIsListedInTheOrderTheQueryMentionsThem() {
    assertThatThrownBy(() -> database.query("cypher", "MATCH (a:Account {accountNumber: $x}) RETURN $y AS v", Map.of()).close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Expected parameter(s): x, y");
  }

  /**
   * The names travel structurally as well as in the message, so a caller that wants to react - prompt for the
   * values, name them in a log, map them to a wire status - never has to parse the message back apart. Issue
   * #5561: the dedicated type is also what lets the Bolt layer answer Neo4j's ParameterMissing rather than
   * SyntaxError, and what the HTTP session ITs assert on.
   */
  @Test
  void theMissingNamesAreCarriedOnTheException() {
    final Throwable thrown = catchThrowable(
        () -> database.query("cypher", "MATCH (a:Account {accountNumber: $x}) RETURN $y AS v, $z AS w", Map.of("z", 1)).close());

    assertThat(thrown).isInstanceOf(CommandParameterMissingException.class);
    // $z was bound, so only the two the caller left out are reported, in the order the query mentions them.
    assertThat(((CommandParameterMissingException) thrown).getMissingParameters()).containsExactly("x", "y");
  }

  /**
   * A missing value is not a syntax error: the statement parses and is semantically well formed. Callers that
   * only distinguish "client sent something wrong" keep working (HTTP still answers 400) because the type
   * still sits under {@link CommandParsingException}.
   */
  @Test
  void theExceptionIsSemanticNotSyntactic() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN $acct AS v", Map.of()).close())
        .isInstanceOf(CommandSemanticException.class)
        .isInstanceOf(CommandParsingException.class);
  }

  /**
   * A write must not run half-way: the guard is rejected before the CREATE it protects.
   */
  @Test
  void aWriteWithAnUnboundParameterIsRejectedBeforeItWrites() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("cypher", """
        MATCH (src:Account {accountNumber:'A'})
        MATCH (dst:Account {accountNumber:'B'})
        WHERE NOT EXISTS { MATCH (src)-[t:TRANSFER {transactionId: $tranId}]->(dst) }
        CREATE (src)-[:TRANSFER {transactionId: $tranId}]->(dst)""", Map.of()).close()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Expected parameter(s): tranId");

    assertThat(count("MATCH ()-[t:TRANSFER]->() RETURN count(*) AS c", Map.of())).isEqualTo(0L);
  }

  @Test
  void aParameterExplicitlyBoundToNullIsBound() {
    try (final ResultSet rs = database.query("cypher", "RETURN $acct AS v", Collections.singletonMap("acct", null))) {
      assertThat(rs.next().<Object>getProperty("v")).isNull();
    }
  }

  @Test
  void parametersTheQueryDoesNotUseAreIgnored() {
    assertThat(count("MATCH (a:Account {accountNumber: $acct}) RETURN count(*) AS c",
        Map.of("acct", "A", "unused", 42))).isEqualTo(1L);
  }

  /**
   * Only real parameter positions count: {@code $acct} inside a string literal is text, not a reference.
   */
  @Test
  void aDollarInsideAStringLiteralIsNotAParameter() {
    try (final ResultSet rs = database.query("cypher", "RETURN 'costs $acct' AS v", Map.of())) {
      assertThat(rs.next().<String>getProperty("v")).isEqualTo("costs $acct");
    }
  }

  /**
   * EXPLAIN never executes the query, so it is the one mode that tolerates unbound parameters - the point of
   * EXPLAIN is inspecting the plan before the values are known. PROFILE does execute, so it does not.
   */
  @Test
  void explainToleratesUnboundParametersButProfileDoesNot() {
    try (final ResultSet rs = database.query("cypher", "EXPLAIN MATCH (a:Account {accountNumber: $acct}) RETURN a", Map.of())) {
      assertThat(rs.hasNext()).isTrue();
    }

    assertThatThrownBy(() -> database.query("cypher", "PROFILE MATCH (a:Account {accountNumber: $acct}) RETURN a", Map.of()).close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Expected parameter(s): acct");
  }

  /**
   * An escaped parameter name binds against the key without the backticks, so the check and the lookup agree.
   */
  @Test
  void anEscapedParameterNameResolvesAgainstThePlainKey() {
    try (final ResultSet rs = database.query("cypher", "RETURN $`my param` AS v", Map.of("my param", 7))) {
      assertThat(((Number) rs.next().<Object>getProperty("v")).intValue()).isEqualTo(7);
    }

    assertThatThrownBy(() -> database.query("cypher", "RETURN $`my param` AS v", Map.of()).close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Expected parameter(s): my param");
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("cypher", query, params)) {
      return ((Number) rs.next().<Object>getProperty("c")).longValue();
    }
  }
}
