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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.query.QuerySession;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4141 (ISO/IEC 39075 GQL, section 2 - Infrastructure & Environment): Session Management statements
 * {@code SESSION SET $name = value}, {@code SESSION RESET}, {@code SESSION CLOSE}.
 * <p>
 * These operate on the {@link QuerySession} attached to the current thread's database context (a server
 * session). This test exercises the engine layer with a fake in-memory session attached to that context,
 * plus the embedded case (no session attached) which must report an actionable error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4141SessionManagementTest {
  private Database    database;
  private FakeSession session;

  /** Minimal in-memory QuerySession standing in for the server session in engine-level tests. */
  private static class FakeSession implements QuerySession {
    final Map<String, Object> params = new HashMap<>();
    boolean                   closed = false;

    @Override
    public void setParameter(final String name, final Object value) {
      params.put(name, value);
    }

    @Override
    public Map<String, Object> getParameters() {
      // Mirror production (HttpSession/BoltSession) so the double cannot mask an engine mutating session params.
      return Collections.unmodifiableMap(params);
    }

    @Override
    public void reset() {
      params.clear();
    }

    @Override
    public void close() {
      // Records the call only. Unlike production (BoltSession clears params; HttpSession removes itself from
      // the manager), the engine does not read params after close(), so the double needs no further effect.
      closed = true;
    }
  }

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue4141SessionManagement");
    if (factory.exists())
      factory.open().drop(); // defend against a leftover db from a previously interrupted run
    database = factory.create();
    database.getSchema().createVertexType("Person");
    session = new FakeSession();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      // Detach the fake session from this thread's context so a later test sees no bound session.
      final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContextIfExists(
          ((DatabaseInternal) database).getDatabasePath());
      if (ctx != null)
        ctx.setQuerySession(null);
      database.drop();
      database = null;
    }
  }

  /** Attaches the fake session to this thread's database context, the way the server does for a request. */
  private void bindSession() {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setQuerySession(session);
  }

  // ---- SESSION SET binds a parameter on the bound session -------------------------------------

  @Test
  void sessionSetBindsParameter() {
    bindSession();
    try (final ResultSet rs = database.command("opencypher", "SESSION SET $greeting = 'hello'")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("operation")).isEqualTo("set");
      assertThat(r.<String>getProperty("name")).isEqualTo("greeting");
      assertThat(r.<String>getProperty("value")).isEqualTo("hello");
    }
    assertThat(session.params).containsEntry("greeting", "hello");
  }

  @Test
  void sessionSetEvaluatesExpressionValue() {
    bindSession();
    database.command("opencypher", "SESSION SET $n = 6 * 7").close();
    assertThat(((Number) session.params.get("n")).intValue()).isEqualTo(42);
  }

  @Test
  void sessionSetCanReferenceAnEarlierSessionParameter() {
    bindSession();
    database.command("opencypher", "SESSION SET $base = 10").close();
    database.command("opencypher", "SESSION SET $derived = $base + 5").close();
    assertThat(((Number) session.params.get("derived")).intValue()).isEqualTo(15);
  }

  @Test
  void sessionSetCanReferenceARequestParameter() {
    bindSession();
    // The value expression must resolve request-supplied parameters, not just session parameters.
    database.command("opencypher", "SESSION SET $derived = $incoming + 1", "incoming", 41).close();
    assertThat(((Number) session.params.get("derived")).intValue()).isEqualTo(42);
  }

  // ---- SESSION RESET clears the session parameters --------------------------------------------

  @Test
  void sessionResetClearsParameters() {
    bindSession();
    session.params.put("x", 1);
    session.params.put("y", 2);
    try (final ResultSet rs = database.command("opencypher", "SESSION RESET")) {
      assertThat(rs.next().<String>getProperty("operation")).isEqualTo("reset");
    }
    assertThat(session.params).isEmpty();
  }

  // ---- Session parameters and the unbound-parameter check (issue #5561) -----------------------

  /**
   * Issue #5561: the parameter-bound check (#5501) runs on the parameters the session has been merged into,
   * so a name the session supplies counts as bound. This is the engine-level statement of what the HTTP and
   * Bolt session ITs assert end to end; getting the order wrong there made every session query fail.
   */
  @Test
  void aSessionParameterSatisfiesTheBoundCheck() {
    bindSession();
    database.command("opencypher", "SESSION SET $threshold = 21").close();

    try (final ResultSet rs = database.query("opencypher", "RETURN $threshold AS t")) {
      assertThat(rs.next().<Number>getProperty("t").intValue()).isEqualTo(21);
    }
  }

  /**
   * Issue #5561: and once {@code SESSION RESET} has cleared it the name is unbound again, so it is rejected
   * rather than silently resolving to null. The rejection is what proves the value is gone - a null result
   * would equally be produced by a query that never consulted the session.
   */
  @Test
  void aResetSessionParameterIsUnboundAgain() {
    bindSession();
    database.command("opencypher", "SESSION SET $threshold = 21").close();
    database.command("opencypher", "SESSION RESET").close();

    assertThatThrownBy(() -> database.query("opencypher", "RETURN $threshold AS t").close())
        .isInstanceOf(CommandParameterMissingException.class)
        .hasMessageContaining("Expected parameter(s): threshold");
  }

  /**
   * Issue #5561: a request that binds the same name shadows the session's value for that request only, and
   * the session keeps its own. This is the no-bleed property in its policy-independent form.
   */
  @Test
  void aRequestParameterShadowsTheSessionParameterWithoutChangingIt() {
    bindSession();
    database.command("opencypher", "SESSION SET $threshold = 21").close();

    try (final ResultSet rs = database.query("opencypher", "RETURN $threshold AS t", Map.of("threshold", 7))) {
      assertThat(rs.next().<Number>getProperty("t").intValue()).isEqualTo(7);
    }

    assertThat(((Number) session.params.get("threshold")).intValue()).isEqualTo(21);
  }

  /**
   * Issue #5561: with no session bound at all (the embedded case, and the server's non-session request path)
   * the name is simply unbound. Nothing another connection set can make it resolve.
   */
  @Test
  void withoutABoundSessionAParameterIsUnbound() {
    session.params.put("threshold", 21); // a session exists somewhere, but is not bound to this thread

    assertThatThrownBy(() -> database.query("opencypher", "RETURN $threshold AS t").close())
        .isInstanceOf(CommandParameterMissingException.class)
        .hasMessageContaining("Expected parameter(s): threshold");
  }

  // ---- SESSION CLOSE closes the session ------------------------------------------------------

  @Test
  void sessionCloseClosesTheSession() {
    bindSession();
    try (final ResultSet rs = database.command("opencypher", "SESSION CLOSE")) {
      assertThat(rs.next().<String>getProperty("operation")).isEqualTo("close");
    }
    assertThat(session.closed).isTrue();
  }

  // ---- Embedded use (no session bound) reports an actionable error ----------------------------

  @ParameterizedTest
  @ValueSource(strings = { "SESSION SET $x = 1", "SESSION RESET", "SESSION CLOSE" })
  void sessionStatementWithoutABoundSessionFails(final String statement) {
    // No QuerySession bound (embedded): the guard at the top of executeSession applies to all three kinds,
    // so none may silently no-op.
    assertThatThrownBy(() -> database.command("opencypher", statement))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("server session");
  }

  // ---- Backward compatibility: session/reset/close remain valid identifiers -------------------

  @Test
  void keywordsRemainUsableAsIdentifiers() {
    database.transaction(() -> {
      try (final ResultSet ignored = database.command("opencypher",
          "CREATE (p:Person {name: 'Sam', session: 1, reset: 2, close: 3})")) {
      }
    });

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {name: 'Sam'}) RETURN p.session AS session, p.reset AS reset, p.close AS close")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("session").intValue()).isEqualTo(1);
      assertThat(r.<Number>getProperty("reset").intValue()).isEqualTo(2);
      assertThat(r.<Number>getProperty("close").intValue()).isEqualTo(3);
    }
  }
}
