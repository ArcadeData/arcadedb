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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5671, part 2: variable-kind checks (the function-argument-type phase - {@code type(m)} needs a
 * relationship, {@code labels(m)} needs a node, and so on) used to be built as one end-state map for the
 * whole statement, applied to every expression in it wherever that expression sits. A name a later
 * {@code WITH} re-binds to something kindless (or to a different kind) lost its kind - or its wrongness -
 * for every clause <i>before</i> that {@code WITH} too, so a check that should have fired against the
 * clause as written silently didn't.
 * <p>
 * {@code MATCH (m:P) WHERE type(m) = 'KNOWS' WITH 1 AS m RETURN m}: {@code type(m)} is written against a
 * {@code MATCH}-bound node, which is always a type error - {@code type()} needs a relationship - regardless
 * of what a later clause does with the name {@code m}. The old end-state map disagreed: by the time it was
 * built, {@code m} was whatever the last clause left it as ({@code WITH 1 AS m} makes it a SCALAR), so the
 * check read the wrong kind and never fired.
 * <p>
 * Now {@link com.arcadedb.query.opencypher.parser.CypherExpressionWalker} advances the variable-kind scope
 * clause by clause as it walks (issue #5671's {@code Visitor#forClauseEntry}), so a clause's own expressions
 * are checked against the scope as of that clause, not the statement's end state - both at the top level and
 * inside a {@code CALL { ... }} body, the shape the issue's own example uses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5671PositionalVariableKindsTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue5671positional").create();
    database.getSchema().createVertexType("P");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void typeOnANodeBeforeARebindingWithIsStillRejectedAtTopLevel() {
    // m is a node from MATCH; type() requires a relationship. The later "WITH 1 AS m" must not retroactively
    // excuse the type() call written against the node.
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (m:P) WHERE type(m) = 'KNOWS' WITH 1 AS m RETURN m"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("type() requires a relationship argument, got node");
  }

  @Test
  void typeOnANodeBeforeARebindingWithIsStillRejectedInsideACallSubqueryBody() {
    // The issue's own example, executed: CALL { ... } bodies are validated positionally too, not just the
    // top-level statement.
    assertThatThrownBy(() -> database.query("opencypher",
        "CALL { MATCH (m:P) WHERE type(m) = 'KNOWS' WITH 1 AS m RETURN m } RETURN m"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("type() requires a relationship argument, got node");
  }

  @Test
  void typeOnANodeIsRejectedEvenWithNoTrailingWithAtAll() {
    // Sanity check the pre-existing behavior (no WITH involved) is unaffected by the positional rework.
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (m:P) WHERE type(m) = 'KNOWS' RETURN m"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("type() requires a relationship argument, got node");
  }

  @Test
  void typeOnARelationshipCarriedThroughAWithIsStillAccepted() {
    // Sanity check the other direction: a kind that a WITH legitimately carries forward unchanged must not
    // start being rejected by the positional rework.
    database.getSchema().createEdgeType("KNOWS");
    try (ResultSet ignored = database.query("opencypher",
        "MATCH ()-[m:KNOWS]->() WITH m AS m RETURN type(m)")) {
      // Statement must parse and plan without a semantic error; an empty database yields zero rows, which is fine.
      assertThatCode(ignored::hasNext).doesNotThrowAnyException();
    }
  }

  @Test
  void typeOnANodeAfterAWithThatKeepsItANodeIsStillRejected() {
    // The check must still fire when the WITH does NOT change the kind - only a kind CHANGE/loss should shift
    // when the check does or doesn't fire.
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (m:P) WITH m AS m WHERE type(m) = 'KNOWS' RETURN m"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("type() requires a relationship argument, got node");
  }
}
