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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6135: {@code RETURN *} rejected a variable that was in scope only because it was exported by a preceding
 * {@code CALL { }} subquery, a {@code WITH} projection, or a {@code CALL ... YIELD}. The check backing
 * {@code RETURN *} scanned only the statement's own {@code MATCH} patterns and {@code UNWIND} variables, so any
 * other way of putting a name into scope was invisible to it - not because those names are not in scope (the
 * executor projects them correctly once {@code RETURN *} is allowed to run at all), but because the parse-time
 * guard that decides whether {@code RETURN *} is legal never looked at them.
 * <p>
 * Neo4j (the openCypher reference implementation) accepts every query below: a {@code CALL { }} subquery's
 * {@code RETURN} items become ordinary variables of the enclosing scope, exactly like a {@code WITH}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6135ReturnStarSubqueryScopeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})");
  }

  @Test
  void returnStarSeesAVariableExportedByACallSubquery() {
    try (final ResultSet rs = database.query("opencypher", "CALL { MATCH (n:P) RETURN n } RETURN *")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.getPropertyNames()).containsExactly("n");
      assertThat(row.<Object>getProperty("n")).isNotNull();
    }
  }

  @Test
  void returnStarSeesAVariableExportedByACallSubqueryAlongsideOuterVariables() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:P {name: 'a'}) CALL { MATCH (m:P {name: 'b'}) RETURN m } RETURN *")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("a", "m");
    }
  }

  @Test
  void returnStarSeesAWithProjectedAlias() {
    try (final ResultSet rs = database.query("opencypher", "WITH 1 AS x RETURN *")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(1);
    }
  }

  @Test
  void returnStarSeesACallYieldVariable() {
    try (final ResultSet rs = database.query("opencypher", "CALL db.labels() YIELD label RETURN *")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getPropertyNames()).containsExactly("label");
    }
  }

  @Test
  void returnStarWithNothingInScopeIsStillRejected() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN *").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NoVariablesInScope");
  }

  @Test
  void returnStarInsideACallSubqueryBodyStillFollowsTheSameRule() {
    assertThatCode(() -> database.query("opencypher", "CALL { MATCH (n:P) WITH n CALL { MATCH (m:P) RETURN m } "
        + "RETURN * } RETURN *").close()).doesNotThrowAnyException();
  }
}
