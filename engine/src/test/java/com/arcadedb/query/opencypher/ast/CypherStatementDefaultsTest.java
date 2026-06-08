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
package com.arcadedb.query.opencypher.ast;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4503: the structural query accessors on {@link CypherStatement} provide empty/neutral
 * {@code default} implementations, so non-query control statements (transaction control, session
 * management, admin/user management, DDL) no longer need to repeat the boilerplate overrides.
 * <p>
 * This locks the refactor's contract: every control-statement class must inherit exactly the
 * empty/neutral values (no clauses, no write flags). If a class ever needs a real clause it has to
 * override the accessor explicitly, and this test will flag the change.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherStatementDefaultsTest {

  // Maintenance note: when a new non-query CypherStatement class is added (a control/admin statement that
  // carries no clauses), add an instance to the list below so it is held to the neutral defaults.
  @Test
  void controlStatementsInheritNeutralDefaults() {
    final List<CypherStatement> controlStatements = List.of(
        new CypherTransactionStatement(CypherTransactionStatement.Kind.BEGIN),
        new CypherTransactionStatement(CypherTransactionStatement.Kind.COMMIT),
        new CypherTransactionStatement(CypherTransactionStatement.Kind.ROLLBACK),
        new CypherSessionStatement(CypherSessionStatement.Kind.RESET),
        new CypherSessionStatement(CypherSessionStatement.Kind.SET, "p", null),
        new CypherSessionStatement(CypherSessionStatement.Kind.CLOSE),
        new CypherAdminStatement(CypherAdminStatement.Kind.SHOW_USERS, null, null, false, false),
        new CypherDDLStatement(CypherDDLStatement.Kind.CREATE_INDEX, null, "idx", "Person",
            List.of("name"), false, false, false));

    for (final CypherStatement s : controlStatements) {
      assertThat(s.getMatchClauses()).isEmpty();
      assertThat(s.getWhereClause()).isNull();
      assertThat(s.getReturnClause()).isNull();
      assertThat(s.hasCreate()).isFalse();
      assertThat(s.hasMerge()).isFalse();
      assertThat(s.hasDelete()).isFalse();
      assertThat(s.getOrderByClause()).isNull();
      assertThat(s.getSkip()).isNull();
      assertThat(s.getLimit()).isNull();
      assertThat(s.getCreateClause()).isNull();
      assertThat(s.getSetClause()).isNull();
      assertThat(s.getDeleteClause()).isNull();
      assertThat(s.getMergeClause()).isNull();
      assertThat(s.getUnwindClauses()).isEmpty();
      assertThat(s.getWithClauses()).isEmpty();

      // Control statements remain non-read-only (drives HA routing / write-context establishment).
      assertThat(s.isReadOnly()).isFalse();

      // Pre-existing defaults remain neutral too.
      assertThat(s.getClausesInOrder()).isEmpty();
      assertThat(s.getCallClauses()).isEmpty();
      assertThat(s.getRemoveClauses()).isEmpty();
      assertThat(s.hasVariableLengthPath()).isFalse();
      assertThat(s.hasFinishClause()).isFalse();
    }
  }

  /**
   * Issue #4505: {@code isServerControlStatement()} names the "not idempotent yet permission-gated as READ"
   * concept. Only transaction control and session management are server-control statements; admin and DDL
   * statements have their own (ADMIN / SCHEMA) gates and must report false, as must ordinary query statements
   * via the interface default.
   */
  @Test
  void onlyTransactionAndSessionAreServerControlStatements() {
    final List<CypherStatement> serverControl = List.of(
        new CypherTransactionStatement(CypherTransactionStatement.Kind.BEGIN),
        new CypherTransactionStatement(CypherTransactionStatement.Kind.COMMIT),
        new CypherTransactionStatement(CypherTransactionStatement.Kind.ROLLBACK),
        new CypherSessionStatement(CypherSessionStatement.Kind.RESET),
        new CypherSessionStatement(CypherSessionStatement.Kind.SET, "p", null),
        new CypherSessionStatement(CypherSessionStatement.Kind.CLOSE));
    for (final CypherStatement s : serverControl)
      assertThat(s.isServerControlStatement()).isTrue();

    final List<CypherStatement> notServerControl = List.of(
        new CypherAdminStatement(CypherAdminStatement.Kind.SHOW_USERS, null, null, false, false),
        new CypherDDLStatement(CypherDDLStatement.Kind.CREATE_INDEX, null, "idx", "Person",
            List.of("name"), false, false, false));
    for (final CypherStatement s : notServerControl)
      assertThat(s.isServerControlStatement()).isFalse();
  }
}
