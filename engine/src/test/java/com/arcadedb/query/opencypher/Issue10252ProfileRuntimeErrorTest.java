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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10252: {@code PROFILE} runs the statement exactly as the statement runs, so a statement that fails fails under
 * {@code PROFILE} too, with the same error, and what it wrote before the error is not committed. The profiled run used to
 * catch the error into the profile text, answer no rows, and let the auto-commit transaction around it commit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10252ProfileRuntimeErrorTest extends TestHelper {
  private static final String FAILING_WRITE = "CREATE (:A) WITH 1 AS z RETURN 1 / (z - 1) AS r";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createVertexType("A"));
  }

  @Test
  void aFailingProfiledWriteFailsAndKeepsNothing() {
    assertThatThrownBy(() -> drain("PROFILE " + FAILING_WRITE)).hasMessageContaining("/ by zero");
    assertThat(countA()).isZero();
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void aFailingProfiledWriteFailsTheSameAsThePlainStatement() {
    assertThatThrownBy(() -> drain(FAILING_WRITE)).hasMessageContaining("/ by zero");
    assertThat(countA()).isZero();

    assertThatThrownBy(() -> drain("PROFILE " + FAILING_WRITE)).hasMessageContaining("/ by zero");
    assertThat(countA()).isZero();
  }

  @Test
  void aFailingProfiledWriteInsideATransactionReachesTheCaller() {
    database.begin();
    try {
      assertThatThrownBy(() -> drain("PROFILE " + FAILING_WRITE)).hasMessageContaining("/ by zero");
    } finally {
      database.rollback();
    }
    assertThat(countA()).isZero();
  }

  @Test
  void aFailingProfiledReadFails() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher", "PROFILE UNWIND [1, 0] AS x RETURN 1 / x AS r")) {
        while (rs.hasNext())
          rs.next();
      }
    }).hasMessageContaining("/ by zero");
  }

  @Test
  void aSucceedingProfiledWriteStillCommitsAndReportsItsProfile() {
    try (final ResultSet rs = database.command("opencypher", "PROFILE CREATE (:A {k: 1}) RETURN 1 AS r")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").intValue()).isEqualTo(1);
      assertThat(rs.getExecutionPlan()).isPresent();
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("OpenCypher Query Profile");
    }
    assertThat(countA()).isEqualTo(1);
  }

  @Test
  void anExplainedFailingWriteRunsNothing() {
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + FAILING_WRITE)) {
      while (rs.hasNext())
        rs.next();
    }
    assertThat(countA()).isZero();
  }

  private void drain(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private long countA() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN count(n) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
