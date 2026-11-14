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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #11689: a CALL subquery RETURN must not export a name already bound in the outer scope.
 */
class CypherCallSubqueryExportShadowIssue11689Test extends TestHelper {

  @Test
  void returningImportedVariableUnderSameNameIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher", "UNWIND [1, 2] AS i CALL { WITH i RETURN i } RETURN i").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  @Test
  void aliasingExpressionToImportedNameIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher", "UNWIND [1, 2] AS i CALL { WITH i RETURN i + 1 AS i } RETURN i").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  @Test
  void exportingNonImportedOuterNameIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher", "UNWIND [1, 2] AS i CALL { RETURN 5 AS i } RETURN i").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  @Test
  void nestedSubqueryClashIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher",
        "UNWIND [1] AS i CALL { WITH i UNWIND [2] AS k CALL { WITH k RETURN k } RETURN i AS j } RETURN j").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("'k' already declared in outer scope");
  }

  @Test
  void distinctExportNameWorks() {
    final List<Long> js = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 2] AS i CALL { WITH i RETURN i AS j } RETURN i, j ORDER BY i")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("i")).longValue()).isEqualTo(((Number) r.getProperty("j")).longValue());
        js.add(((Number) r.getProperty("j")).longValue());
      }
    }
    assertThat(js).containsExactly(1L, 2L);
  }

  @Test
  void unitSubqueryIsUnaffected() {
    database.transaction(() -> database.command("opencypher", "UNWIND [1, 2] AS i CALL { WITH i CREATE (:N {v: i}) } RETURN i").close());
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:N) RETURN count(n) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
    }
  }
}
