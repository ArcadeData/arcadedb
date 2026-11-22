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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #12982: openCypher SKIP/LIMIT values above {@link Integer#MAX_VALUE}
 * wrapped to negative ints via {@code Number.intValue()} and were rejected with
 * {@code NegativeIntegerArgument}.
 */
class CypherSkipLimitLongIssue12982Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "UNWIND range(1, 5) AS i CREATE (:N {i: i})");
  }

  @Test
  void limitLiteralAboveIntMaxReturnsAllRows() {
    assertThat(queryValues("MATCH (n:N) RETURN n.i AS i ORDER BY i LIMIT 3000000000", Map.of()))
        .containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void limitParameterAboveIntMaxReturnsAllRows() {
    assertThat(queryValues("MATCH (n:N) RETURN n.i AS i ORDER BY i LIMIT $p", Map.of("p", 3_000_000_000L)))
        .containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void skipLiteralAboveIntMaxReturnsNoRows() {
    assertThat(queryValues("MATCH (n:N) RETURN n.i AS i ORDER BY i SKIP 3000000000", Map.of()))
        .isEmpty();
  }

  @Test
  void skipParameterAboveIntMaxReturnsNoRows() {
    assertThat(queryValues("MATCH (n:N) RETURN n.i AS i ORDER BY i SKIP $p", Map.of("p", 3_000_000_000L)))
        .isEmpty();
  }

  @Test
  void withClauseSkipLimitAboveIntMax() {
    assertThat(queryValues("MATCH (n:N) WITH n ORDER BY n.i LIMIT 3000000000 RETURN n.i AS i", Map.of()))
        .containsExactly(1L, 2L, 3L, 4L, 5L);
    assertThat(queryValues("MATCH (n:N) WITH n ORDER BY n.i SKIP 3000000000 RETURN n.i AS i", Map.of()))
        .isEmpty();
  }

  @Test
  void negativeLiteralAndLongParameterStillRejectedWithOriginalValue() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:N) RETURN n.i AS i LIMIT -1"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NegativeIntegerArgument")
        .hasMessageContaining("-1");

    assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:N) RETURN n.i AS i SKIP -1"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NegativeIntegerArgument")
        .hasMessageContaining("-1");

    assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:N) RETURN n.i AS i LIMIT $p",
        Map.of("p", -3_000_000_000L)))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NegativeIntegerArgument")
        .hasMessageContaining("-3000000000");
  }

  private List<Object> queryValues(final String query, final Map<String, Object> params) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = params.isEmpty()
        ? database.query("opencypher", query)
        : database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty("i"));
      }
    }
    return values;
  }
}
