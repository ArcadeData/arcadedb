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
 * Issue #12982: a LIMIT (or a SKIP) above the largest int was refused as a negative value: the number was read as an int
 * and wrapped, so 3000000000 was "negative". The reference implementation accepts it: a limit above the largest int is every
 * row, the rows a statement answers being counted with an int.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLimitAboveIntegerMaxIssue12982Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Node {id: 1}), (:Node {id: 2}), (:Node {id: 3})"));
  }

  @Test
  void aLimitAboveTheLargestIntIsEveryRow() {
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id LIMIT 3000000000")).containsExactly(1L, 2L, 3L);
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id LIMIT $p", Map.of("p", 3000000000L)))
        .containsExactly(1L, 2L, 3L);
    // the largest int itself was already accepted
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id LIMIT 2147483647")).containsExactly(1L, 2L, 3L);
    // a limit above the largest int does not change what a smaller one answers
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id LIMIT 2")).containsExactly(1L, 2L);
  }

  @Test
  void aSkipAboveTheLargestIntSkipsEveryRow() {
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id SKIP 3000000000")).isEmpty();
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id SKIP $p", Map.of("p", 3000000000L))).isEmpty();
  }

  /** A value that is really negative is still refused, and the message carries the value the caller wrote. */
  @Test
  void aNegativeLimitIsStillRefused() {
    assertRefused("MATCH (n:Node) RETURN n.id AS id LIMIT -1");
    assertRefused("MATCH (n:Node) RETURN n.id AS id SKIP -1");
    assertRefused("MATCH (n:Node) RETURN n.id AS id LIMIT $p", Map.of("p", -1L));
    assertRefused("MATCH (n:Node) RETURN n.id AS id LIMIT $p", Map.of("p", -3000000000L));
  }

  /** The top-K sort keeps SKIP + LIMIT rows: that sum must not wrap to a negative number when the limit is above the largest int. */
  @Test
  void aSmallSkipWithALimitAboveTheLargestIntSkipsOnlyTheSkippedRows() {
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id SKIP 1 LIMIT 3000000000")).containsExactly(2L, 3L);
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id SKIP $s LIMIT $l", Map.of("s", 1L, "l", 3000000000L)))
        .containsExactly(2L, 3L);
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id SKIP 2147483647 LIMIT 2147483647")).isEmpty();
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id DESC SKIP 2 LIMIT 3000000000")).containsExactly(1L);
  }

  @Test
  void aSkipAndALimitBothAboveTheLargestIntAnswerNothing() {
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id SKIP 3000000000 LIMIT 3000000000")).isEmpty();
    assertThat(ids("MATCH (n:Node) RETURN n.id AS id ORDER BY id SKIP $s LIMIT $l", Map.of("s", 3000000000L, "l", 3000000000L)))
        .isEmpty();
  }

  /** The same paging, behind a WITH and over a statement without a MATCH: they are planned by other blocks of the step builder. */
  @Test
  void theSameHoldsBehindAWithAndOverAnUnwind() {
    assertThat(ids("MATCH (n:Node) WITH n.id AS id ORDER BY id SKIP 1 LIMIT 3000000000 RETURN id")).containsExactly(2L, 3L);
    assertThat(ids("MATCH (n:Node) WITH n.id AS id ORDER BY id SKIP 3000000000 LIMIT 3000000000 RETURN id")).isEmpty();
    assertThat(ids("MATCH (n:Node) WITH n.id AS id ORDER BY id LIMIT 3000000000 RETURN id")).containsExactly(1L, 2L, 3L);
    assertThat(ids("UNWIND [3, 1, 2] AS id RETURN id ORDER BY id SKIP 1 LIMIT 3000000000")).containsExactly(2L, 3L);
    assertThat(ids("UNWIND [3, 1, 2] AS id RETURN id ORDER BY id SKIP 3000000000 LIMIT 3000000000")).isEmpty();
  }

  private void assertRefused(final String query, final Map<String, Object> params) {
    assertThatThrownBy(() -> rows(query, params)).as(query)
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("negative");
  }

  private void assertRefused(final String query) {
    assertRefused(query, null);
  }

  private List<Long> ids(final String query) {
    return ids(query, null);
  }

  private List<Long> ids(final String query, final Map<String, Object> params) {
    final List<Long> out = new ArrayList<>();
    for (final Result row : rows(query, params))
      out.add(row.<Number>getProperty("id").longValue());
    return out;
  }

  private List<Result> rows(final String query, final Map<String, Object> params) {
    final List<Result> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        out.add(rs.next());
    }
    return out;
  }
}
