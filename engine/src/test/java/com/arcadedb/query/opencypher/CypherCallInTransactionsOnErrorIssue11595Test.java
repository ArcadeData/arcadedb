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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #11595: {@code CALL { ... } IN TRANSACTIONS ... ON ERROR CONTINUE}, {@code ON ERROR BREAK} and
 * {@code REPORT STATUS AS s} were parsed and ignored, so a failing batch failed the statement as {@code ON ERROR FAIL} does.
 * A UNIQUE key already holds {@code i = 25}, so of the five batches of 10 rows the third fails.
 */
class CypherCallInTransactionsOnErrorIssue11595Test extends TestHelper {

  private static final String BODY = "UNWIND range(1, 50) AS i CALL { WITH i CREATE (:X {i: i}) } IN TRANSACTIONS OF 10 ROWS ";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE X");
    database.command("sql", "CREATE PROPERTY X.i INTEGER");
    database.command("sql", "CREATE INDEX ON X (i) UNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:X {i: 25})"));
  }

  @Test
  void onErrorFailStillFailsTheStatement() {
    assertThatThrownBy(() -> database.command("opencypher", BODY + "ON ERROR FAIL"));
    assertThat(countX()).isEqualTo(21L);
  }

  @Test
  void onErrorContinueSkipsTheFailedBatch() {
    assertThat(single(BODY + "ON ERROR CONTINUE RETURN count(*) AS c").<Number>getProperty("c").longValue()).isEqualTo(50L);
    assertThat(countX()).isEqualTo(41L);
  }

  @Test
  void onErrorBreakStopsAtTheFailedBatch() {
    assertThat(single(BODY + "ON ERROR BREAK RETURN count(*) AS c").<Number>getProperty("c").longValue()).isEqualTo(50L);
    assertThat(countX()).isEqualTo(21L);
  }

  @Test
  void reportStatusWithContinue() {
    final Map<String, Long> byCommitted = grouped(
        BODY + "ON ERROR CONTINUE REPORT STATUS AS s RETURN s.committed AS k, count(*) AS c");
    assertThat(byCommitted).isEqualTo(Map.of("false", 10L, "true", 40L));

    final List<Result> failed = rows(
        "UNWIND range(21, 30) AS i CALL { WITH i CREATE (:X {i: i}) } IN TRANSACTIONS OF 10 ROWS ON ERROR CONTINUE "
            + "REPORT STATUS AS s RETURN i, s");
    assertThat(failed).hasSize(10);
    for (final Result row : failed) {
      final Map<?, ?> status = row.getProperty("s");
      assertThat(status.get("started")).isEqualTo(true);
      assertThat(status.get("committed")).isEqualTo(false);
      assertThat(status.get("transactionId")).isNotNull();
      assertThat((String) status.get("errorMessage")).contains("Duplicated key");
    }
  }

  @Test
  void reportStatusWithBreak() {
    final Map<String, Long> byStarted = grouped(
        BODY + "ON ERROR BREAK REPORT STATUS AS s RETURN s.started AS k, count(*) AS c");
    assertThat(byStarted).isEqualTo(Map.of("false", 20L, "true", 30L));

    database.transaction(() -> database.command("opencypher", "MATCH (x:X) WHERE x.i <> 25 DELETE x"));
    final List<Result> notStarted = rows(BODY + "ON ERROR BREAK REPORT STATUS AS s WITH i, s WHERE i > 30 RETURN i, s");
    assertThat(notStarted).hasSize(20);
    for (final Result row : notStarted) {
      final Map<?, ?> status = row.getProperty("s");
      assertThat(status.get("started")).isEqualTo(false);
      assertThat(status.get("committed")).isEqualTo(false);
      assertThat(status.get("transactionId")).isNull();
      assertThat(status.get("errorMessage")).isNull();
    }
  }

  @Test
  void aFailedBatchBindsTheReturnedVariablesToNull() {
    final Result row = single("UNWIND range(1, 50) AS i CALL { WITH i CREATE (x:X {i: i}) RETURN x.i AS v } "
        + "IN TRANSACTIONS OF 10 ROWS ON ERROR CONTINUE RETURN count(v) AS c, count(*) AS rows");
    assertThat(row.<Number>getProperty("c").longValue()).isEqualTo(40L);
    assertThat(row.<Number>getProperty("rows").longValue()).isEqualTo(50L);
  }

  @Test
  void aFailedBatchDoesNotRollBackTheCallersTransaction() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Pre {v: 1})");
      database.command("opencypher", BODY + "ON ERROR CONTINUE");
      assertThat(countLabel("Pre")).isEqualTo(1L);
    });
    assertThat(countLabel("Pre")).isEqualTo(1L);
    assertThat(countX()).isEqualTo(41L);
  }

  @Test
  void reportStatusNeedsContinueOrBreak() {
    assertThatThrownBy(() -> database.command("opencypher", BODY + "REPORT STATUS AS s RETURN s"))
        .hasMessageContaining("REPORT STATUS");
    assertThatThrownBy(() -> database.command("opencypher", BODY + "ON ERROR FAIL REPORT STATUS AS s RETURN s"))
        .hasMessageContaining("REPORT STATUS");
  }

  @Test
  void onErrorRetryIsRefusedRatherThanRunWithoutItsRetries() {
    for (final String clause : new String[] { "ON ERROR RETRY THEN CONTINUE", "ON ERROR RETRY FOR 5 SECONDS THEN BREAK",
        "ON ERROR RETRY" })
      assertThatThrownBy(() -> database.command("opencypher", BODY + clause)).as(clause).hasMessageContaining("ON ERROR RETRY");
    assertThat(countX()).isEqualTo(1L);
  }

  /**
   * A nested IN TRANSACTIONS with the default ON ERROR FAIL rolls back every level on failure, the caller's transaction
   * included, which an outer CONTINUE or BREAK cannot take back: the combination is refused before anything runs.
   */
  @Test
  void aNestedInTransactionsUnderContinueOrBreakIsRefused() {
    for (final String mode : new String[] { "CONTINUE", "BREAK" })
      assertThatThrownBy(() -> database.command("opencypher",
          "UNWIND range(1, 3) AS i CALL { WITH i CALL { WITH i CREATE (:X {i: i + 100}) } IN TRANSACTIONS } "
              + "IN TRANSACTIONS OF 1 ROWS ON ERROR " + mode)).as(mode).hasMessageContaining("cannot contain");
    assertThat(countX()).isEqualTo(1L);
  }

  /**
   * A label written in a failed batch is undone with it: the outer row is not redirected to the replacement record the
   * rolled-back batch created.
   */
  @Test
  void aLabelWrittenInAFailedBatchIsNotFollowed() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 24}), (:A {id: 25}), (:A {id: 26})"));
    final List<Result> result = rows("MATCH (n:A) WITH n ORDER BY n.id CALL { WITH n SET n:B CREATE (:X {i: n.id}) } "
        + "IN TRANSACTIONS OF 1 ROWS ON ERROR CONTINUE RETURN n.id AS id, labels(n) AS l ORDER BY id");
    assertThat(result).hasSize(3);
    assertThat(result.get(1).<Number>getProperty("id").intValue()).isEqualTo(25);
    assertThat(result.get(1).<List<Object>>getProperty("l")).containsExactly("A");
    assertThat(result.get(0).<List<Object>>getProperty("l")).containsExactlyInAnyOrder("A", "B");
    assertThat(single("MATCH (n:A {id: 25}) RETURN labels(n) AS l").<List<Object>>getProperty("l")).containsExactly("A");
  }

  private long countX() {
    return countLabel("X");
  }

  private long countLabel(final String label) {
    return single("MATCH (n:" + label + ") RETURN count(n) AS c").<Number>getProperty("c").longValue();
  }

  private Result single(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(1);
    return rows.getFirst();
  }

  private List<Result> rows(final String query) {
    final List<Result> out = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next());
    }
    return out;
  }

  private Map<String, Long> grouped(final String query) {
    final Map<String, Long> out = new TreeMap<>();
    for (final Result row : rows(query))
      out.put(String.valueOf(row.<Object>getProperty("k")), row.<Number>getProperty("c").longValue());
    return out;
  }
}
