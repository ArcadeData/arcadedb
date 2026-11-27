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
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.query.opencypher.procedures.algo.AlgoAllSimplePaths;
import com.arcadedb.query.opencypher.procedures.algo.AlgoKShortestPaths;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for #12885: caller mistakes that answered HTTP 500. A string or a null in a numeric argument of
 * {@code path.expand} (and its {@code apoc.path.expand} alias), {@code algo.kShortestPaths} and {@code algo.allSimplePaths}
 * was cast unchecked into a {@code ClassCastException} or a {@code NullPointerException}; they now raise an
 * {@link IllegalArgumentException} naming the argument, which the HTTP layer answers 400. Temporal arithmetic and
 * {@code datetime.fromepoch} past the supported year range, the issue's second group, were already answered with an
 * {@link ArithmeticErrorException} keeping the {@code java.time} message (#12706, #12744): those cases only pin it.
 */
class Issue12885ArgumentAndTemporalRangeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.id INTEGER");
      database.command("sql", "CREATE PROPERTY P.d DATE");
      database.command("sql", "CREATE PROPERTY P.dt DATETIME");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("opencypher", "CREATE (:P {id: 1})-[:K]->(:P {id: 2})");
      database.command("sql", "UPDATE P SET d = ?, dt = ? WHERE id = 1", LocalDate.of(2020, 1, 1), LocalDateTime.of(2020, 1, 1, 0, 0));
    });
  }

  @Test
  void pathExpandRefusesANonNumberOrNullDepth() {
    for (final String procedure : new String[] { "path.expand", "apoc.path.expand" }) {
      assertIllegalArgument("MATCH (n:P {id: 1}) CALL " + procedure + "(n, 'K', null, 'x', 3) YIELD path RETURN count(*)",
          "minDepth must be a number, got String");
      assertIllegalArgument("MATCH (n:P {id: 1}) CALL " + procedure + "(n, 'K', null, null, 3) YIELD path RETURN count(*)",
          "minDepth cannot be null");
      assertIllegalArgument("MATCH (n:P {id: 1}) CALL " + procedure + "(n, 'K', null, 1, 'x') YIELD path RETURN count(*)",
          "maxDepth must be a number, got String");
      assertIllegalArgument("MATCH (n:P {id: 1}) CALL " + procedure + "(n, 'K', null, 1, null) YIELD path RETURN count(*)",
          "maxDepth cannot be null");
      assertCount("MATCH (n:P {id: 1}) CALL " + procedure + "(n, 'K>', null, 1, 3) YIELD path RETURN count(*) AS c", 1L);
    }
  }

  @Test
  void kShortestPathsRefusesANonNumberOrNullK() {
    assertIllegalArgument("MATCH (n:P {id: 1}), (m:P {id: 2}) CALL algo.kShortestPaths(n, m, 'x') YIELD path RETURN count(*)",
        AlgoKShortestPaths.NAME + "(): k must be a number, got String");
    assertIllegalArgument("MATCH (n:P {id: 1}), (m:P {id: 2}) CALL algo.kShortestPaths(n, m, null) YIELD path RETURN count(*)",
        AlgoKShortestPaths.NAME + "(): k cannot be null");
    assertCount("MATCH (n:P {id: 1}), (m:P {id: 2}) CALL algo.kShortestPaths(n, m, 2) YIELD path RETURN count(*) AS c", 1L);
  }

  @Test
  void allSimplePathsRefusesANonNumberOrNullMaxDepth() {
    assertIllegalArgument(
        "MATCH (n:P {id: 1}), (m:P {id: 2}) CALL algo.allSimplePaths(n, m, 'K', 'x') YIELD path RETURN count(*)",
        AlgoAllSimplePaths.NAME + "(): maxDepth must be a number, got String");
    assertIllegalArgument(
        "MATCH (n:P {id: 1}), (m:P {id: 2}) CALL algo.allSimplePaths(n, m, 'K', null) YIELD path RETURN count(*)",
        AlgoAllSimplePaths.NAME + "(): maxDepth cannot be null");
    assertCount("MATCH (n:P {id: 1}), (m:P {id: 2}) CALL algo.allSimplePaths(n, m, 'K', 3) YIELD path RETURN count(*) AS c", 1L);
  }

  @Test
  void temporalArithmeticPastTheYearRangeIsAnArithmeticError() {
    assertArithmeticError("RETURN date('+999999999-12-31') + duration({days: 1}) AS v");
    assertArithmeticError("RETURN duration({days: 1}) + date('+999999999-12-31') AS v");
    assertArithmeticError("RETURN date('-999999999-01-01') - duration({months: 1}) AS v");
    assertArithmeticError("RETURN datetime({year: 999999999, month: 12, day: 31}) + duration({years: 10}) AS v");
    assertArithmeticError("RETURN datetime({year: -999999999}) - duration({days: 1}) AS v");
    assertArithmeticError("RETURN localdatetime({year: -999999999}) - duration({years: 1}) AS v");
    assertArithmeticError("RETURN localdatetime({year: 999999999, month: 12, day: 31}) + duration({hours: 24}) AS v");
    // the values a DATE and a DATETIME property hand back, java.time.LocalDate and LocalDateTime
    assertArithmeticError("MATCH (n:P {id: 1}) RETURN n.d + duration({years: 999999999}) AS v");
    assertArithmeticError("MATCH (n:P {id: 1}) RETURN n.dt + duration({years: 999999999}) AS v");
    // a day count past a long, which java.time refuses with an ArithmeticException rather than a DateTimeException
    assertArithmeticError("RETURN date('2020-01-01') + duration({days: 9223372036854775807}) AS v");

    try (final ResultSet rs = database.query("opencypher",
        "RETURN date('2020-01-31') + duration({months: 1}) AS d, datetime('2020-01-01T00:00:00Z') - duration({days: 1}) AS dt")) {
      final var row = rs.next();
      assertThat(row.<CypherDate>getProperty("d").getValue()).isEqualTo(LocalDate.of(2020, 2, 29));
      assertThat(row.<CypherDateTime>getProperty("dt").getValue().toLocalDate()).isEqualTo(LocalDate.of(2019, 12, 31));
    }
  }

  @Test
  void fromEpochPastTheInstantRangeIsAnArithmeticError() {
    assertFromEpochError("RETURN datetime.fromepoch(9223372036854775807, 0) AS v", "datetime.fromepoch()",
        "Instant exceeds minimum or maximum instant");
    assertFromEpochError("RETURN datetime.fromepoch(-9223372036854775807, 0) AS v", "datetime.fromepoch()",
        "Instant exceeds minimum or maximum instant");
    // the nanoseconds carried into the seconds overflow a long
    assertFromEpochError("RETURN datetime.fromepoch(9223372036854775807, 2000000000) AS v", "datetime.fromepoch()", "overflow");

    try (final ResultSet rs = database.query("opencypher",
        "RETURN datetime.fromepoch(0, 0) AS a, datetime.fromepochmillis(9223372036854775807) AS b")) {
      final var row = rs.next();
      assertThat(row.<CypherDateTime>getProperty("a").getValue().toLocalDate()).isEqualTo(LocalDate.of(1970, 1, 1));
      assertThat(row.<CypherDateTime>getProperty("b").getValue().getYear()).isEqualTo(292278994);
    }
  }

  private void assertCount(final String cypher, final long expected) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as(cypher).isEqualTo(expected);
    }
  }

  private void assertIllegalArgument(final String cypher, final String message) {
    final Throwable error = run(cypher);
    final IllegalArgumentException clientError = CauseChain.find(error, IllegalArgumentException.class);
    assertThat(clientError).as(cypher + " -> " + error).isNotNull();
    assertThat(clientError.getMessage()).as(cypher).contains(message);
  }

  private void assertArithmeticError(final String cypher) {
    final Throwable error = run(cypher);
    final ArithmeticErrorException clientError = CauseChain.find(error, ArithmeticErrorException.class);
    assertThat(clientError).as(cypher + " -> " + error).isNotNull();
    assertThat(clientError.getMessage()).as(cypher).startsWith("Temporal overflow: ");
  }

  private void assertFromEpochError(final String cypher, final String prefix, final String message) {
    final Throwable error = run(cypher);
    final ArithmeticErrorException clientError = CauseChain.find(error, ArithmeticErrorException.class);
    assertThat(clientError).as(cypher + " -> " + error).isNotNull();
    assertThat(clientError.getMessage()).as(cypher).startsWith(prefix).contains(message);
  }

  private Throwable run(final String cypher) {
    final Throwable error = catchThrowable(() -> {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    });
    assertThat(error).as(cypher).isNotNull();
    return error;
  }
}
