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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #12983: the batch size of {@code CALL { ... } IN TRANSACTIONS OF n ROWS} was narrowed with
 * {@code intValue()} and never validated, so {@code OF 0 ROWS}, {@code OF -1 ROWS} and {@code OF 4294967296 ROWS} (2^32
 * wraps to 0) collected an empty first batch, which the step read as the end of its input: every outer row was dropped,
 * the body never ran and the statement answered success. Neo4j refuses a batch size that is not a positive integer.
 */
class CypherCallInTransactionsBatchSizeIssue12983Test extends TestHelper {

  private static final String PREFIX = "UNWIND range(1, 5) AS i CALL { WITH i CREATE (:X {i: i}) } IN TRANSACTIONS OF ";

  private long countX() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (x:X) RETURN count(x) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "0", "-1", "-9223372036854775808", "1.5", "0.0", "'3'" })
  void aBatchSizeThatIsNotAPositiveIntegerIsRefused(final String size) {
    assertThatThrownBy(() -> database.command("opencypher", PREFIX + size + " ROWS").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("batch size");
    assertThat(countX()).as("a refused statement wrote nothing").isZero();
  }

  @Test
  void aParameterThatIsNotAPositiveIntegerIsRefused() {
    for (final Object size : new Object[] { 0, -3, 2.5d, "4" })
      assertThatThrownBy(() -> database.command("opencypher", PREFIX + "$size ROWS", Map.of("size", size)).close())
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("batch size");
    assertThat(countX()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "1", "2", "5", "6", "2147483647", "2147483648", "4294967296", "4294967297", "9223372036854775807" })
  void aPositiveBatchSizeRunsEveryRowWhateverItsMagnitude(final String size) {
    database.command("opencypher", PREFIX + size + " ROWS").close();
    assertThat(countX()).isEqualTo(5L);
  }

  @Test
  void aBatchSizeBeyondTheIntRangeIsOneBatchOfEverything() {
    try (final ResultSet rs = database.command("opencypher",
        "UNWIND range(1, 5) AS i CALL { WITH i CREATE (:X {i: i}) } IN TRANSACTIONS OF 4294967296 ROWS "
            + "ON ERROR CONTINUE REPORT STATUS AS s RETURN count(DISTINCT s.transactionId) AS batches")) {
      assertThat(rs.next().<Number>getProperty("batches").longValue()).isEqualTo(1L);
    }
    assertThat(countX()).isEqualTo(5L);
  }

  /** A BigDecimal parameter keeps its fraction and a BigInteger its size: {@code longValue()} dropped the one and wrapped the other. */
  @Test
  void aBigNumberParameterIsReadByItsValueNotItsLow64Bits() {
    assertThatThrownBy(() -> database.command("opencypher", PREFIX + "$size ROWS", Map.of("size", new BigDecimal("1.5"))).close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("batch size");
    for (final Object size : new Object[] { new BigInteger("-9223372036854775809"), new BigInteger("-18446744073709551615"),
        new BigDecimal("0"), new BigDecimal("-2") })
      assertThatThrownBy(() -> database.command("opencypher", PREFIX + "$size ROWS", Map.of("size", size)).close())
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("batch size");
    assertThat(countX()).as("a refused statement wrote nothing").isZero();

    // an integral BigDecimal and a BigInteger past the long range are sizes: 2^64 + 1 is one batch of everything, not a batch of 1
    for (final Object size : new Object[] { new BigDecimal("2"), new BigDecimal("2.0"), new BigInteger("9223372036854775808"),
        new BigInteger("18446744073709551617"), new BigDecimal("1E+30") }) {
      try (final ResultSet rs = database.command("opencypher", PREFIX + "$size ROWS ON ERROR CONTINUE REPORT STATUS AS s "
          + "RETURN count(DISTINCT s.transactionId) AS batches", Map.of("size", size))) {
        final long batches = rs.next().<Number>getProperty("batches").longValue();
        assertThat(batches).as("batches of " + size).isEqualTo(size instanceof BigDecimal d && d.intValue() == 2 ? 3L : 1L);
      }
    }
    assertThat(countX()).isEqualTo(25L);
  }
}
