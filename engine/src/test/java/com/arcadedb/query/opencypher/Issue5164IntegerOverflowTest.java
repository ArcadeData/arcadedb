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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5164: 64-bit integer arithmetic overflow must fail the query
 * (like Neo4j, the OpenCypher reference implementation) instead of silently wrapping around with
 * two's-complement semantics. Silent wraparound produces mathematically wrong results that look
 * valid and can be persisted to storage.
 * <p>
 * Neo4j raises {@code long overflow} for {@code +}, {@code -} and {@code *} that exceed the
 * {@code long} range, as well as the single division overflow case {@code Long.MIN_VALUE / -1}.
 * Mixed/floating-point arithmetic keeps IEEE 754 semantics (overflow becomes {@code ±Infinity}),
 * which this test also locks in so a future "fix" does not over-reach.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5164IntegerOverflowTest {
  private static final long MAX = Long.MAX_VALUE;
  private static final long MIN = Long.MIN_VALUE;

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-issue5164").create();
    database.getSchema().createVertexType("U");
    database.transaction(() -> database.newVertex("U")
        .set("id", 1).set("max", MAX).set("min", MIN).set("neg", -1L).set("two", 2L).save());
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private void assertOverflow(final String cypher) {
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      while (rs.hasNext()) // execution is lazy, force full evaluation
        rs.next();
    }).hasMessageContaining("overflow");
  }

  private long asLong(final String cypher) {
    final ResultSet rs = database.query("opencypher", cypher);
    assertThat(rs.hasNext()).isTrue();
    return rs.next().<Number>getProperty("r").longValue();
  }

  // ---- Constant-folded overflow (both operands are literals) ----

  @Test
  void addOverflowConstant() {
    assertOverflow("RETURN 9223372036854775807 + 1 AS r");
  }

  @Test
  void multiplyOverflowConstant() {
    assertOverflow("RETURN 9223372036854775807 * 2 AS r");
  }

  // ---- Runtime overflow (operand comes from a property, no constant folding) ----

  @Test
  void addOverflowRuntime() {
    assertOverflow("MATCH (u:U) RETURN u.max + 1 AS r");
  }

  @Test
  void subtractUnderflowRuntime() {
    // -9223372036854775808 - 1
    assertOverflow("MATCH (u:U) RETURN u.min - 1 AS r");
  }

  @Test
  void multiplyOverflowRuntime() {
    assertOverflow("MATCH (u:U) RETURN u.max * u.two AS r");
  }

  @Test
  void divideOverflowRuntime() {
    // Long.MIN_VALUE / -1 is the only overflowing division.
    assertOverflow("MATCH (u:U) RETURN u.min / u.neg AS r");
  }

  // ---- Overflow must also block a write, never persist the wrapped value ----

  @Test
  void overflowInWriteIsRejected() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "CREATE (x:Overflow {v: 9223372036854775807 + 1}) RETURN x.v");
      while (rs.hasNext())
        rs.next();
    })).hasMessageContaining("overflow");
    // Nothing should have been persisted.
    final ResultSet check = database.query("opencypher", "MATCH (x:Overflow) RETURN count(x) AS r");
    assertThat(check.next().<Number>getProperty("r").longValue()).isEqualTo(0L);
  }

  // ---- Non-overflowing integer arithmetic still works and stays integral ----

  @Test
  void nonOverflowingArithmeticStillWorks() {
    assertThat(asLong("RETURN 9223372036854775806 + 1 AS r")).isEqualTo(MAX);
    assertThat(asLong("RETURN 2 + 3 AS r")).isEqualTo(5L);
    assertThat(asLong("RETURN 1000000 * 1000000 AS r")).isEqualTo(1_000_000_000_000L);
    assertThat(asLong("MATCH (u:U) RETURN u.max - 1 AS r")).isEqualTo(MAX - 1);
  }

  // ---- Floating-point / mixed arithmetic keeps IEEE 754 semantics (Neo4j-compatible) ----

  @Test
  void floatOverflowYieldsInfinityNotError() {
    final ResultSet rs = database.query("opencypher", "RETURN 1.0e308 * 10 AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
  }

  @Test
  void mixedIntFloatBeyondLongRangeDoesNotError() {
    // 9223372036854775807 * 2.0 uses double arithmetic, no overflow error.
    final ResultSet rs = database.query("opencypher", "RETURN 9223372036854775807 * 2.0 AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("r").doubleValue()).isGreaterThan(0.0);
  }
}
