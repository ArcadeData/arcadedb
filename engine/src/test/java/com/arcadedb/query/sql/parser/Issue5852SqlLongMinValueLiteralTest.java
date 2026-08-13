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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5852: {@code Long.MIN_VALUE} (-9223372036854775808) could not be written as a SQL
 * literal.
 * <p>
 * The positive magnitude {@code 9223372036854775808} is one past {@code Long.MAX_VALUE}, so it overflows both
 * {@code Integer.parseInt} and {@code Long.parseLong} on its own - the standard two's-complement-minimum hazard.
 * {@link SQLASTBuilder#visitIntegerLiteral} parsed that bare magnitude before the enclosing unary minus was ever
 * applied ({@code -X} was rewritten to {@code 0 - X}), so the conversion failed before the sign could rescue it.
 * Every other {@code long} value, including {@code Long.MIN_VALUE + 1} and {@code Long.MAX_VALUE}, parsed fine, and
 * the same value was reachable through arithmetic ({@code 0 - 9223372036854775807 - 1}) or through a bound
 * parameter - only the direct literal spelling failed.
 */
class Issue5852SqlLongMinValueLiteralTest {

  @Test
  void longMinValueLiteralParsesToTheCorrectValue() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue5852LiteralSelect", db -> {
      try (final ResultSet rs = db.query("sql", "SELECT -9223372036854775808 AS r")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("r")).isEqualTo(Long.MIN_VALUE);
      }
    });
  }

  /**
   * The boundary just above {@code Long.MIN_VALUE} must keep working exactly as before: it is a normal literal
   * (its bare magnitude fits in a long), so it still goes through the pre-existing {@code 0 - X} path.
   */
  @Test
  void adjacentLongValuesStillParseCorrectly() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue5852Adjacent", db -> {
      try (final ResultSet rs = db.query("sql", "SELECT -9223372036854775807 AS r")) {
        assertThat(rs.next().<Long>getProperty("r")).isEqualTo(-9223372036854775807L);
      }
      try (final ResultSet rs = db.query("sql", "SELECT 9223372036854775807 AS r")) {
        assertThat(rs.next().<Long>getProperty("r")).isEqualTo(Long.MAX_VALUE);
      }
      try (final ResultSet rs = db.query("sql", "SELECT -2147483648 AS r")) {
        assertThat(rs.next().<Long>getProperty("r")).isEqualTo(-2147483648L);
      }
    });
  }

  /**
   * The explicit {@code L}/{@code l}-suffixed spelling must fold the same way as the bare literal: the suffix only
   * ever forces the {@code long} conversion, which is already what the fold produces.
   */
  @Test
  void longSuffixedLongMinValueLiteralParsesToTheCorrectValue() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue5852LiteralSuffixSelect", db -> {
      try (final ResultSet rs = db.query("sql", "SELECT -9223372036854775808L AS r")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("r")).isEqualTo(Long.MIN_VALUE);
      }
      try (final ResultSet rs = db.query("sql", "SELECT -9223372036854775808l AS r")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("r")).isEqualTo(Long.MIN_VALUE);
      }
    });
  }

  /**
   * A magnitude that overflows even after the sign is folded in (more digits than {@code Long.MIN_VALUE}'s) must
   * still be rejected with the original error, not silently accepted.
   */
  @Test
  void largerOverflowIsStillRejected() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue5852StillInvalid", db -> {
      assertThatThrownBy(() -> db.query("sql", "SELECT -99223372036854775808 AS r").close())
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("Invalid integer");
    });
  }

  /**
   * The end-to-end scenario from the issue: a stored {@code Long.MIN_VALUE} row, unreachable by its own literal
   * value in a {@code WHERE} predicate before the fix - including through a {@code UNIQUE} index.
   */
  @Test
  void storedLongMinValueIsReachableByItsOwnLiteral() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue5852IndexLookup", db -> {
      db.getSchema().createVertexType("V");
      db.getSchema().getType("V").createProperty("k", Type.LONG);
      db.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");

      db.transaction(() -> db.command("sql", "INSERT INTO V SET k = 0 - 9223372036854775807 - 1"));

      try (final ResultSet rs = db.query("sql", "SELECT FROM V WHERE k = -9223372036854775808")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("k")).isEqualTo(Long.MIN_VALUE);
      }
    });
  }
}
