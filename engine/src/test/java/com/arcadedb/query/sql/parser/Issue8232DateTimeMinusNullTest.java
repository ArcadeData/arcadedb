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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8232: {@code MINUS.apply(Object, Object)} only guarded a null operand next to a
 * {@link Number} (lines handling {@code left instanceof Number && right == null} and its mirror). The date branch
 * that follows had no such guard, so {@code DateUtils.dateTimeToTimestamp(null, ...)} returned {@code null} and the
 * unboxing in {@code apply(Long, Long)} threw a {@link NullPointerException} - taking the whole query down, since the
 * everyday trigger is simply a missing/optional field next to a stored {@code DATETIME}.
 * <p>
 * {@code date - null} now returns the date unchanged, matching {@code date + null} and {@code number - null}.
 * {@code null - date} returns {@code null}: unlike a number, a date has no meaningful negative to fall back to.
 */
class Issue8232DateTimeMinusNullTest {

  @Test
  void dateMinusNullOperatorReturnsTheDateUnchanged() {
    final LocalDateTime dt = LocalDateTime.now();
    assertThat(MathExpression.Operator.MINUS.apply(dt, null)).isEqualTo(dt);
    assertThat(MathExpression.Operator.MINUS.apply(null, dt)).isNull();
    assertThat(MathExpression.Operator.MINUS.apply((Object) null, null)).isNull();
  }

  /**
   * End to end through the SQL engine, reproducing the issue's exact repro: a stored {@code DATETIME} minus a field
   * absent on the record, and minus an explicit {@code null}, in both operand orders.
   */
  @Test
  void queryOverMissingOrNullFieldNextToADatetimeDoesNotThrow() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue8232DateTimeMinusNull", db -> {
      db.getSchema().createDocumentType("D").createProperty("dt", Type.DATETIME);
      db.getSchema().getType("D").createProperty("n", Type.LONG);
      db.transaction(() -> db.newDocument("D").set("dt", LocalDateTime.now()).set("n", 5L).save());

      try (final ResultSet rs = db.query("sql", "select dt - nosuch as x, dt from D")) {
        final var result = rs.next();
        assertThat(result.<Object>getProperty("x")).isEqualTo(result.getProperty("dt"));
      }

      try (final ResultSet rs = db.query("sql", "select dt - null as x, dt from D")) {
        final var result = rs.next();
        assertThat(result.<Object>getProperty("x")).isEqualTo(result.getProperty("dt"));
      }

      try (final ResultSet rs = db.query("sql", "select nosuch - dt as x from D")) {
        assertThat(rs.next().<Object>getProperty("x")).isNull();
      }

      try (final ResultSet rs = db.query("sql", "select null - dt as x from D")) {
        assertThat(rs.next().<Object>getProperty("x")).isNull();
      }

      // control: the paths that already worked must keep working
      try (final ResultSet rs = db.query("sql", "select n - nosuch as x from D")) {
        assertThat(rs.next().<Object>getProperty("x")).isEqualTo(5L);
      }
      try (final ResultSet rs = db.query("sql", "select dt + nosuch as x, dt from D")) {
        final var result = rs.next();
        assertThat(result.<Object>getProperty("x")).isEqualTo(result.getProperty("dt"));
      }
    });
  }
}
