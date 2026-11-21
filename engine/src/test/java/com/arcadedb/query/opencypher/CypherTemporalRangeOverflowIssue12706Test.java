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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12706: a date or datetime moved by a duration out of the java.time range, and the epochMillis of a datetime past the long range,
 * answered the raw {@code DateTimeException} / {@code ArithmeticException} of java.time, which the HTTP layer answers as a 500. They are the
 * caller's values, an {@link ArithmeticErrorException} like every other overflow of a temporal value.
 */
class CypherTemporalRangeOverflowIssue12706Test extends TestHelper {
  @Test
  void aDateMovedPastTheSupportedYearsIsAnArithmeticError() {
    assertOverflow("RETURN date('2020-01-01') + duration('P1000000000Y') AS v");
    assertOverflow("RETURN duration('P1000000000Y') + date('2020-01-01') AS v");
    assertOverflow("RETURN date('+999999999-12-31') + duration('P1D') AS v");
    assertOverflow("RETURN date('-999999999-01-01') - duration('P1D') AS v");
    assertOverflow("RETURN date('2020-01-01') + duration({months: 9223372036854775807}) AS v");
    assertOverflow("RETURN date('2020-01-01') + duration({days: 999999999999}) AS v");
  }

  @Test
  void aDatetimeMovedPastTheSupportedRangeIsAnArithmeticError() {
    assertOverflow("RETURN datetime('2020-01-01T00:00Z') + duration('P1000000000Y') AS v");
    assertOverflow("RETURN datetime('2020-01-01T00:00Z') - duration({months: 9223372036854775807}) AS v");
    assertOverflow("RETURN datetime('2020-01-01T00:00Z') + duration({seconds: 9223372036854775807}) AS v");
    assertOverflow("RETURN localdatetime('2020-01-01T00:00') + duration({days: 999999999999}) AS v");
  }

  @Test
  void epochMillisPastTheLongRangeIsAnArithmeticError() {
    assertOverflow("RETURN datetime('+999999999-01-01T00:00Z').epochMillis AS v");
  }

  @Test
  void arithmeticInsideTheRangeIsUnchanged() {
    assertThat(single("RETURN date('2020-01-31') + duration('P1M') AS v").toString()).isEqualTo("2020-02-29");
    assertThat(single("RETURN date('2020-01-01') - duration('P1000000000Y') AS v").toString()).isEqualTo("-999997980-01-01");
    assertThat(single("RETURN datetime('2020-01-01T00:00Z') + duration('PT1.5S') AS v").toString()).isEqualTo("2020-01-01T00:00:01.500Z");
    assertThat(single("RETURN datetime('2020-01-01T00:00Z').epochMillis AS v")).isEqualTo(1577836800000L);
  }

  private void assertOverflow(final String query) {
    assertThatThrownBy(() -> single(query)).as(query).isInstanceOf(ArithmeticErrorException.class);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }
}
