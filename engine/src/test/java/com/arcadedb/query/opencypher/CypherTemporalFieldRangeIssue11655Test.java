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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #11655: a calendar field outside the range of its year, quarter or week is refused as the caller's error, as Neo4j's DateValueTest
 * expects (parse("2017W53") throws: 2017 has 52 ISO weeks). Week 53 of a 52-week year and a dayOfQuarter past the end of its
 * quarter rolled over into the next year or quarter, and the other out-of-range fields failed with a DateTimeException that
 * answered as an internal error.
 */
class CypherTemporalFieldRangeIssue11655Test extends TestHelper {

  @Test
  void aFieldBeyondItsYearOrQuarterIsRefusedInsteadOfRollingOver() {
    for (final String expression : new String[] { "date('2017W53')", "date('2017-W53')", "date('2017-W53-7')",
        "date({year: 2017, week: 53})", "date({year: 2017, week: 53, dayOfWeek: 3})", "localdatetime('2017-W53-1T10:00')",
        "datetime('2017-W53-1T10:00Z')", "datetime({year: 2017, week: 53, dayOfWeek: 1})",
        "date({year: 2017, quarter: 1, dayOfQuarter: 91})", "localdatetime({year: 2017, quarter: 1, dayOfQuarter: 95})",
        "date({date: date('2017-01-15'), dayOfQuarter: 91})" })
      assertRefused(expression);
  }

  @Test
  void anOutOfRangeFieldIsTheCallersError() {
    for (final String expression : new String[] { "date({year: 2017, week: 54})", "date({year: 2015, week: 0})",
        "date({year: 2017, week: 52, dayOfWeek: 8})", "date({year: 2017, ordinalDay: 366})", "date({year: 2017, month: 2, day: 29})",
        "date({year: 2017, quarter: 5})", "localtime({hour: 25})", "time({hour: 10, minute: 61})",
        "localdatetime({year: 2017, month: 13, day: 1})", "datetime({year: 2017, month: 2, day: 30})" })
      assertRefused(expression);
  }

  @Test
  void theLastValidValueOfEachFieldIsAccepted() {
    assertThat(date("date('2015W53')")).isEqualTo("2015-12-28");
    assertThat(date("date('2015-W53-7')")).isEqualTo("2016-01-03");
    assertThat(date("date({year: 2015, week: 53, dayOfWeek: 7})")).isEqualTo("2016-01-03");
    assertThat(date("date('2017-W52-7')")).isEqualTo("2017-12-31");
    assertThat(date("date({year: 2017, quarter: 1, dayOfQuarter: 90})")).isEqualTo("2017-03-31");
    assertThat(date("date({year: 2016, quarter: 1, dayOfQuarter: 91})")).isEqualTo("2016-03-31");
    assertThat(date("date({year: 2017, quarter: 3, dayOfQuarter: 92})")).isEqualTo("2017-09-30");
    assertThat(date("date({date: date('2017-01-15'), dayOfQuarter: 90})")).isEqualTo("2017-03-31");
    assertThat(date("date('2016-366')")).isEqualTo("2016-12-31");
  }

  private void assertRefused(final String expression) {
    assertThatThrownBy(() -> date(expression)).as(expression).isInstanceOf(CommandSemanticException.class);
  }

  private String date(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v")) {
      return rs.next().getProperty("v");
    }
  }
}
