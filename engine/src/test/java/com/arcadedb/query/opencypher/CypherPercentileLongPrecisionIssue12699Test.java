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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12699: {@code percentileDisc()} and {@code percentileCont()} sorted their values as doubles, which holds no integer past 2^53,
 * so LONG values that differ in their low digits stayed in input order and {@code percentileDisc()} answered the wrong element.
 */
class CypherPercentileLongPrecisionIssue12699Test extends TestHelper {
  private static final String DESCENDING = "[1500000000000000003, 1500000000000000002, 1500000000000000001]";
  private static final String SHUFFLED = "[1500000000000000003, 1500000000000000001, 1500000000000000002]";

  @Test
  void percentileDiscOfLongsPastTheDoublePrecision() {
    for (final String input : new String[] { DESCENDING, SHUFFLED }) {
      assertThat(single("UNWIND " + input + " AS x RETURN percentileDisc(x, 0.0) AS v")).isEqualTo(1500000000000000001L);
      assertThat(single("UNWIND " + input + " AS x RETURN percentileDisc(x, 0.5) AS v")).isEqualTo(1500000000000000002L);
      assertThat(single("UNWIND " + input + " AS x RETURN percentileDisc(x, 1.0) AS v")).isEqualTo(1500000000000000003L);
    }
  }

  @Test
  void percentileDiscOfALongAndAFloatThatDoubleCannotTellApart() {
    assertThat(single("UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN percentileDisc(x, 0.0) AS v")).isEqualTo(9007199254740992.0);
    assertThat(single("UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN percentileDisc(x, 1.0) AS v")).isEqualTo(9007199254740993L);
  }

  @Test
  void percentilesOfSmallNumbersAreUnchanged() {
    assertThat(single("UNWIND [4, 1, 3, 2] AS x RETURN percentileDisc(x, 0.5) AS v")).isEqualTo(2L);
    assertThat(single("UNWIND [4, 1, 3, 2] AS x RETURN percentileDisc(x, 1.0) AS v")).isEqualTo(4L);
    assertThat(single("UNWIND [4, 1, 3, 2] AS x RETURN percentileCont(x, 0.5) AS v")).isEqualTo(2.5);
    assertThat(single("UNWIND [4, 1.5, 3, 2] AS x RETURN percentileCont(x, 0.0) AS v")).isEqualTo(1.5);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }
}
