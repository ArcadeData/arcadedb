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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8571: the temporal constructors and the {@code *.truncate()} functions combined their
 * {@code millisecond}/{@code microsecond}/{@code nanosecond} fields in 32-bit int arithmetic, so a total at or beyond
 * 2.147483648s wrapped silently into a different, plausible fraction ({@code time({hour: 12, millisecond: 5000})}
 * answered {@code 12:00:00.705032704Z}) while the smaller {@code millisecond: 2000} was refused. Each component is now
 * range-checked the way Neo4j checks it, and the other components ({@code hour}, {@code year}, ...) no longer wrap
 * through {@code intValue()} either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8571TemporalSubSecondOverflowTest extends TestHelper {

  @ParameterizedTest
  @ValueSource(strings = {
      "time({hour: 12, millisecond: 5000})",
      "time({hour: 12, millisecond: 4295})",
      "time({hour: 12, millisecond: 2000})",
      "time({hour: 12, microsecond: 5000000})",
      "localtime({hour: 12, microsecond: 5000000})",
      "localtime({hour: 12, nanosecond: 3000000000})",
      "localdatetime({year: 2024, month: 1, day: 1, hour: 0, millisecond: 5000})",
      "datetime({year: 2024, month: 1, day: 1, nanosecond: 3000000000})",
      "datetime.truncate('second', datetime({year: 2024, month: 1, day: 1}), {millisecond: 5000})",
      "localdatetime.truncate('second', localdatetime({year: 2024, month: 1, day: 1}), {microsecond: 5000000})",
      "time.truncate('second', time({hour: 12}), {millisecond: 5000})",
      "localtime.truncate('second', localtime({hour: 12}), {nanosecond: 3000000000})",
      // Neo4j's per-component ranges: a finer component cannot carry into a coarser one that is also given
      "time({hour: 12, millisecond: 1, microsecond: 1000})",
      "time({hour: 12, microsecond: 1, nanosecond: 1000})",
      "time({hour: 12, millisecond: 1, nanosecond: 1000000})",
      "time({hour: 12, millisecond: -1})",
      // The other components wrapped through intValue() the same way: 4294967308 = 2^32 + 12
      "time({hour: 4294967308})",
      "localtime({hour: 4294967308})",
      "date({year: 4294969320, month: 1, day: 1})",
      "localdatetime({year: 2024, month: 1, day: 1, hour: 4294967308})" })
  void outOfRangeSubSecondFieldIsRefusedInsteadOfWrapping(final String expression) {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v")) {
        rs.next().getProperty("v");
      }
    }).as(expression).hasStackTraceContaining("Invalid value for");
  }

  @Test
  void inRangeSubSecondFieldsStillCombine() {
    assertThat(cypher("time({hour: 12, millisecond: 999})")).isEqualTo("12:00:00.999Z");
    assertThat(cypher("time({hour: 12, millisecond: 645, microsecond: 876, nanosecond: 123})")).isEqualTo("12:00:00.645876123Z");
    assertThat(cypher("localtime({hour: 12, microsecond: 999999})")).isEqualTo("12:00:00.999999");
    assertThat(cypher("localtime({hour: 12, nanosecond: 999999999})")).isEqualTo("12:00:00.999999999");
    assertThat(cypher("localtime({hour: 12, millisecond: 1, nanosecond: 999999})")).isEqualTo("12:00:00.001999999");
    assertThat(cypher("datetime({year: 2024, month: 1, day: 1, microsecond: 5})")).isEqualTo("2024-01-01T00:00:00.000005Z");
    // truncate keeps the portion the adjustment map does not name
    assertThat(cypher("localtime.truncate('millisecond', localtime('12:00:00.123456789'), {nanosecond: 2})"))
        .isEqualTo("12:00:00.123000002");
  }

  private String cypher(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v")) {
      return rs.next().getProperty("v");
    }
  }
}
