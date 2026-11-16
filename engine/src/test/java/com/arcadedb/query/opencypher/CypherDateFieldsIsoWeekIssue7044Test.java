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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7044, a follow-up to #4554: {@code date.field(ts, 'weekofyear')} was corrected to use
 * {@code WeekFields.ISO.weekOfWeekBasedYear()}, but its sibling {@code date.fields(dateStr)} kept using
 * {@code WeekFields.ISO.weekOfYear()}, which reports {@code 0} for the partial week that precedes the first full week of
 * the calendar year.
 * <p>
 * Since issue #12106 {@code date.fields} answers APOC's keys only (years, months, weekdays, days, hours, minutes, seconds,
 * zoneid), so the ISO week is read through {@code date.field}. Both query engines reach the stateless registry, so these
 * tests pin the ISO-8601 answer on both surfaces.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class CypherDateFieldsIsoWeekIssue7044Test extends TestHelper {
  /** 2023-01-01 00:00:00 UTC, a Sunday: ISO week 52 of the week-based-year 2022, never week 0. */
  private static final long JAN_1_2023 = 1672531200000L;

  @Test
  void dateFieldReportsIsoWeekOverCypher() {
    final Result row = database.query("opencypher", "RETURN date.field($t, 'weekofyear') AS w, date.field($t, 'weekyear') AS y, "
        + "date.field($t, 'years') AS year", Map.of("t", JAN_1_2023)).next();
    assertThat(row.<Long>getProperty("w")).isEqualTo(52L);
    assertThat(row.<Long>getProperty("y")).isEqualTo(2022L);
    assertThat(row.<Long>getProperty("year")).isEqualTo(2023L);
  }

  /** The same function over SQL, since the stateless registry backs both query engines. */
  @Test
  void dateFieldReportsIsoWeekOverSql() {
    final Result row = database.query("sql", "SELECT date.field(?, 'weekofyear') AS w, date.field(?, 'weekyear') AS y", JAN_1_2023,
        JAN_1_2023).next();
    assertThat(row.<Long>getProperty("w")).isEqualTo(52L);
    assertThat(row.<Long>getProperty("y")).isEqualTo(2022L);
  }
}
