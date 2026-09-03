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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7044, a follow-up to #4554: {@code date.field(ts, 'weekofyear')} was corrected to use
 * {@code WeekFields.ISO.weekOfWeekBasedYear()}, but its sibling {@code date.fields(dateStr)} - registered
 * on the very next line of {@code CypherFunctionRegistry} - kept using {@code WeekFields.ISO.weekOfYear()},
 * which reports {@code 0} for the partial week that precedes the first full week of the calendar year.
 * The two functions therefore disagreed about the same instant under the same key name.
 * <p>
 * Both functions are stateless registry entries, so they are reachable from openCypher <i>and</i> from SQL;
 * these tests pin the ISO-8601 answer on both surfaces.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class CypherDateFieldsIsoWeekIssue7044Test extends TestHelper {

  /** 2023-01-01 is a Sunday: ISO week 52 of the week-based-year 2022, never week 0. */
  @Test
  void dateFieldsReportsIsoWeekOverCypher() {
    final ResultSet rs = database.query("opencypher", "RETURN date.fields('2023-01-01T00:00:00') AS f");
    assertThat(rs.hasNext()).isTrue();

    final Map<String, Object> fields = rs.next().getProperty("f");
    assertThat(fields).containsEntry("weekOfYear", 52L);
    assertThat(fields).containsEntry("weekBasedYear", 2022L);
    assertThat(fields).containsEntry("year", 2023L);
    assertThat(fields).containsEntry("dayOfYear", 1L);
  }

  /** The same function over SQL, since the stateless registry backs both query engines. */
  @Test
  void dateFieldsReportsIsoWeekOverSql() {
    final ResultSet rs = database.query("sql", "SELECT date.fields('2023-01-01T00:00:00') AS f");
    assertThat(rs.hasNext()).isTrue();

    final Map<String, Object> fields = rs.next().getProperty("f");
    assertThat(fields).containsEntry("weekOfYear", 52L);
    assertThat(fields).containsEntry("weekBasedYear", 2022L);
  }

}
