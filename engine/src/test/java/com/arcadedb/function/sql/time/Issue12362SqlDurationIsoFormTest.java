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
package com.arcadedb.function.sql.time;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12362: the SQL {@code duration()} refused the one-argument form openCypher accepts, so {@code ORDER BY dt + duration('PT1H')}
 * answered an arity error. It now reads an ISO-8601 text or a map of components with the openCypher parser, into the same exact
 * {@link Duration} the two-argument form builds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12362SqlDurationIsoFormTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Ev");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Ev SET name = 'a', dt = ?", LocalDateTime.of(2025, 1, 1, 0, 0));
      database.command("sql", "INSERT INTO Ev SET name = 'b', dt = ?", LocalDateTime.of(2025, 1, 2, 0, 0));
    });
  }

  @Test
  void orderByADatetimePlusAnIsoDurationSorts() {
    assertThat(names("SELECT name FROM Ev ORDER BY dt + duration('PT1H')")).containsExactly("a", "b");
    assertThat(names("SELECT name FROM Ev ORDER BY dt + duration('PT1H') DESC")).containsExactly("b", "a");
    assertThat(names("SELECT name, dt + duration('PT1H') AS x FROM Ev ORDER BY x DESC")).containsExactly("b", "a");
    assertThat(names("SELECT name FROM Ev ORDER BY duration('PT1H') + dt DESC")).containsExactly("b", "a");
  }

  @Test
  void theProjectedValueIsTheDatetimeMovedByTheDuration() {
    try (final ResultSet rs = database.query("sql", "SELECT dt + duration('PT1H30M') AS x FROM Ev WHERE name = 'a'")) {
      assertThat(rs.next().<Object>getProperty("x")).isEqualTo(LocalDateTime.of(2025, 1, 1, 1, 30));
    }
  }

  @Test
  void theOneArgumentFormsBuildTheSameDurationAsTheTwoArgumentForm() {
    assertThat(duration("duration('PT1H')")).isEqualTo(duration("duration(1, 'hour')"));
    assertThat(duration("duration('PT1H30M')")).isEqualTo(Duration.ofMinutes(90));
    assertThat(duration("duration('P2DT3H')")).isEqualTo(Duration.ofDays(2).plusHours(3));
    assertThat(duration("duration('P1W')")).isEqualTo(Duration.ofDays(7));
    assertThat(duration("duration('PT0.5S')")).isEqualTo(Duration.ofMillis(500));
    assertThat(duration("duration('-PT2M')")).isEqualTo(Duration.ofMinutes(-2));
    assertThat(duration("duration({hours: 1, minutes: 30})")).isEqualTo(Duration.ofMinutes(90));
    assertThat(duration("duration({days: 4, minutes: 6})")).isEqualTo(Duration.ofDays(4).plusMinutes(6));
  }

  @Test
  void aParameterCarriesTheIsoText() {
    try (final ResultSet rs = database.query("sql", "SELECT duration(:d) AS d", Map.of("d", "PT45S"))) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo(Duration.ofSeconds(45));
    }
  }

  @Test
  void nullIsNull() {
    try (final ResultSet rs = database.query("sql", "SELECT duration(null) AS d")) {
      assertThat(rs.next().<Object>getProperty("d")).isNull();
    }
  }

  @Test
  void calendarComponentsAreRefusedAsTheTwoArgumentFormRefusesTheirUnits() {
    for (final String argument : new String[] { "'P1M'", "'P1Y'", "{months: 2}" })
      assertThatThrownBy(() -> duration("duration(" + argument + ")"))
          .isInstanceOf(InvalidArgumentException.class)
          .hasMessageContaining("years and months have no fixed length");
  }

  @Test
  void anAmountPastTheRangeOfADurationIsAClearArgumentError() {
    assertThatThrownBy(() -> duration("duration({days: 200000000000000})"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("overflows the range of a duration");
  }

  @Test
  void aTextThatIsNotADurationIsAClearArgumentError() {
    assertThatThrownBy(() -> duration("duration('one hour')"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("cannot read 'one hour' as a duration")
        .hasMessageContaining("ISO-8601");
  }

  @Test
  void aWrongNumberOfArgumentsNamesTheAcceptedForms() {
    // issue #11944: the count is the declared 1..2 arity, refused by the call-site gate in the uniform wording
    assertThatThrownBy(() -> duration("duration(1, 'hour', 2)"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expects 1-2 arguments but got 3");
  }

  private Duration duration(final String expression) {
    try (final ResultSet rs = database.query("sql", "SELECT " + expression + " AS d")) {
      return rs.next().getProperty("d");
    }
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        names.add(row.getProperty("name"));
      }
    }
    return names;
  }
}
