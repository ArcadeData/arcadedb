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
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10055: {@link CypherDuration#parse} built its ISO pattern with every designator optional and
 * every number as any run of digits, dots and minus signs, so a text with no component at all ({@code P}, {@code PT}), a
 * dangling {@code T} ({@code P1YT}) or a malformed number parsed. The accepted and refused shapes below are the ones Neo4j
 * 2026.09 answers {@code RETURN duration(...)} with.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10055DurationParseTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue10055");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void textWithoutAnyComponentIsRefused() {
    for (final String text : new String[] { "P", "PT", "P1YT", "P1DT", "-P", "+P", "-PT" })
      assertThatThrownBy(() -> CypherDuration.parse(text)).as(text).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void malformedNumbersAreRefusedAsMalformedNotAsOverflow() {
    // "P-Y" used to reach Long.parseLong("-") and be reported as an overflow, "P.Y" escaped as a NumberFormatException
    for (final String text : new String[] { "P-Y", "P.Y", "P.5Y", "PT1.S", "PT-.5S", "PT--1S", "PT1.2.3S", "P1-2D", "P1Y2Y" })
      assertThatThrownBy(() -> CypherDuration.parse(text)).as(text).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void onlyTheLastComponentMayCarryAFraction() {
    assertThatThrownBy(() -> CypherDuration.parse("P1.5Y2M")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> CypherDuration.parse("PT1.5H30M")).isInstanceOf(IllegalArgumentException.class);
    assertThat(CypherDuration.parse("P1.5Y").toString()).isEqualTo("P1Y6M");
    assertThat(CypherDuration.parse("P-0.5D").toString()).isEqualTo("PT-12H");
    assertThat(CypherDuration.parse("P1Y2M1.5W").toString()).isEqualTo("P1Y2M10DT12H");
  }

  @Test
  void aFractionDoesNotRoundTheWholeCountsThroughADouble() {
    // 2^53 + 1 months: a double holds 2^53, so reading the whole text through doubles lost a month
    final CypherDuration duration = CypherDuration.parse("P9007199254740993M0.5D");
    assertThat(duration.getMonths()).isEqualTo(9_007_199_254_740_993L);
    assertThat(duration.getDays()).isZero();
    assertThat(duration.getSeconds()).isEqualTo(12 * 3600L);

    // The whole part of the fractional component itself stays exact too
    assertThat(CypherDuration.parse("P9007199254740993.5D").getDays()).isEqualTo(9_007_199_254_740_993L);
    assertThat(CypherDuration.parse("P9007199254740993.5D").getSeconds()).isEqualTo(12 * 3600L);
    assertThat(CypherDuration.parse("-P9007199254740993.5D").getDays()).isEqualTo(-9_007_199_254_740_993L);
  }

  @Test
  void validTextsStillParse() {
    assertThat(CypherDuration.parse("PT0S").toString()).isEqualTo("PT0S");
    assertThat(CypherDuration.parse("P1Y").toString()).isEqualTo("P1Y");
    assertThat(CypherDuration.parse("P-1Y").toString()).isEqualTo("P-1Y");
    assertThat(CypherDuration.parse("P1Y2M3W4DT5H6M7.5S").toString()).isEqualTo("P1Y2M25DT5H6M7.5S");
    assertThat(CypherDuration.parse("P0001-00-00T00:00:00").toString()).isEqualTo("P1Y");
  }

  @Test
  void signsAndDecimalCommaAreReadAsNeo4jReadsThem() {
    assertThat(CypherDuration.parse("+P1Y").toString()).isEqualTo("P1Y");
    assertThat(CypherDuration.parse("PT+1S").toString()).isEqualTo("PT1S");
    assertThat(CypherDuration.parse("PT1,5S").toString()).isEqualTo("PT1.5S");
    // A leading minus negates the whole duration, components included
    assertThat(CypherDuration.parse("-P1Y2DT3S")).isEqualTo(CypherDuration.parse("P-1Y-2DT-3S"));
    assertThat(CypherDuration.parse("-PT0.5S").toString()).isEqualTo("PT-0.5S");
    assertThat(CypherDuration.parse("-P-1D").toString()).isEqualTo("P1D");
    // A signed seconds component with a fraction takes the exact decimal path
    assertThat(CypherDuration.parse("PT+1.5S").toString()).isEqualTo("PT1.5S");
    assertThat(CypherDuration.parse("PT-0.5S").toString()).isEqualTo("PT-0.5S");
    assertThat(CypherDuration.parse("-P1Y2DT3.5S")).isEqualTo(CypherDuration.parse("P-1Y-2DT-3.5S"));
    assertThat(CypherDuration.parse("-P1Y2DT3.5S").toString()).isEqualTo("P-1Y-2DT-3.5S");
    // The negated nanoseconds fold back into the seconds
    assertThat(CypherDuration.parse("-P1DT0.000000001S")).isEqualTo(CypherDuration.parse("P-1DT-0.000000001S"));
    assertThat(CypherDuration.parse("-P1DT0.000000001S").getNanosAdjustment()).isEqualTo(999_999_999);
    assertThat(CypherDuration.parse("-P1DT0.000000001S").getSeconds()).isEqualTo(-1L);
  }

  @Test
  void cypherDurationFunctionRefusesTheEmptyShapes() {
    for (final String text : new String[] { "P", "PT", "P1YT" })
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("opencypher", "RETURN duration('" + text + "') AS r")) {
          rs.next();
        }
      }).as(text).isInstanceOf(RuntimeException.class);

    try (final ResultSet rs = database.query("opencypher", "RETURN duration('+P1Y') AS r")) {
      assertThat(rs.next().<Object>getProperty("r").toString()).isEqualTo("P1Y");
    }
  }

  @Test
  void aLegacyTextThatIsNotADurationStaysText() {
    // A legacy stored text is read back as a duration only when it is one: "P" or "PT" is a string
    assertThat(TemporalUtil.parseLegacyText("PT")).isEqualTo("PT");
    assertThat(TemporalUtil.parseLegacyText("P1YT")).isEqualTo("P1YT");
    assertThat(TemporalUtil.parseLegacyText("P1Y")).isInstanceOf(CypherDuration.class);
  }
}
