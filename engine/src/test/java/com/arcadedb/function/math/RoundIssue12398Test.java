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
package com.arcadedb.function.math;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * round() with an explicit precision and an explicit rounding mode, as Neo4j answers them (issue #12398).
 * <p>
 * Two things were wrong: {@code round(x, 0)} rounded a negative tie away from zero while {@code round(x)} rounded
 * it toward positive infinity, so the two forms of the same function disagreed on -0.5, -1.5, -2.5 and so on; and
 * the rounding mode {@code UNNECESSARY}, which Neo4j accepts, was refused as an unknown mode.
 */
class RoundIssue12398Test {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final DatabaseFactory factory = new DatabaseFactory(
        "./target/databases/testIssue12398-" + testInfo.getTestMethod().get().getName());
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * A value rounded to a whole number ties toward positive infinity, the way {@code round(x)} already did: the two
   * forms of the function must not disagree on the same value.
   */
  @Test
  void precisionZeroTiesTowardPositiveInfinity() {
    assertThat(round("RETURN round(-2.5, 0) AS v")).isEqualTo(-2.0);
    assertThat(round("RETURN round(-0.5, 0) AS v")).isEqualTo(0.0);
    assertThat(round("RETURN round(-1.5, 0) AS v")).isEqualTo(-1.0);
    assertThat(round("RETURN round(-12.5, 0) AS v")).isEqualTo(-12.0);
    assertThat(round("RETURN round(2.5, 0) AS v")).isEqualTo(3.0);
    assertThat(round("RETURN round(3.5, 0) AS v")).isEqualTo(4.0);
  }

  /** The single-argument form keeps its own answer: the control of the test above. */
  @Test
  void noPrecisionStillTiesTowardPositiveInfinity() {
    assertThat(round("RETURN round(-2.5) AS v")).isEqualTo(-2.0);
    assertThat(round("RETURN round(-0.5) AS v")).isEqualTo(0.0);
    assertThat(round("RETURN round(2.5) AS v")).isEqualTo(3.0);
  }

  /** A value rounded to one or more decimals ties away from zero, as it always did and as Neo4j does. */
  @Test
  void higherPrecisionTiesAwayFromZero() {
    assertThat(round("RETURN round(-2.25, 1) AS v")).isEqualTo(-2.3);
    assertThat(round("RETURN round(-2.05, 1) AS v")).isEqualTo(-2.1);
    assertThat(round("RETURN round(-0.05, 1) AS v")).isEqualTo(-0.1);
    assertThat(round("RETURN round(0.25, 1) AS v")).isEqualTo(0.3);
    assertThat(round("RETURN round(-2.675, 2) AS v")).isEqualTo(-2.68);
  }

  /** An explicitly written mode is the caller's choice and wins over the default. */
  @Test
  void explicitModeWinsOverTheDefault() {
    assertThat(round("RETURN round(-2.5, 0, 'HALF_UP') AS v")).isEqualTo(-3.0);
    assertThat(round("RETURN round(-2.5, 0, 'HALF_DOWN') AS v")).isEqualTo(-2.0);
    assertThat(round("RETURN round(-2.25, 1, 'HALF_UP') AS v")).isEqualTo(-2.3);
    assertThat(round("RETURN round(-2.25, 1, 'FLOOR') AS v")).isEqualTo(-2.3);
    assertThat(round("RETURN round(-2.25, 1, 'CEILING') AS v")).isEqualTo(-2.2);
  }

  /** UNNECESSARY answers the value when no rounding is needed. */
  @Test
  void unnecessaryModeAnswersWhenNoRoundingIsNeeded() {
    assertThat(round("RETURN round(1.0, 0, 'UNNECESSARY') AS v")).isEqualTo(1.0);
    assertThat(round("RETURN round(2.5, 1, 'UNNECESSARY') AS v")).isEqualTo(2.5);
  }

  /** UNNECESSARY refuses a value that would have to be rounded, the way Neo4j refuses it. */
  @Test
  void unnecessaryModeRefusesAValueThatWouldBeRounded() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN round(1.5, 0, 'UNNECESSARY') AS v").next())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Rounding necessary");
  }

  /** An unknown mode is still refused, and the message still names the modes that are accepted. */
  @Test
  void unknownModeIsStillRefused() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN round(1.5, 0, 'SIDEWAYS') AS v").next())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("unknown rounding mode");
  }

  private double round(final String query) {
    final ResultSet rs = database.query("cypher", query);
    assertThat(rs.hasNext()).as("no row for: %s", query).isTrue();
    return ((Number) rs.next().getProperty("v")).doubleValue();
  }
}
