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

import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7912: {@code CreateContinuousAggregateStatement} overrode only the no-arg
 * {@code toString()}, had no {@code copy()} and no identity elements. Each of those three gaps was closed on a
 * NEIGHBOURING statement by an already-merged fix - {@code CREATE TRIGGER} (#7794), {@code DROP}/{@code REFRESH
 * CONTINUOUS AGGREGATE} (#7800 item 3) - and the CREATE arm was missed only because the corpus behind those
 * reports had no {@code CREATE CONTINUOUS AGGREGATE} in it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7912CreateContinuousAggregateStatementTest extends AbstractParserTest {

  @Test
  void reRendersAsSqlNotAsJavaDebugDump() {
    final Statement result = (Statement) checkRightSyntax(
        "CREATE CONTINUOUS AGGREGATE ca1 AS SELECT count(*) FROM Sensor");

    assertThat(result.toString()).isEqualTo("CREATE CONTINUOUS AGGREGATE ca1 AS SELECT count(*) FROM Sensor");
  }

  @Test
  void reRendersIfNotExists() {
    final Statement result = (Statement) checkRightSyntax(
        "CREATE CONTINUOUS AGGREGATE IF NOT EXISTS ca2 AS SELECT count(*) FROM Sensor");

    assertThat(result.toString()).isEqualTo("CREATE CONTINUOUS AGGREGATE IF NOT EXISTS ca2 AS SELECT count(*) FROM Sensor");
  }

  /**
   * The failure the issue was filed for: an {@code IF} block renders its inner statements through
   * {@code toString(Map, StringBuilder)}, and the inherited one throws.
   */
  @Test
  void rendersInsideIfBlockWithoutThrowing() {
    checkRightSyntax("IF (1 = 1) { CREATE CONTINUOUS AGGREGATE ca3 AS SELECT count(*) FROM Sensor; }");
  }

  @Test
  void rendersInsideAScriptWithoutThrowing() {
    final List<Statement> script = new SQLAntlrParser(null)
        .parseScript("BEGIN;\nCREATE CONTINUOUS AGGREGATE ca4 AS SELECT count(*) FROM Sensor;\nCOMMIT;");

    final StringBuilder builder = new StringBuilder();
    for (final Statement statement : script)
      statement.toString(null, builder.append('\n'));

    assertThat(builder.toString()).contains("CREATE CONTINUOUS AGGREGATE ca4 AS SELECT count(*) FROM Sensor");
  }

  @Test
  void copyPreservesEveryField() {
    final CreateContinuousAggregateStatement stmt = (CreateContinuousAggregateStatement) new SQLAntlrParser(null)
        .parse("CREATE CONTINUOUS AGGREGATE IF NOT EXISTS ca5 AS SELECT count(*) FROM Sensor");
    final CreateContinuousAggregateStatement copy = stmt.copy();

    assertThat(copy.ifNotExists).isTrue();
    assertThat(copy.name.getStringValue()).isEqualTo("ca5");
    assertThat(copy.toString()).isEqualTo(stmt.toString());
    assertThat(copy).isEqualTo(stmt);
    assertThat(copy.hashCode()).isEqualTo(stmt.hashCode());
  }

  /**
   * Two statements parsed from the identical text must compare equal, or the statement cache treats every
   * occurrence as a new entry.
   */
  @Test
  void identicalStatementsCompareEqual() {
    final String sql = "CREATE CONTINUOUS AGGREGATE ca6 AS SELECT count(*) FROM Sensor";
    final Statement a = new SQLAntlrParser(null).parse(sql);
    final Statement b = new SQLAntlrParser(null).parse(sql);

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  @Test
  void differentNamesDoNotCompareEqual() {
    final Statement a = new SQLAntlrParser(null).parse("CREATE CONTINUOUS AGGREGATE caA AS SELECT count(*) FROM Sensor");
    final Statement b = new SQLAntlrParser(null).parse("CREATE CONTINUOUS AGGREGATE caB AS SELECT count(*) FROM Sensor");

    assertThat(a).isNotEqualTo(b);
  }

  /**
   * The secondary point of the issue: the no-arg {@code toString()} ignored the params map, so a parameterised
   * sub-select re-rendered its placeholder raw instead of bound.
   */
  @Test
  void bindsParametersOfTheSubSelect() {
    final CreateContinuousAggregateStatement stmt = (CreateContinuousAggregateStatement) new SQLAntlrParser(null)
        .parse("CREATE CONTINUOUS AGGREGATE ca7 AS SELECT count(*) FROM Sensor WHERE value > :threshold");

    final StringBuilder builder = new StringBuilder();
    stmt.toString(Map.of("threshold", 10), builder);

    assertThat(builder.toString()).contains("10").doesNotContain(":threshold");
  }
}
