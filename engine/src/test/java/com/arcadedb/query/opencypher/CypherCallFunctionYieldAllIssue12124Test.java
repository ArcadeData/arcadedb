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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12124: {@code CALL <SQL function used as a procedure>(...) YIELD * RETURN *} was refused at parse time with
 * {@code NoVariablesInScope}, because a function-backed call declares no output columns before it runs. Its columns are
 * known only from the rows it answers, so {@code YIELD *} must bind them at run time, as the same call without a YIELD does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCallFunctionYieldAllIssue12124Test extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE D");
      database.command("sql", "CREATE PROPERTY D.e ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON D (e) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      database.command("sql", "INSERT INTO D SET name = 'a', e = [1.0, 0.0, 0.0]");
      database.command("sql", "INSERT INTO D SET name = 'b', e = [0.0, 1.0, 0.0]");
    });
  }

  @Test
  void yieldAllReturnAllOfAFunctionBackedCallReturnsItsColumns() {
    final List<Result> rows = rows("CALL vector.neighbors('D[e]', [1.0, 0.1, 0.0], 2) YIELD * RETURN *");
    assertThat(rows).hasSize(2);
    assertThat((String) rows.getFirst().getProperty("name")).isEqualTo("a");
    assertThat(rows.getFirst().getPropertyNames()).contains("name", "distance");
    assertThat((Object) rows.getFirst().getProperty("distance")).isNotNull();
  }

  @Test
  void yieldAllAnswersTheSameColumnsAsNoYield() {
    final List<Result> withoutYield = rows("CALL vector.neighbors('D[e]', [1.0, 0.1, 0.0], 2)");
    final List<Result> yieldAll = rows("CALL vector.neighbors('D[e]', [1.0, 0.1, 0.0], 2) YIELD * RETURN *");
    assertThat(yieldAll).hasSize(withoutYield.size());
    for (int i = 0; i < yieldAll.size(); i++) {
      assertThat(yieldAll.get(i).getPropertyNames()).containsExactlyInAnyOrderElementsOf(withoutYield.get(i).getPropertyNames());
      assertThat((Object) yieldAll.get(i).getProperty("name")).isEqualTo(withoutYield.get(i).getProperty("name"));
    }
  }

  @Test
  void yieldAllThenANamedColumn() {
    final List<Result> rows = rows("CALL vector.neighbors('D[e]', [1.0, 0.1, 0.0], 2) YIELD * RETURN name, distance ORDER BY distance");
    assertThat(rows).hasSize(2);
    assertThat((String) rows.getFirst().getProperty("name")).isEqualTo("a");
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("b");
  }

  @Test
  void yieldAllThroughWithStar() {
    final List<Result> rows = rows("CALL vector.neighbors('D[e]', [1.0, 0.1, 0.0], 2) YIELD * WITH * WHERE name = 'b' RETURN *");
    assertThat(rows).hasSize(1);
    assertThat((String) rows.getFirst().getProperty("name")).isEqualTo("b");
  }

  @Test
  void procedureYieldAllStillWorks() {
    final List<Result> rows = rows("CALL db.index.vector.queryNodes('D[e]', 2, [1.0, 0.1, 0.0]) YIELD * RETURN *");
    assertThat(rows).hasSize(2);
    assertThat(rows.getFirst().getPropertyNames()).containsExactlyInAnyOrder("node", "score");
  }

  @Test
  void aDefinedFunctionYieldsItsColumnsToo() {
    database.command("sql", "DEFINE FUNCTION math.double \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");
    try (final ResultSet rs = database.command("opencypher", "CALL math.double(4) YIELD * RETURN *")) {
      assertThat(rs.next().<Number>getProperty("value").intValue()).isEqualTo(8);
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.command("opencypher", "CALL math.double(4) YIELD * RETURN value * 10 AS v")) {
      assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(80);
    }
  }

  @Test
  void aColumnNoRowCarriesReadsAsNull() {
    final List<Result> rows = rows("CALL vector.neighbors('D[e]', [1.0, 0.1, 0.0], 2) YIELD * RETURN name, missing");
    assertThat(rows).hasSize(2);
    assertThat((Object) rows.getFirst().getProperty("missing")).isNull();
  }

  @Test
  void aProjectionAfterYieldAllClosesTheScopeAgain() {
    assertThatThrownBy(() -> rows("CALL vector.neighbors('D[e]', [1.0, 0.1, 0.0], 2) YIELD * WITH name RETURN distance"))
        .hasMessageContaining("UndefinedVariable");
  }

  @Test
  void returnStarWithNothingInScopeIsStillRefused() {
    assertThatThrownBy(() -> rows("RETURN *")).isInstanceOf(CommandParsingException.class).hasMessageContaining("NoVariablesInScope");
  }

  private List<Result> rows(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", cypher)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }
}
