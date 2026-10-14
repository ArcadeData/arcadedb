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
package com.arcadedb.query.opencypher.procedures;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8124: every procedure reads an optional trailing argument through the helpers of {@code Procedure}
 * ({@code argOrDefault}, {@code optionalMapArg}, {@code optionalNumberArg}) instead of a private helper per procedure
 * family, under one rule: an absent argument and an explicit {@code null} both resolve to the declared default.
 * <p>
 * The private helpers disagreed on that rule, and the {@code args.length > n ? f(args[n]) : default} reads most procedures
 * used had no rule at all: an explicit {@code null} reached an unboxing or a {@code toString()} as a
 * {@code NullPointerException}, or resolved to a default other than the declared one, and a value of the wrong type was
 * either a bare {@code ClassCastException} or silently replaced by the default.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8124ProcedureOptionalArgumentsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Node");
    database.command("sql", "CREATE EDGE TYPE R");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Node SET name = 'A'");
      database.command("sql", "INSERT INTO Node SET name = 'B'");
      database.command("sql", "INSERT INTO Node SET name = 'C'");
      database.command("sql", "CREATE EDGE R FROM (SELECT FROM Node WHERE name = 'A') TO (SELECT FROM Node WHERE name = 'B') SET w = 1");
      database.command("sql", "CREATE EDGE R FROM (SELECT FROM Node WHERE name = 'B') TO (SELECT FROM Node WHERE name = 'C') SET w = 1");
    });
  }

  @Test
  void helpersResolveAnAbsentOrNullArgumentToTheDefault() {
    final CypherProcedure procedure = CypherProcedureRegistry.get("merge.node");

    assertThat(procedure.argOrDefault(new Object[] { "a" }, 1, "dflt")).isEqualTo("dflt");
    assertThat(procedure.argOrDefault(new Object[] { "a", null }, 1, "dflt")).isEqualTo("dflt");
    assertThat(procedure.argOrDefault(new Object[] { "a", "given" }, 1, "dflt")).isEqualTo("given");

    assertThat(procedure.optionalMapArg(new Object[] { "a" }, 1, "props")).isEqualTo(Collections.emptyMap());
    assertThat(procedure.optionalMapArg(new Object[] { "a", null }, 1, "props")).isEqualTo(Collections.emptyMap());
    assertThat(procedure.optionalMapArg(new Object[] { "a", Map.of("k", 1) }, 1, "props")).isEqualTo(Map.of("k", 1));

    assertThat(procedure.optionalNumberArg(new Object[] { "a" }, 1, "n", 7)).isEqualTo(7);
    assertThat(procedure.optionalNumberArg(new Object[] { "a", null }, 1, "n", 7)).isEqualTo(7);
    assertThat(procedure.optionalNumberArg(new Object[] { "a", 3L }, 1, "n", 7)).isEqualTo(3L);
    // a null default stays null rather than failing on the absent value
    assertThat(procedure.optionalNumberArg(new Object[] { "a" }, 1, "n", null)).isNull();
    assertThat(procedure.optionalNumberArg(new Object[] { "a", null }, 1, "n", null)).isNull();
  }

  @Test
  void helpersRefuseAWrongTypeNamingTheParameter() {
    final CypherProcedure procedure = CypherProcedureRegistry.get("merge.node");

    assertThatThrownBy(() -> procedure.optionalMapArg(new Object[] { "a", "x" }, 1, "onCreateProps"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("merge.node(): onCreateProps must be a map, got String");
    assertThatThrownBy(() -> procedure.optionalNumberArg(new Object[] { "a", "x" }, 1, "limit", 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("merge.node(): limit must be a number, got String");
  }

  @Test
  void anExplicitNullInADefaultedSlotIsTheDefault() {
    // Each of these raised a NullPointerException: Boolean.parseBoolean(null.toString()), ((Number) null).doubleValue(),
    // NumberUtils.saturateToInt(null), ((Number) null).longValue()
    assertThat(rows("CALL algo.closeness('R', 'BOTH', null) YIELD node, score RETURN node, score")).hasSize(3);
    assertThat(rows("CALL algo.harmonic('R', 'BOTH', null) YIELD node, score RETURN node, score")).hasSize(3);
    assertThat(rows("MATCH (a:Node {name: 'A'}) CALL algo.bfs(a, 'R', 'OUT', null) YIELD node RETURN node")).hasSize(2);
    assertThat(rows("MATCH (a:Node {name: 'A'}) CALL algo.randomWalk(a, 2, 'R', 'OUT', null) YIELD path RETURN path")).hasSize(1);

    // merge.node, do.when and refactor.mergeNodes: explicit null maps, which their private helpers resolved differently
    assertThat(rows("CALL merge.node(['Node'], {name: 'A'}, null, null) YIELD node RETURN node")).hasSize(1);
    assertThat(rows("CALL do.when(true, 'RETURN 1 AS x', '', null) YIELD value RETURN value")).hasSize(1);
  }

  @Test
  void anExplicitNullDirectionIsTheDeclaredDefaultNotBoth() {
    // algo.dijkstra.singleSource declares OUT: an explicit null used to read as BOTH, reaching A backwards from B
    final List<Result> fromB = rows("MATCH (b:Node {name: 'B'}) CALL algo.dijkstra.singleSource(b, 'R', 'w', null) "
        + "YIELD node RETURN node.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    for (final Result r : fromB)
      names.add(r.getProperty("name"));
    assertThat(names).containsExactly("C");
  }

  @Test
  void aNumberOfTheWrongTypeIsRefusedNotReplacedByTheDefault() {
    // algo.leiden read 'ten' as "no value" and ran with the default 10 iterations; algo.simRank raised ClassCastException
    assertThatThrownBy(() -> rows("CALL algo.leiden('R', 'ten') YIELD node RETURN node"))
        .hasStackTraceContaining("algo.leiden(): maxIterations must be a number, got String");
    assertThatThrownBy(() -> rows("MATCH (a:Node {name: 'A'}) CALL algo.randomWalk(a, 2, 'R', 'OUT', 'seed') YIELD path RETURN path"))
        .hasStackTraceContaining("algo.randomWalk(): seed must be a number, got String");
  }

  private List<Result> rows(final String query) {
    // command, not query: merge.node writes
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
    });
    return rows;
  }
}
