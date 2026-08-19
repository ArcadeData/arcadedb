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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.LongRangeList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6403: the {@code coll.*} namespace and the Cypher list builtins answer the same question about their
 * argument and had drifted apart on every part of the answer.
 * <p>
 * The cause was one method - {@code AbstractCollFunction.asList} - which accepted only a {@code Collection},
 * where the builtins go through {@code CypherFunctionHelper.requireListArgument}. Three consequences, all
 * asserted here against the builtin that is the family's counterpart, so the two are held to the same answer
 * rather than each to a remembered one:
 * <ol>
 *   <li>a numeric-array parameter is a Cypher LIST (issue #4284) and reached {@code reverse()} but not
 *   {@code coll.sort()};</li>
 *   <li>a type error is the caller's mistake, so it is a {@link CommandSemanticException} and HTTP 400 - the
 *   {@code CommandExecutionException} the family raised is mapped to 500, and the same mistake by the same
 *   client read as a server fault through one name and as their own through the other (issues #5476/#5477/#5222);</li>
 *   <li>{@code coll.toSet} and {@code coll.distinct} were two copies of one behaviour, which is two places to
 *   fix - the arrangement issue #6354 exists to remove.</li>
 * </ol>
 * Also covered: {@code coll.min}/{@code max}/{@code sum}/{@code avg} answer a lazily evaluated range from its
 * shape instead of walking it, which is the same "answer exactly where the answer is exact" principle as
 * issue #6353.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCollFunctionParityIssue6403Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-coll-parity-6403");
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

  // ---------------------------------------------------------------------------------------------------------
  // 1. An array parameter is a LIST for the whole family, not only for the builtins
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aPrimitiveArrayParameterIsAListForCollFunctionsToo() {
    final Map<String, Object> params = Map.of("arr", new long[] { 3L, 1L, 2L });

    // The builtins already accepted it (issue #4284) - asserted alongside so the two stay the same answer.
    assertThat(list("RETURN reverse($arr) AS r", params)).containsExactly(2L, 1L, 3L);
    assertThat(list("RETURN tail($arr) AS r", params)).containsExactly(1L, 2L);

    assertThat(list("RETURN coll.sort($arr) AS r", params)).containsExactly(1L, 2L, 3L);
    assertThat(list("RETURN coll.distinct($arr) AS r", params)).containsExactly(3L, 1L, 2L);
    assertThat(list("RETURN coll.union($arr, $arr) AS r", params)).containsExactly(3L, 1L, 2L);
    assertThat(list("RETURN coll.flatten($arr) AS r", params)).containsExactly(3L, 1L, 2L);
    assertThat(single("RETURN coll.indexOf($arr, 1) AS r", params)).isEqualTo(1L);
    assertThat(single("RETURN coll.max($arr) AS r", params)).isEqualTo(3L);
    assertThat(single("RETURN coll.sum($arr) AS r", params)).isEqualTo(6.0);
  }

  @Test
  void anObjectArrayParameterIsAListForCollFunctionsToo() {
    final Map<String, Object> params = Map.of("arr", new Object[] { "b", "a", "b" });

    assertThat(list("RETURN coll.distinct($arr) AS r", params)).containsExactly("b", "a");
    assertThat(list("RETURN coll.toSet($arr) AS r", params)).containsExactly("b", "a");
    assertThat(list("RETURN coll.sort($arr) AS r", params)).containsExactly("a", "b", "b");
  }

  // ---------------------------------------------------------------------------------------------------------
  // 2. A type error is the caller's mistake, and reads the same way through either family
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aNonListArgumentIsAClientTypeErrorForCollFunctionsToo() {
    // The builtin, unchanged, as the reference wording.
    assertThatThrownBy(() -> drain("RETURN tail(42) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Type mismatch: tail() expects a LIST<ANY> argument but got INTEGER");

    for (final String call : new String[] { "coll.sort(42)", "coll.distinct(42)", "coll.toSet(42)",
        "coll.flatten(42)", "coll.pairsMin(42)", "coll.min(42)", "coll.max(42)", "coll.sum(42)", "coll.avg(42)",
        "coll.indexOf(42, 1)", "coll.insert(42, 0, 1)", "coll.remove(42, 0)", "coll.union(42, [1])",
        "coll.unionAll(42, [1])" })
      assertThatThrownBy(() -> drain("RETURN " + call + " AS r"))
          .as("%s must report a client type error, not a server fault", call)
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("Type mismatch")
          .hasMessageContaining("but got INTEGER");
  }

  @Test
  void aWrongArgumentCountIsAClientErrorForCollFunctionsToo() {
    // The hand-rolled "requires exactly N arguments" checks were CommandExecutionException, i.e. HTTP 500,
    // where checkArity - which every other function uses - raises CommandSemanticException.
    for (final String call : new String[] { "coll.sort([1], [2])", "coll.distinct([1], [2])", "coll.min([1], [2])",
        "coll.max([1], [2])", "coll.flatten()", "coll.indexOf([1])", "coll.insert([1], 0)", "coll.remove([1])" })
      assertThatThrownBy(() -> drain("RETURN " + call + " AS r"))
          .as("%s must report a client error", call)
          .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void aNonNumericFlattenDepthIsAClientErrorToo() {
    // Found in code review: depth silently stayed at its default of 1 instead of raising, for any argument that
    // was neither null, a Number nor a Boolean.
    assertThatThrownBy(() -> drain("RETURN coll.flatten([[1, 2], [3]], 'x') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Type mismatch");
  }

  @Test
  void anOutOfRangeIndexIsAClientErrorToo() {
    assertThatThrownBy(() -> drain("RETURN coll.insert([1, 2], -1, 0) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("negative");
    assertThatThrownBy(() -> drain("RETURN coll.remove([1, 2], 10) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("out of range");
    // A non-numeric index used to reach ((Number) args[1]) and surface as a bare ClassCastException.
    assertThatThrownBy(() -> drain("RETURN coll.remove([1, 2], 'x') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Type mismatch");
  }

  @Test
  void nullStillPropagatesRatherThanRaising() {
    // The one exception to the type rule, and the same one the builtins make: null in, null out.
    assertThat(single("RETURN coll.sort(null) AS r", Map.of())).isNull();
    assertThat(single("RETURN coll.distinct(null) AS r", Map.of())).isNull();
    assertThat(single("RETURN tail(null) AS r", Map.of())).isNull();
  }

  // ---------------------------------------------------------------------------------------------------------
  // 3. coll.toSet is coll.distinct
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void toSetAndDistinctAreOneImplementation() {
    assertThat(CypherFunctionRegistry.get("coll.toSet")).isInstanceOf(CypherFunctionRegistry.get("coll.distinct")
        .getClass());
    assertThat(list("RETURN coll.toSet([1, 1, 2]) AS r", Map.of()))
        .isEqualTo(list("RETURN coll.distinct([1, 1, 2]) AS r", Map.of()));
    // Both names keep answering, including through the apoc. prefix a migrating catalogue writes (issue #6157).
    assertThat(CypherFunctionRegistry.get("apoc.coll.toSet")).isSameAs(CypherFunctionRegistry.get("coll.toSet"));
  }

  // ---------------------------------------------------------------------------------------------------------
  // The range short-circuits, which must survive the routing change and gain the aggregates
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aRangeIsStillAnsweredWithoutBeingCopied() {
    assertThat(single("RETURN coll.sort(range(1, 5)) AS r", Map.of())).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.distinct(range(1, 5)) AS r", Map.of())).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.toSet(range(1, 5)) AS r", Map.of())).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.flatten(range(1, 5)) AS r", Map.of())).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.remove(range(1, 5), 0, 2) AS r", Map.of())).isInstanceOf(LongRangeList.class);
  }

  @Test
  void theAggregatesAnswerARangeFromItsShape() {
    // The endpoints and the closed form, not a walk: with arcadedb.queryMaxRangeSize raised these are the
    // difference between an answer and a billion iterations.
    assertThat(single("RETURN coll.min(range(1, 10)) AS r", Map.of())).isEqualTo(1L);
    assertThat(single("RETURN coll.max(range(1, 10)) AS r", Map.of())).isEqualTo(10L);
    assertThat(single("RETURN coll.min(range(10, 1, -1)) AS r", Map.of())).isEqualTo(1L);
    assertThat(single("RETURN coll.max(range(10, 1, -1)) AS r", Map.of())).isEqualTo(10L);
    assertThat(single("RETURN coll.sum(range(1, 10)) AS r", Map.of())).isEqualTo(55.0);
    assertThat(single("RETURN coll.avg(range(1, 10)) AS r", Map.of())).isEqualTo(5.5);
    assertThat(single("RETURN coll.sum(range(1, 10, 3)) AS r", Map.of())).isEqualTo(22.0);
    assertThat(single("RETURN coll.avg(range(1, 10, 3)) AS r", Map.of())).isEqualTo(5.5);

    // The counterweight: the closed form has to give the same answer the walk gave.
    assertThat(single("RETURN coll.sum([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) AS r", Map.of())).isEqualTo(55.0);
    assertThat(single("RETURN coll.avg([1, 4, 7, 10]) AS r", Map.of())).isEqualTo(5.5);
    assertThat(single("RETURN coll.min([]) AS r", Map.of())).isNull();
    assertThat(single("RETURN coll.avg(range(1, 0)) AS r", Map.of())).isNull();
  }

  // ---------------------------------------------------------------------------------------------------------

  private Object single(final String query, final Map<String, Object> params) {
    try (final ResultSet resultSet = database.query("opencypher", query, params)) {
      return resultSet.next().getProperty("r");
    }
  }

  @SuppressWarnings("unchecked")
  private List<Object> list(final String query, final Map<String, Object> params) {
    return (List<Object>) single(query, params);
  }

  private void drain(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        resultSet.next();
    }
  }
}
