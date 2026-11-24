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
package com.arcadedb.function.polyglot;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lists and maps that cross between the database and a JavaScript function: a value that is referred to several times is
 * converted once, and what the function answers is held to the cap of in-heap elements of a query.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class JavascriptFunctionSharedValuesTest extends TestHelper {
  private static final int FAN_OUT = 16;
  private static final int LEVELS  = 10;

  @Test
  @Timeout(60)
  void sharedListArgumentFromSql() {
    defineFunctions();

    // 16 REFERENCES TO THE LEVEL BELOW, TEN TIMES: SMALL WHEN SHARED, 16^10 LISTS WHEN EVERY REFERENCE IS A COPY
    assertThat(firstValue("sql", sqlWithSharedLevels("`listfn.size`($level" + LEVELS + ")"))).isEqualTo(FAN_OUT);
    assertThat(firstValue("sql", sqlWithSharedLevels("`listfn.size`($level1)"))).isEqualTo(FAN_OUT);
  }

  @Test
  @Timeout(60)
  void sharedListArgumentFromOpenCypher() {
    defineFunctions();

    assertThat(firstValue("opencypher", "RETURN listfn.size(" + cypherSharedLevels(LEVELS) + ") AS r")).isEqualTo(FAN_OUT);
  }

  @Test
  @Timeout(60)
  void sharedMapArgumentFromSql() {
    defineFunctions();

    final String map = "{'a': $level" + LEVELS + ", 'b': $level" + LEVELS + "}";
    assertThat(firstValue("sql", sqlWithSharedLevels("`listfn.keys`(" + map + ")"))).isEqualTo(2);
  }

  @Test
  @Timeout(60)
  void sharedListArgumentReachesTheFunctionOnce() {
    defineFunctions();

    // THE SAME LIST AT TWO POSITIONS IS ONE LIST FOR THE FUNCTION, AS IT IS ONE LIST FOR THE CALLER
    assertThat(firstValue("sql", sqlWithSharedLevels("`listfn.sameFirstTwo`($level3)"))).isEqualTo(true);
    assertThat(firstValue("sql", "SELECT `listfn.sameFirstTwo`([[1], [1]]) AS r")).isEqualTo(false);
  }

  @Test
  void sharedListsOfTheHelpersAreConvertedOnce() {
    final List<Object> leaf = new ArrayList<>(List.of(1, 2));
    final List<Object> shared = new ArrayList<>(List.of(leaf, leaf));
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("first", shared);
    map.put("second", shared);

    final ProxyArray array = JavascriptFunctionDefinition.toDeepProxyList(List.of(shared, shared));
    assertThat(array.get(0)).isSameAs(array.get(1));
    final ProxyArray sharedArray = (ProxyArray) array.get(0);
    assertThat(sharedArray.get(0)).isSameAs(sharedArray.get(1));

    final ProxyObject object = JavascriptFunctionDefinition.toDeepProxyObject(map);
    assertThat(object.getMember("first")).isSameAs(object.getMember("second"));
  }

  @Test
  void collectionThatContainsItselfIsConverted() {
    defineFunctions();

    final List<Object> list = new ArrayList<>();
    list.add(1);
    list.add(list);
    assertThat(database.getSchema().getFunction("listfn", "size").execute(list)).isEqualTo(2);

    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("self", map);
    assertThat(database.getSchema().getFunction("listfn", "keys").execute(map)).isEqualTo(1);
  }

  @Test
  @Timeout(60)
  void sparseArrayResultIsHeldToTheCap() {
    defineFunctions();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);

    // ONE ELEMENT STORED AT A HIGH INDEX: THE ARRAY IS ONE MILLION AND ONE ELEMENTS LONG
    assertThatThrownBy(() -> firstValue("sql", "SELECT `listfn.sparse`(1000000).size() AS r")).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey())
        .hasMessageContaining("listfn.sparse");
    assertThatThrownBy(() -> firstValue("opencypher", "RETURN size(listfn.sparse(1000000)) AS r")).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

    // UNDER THE CAP THE HOLES ARE NULLS, AS THEY WERE
    assertThat(firstValue("sql", "SELECT `listfn.sparse`(10).size() AS r")).isEqualTo(11);
  }

  @Test
  @Timeout(60)
  void sparseArrayInsideAnObjectIsHeldToTheCap() {
    defineFunctions();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);

    assertThatThrownBy(() -> firstValue("sql", "SELECT `listfn.sparseInObject`(1000000) AS r")).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  @Test
  @Timeout(60)
  void denseArrayResultIsHeldToTheCapToo() {
    defineFunctions();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);

    assertThat(firstValue("sql", "SELECT `listfn.range`(500).size() AS r")).isEqualTo(500);
    assertThatThrownBy(() -> firstValue("sql", "SELECT `listfn.range`(5000).size() AS r")).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

    // NO LIMIT WHEN THE SETTING SAYS SO
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);
    assertThat(firstValue("sql", "SELECT `listfn.range`(5000).size() AS r")).isEqualTo(5000);
  }

  @Test
  @Timeout(60)
  void sharedStructureAnsweredByTheFunctionIsHeldToTheCap() {
    defineFunctions();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);

    // THE FUNCTION ANSWERS ITS ARGUMENT: 16^4 LISTS WHEN EVERY REFERENCE BECOMES A LIST OF ITS OWN
    final StringBuilder sql = new StringBuilder("SELECT `listfn.same`($level4).size() AS r LET $level0 = [1]");
    appendLevels(sql, 4);
    assertThatThrownBy(() -> firstValue("sql", sql.toString())).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  @Test
  void smallStructureAnsweredByTheFunctionIsConverted() {
    defineFunctions();

    final Object result = database.getSchema().getFunction("listfn", "same")
        .execute(List.of(List.of(1, 2), Map.of("a", List.of("x"))));
    assertThat(result).isInstanceOf(List.class);
    final List<?> list = (List<?>) result;
    assertThat(list).hasSize(2);
    assertThat(list.getFirst()).isEqualTo(List.of(1, 2));
    assertThat(list.get(1)).isEqualTo(Map.of("a", List.of("x")));
  }

  private void defineFunctions() {
    database.command("sql", "DEFINE FUNCTION listfn.size \"return items.length\" PARAMETERS [items] LANGUAGE js");
    database.command("sql", "DEFINE FUNCTION listfn.keys \"return Object.keys(items).length\" PARAMETERS [items] LANGUAGE js");
    database.command("sql", "DEFINE FUNCTION listfn.sameFirstTwo \"return items[0] === items[1]\" PARAMETERS [items] LANGUAGE js");
    database.command("sql", "DEFINE FUNCTION listfn.same \"return items\" PARAMETERS [items] LANGUAGE js");
    database.command("sql", "DEFINE FUNCTION listfn.sparse \"var c = []; c[n] = 1; return c\" PARAMETERS [n] LANGUAGE js");
    database.command("sql", "DEFINE FUNCTION listfn.sparseInObject \"var c = []; c[n] = 1; return {values: c}\" PARAMETERS [n] LANGUAGE js");
    database.command("sql", "DEFINE FUNCTION listfn.range \"var c = []; for (var i = 0; i < n; i++) c.push(i); return c\" PARAMETERS [n] LANGUAGE js");
  }

  private Object firstValue(final String language, final String query) {
    try (final ResultSet resultSet = database.command(language, query)) {
      return resultSet.next().getProperty("r");
    }
  }

  private static String sqlWithSharedLevels(final String projection) {
    final StringBuilder sql = new StringBuilder("SELECT ").append(projection).append(" AS r LET $level0 = [1]");
    appendLevels(sql, LEVELS);
    return sql.toString();
  }

  private static void appendLevels(final StringBuilder sql, final int levels) {
    for (int level = 1; level <= levels; level++) {
      sql.append(", $level").append(level).append(" = [");
      for (int i = 0; i < FAN_OUT; i++)
        sql.append(i > 0 ? ", " : "").append("$level").append(level - 1);
      sql.append("]");
    }
  }

  private static String cypherSharedLevels(final int levels) {
    final StringBuilder items = new StringBuilder();
    for (int i = 0; i < FAN_OUT; i++)
      items.append(i > 0 ? ", " : "").append("a");
    return "reduce(a = [1], i IN range(1, " + levels + ") | [" + items + "])";
  }
}
