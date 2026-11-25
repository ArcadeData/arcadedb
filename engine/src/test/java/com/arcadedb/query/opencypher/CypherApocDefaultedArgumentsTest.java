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
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code apoc.*} functions accept every argument count APOC 5.26 accepts, with APOC's defaults for the omitted ones.
 */
class CypherApocDefaultedArgumentsTest {

  /**
   * Issue #12105: text.lpad/rpad without the pad, text.indexOf with {@code to}, map.removeKey/removeKeys with a config,
   * map.submap with default values and {@code fail}, number.format with a language and path.create without relationships
   * were refused with "expects N arguments". Every expected value is the answer of Neo4j 5.26 with APOC 5.26.33.
   */
  @Nested
  class Issue12105ApocDefaultedArguments extends TestHelper {

    @Test
    void lpadAndRpadPadWithASpaceByDefault() {
      assertThat(value("RETURN apoc.text.lpad('123', 5) AS r")).isEqualTo("  123");
      assertThat(value("RETURN apoc.text.rpad('123', 5) AS r")).isEqualTo("123  ");
    }

    @Test
    void lpadAndRpadAnswerTheTextUnchangedForANegativeLength() {
      assertThat(value("RETURN apoc.text.lpad('123', -1) AS r")).isEqualTo("123");
      assertThat(value("RETURN apoc.text.rpad('123', -3, 'x') AS r")).isEqualTo("123");
    }

    @Test
    void indexOfToBoundsTheSearch() {
      assertThat(value("RETURN apoc.text.indexOf('hello', 'l', 0, 2) AS r")).isEqualTo(-1L);
      assertThat(value("RETURN apoc.text.indexOf('hello', 'l', 0, 3) AS r")).isEqualTo(2L);
      assertThat(value("RETURN apoc.text.indexOf('hello', 'lo', 0, 4) AS r")).isEqualTo(-1L);
      assertThat(value("RETURN apoc.text.indexOf('hello', 'lo', 0, 5) AS r")).isEqualTo(3L);
      assertThat(value("RETURN apoc.text.indexOf('hello', '', 2, 3) AS r")).isEqualTo(2L);
    }

    @Test
    void indexOfToOfMinusOneOrPastTheEndSearchesTheWholeText() {
      assertThat(value("RETURN apoc.text.indexOf('hello', 'l', 3, -1) AS r")).isEqualTo(3L);
      assertThat(value("RETURN apoc.text.indexOf('hello', 'l', 0, 99) AS r")).isEqualTo(2L);
      assertThat(value("RETURN apoc.text.indexOf('hello', '', 5) AS r")).isEqualTo(5L);
    }

    @Test
    void indexOfToAtOrBeforeFromFindsNothing() {
      assertThat(value("RETURN apoc.text.indexOf('hello', 'l', 3, 2) AS r")).isEqualTo(-1L);
      assertThat(value("RETURN apoc.text.indexOf('hello', '', 2, 2) AS r")).isEqualTo(-1L);
      assertThat(value("RETURN apoc.text.indexOf('hello', '', 0, 0) AS r")).isEqualTo(-1L);
      assertThat(value("RETURN apoc.text.indexOf('hello', '', 5, 5) AS r")).isEqualTo(-1L);
      assertThat(value("RETURN apoc.text.indexOf('hello', 'l', 0, -5) AS r")).isEqualTo(-1L);
    }

    @Test
    void removeKeyAndRemoveKeysTakeAConfig() {
      assertThat(value("RETURN apoc.map.removeKey({a:1, b:2}, 'a', {}) AS r")).isEqualTo(map("b", 2L));
      assertThat(value("RETURN apoc.map.removeKeys({a:1, b:2}, ['a'], {recursive:true}) AS r")).isEqualTo(map("b", 2L));
      assertThat(value("RETURN apoc.map.removeKeys({a:{}}, ['x'], {recursive:false}) AS r")).isEqualTo(map("a", map()));
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2}}, ['x'], null) AS r")).isEqualTo(
          map("a", map("x", 1L, "y", 2L)));
    }

    @Test
    void removeKeysRecursiveCleansNestedMapsAndTheMapsOfAList() {
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2, z:{x:2}}, x:3, c:[[{x:1}]]}, ['x'], {recursive:true}) AS r"))
          .isEqualTo(map("a", map("y", 2L), "c", List.of(List.of(map("x", 1L)))));
      assertThat(value("RETURN apoc.map.removeKeys({a:[{x:1, y:2}, {x:1}, 5], b:1}, ['x'], {recursive:true}) AS r")).isEqualTo(
          map("a", List.of(map("y", 2L), 5L), "b", 1L));
      assertThat(value("RETURN apoc.map.removeKeys({a:[], b:{}, c:[1, 2]}, ['x'], {recursive:true}) AS r")).isEqualTo(
          map("c", List.of(1L, 2L)));
      assertThat(value("RETURN apoc.map.removeKeys({a:{b:{x:1}}, x:1}, ['x'], {recursive:true}) AS r")).isEqualTo(map());
    }

    /**
     * A list that reaches the engine as an array (a parameter, or an ARRAY_OF_* property) is the same LIST to the
     * recursive removal as to every other list function (issue #11582): its maps are cleaned and it is dropped once empty.
     */
    @Test
    void removeKeysRecursiveReadsAnArrayAsAList() {
      final String query = "RETURN apoc.map.removeKeys({a: $l, b: 1}, ['x'], {recursive:true}) AS r";
      assertThat(value(query, Map.of("l", new Object[] { map("x", 1L, "y", 2L), map("x", 1L), 5L }))).isEqualTo(
          map("a", List.of(map("y", 2L), 5L), "b", 1L));
      assertThat(value(query, Map.of("l", new Object[] { map("x", 1L) }))).isEqualTo(map("b", 1L));
      assertThat(value(query, Map.of("l", new Object[0]))).isEqualTo(map("b", 1L));
      assertThat(value(query, Map.of("l", new long[0]))).isEqualTo(map("b", 1L));
      // An array holding no map is left as it is
      final Object kept = ((Map<?, ?>) value(query, Map.of("l", new long[] { 1L, 2L }))).get("a");
      assertThat(CypherFunctionHelper.asListOrNull(kept)).containsExactly(1L, 2L);
      // Without recursive nothing below the top level is touched
      assertThat(value("RETURN apoc.map.removeKeys({a: $l, b: 1}, ['x']) AS r", Map.of("l", new Object[0]))).asInstanceOf(
          InstanceOfAssertFactories.MAP).containsOnlyKeys("a", "b");
    }

    @Test
    void removeKeysReadsRecursiveAsAPOCReadsAFlag() {
      final Map<String, Object> removed = map("a", map("y", 2L));
      final Map<String, Object> kept = map("a", map("x", 1L, "y", 2L));
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2}}, ['x'], {recursive:'true'}) AS r")).isEqualTo(removed);
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2}}, ['x'], {recursive:1}) AS r")).isEqualTo(removed);
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2}}, ['x'], {recursive:[]}) AS r")).isEqualTo(removed);
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2}}, ['x'], {recursive:'NO'}) AS r")).isEqualTo(kept);
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2}}, ['x'], {recursive:'0'}) AS r")).isEqualTo(kept);
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2}}, ['x'], {recursive:0.5}) AS r")).isEqualTo(kept);
      assertThat(value("RETURN apoc.map.removeKeys({a:{x:1, y:2}}, ['x'], {recursive:null}) AS r")).isEqualTo(kept);
    }

    @Test
    void removeKeyRecursesOnlyWhenTheTopLevelHoldsTheKey() {
      assertThat(value("RETURN apoc.map.removeKey({a:{x:1, y:2}, x:3}, 'x', {recursive:true}) AS r")).isEqualTo(
          map("a", map("y", 2L)));
      assertThat(value("RETURN apoc.map.removeKey({a:{x:1, y:2}}, 'x', {recursive:true}) AS r")).isEqualTo(
          map("a", map("x", 1L, "y", 2L)));
    }

    @Test
    void submapTakesDefaultValuesByPosition() {
      assertThat(value("RETURN apoc.map.submap({a:1}, ['a','z'], [0], false) AS r")).isEqualTo(map("a", 1L, "z", null));
      assertThat(value("RETURN apoc.map.submap({a:1}, ['z','a'], [7, 8]) AS r")).isEqualTo(map("z", 7L, "a", 1L));
      assertThat(value("RETURN apoc.map.submap({a:1}, ['z','a','q'], [7], false) AS r")).isEqualTo(
          map("z", 7L, "a", 1L, "q", null));
      assertThat(value("RETURN apoc.map.submap({a:1}, ['a','z'], null, false) AS r")).isEqualTo(map("a", 1L, "z", null));
      assertThat(value("RETURN apoc.map.submap({a:null}, ['a'], [5], false) AS r")).isEqualTo(map("a", null));
      assertThat(value("RETURN apoc.map.submap({a:1, b:2}, ['b','a']) AS r")).isEqualTo(map("b", 2L, "a", 1L));
    }

    @Test
    void submapFailsByDefaultOnAKeyWithoutValue() {
      assertThatThrownBy(() -> value("RETURN apoc.map.submap({a:1}, ['a','z']) AS r")).hasStackTraceContaining(
          "key 'z' is not one of the map's keys");
      assertThatThrownBy(() -> value("RETURN apoc.map.submap({a:1}, ['a','z'], [0]) AS r")).hasStackTraceContaining(
          "key 'z' is not one of the map's keys");
      // A null default is no default, as in APOC: the missing key still fails, and with fail = false maps to null
      assertThatThrownBy(() -> value("RETURN apoc.map.submap({}, ['x'], [null]) AS r")).hasStackTraceContaining(
          "key 'x' is not one of the map's keys");
      assertThatThrownBy(() -> value("RETURN apoc.map.submap({a:1}, ['a','x'], [5, null], true) AS r")).hasStackTraceContaining(
          "key 'x' is not one of the map's keys");
      assertThat(value("RETURN apoc.map.submap({}, ['x'], [null], false) AS r")).isEqualTo(map("x", null));
    }

    @Test
    void submapOfANullMapOrNullKeysIsNull() {
      assertThat(value("RETURN apoc.map.submap(null, ['a']) AS r")).isNull();
      assertThat(value("RETURN apoc.map.submap(null, ['a'], [1, 2, 3], true) AS r")).isNull();
      assertThat(value("RETURN apoc.map.submap({a:1}, null) AS r")).isNull();
      assertThat(value("RETURN apoc.map.submap({}, null) AS r")).isNull();
    }

    @Test
    void numberFormatTakesABareLanguageCode() {
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', 'de') AS r")).isEqualTo("1.234,50");
      assertThat(value("RETURN apoc.number.format(1234.5, '', 'de') AS r")).isEqualTo("1.234,5");
      assertThat(value("RETURN apoc.number.format(1234, '', 'de') AS r")).isEqualTo("1.234");
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', 'it') AS r")).isEqualTo("1.234,50");
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', 'en') AS r")).isEqualTo("1,234.50");
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', '') AS r")).isEqualTo("1,234.50");
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', null) AS r")).isEqualTo("1,234.50");
    }

    @Test
    void numberFormatAnswersNullForAnythingButABareLanguageCode() {
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', 'de-DE') AS r")).isNull();
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', 'de_DE') AS r")).isNull();
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', 'DE') AS r")).isNull();
      assertThat(value("RETURN apoc.number.format(1234.5, '#,##0.00', 'xx') AS r")).isNull();
    }

    @Test
    void pathCreateWithoutRelationshipsIsTheStartNode() {
      database.transaction(() -> database.command("opencypher", "CREATE (:P12105 {uid: 1})-[:R]->(:P12105 {uid: 2})"));
      final Result row = row("MATCH (a:P12105 {uid: 1}) WITH apoc.path.create(a) AS p "
          + "RETURN length(p) AS len, [n IN nodes(p) | n.uid] AS uids");
      assertThat(row.<Number>getProperty("len").longValue()).isEqualTo(0L);
      assertThat(row.<List<Number>>getProperty("uids")).extracting(Number::longValue).containsExactly(1L);
      assertThat(value("RETURN apoc.path.create(null) AS r")).isNull();
    }

    private Object value(final String query) {
      return row(query).getProperty("r");
    }

    private Result row(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next();
      }
    }

    private Object value(final String query, final Map<String, Object> parameters) {
      try (final ResultSet rs = database.query("opencypher", query, parameters)) {
        return rs.next().getProperty("r");
      }
    }

    private static Map<String, Object> map(final Object... keysAndValues) {
      final Map<String, Object> map = new LinkedHashMap<>();
      for (int i = 0; i < keysAndValues.length; i += 2)
        map.put((String) keysAndValues[i], keysAndValues[i + 1]);
      return map;
    }
  }
}
