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
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code apoc.map.*} functions answer what APOC 5.26 answers for the same arguments.
 */
class CypherApocMapFunctionsTest {

  /**
   * Issue #12107: groupBy keeps one element per key and reads node and relationship properties, sortedProperties sorts
   * case-insensitively by default and keeps null values, values skips missing keys, clean drops null values and fromPairs
   * maps a key-only pair to null. Every expected value is the answer of Neo4j 5.26 with APOC 5.26.33.
   */
  @Nested
  class Issue12107ApocMapSemantics extends TestHelper {

    @Test
    void groupByKeepsTheLastElementPerKey() {
      assertThat(value("RETURN apoc.map.groupBy([{k:1, v:'x'}, {k:1, v:'y'}, {k:2, v:'z'}], 'k') AS r")).isEqualTo(
          Map.of("1", Map.of("k", 1L, "v", "y"), "2", Map.of("k", 2L, "v", "z")));
    }

    @Test
    void groupBySkipsElementsWithoutTheKey() {
      assertThat(value("RETURN apoc.map.groupBy([{k:1}, {v:2}, {k:null}, 5, 'x', null, [1]], 'k') AS r")).isEqualTo(
          Map.of("1", Map.of("k", 1L)));
      assertThat(value("RETURN apoc.map.groupBy([{k:1}], null) AS r")).isEqualTo(Map.of());
    }

    @Test
    void groupByMultiKeepsEveryElementPerKey() {
      assertThat(value("RETURN apoc.map.groupByMulti([{k:1, v:'x'}, {v:2}, {k:1, v:'y'}, {k:2, v:'z'}, 5], 'k') AS r")).isEqualTo(
          Map.of("1", List.of(Map.of("k", 1L, "v", "x"), Map.of("k", 1L, "v", "y")), "2", List.of(Map.of("k", 2L, "v", "z"))));
      assertThat(value("RETURN apoc.map.groupByMulti([{k:1}], null) AS r")).isEqualTo(Map.of());
    }

    @Test
    void groupByReadsNodeAndRelationshipProperties() {
      createGraph();
      assertThat(uids(value("MATCH (n:P) WITH n ORDER BY n.uid RETURN apoc.map.groupBy(collect(n), 'name') AS r"))).isEqualTo(
          Map.of("a", 3, "b", 2));
      assertThat(uids(value("MATCH (n:P) WITH n ORDER BY n.uid RETURN apoc.map.groupByMulti(collect(n), 'name') AS r"))).isEqualTo(
          Map.of("a", List.of(1, 3), "b", List.of(2)));
      assertThat(uids(value("MATCH ()-[x:K]->() WITH x ORDER BY x.uid RETURN apoc.map.groupBy(collect(x), 'w') AS r"))).isEqualTo(
          Map.of("2", 12));
      assertThat(
          uids(value("MATCH ()-[x:K]->() WITH x ORDER BY x.uid RETURN apoc.map.groupByMulti(collect(x), 'w') AS r"))).isEqualTo(
          Map.of("2", List.of(10, 12)));
    }

    @Test
    void sortedPropertiesIgnoresCaseByDefaultAndKeepsNulls() {
      assertThat(value("RETURN apoc.map.sortedProperties({b:1, A:2, a:3, B:4, c:null}) AS r")).isEqualTo(
          List.of(List.of("A", 2L), List.of("a", 3L), List.of("b", 1L), List.of("B", 4L), Arrays.asList("c", null)));
      assertThat(value("RETURN apoc.map.sortedProperties({b:1, A:2, a:3, B:4, c:null}, true) AS r")).isEqualTo(
          List.of(List.of("A", 2L), List.of("a", 3L), List.of("b", 1L), List.of("B", 4L), Arrays.asList("c", null)));
      assertThat(value("RETURN apoc.map.sortedProperties({b:1, A:2, a:3, B:4, c:null}, false) AS r")).isEqualTo(
          List.of(List.of("A", 2L), List.of("B", 4L), List.of("a", 3L), List.of("b", 1L), Arrays.asList("c", null)));
    }

    @Test
    void valuesSkipsMissingKeysUnlessAskedForNulls() {
      assertThat(value("RETURN apoc.map.values({a:1, b:null}, ['b', 'missing', 'a', null]) AS r")).isEqualTo(
          Arrays.asList(null, 1L));
      assertThat(value("RETURN apoc.map.values({a:1, b:null}, ['b', 'missing', 'a', null], true) AS r")).isEqualTo(
          Arrays.asList(null, null, 1L, null));
      assertThat(value("RETURN apoc.map.values({a:1, b:2}, ['a', 'missing'], false) AS r")).isEqualTo(List.of(1L));
      assertThat(value("RETURN apoc.map.values({a:1, b:2}) AS r")).isEqualTo(List.of());
      assertThat(value("RETURN apoc.map.values({a:1, b:2}, null) AS r")).isEqualTo(List.of());
    }

    @Test
    void cleanDropsNullValuesAndValuesListedAsStrings() {
      assertThat(value("RETURN apoc.map.clean({a:1, b:'', c:null}, [], ['']) AS r")).isEqualTo(Map.of("a", 1L));
      assertThat(value("RETURN apoc.map.clean({a:1, b:'', c:null, d:2, e:true, f:2.0}, ['a'], ['', '2', 'true']) AS r")).isEqualTo(
          Map.of("f", 2.0));
      assertThat(value("RETURN apoc.map.clean({a:1, b:2.0, c:'1', d:[1, 2], e:'[1, 2]'}, [], [2, 1, [1, 2]]) AS r")).isEqualTo(
          Map.of("b", 2.0, "c", "1", "e", "[1, 2]"));
    }

    @Test
    void fromPairsMapsAKeyOnlyPairToNull() {
      final Map<String, Object> expected = new LinkedHashMap<>();
      expected.put("a", null);
      expected.put("b", 1L);
      expected.put("4", 5L);
      assertThat(value("RETURN apoc.map.fromPairs([['a'], ['b', 1, 2], [], [null, 3], [4, 5]]) AS r")).isEqualTo(expected);
    }

    @Test
    void aNodeIsTheMapOfItsProperties() {
      createGraph();
      assertThat(value("MATCH (n:P {uid:1}) RETURN apoc.map.sortedProperties(n) AS r")).isEqualTo(
          List.of(List.of("name", "a"), List.of("uid", 1)));
      assertThat(value("MATCH (n:P {uid:1}) RETURN apoc.map.values(n, ['name', 'x']) AS r")).isEqualTo(List.of("a"));
      assertThat(value("MATCH (n:P {uid:1}) RETURN apoc.map.clean(n, ['uid'], []) AS r")).isEqualTo(Map.of("name", "a"));
    }

    private void createGraph() {
      database.transaction(() -> database.command("opencypher",
          "CREATE (a:P {uid:1, name:'a'})-[:K {w:2, uid:10}]->(b:P {uid:2, name:'b'}), (c:P {uid:3, name:'a'}), (d:P {uid:4}), "
              + "(b)-[:K {uid:11}]->(c), (c)-[:K {w:2, uid:12}]->(d)"));
    }

    private Object value(final String query) {
      try (final ResultSet result = database.query("opencypher", query)) {
        return result.next().getProperty("r");
      }
    }

    /** The groups with every node or relationship replaced by its uid. */
    private Map<String, Object> uids(final Object groups) {
      assertThat(groups).isInstanceOf(Map.class);
      final Map<String, Object> out = new LinkedHashMap<>();
      for (final Map.Entry<?, ?> entry : ((Map<?, ?>) groups).entrySet()) {
        if (entry.getValue() instanceof List<?> list) {
          final List<Object> ids = new ArrayList<>();
          for (final Object element : list)
            ids.add(((Document) element).get("uid"));
          out.put(entry.getKey().toString(), ids);
        } else
          out.put(entry.getKey().toString(), ((Document) entry.getValue()).get("uid"));
      }
      return out;
    }
  }
}
