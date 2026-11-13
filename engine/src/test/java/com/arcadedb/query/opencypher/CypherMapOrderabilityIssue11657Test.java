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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11657: maps order by size, then by their keys in order, then by the values of those keys, as Neo4j's AnyValueComparatorTest lists
 * them. ORDER BY compared their toString() instead, so {a: 10} sorted before {a: 9} and {} after every other map, and min() and
 * max() found no order between two maps at all and kept the first row.
 */
class CypherMapOrderabilityIssue11657Test extends TestHelper {

  // The maps of Neo4j's AnyValueComparatorTest.objs, in the order it expects
  private static final List<String> NEO4J_ORDER = List.of("{}", "{`1`: 'a'}", "{`1`: 'b'}", "{`2`: 'a'}",
      "{`1`: {`1`: {`1`: 'a'}}, `2`: 'x'}", "{`1`: {`1`: {`1`: 'b'}}, `2`: 'x'}", "{`1`: 'a', `2`: 'b'}", "{`1`: 'b', `2`: {}}",
      "{`1`: 'b', `2`: {`10`: 'a'}}", "{`1`: 'b', `2`: {`10`: 'b'}}", "{`1`: 'b', `2`: {`20`: 'a'}}", "{`1`: 'b', `2`: 'a'}");

  @Test
  void mapsOrderBySizeThenKeysThenValues() {
    final String list = "[" + String.join(", ", NEO4J_ORDER.reversed()) + "]";
    final int last = NEO4J_ORDER.size() - 1;
    // the reversed list sorted back into Neo4j's order: position i holds the map Neo4j puts at i
    assertThat(ints("WITH " + list + " AS l UNWIND range(0, size(l) - 1) AS i RETURN " + last + " - i AS v ORDER BY l[i]"))
        .containsExactlyElementsOf(IntStream.rangeClosed(0, last).boxed().toList());
    assertThat(ints("UNWIND [{a: 10}, {a: 9}, {a: 2}, {a: -1}, {a: -2}] AS m RETURN m.a AS v ORDER BY m")).containsExactly(-2, -1, 2, 9,
        10);
    assertThat(ints("UNWIND [{a: 10}, {a: 9}, {a: 2}] AS m RETURN m.a AS v ORDER BY m DESC")).containsExactly(10, 9, 2);
    assertThat(ints("UNWIND [[{a: 10}], [{a: 9}]] AS l RETURN l[0].a AS v ORDER BY l")).containsExactly(9, 10);
  }

  @Test
  void minAndMaxOfMapsFollowTheSameOrder() {
    assertThat(ints("UNWIND [{a: 10}, {a: 2}, {a: 9}] AS m WITH min(m) AS x RETURN x.a AS v")).containsExactly(2);
    assertThat(ints("UNWIND [{a: 9}, {a: 10}, {a: 2}] AS m WITH max(m) AS x RETURN x.a AS v")).containsExactly(10);
    assertThat(ints("UNWIND [{a: 1, b: 1}, {a: 5}] AS m WITH max(m) AS x RETURN size(keys(x)) AS v")).containsExactly(2);
  }

  @Test
  void mapsWithNonStringKeysFromParametersCompareByTheirValues() {
    // A Java parameter map may carry Integer keys: the values must be looked up with the original key, not its string form
    final List<Integer> order = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "UNWIND range(0, size($m) - 1) AS i RETURN i AS v ORDER BY $m[i]",
        Map.of("m", List.of(Map.of(1, 10), Map.of(1, 9))))) {
      rs.forEachRemaining(r -> order.add(r.<Number>getProperty("v").intValue()));
    }
    assertThat(order).containsExactly(1, 0);
  }

  private List<Integer> ints(final String query) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.forEachRemaining(r -> values.add(r.<Number>getProperty("v").intValue()));
    }
    return values;
  }
}
