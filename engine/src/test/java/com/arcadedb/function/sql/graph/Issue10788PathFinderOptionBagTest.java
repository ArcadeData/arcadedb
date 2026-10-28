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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.index.fulltext.MoreLikeThisConfig;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10788: an option of the {@code astar()} / {@code dijkstra()} option bag that is not a value of its kind is refused with a
 * message naming the option, never read as another value: {@code maxDepth: 2.5} is not 2, {@code maxDepth: 'abc'} is not the
 * default, a negative {@code maxDepth} is not "no path", and {@code parallel: 'yes'} is not {@code false}. A value of the kind is
 * accepted at its full range: {@code maxDepth} is a long, so 3000000000 is a bound like any other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10788PathFinderOptionBagTest extends TestHelper {

  private RID source;
  private RID target;

  @BeforeEach
  void buildChain() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      for (int i = 0; i <= 3; i++)
        database.command("sql", "CREATE VERTEX V SET nm = ?", "v" + i);
      for (int i = 0; i < 3; i++)
        database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE nm = ?) TO (SELECT FROM V WHERE nm = ?) SET w = 1.0", "v" + i,
            "v" + (i + 1));
    });
    source = rid("v0");
    target = rid("v3");
  }

  @ParameterizedTest
  @ValueSource(strings = { "astar", "dijkstra" })
  void aFractionalMaxDepthIsRefused(final String function) {
    assertRefused(function, "maxDepth: 2.5", "maxDepth");
    assertRefused(function, "maxDepth: '2.5'", "maxDepth");
  }

  @ParameterizedTest
  @ValueSource(strings = { "astar", "dijkstra" })
  void anUnparsableMaxDepthIsRefused(final String function) {
    assertRefused(function, "maxDepth: 'abc'", "maxDepth");
  }

  @ParameterizedTest
  @ValueSource(strings = { "astar", "dijkstra" })
  void aNegativeMaxDepthIsRefused(final String function) {
    assertRefused(function, "maxDepth: -1", "maxDepth");
  }

  @ParameterizedTest
  @ValueSource(strings = { "astar", "dijkstra" })
  void aMaxDepthBeyondTheIntRangeIsABoundLikeAnyOther(final String function) {
    assertThat(path(function, "maxDepth: 3000000000")).hasSize(4);
    assertThat(path(function, "maxDepth: '3000000000'")).hasSize(4);
    assertThat(path(function, "maxDepth: 3")).hasSize(4);
    assertThat(path(function, "maxDepth: 2")).isEmpty();
    assertThat(path(function, "maxDepth: 0")).isEmpty();
    // a whole number spelled as a decimal is that number
    assertThat(path(function, "maxDepth: 3.0")).hasSize(4);
  }

  @ParameterizedTest
  @ValueSource(strings = { "astar", "dijkstra" })
  void aBooleanOptionThatIsNotABooleanIsRefused(final String function) {
    assertRefused(function, "emptyIfMaxDepth: 'yes'", "emptyIfMaxDepth");
    assertRefused(function, "emptyIfMaxDepth: 1", "emptyIfMaxDepth");
    // the spellings of a boolean are accepted
    assertThat(path(function, "emptyIfMaxDepth: 'TRUE'")).hasSize(4);
    assertThat(path(function, "emptyIfMaxDepth: false")).hasSize(4);
  }

  @ParameterizedTest
  @ValueSource(strings = { "parallel: 'yes'", "tieBreaker: 'ture'", "dFactor: 'abc'" })
  void anAstarOnlyOptionOfTheWrongKindIsRefused(final String option) {
    assertRefused("astar", option, option.substring(0, option.indexOf(':')));
  }

  /** The option reader is shared by every function option bag: a boolean option of a non-graph function is held to the same rule. */
  @Test
  void aBooleanOptionOfAnotherFunctionIsHeldToTheSameRule() {
    assertThatThrownBy(() -> MoreLikeThisConfig.fromOptions(
        new FunctionOptions("search_index_more", Map.of("excludeSource", "yes"), MoreLikeThisConfig.OPTION_KEYS))).hasMessageContaining(
        "Option 'excludeSource'").hasMessageContaining("must be a boolean");
    assertThatThrownBy(() -> MoreLikeThisConfig.fromOptions(
        new FunctionOptions("search_index_more", Map.of("boostByScore", 1), MoreLikeThisConfig.OPTION_KEYS))).hasMessageContaining(
        "Option 'boostByScore'");
    assertThat(MoreLikeThisConfig.fromOptions(
        new FunctionOptions("search_index_more", Map.of("excludeSource", "FALSE"), MoreLikeThisConfig.OPTION_KEYS)).isExcludeSource()).isFalse();
  }

  /** shortestPath() reads maxDepth as an int: a null one is no bound, as for astar() and dijkstra(), and a negative one is refused. */
  @Test
  void aShortestPathMaxDepthIsNullForNoBoundAndNeverNegative() {
    assertThat(shortestPath("{maxDepth: null}")).hasSize(4);
    assertThat(shortestPath("{maxDepth: 3}")).hasSize(4);
    assertThat(shortestPath("{maxDepth: 2}")).isEmpty();
    assertThatThrownBy(() -> shortestPath("{maxDepth: -1}")).hasMessageContaining("Option 'maxDepth'").hasMessageContaining("negative");
    assertThatThrownBy(() -> shortestPath("{maxDepth: 2.5}")).hasMessageContaining("Option 'maxDepth'");
    assertThatThrownBy(() -> shortestPath("{edge: 'yes'}")).hasMessageContaining("Option 'edge'");
  }

  // ---- helpers

  private void assertRefused(final String function, final String option, final String optionName) {
    assertThatThrownBy(() -> path(function, option)).as("%s with %s", function, option)
        .hasMessageContaining("Option '" + optionName + "'").hasMessageContaining(function);
  }

  private List<?> path(final String function, final String option) {
    final String options = "{direction: 'OUT', edgeTypeNames: ['E']" + (function.equals("astar") ? ", vertexAxisNames: []" : "") + ", "
        + option + "}";
    try (final ResultSet resultSet = database.query("sql", "SELECT " + function + "(?, ?, 'w', " + options + ") AS p", source, target)) {
      return resultSet.next().getProperty("p");
    }
  }

  private List<?> shortestPath(final String options) {
    try (final ResultSet resultSet = database.query("sql", "SELECT shortestPath(?, ?, 'OUT', 'E', " + options + ") AS p", source, target)) {
      return resultSet.next().getProperty("p");
    }
  }

  private RID rid(final String name) {
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM V WHERE nm = ?", name)) {
      return resultSet.next().getIdentity().orElseThrow();
    }
  }
}
