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
package com.arcadedb.gremlin;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.util.GremlinValueComparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12018: ArcadeDB's copy of {@link GremlinValueComparator} lacked two rules of TinkerPop 3.8.2's {@code comparable()}:
 * NaN is not comparable, and two lists, sets or maps are comparable only when their contents are. {@code lt/lte/gt/gte} raised
 * {@code IllegalStateException} instead of answering false, so a single stored NaN made every range {@code has()} on the
 * property fail, and two maps whose values are not comparable compared as ordered. The deliberate date handling of the copy
 * (every date/time class compared by the instant it denotes) stays.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12018ComparabilityTest {
  private static final String DB_PATH = "./target/testgremlin-12018";

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    // A run that crashed before its teardown would otherwise leave the type "P" behind
    FileUtils.deleteRecursively(new File(DB_PATH));
    graph = ArcadeGraph.open(DB_PATH);
    graph.getDatabase().getSchema().createVertexType("P");
    graph.getDatabase().transaction(() -> {
      graph.getDatabase().newVertex("P").set("n", 1).set("x", 2.5).save();
      graph.getDatabase().newVertex("P").set("n", 2).set("x", 0.5).save();
      graph.getDatabase().newVertex("P").set("n", 3).set("x", Double.NaN).save();
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aStoredNaNDoesNotBreakARangeFilter() {
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.V().has("x", P.gt(1)).values("n").toList()).containsExactly(1);
    assertThat(g.V().has("x", P.lt(1)).values("n").toList()).containsExactly(2);
    assertThat(g.V().hasLabel("P").has("x", P.gte(0)).values("n").toList()).containsExactlyInAnyOrder(1, 2);
    assertThat(g.V().has("x", P.between(0, 3)).count().next()).isEqualTo(2L);
    try (final ResultSet rs = graph.gremlin("g.V().has('x', gt(1)).count()").execute()) {
      assertThat(rs.next().<Long>getProperty("result")).isEqualTo(1L);
    }
  }

  @Test
  void nanIsNeitherLessNorGreater() {
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.inject(Double.NaN).is(P.lt(1)).count().next()).isZero();
    assertThat(g.inject(1).is(P.gt(Double.NaN)).count().next()).isZero();
    assertThat(g.inject(Double.NaN).is(P.lte(Double.NaN)).count().next()).isZero();
    assertThat(g.inject(Double.NaN).is(P.eq(Double.NaN)).count().next()).isZero();
    assertThat(g.inject(Double.NaN).is(P.neq(1)).count().next()).isEqualTo(1L);
  }

  @Test
  void collectionsWithIncomparableContentsAreNotOrdered() {
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.inject(List.of(1, "a")).is(P.lt(List.of(1, 2))).count().next()).isZero();
    assertThat(g.inject(List.of(1, 2)).is(P.gte(List.of(1, "a"))).count().next()).isZero();
    assertThat(g.inject(Set.of(1, 2)).is(P.lt(Set.of("a", 1))).count().next()).isZero();
    assertThat(g.inject(Map.of("a", "x")).is(P.gt(Map.of("a", 1))).count().next()).isZero();
    // The keys differ but the values are not comparable: not ordered either (was lt = true)
    assertThat(g.inject(Map.of("a", "x")).is(P.lt(Map.of("b", 1))).count().next()).isZero();
    // Comparable contents still order element by element
    assertThat(g.inject(List.of(1, 2)).is(P.lt(List.of(1, 3))).count().next()).isEqualTo(1L);
    assertThat(g.inject(Map.of("a", 1)).is(P.lt(Map.of("a", 2))).count().next()).isEqualTo(1L);
  }

  @Test
  void dateClassesStillCompareByInstant() {
    final OffsetDateTime offset = OffsetDateTime.parse("2020-01-01T00:00:00Z");
    final Date date = Date.from(offset.toInstant());
    final LocalDateTime local = LocalDateTime.parse("2020-01-01T00:00:00");
    assertThat(GremlinValueComparator.COMPARABILITY.equals(date, offset)).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(local, date)).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.compare(date, Date.from(offset.toInstant().plusSeconds(1)))).isNegative();
    assertThat(GremlinValueComparator.comparable(local, offset)).isTrue();
  }

  /**
   * TinkerPop's {@code COMPARABILITY.equals()} answers false as soon as two hash codes differ, skipping only numbers, collections
   * and maps. A Map.Entry, a Property or a Path mixes the hash codes of what it holds, so it told apart two equal dates of
   * different classes (#3870) and {@code 1} from {@code 1.0}. ArcadeDB's copy has no such shortcut.
   */
  @Test
  void eqOnCompositesFollowsTheContentsNotTheirHashCodes() {
    final OffsetDateTime offset = OffsetDateTime.parse("2020-01-01T00:00:00Z");
    final Date date = Date.from(offset.toInstant());
    final LocalDateTime local = LocalDateTime.parse("2020-01-01T00:00:00");

    assertThat(GremlinValueComparator.COMPARABILITY.equals(Map.entry("d", date), Map.entry("d", local))).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(Map.entry("d", offset), Map.entry("d", date))).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(Map.entry("n", 1), Map.entry("n", 1.0))).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(new DetachedProperty<>("d", date), new DetachedProperty<>("d", local))).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(new DetachedProperty<>("n", 1), new DetachedProperty<>("n", 1.0))).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(Map.entry("d", date), Map.entry("d", local.plusSeconds(1)))).isFalse();

    final GraphTraversalSource g = graph.traversal();
    assertThat(g.inject(Map.of("d", date)).unfold().is(P.eq(Map.entry("d", local))).count().next()).isEqualTo(1L);
    assertThat(g.inject(Map.of("d", date)).unfold().is(P.neq(Map.entry("d", local))).count().next()).isZero();
  }

  @Test
  void comparableFollowsTinkerPop() {
    assertThat(GremlinValueComparator.comparable(Double.NaN, 1)).isFalse();
    assertThat(GremlinValueComparator.comparable(List.of(1, "a"), List.of(1, 2))).isFalse();
    assertThat(GremlinValueComparator.comparable(List.of(1, 2), List.of(1L, 2.5))).isTrue();
    assertThat(GremlinValueComparator.comparable(Set.of(1), Set.of("a"))).isFalse();
    assertThat(GremlinValueComparator.comparable(Map.of("a", 1), Map.of("a", 2L))).isTrue();
    // An array is a list, as in TinkerPop: it orders and is equal element by element (was: compared by identity)
    assertThat(GremlinValueComparator.comparable(new int[] { 1, 2 }, List.of(1, 2))).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.compare(new int[] { 1, 2 }, List.of(1, 3))).isNegative();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(new int[] { 1, 2 }, List.of(1, 2))).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(new byte[] { 1, 2 }, new byte[] { 1, 2 })).isTrue();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(new byte[] { 1, 2 }, new byte[] { 1, 3 })).isFalse();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(new int[] { 1, 2 }, List.of(1, 2, 3))).isFalse();
  }

  /**
   * {@code comparable()} sorts the contents of a Set or a Map before it tests them. A value whose {@code compareTo()} raises
   * there must still reduce to false for {@code eq}, as it did when only the comparator sorted them.
   */
  @Test
  void eqStaysFalseWhenSortingTheContentsRaises() {
    final Set<Object> first = Set.of(new RaisingComparable(1), new RaisingComparable(2));
    final Set<Object> second = Set.of(new RaisingComparable(3), new RaisingComparable(4));
    assertThat(GremlinValueComparator.COMPARABILITY.equals(first, second)).isFalse();
    assertThat(GremlinValueComparator.COMPARABILITY.equals(Map.of(new RaisingComparable(1), 1, new RaisingComparable(2), 2),
        Map.of(new RaisingComparable(3), 1, new RaisingComparable(4), 2))).isFalse();

    final GraphTraversalSource g = graph.traversal();
    assertThat(g.inject(first).is(P.eq(second)).count().next()).isZero();
    assertThat(g.inject(first).is(P.neq(second)).count().next()).isEqualTo(1L);
  }

  private record RaisingComparable(int id) implements Comparable<RaisingComparable> {
    @Override
    public int compareTo(final RaisingComparable other) {
      throw new IllegalStateException("not ordered");
    }
  }
}
