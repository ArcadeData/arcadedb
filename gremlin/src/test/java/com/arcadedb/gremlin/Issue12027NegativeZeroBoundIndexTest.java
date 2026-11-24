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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12027: Gremlin orders a negative zero below 0 ({@code 0 > -0.0} is true, as in {@link Double#compare}), while the index
 * of a property reads -0.0 and 0 as the same key. A strict bound at a zero was handed to the index exclusive, so
 * {@code has(k, gt(-0.0d))} through an index on an INTEGER property missed the 0 that the scan returns, and {@code lt(0)} through
 * an index on a DOUBLE property missed the -0.0. The index answer must equal the scan answer, and the bounds no zero-keyed entry
 * can satisfy ({@code gt(0)} and {@code lt(0)} on an integral key) must keep reading no entry of the zero key.
 */
class Issue12027NegativeZeroBoundIndexTest {
  private static final String[] TYPES = { "INTEGER", "LONG", "SHORT", "DOUBLE", "FLOAT" };

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue12027");
    for (final String t : TYPES)
      for (final boolean indexed : new boolean[] { false, true }) {
        final String type = "V_" + t + (indexed ? "_idx" : "_scan");
        graph.getDatabase().command("sql", "CREATE VERTEX TYPE " + type).close();
        graph.getDatabase().command("sql", "CREATE PROPERTY " + type + ".x " + t).close();
        if (indexed)
          graph.getDatabase().command("sql", "CREATE INDEX ON " + type + " (x) NOTUNIQUE").close();
        for (final Object v : valuesOf(t))
          graph.addVertex(T.label, type, "x", v);
      }
    graph.tx().commit();
  }

  private static Object[] valuesOf(final String type) {
    return switch (type) {
      case "LONG" -> new Object[] { -1L, 0L, 1L };
      case "SHORT" -> new Object[] { (short) -1, (short) 0, (short) 1 };
      case "DOUBLE" -> new Object[] { -1.0, -0.0, 0.0, 1.0 };
      case "FLOAT" -> new Object[] { -1.0f, -0.0f, 0.0f, 1.0f };
      default -> new Object[] { -1, 0, 1 };
    };
  }

  @AfterEach
  void teardown() {
    graph.drop();
  }

  private List<String> run(final String type, final P<?> predicate) {
    final List<String> result = new ArrayList<>();
    for (final Object o : graph.traversal().V().hasLabel(type).has("x", (P) predicate).values("x").toList())
      result.add(String.valueOf(o));
    result.sort(null);
    return result;
  }

  @Test
  void indexAnswersLikeTheScanForZeroBounds() {
    final P<?>[] predicates = { P.gt(-0.0), P.gte(-0.0), P.lt(-0.0), P.lte(-0.0), P.gt(0.0), P.gte(0.0), P.lt(0.0), P.lte(0.0),
        P.gt(-0.0f), P.lt(0.0f), P.gt(0), P.lt(0), P.eq(-0.0), P.eq(0.0) };
    for (final String t : TYPES)
      for (final P<?> p : predicates)
        assertThat(run("V_" + t + "_idx", p)).as(t + " " + p).isEqualTo(run("V_" + t + "_scan", p));
  }

  @Test
  void theNegativeZeroBoundStillUsesTheIndex() {
    assertThat(graph.traversal().V().hasLabel("V_INTEGER_idx").has("x", P.gt(-0.0)).explain().toString()).contains(
        "ArcadeFilterByIndexStep");
    assertThat(run("V_INTEGER_idx", P.gt(-0.0))).containsExactly("0", "1");
  }

  @Test
  void integralIndexesAnswerTheSignedZeroBounds() {
    for (final String t : new String[] { "INTEGER", "LONG", "SHORT" }) {
      final String idx = "V_" + t + "_idx";
      assertThat(run(idx, P.gt(-0.0))).as(t + " gt(-0.0)").containsExactly("0", "1");
      assertThat(run(idx, P.gt(-0.0f))).as(t + " gt(-0.0f)").containsExactly("0", "1");
      assertThat(run(idx, P.gt(0.0))).as(t + " gt(0.0)").containsExactly("1");
      assertThat(run(idx, P.gt(0))).as(t + " gt(0)").containsExactly("1");
      assertThat(run(idx, P.lt(0))).as(t + " lt(0)").containsExactly("-1");
      assertThat(run(idx, P.lt(0.0))).as(t + " lt(0.0)").containsExactly("-1");
      assertThat(run(idx, P.lt(-0.0))).as(t + " lt(-0.0)").containsExactly("-1");
    }
  }

  @Test
  void floatingIndexesAnswerTheSignedZeroBounds() {
    for (final String t : new String[] { "DOUBLE", "FLOAT" }) {
      final String idx = "V_" + t + "_idx";
      // -0.0 sorts below 0 and 0.0 in Gremlin, whereas the index holds both under one key
      assertThat(run(idx, P.lt(0))).as(t + " lt(0)").containsExactly("-0.0", "-1.0");
      assertThat(run(idx, P.lt(0.0))).as(t + " lt(0.0)").containsExactly("-0.0", "-1.0");
      assertThat(run(idx, P.lt(0.0f))).as(t + " lt(0.0f)").containsExactly("-0.0", "-1.0");
      assertThat(run(idx, P.lt(-0.0))).as(t + " lt(-0.0)").containsExactly("-1.0");
      assertThat(run(idx, P.gt(-0.0))).as(t + " gt(-0.0)").containsExactly("0.0", "1.0");
      assertThat(run(idx, P.gt(-0.0f))).as(t + " gt(-0.0f)").containsExactly("0.0", "1.0");
      assertThat(run(idx, P.gt(0))).as(t + " gt(0)").containsExactly("1.0");
      assertThat(run(idx, P.gt(0.0))).as(t + " gt(0.0)").containsExactly("1.0");
    }
  }

  /**
   * The step is a candidate generator, and a candidate costs a record load before the {@code HasStep} drops it: a bound at zero
   * that no zero-keyed entry can satisfy (the common {@code gt(0)} and {@code lt(0)} of an integral key) must not read the
   * entries of the zero key.
   */
  @Test
  void aBoundNoZeroKeyedEntryCanSatisfyReadsNoZeroKeyedCandidate() {
    for (final String t : new String[] { "INTEGER", "LONG", "SHORT" }) {
      final String idx = "V_" + t + "_idx";
      assertThat(candidatesOf(idx, P.gt(0))).as(t + " gt(0)").isEqualTo(1);
      assertThat(candidatesOf(idx, P.gt(0.0))).as(t + " gt(0.0)").isEqualTo(1);
      assertThat(candidatesOf(idx, P.lt(0))).as(t + " lt(0)").isEqualTo(1);
      assertThat(candidatesOf(idx, P.lt(0.0))).as(t + " lt(0.0)").isEqualTo(1);
      assertThat(candidatesOf(idx, P.lt(-0.0))).as(t + " lt(-0.0)").isEqualTo(1);
      // the one bound that reads the zero key: 0 is above -0.0
      assertThat(candidatesOf(idx, P.gt(-0.0))).as(t + " gt(-0.0)").isEqualTo(2);
    }
    for (final String t : new String[] { "DOUBLE", "FLOAT" }) {
      final String idx = "V_" + t + "_idx";
      assertThat(candidatesOf(idx, P.gt(0))).as(t + " gt(0)").isEqualTo(1);
      assertThat(candidatesOf(idx, P.gt(0.0))).as(t + " gt(0.0)").isEqualTo(1);
      assertThat(candidatesOf(idx, P.lt(-0.0))).as(t + " lt(-0.0)").isEqualTo(1);
      // -0.0 and 0.0 are two entries of one key: lt(0) reads the key for the -0.0 below 0, and gt(-0.0) for the 0.0 above -0.0
      assertThat(candidatesOf(idx, P.lt(0))).as(t + " lt(0)").isEqualTo(3);
      assertThat(candidatesOf(idx, P.gt(-0.0))).as(t + " gt(-0.0)").isEqualTo(3);
    }
  }

  /** The candidates the index step emits for {@code has(x, predicate)}, before the {@code HasStep} that follows it drops any. */
  private int candidatesOf(final String type, final P<?> predicate) {
    final Traversal.Admin<?, ?> traversal = graph.traversal().V().hasLabel(type).has("x", (P) predicate).asAdmin();
    traversal.applyStrategies();
    final List<ArcadeFilterByIndexStep> steps = TraversalHelper.getStepsOfClass(ArcadeFilterByIndexStep.class, traversal);
    // THE STEP MUST BE THERE, OR THE COUNT BELOW SAYS NOTHING ABOUT THE INDEX
    assertThat(steps).as(type + " " + predicate).hasSize(1);
    final ArcadeFilterByIndexStep<?, ?> step = steps.get(0);
    int candidates = 0;
    while (step.hasNext()) {
      step.next();
      ++candidates;
    }
    step.close();
    return candidates;
  }
}
