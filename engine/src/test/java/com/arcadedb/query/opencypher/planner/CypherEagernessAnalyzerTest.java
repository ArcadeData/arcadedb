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
package com.arcadedb.query.opencypher.planner;

import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.MergeClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.SetClause;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit coverage for the shape comparison behind the eager read/write barrier of issue #7171.
 * <p>
 * {@code CypherEagerWriteBarrierIssue7171Test} pins the observable behaviour through whole queries; this
 * pins the decision itself, so a change to one rule (label matching, an untyped relationship pattern,
 * skipping an already-bound variable, the aggregation and barrier resets) says which rule it broke.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherEagernessAnalyzerTest {
  private static final Set<String> NOTHING_BOUND = Set.of();

  @Test
  void aWriteWithNoReadAheadOfItNeedsNoBarrier() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();

    assertThat(analyzer.hasPendingRead()).isFalse();
    assertThat(analyzer.needsBarrier(createOf(node("Log", "x")), NOTHING_BOUND)).isFalse();
    assertThat(analyzer.needsBarrierForWriteProcedure()).isFalse();
  }

  @Test
  void creatingALabelThatIsBeingScannedConflicts() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node("Person", "n")));

    assertThat(analyzer.needsBarrier(createOf(node("Person", null)), NOTHING_BOUND)).isTrue();
    assertThat(analyzer.needsBarrier(createOf(node("Company", null)), NOTHING_BOUND)).isFalse();
  }

  @Test
  void anUnlabelledReadPatternIsFedByAnyCreatedVertex() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node(null, "n")));

    assertThat(analyzer.needsBarrier(createOf(node("Anything", null)), NOTHING_BOUND)).isTrue();
  }

  @Test
  void anUntypedRelationshipPatternIsFedByAnyCreatedEdge() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(path(node("A", "a"), relationship(null, "r"), node("B", "b"))));

    // Both endpoints are already bound, so only the edge counts - and an untyped [r] matches it.
    assertThat(analyzer.needsBarrier(mergeOf(path(node(null, "a"), relationship("rt2", null), node(null, "b"))),
        Set.of("a", "b"))).isTrue();
  }

  @Test
  void aRelationshipTypeNoPatternReadsDoesNotConflict() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(path(node("A", "a"), relationship("KNOWS", "r"), node("B", "b"))));

    assertThat(analyzer.needsBarrier(createOf(path(node(null, "a"), relationship("SCORED", null), node(null, "b"))),
        Set.of("a", "b"))).isFalse();
  }

  @Test
  void anAlreadyBoundNodeIsAReferenceNotACreation() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node("A", "a")));
    analyzer.observeRead(matchOf(node("B", "b")));

    final CreateClause create = createOf(path(node(null, "a"), relationship("R", null), node(null, "b")));
    assertThat(analyzer.needsBarrier(create, Set.of("a", "b")))
        .as("no vertex is created and R is read by no pattern")
        .isFalse();
    assertThat(analyzer.needsBarrier(create, NOTHING_BOUND))
        .as("unbound, the same unlabelled nodes would be fresh vertices")
        .isTrue();
  }

  @Test
  void aWriteProcedureConflictsWithAnyReadAtAll() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node("Person", "n")));

    assertThat(analyzer.needsBarrierForWriteProcedure()).isTrue();
  }

  @Test
  void aForeachIsWeighedByWhatItsBodyWrites() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node("Person", "n")));

    assertThat(analyzer.needsBarrier(foreachCreating(node("Person", null)), NOTHING_BOUND)).isTrue();
    assertThat(analyzer.needsBarrier(foreachCreating(node("Untouched", null)), NOTHING_BOUND)).isFalse();
    assertThat(analyzer.needsBarrier(emptyForeach(), NOTHING_BOUND)).as("an empty body writes nothing").isFalse();
  }

  /**
   * A body that only updates existing entities creates nothing, so it can add no row to an enumeration.
   * Property and label writes are a different hazard class and deliberately out of this barrier's scope -
   * see the note on {@link CypherEagernessAnalyzer}.
   */
  @Test
  void aForeachBodyThatOnlyUpdatesNeedsNoBarrier() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node("Person", "n")));

    assertThat(analyzer.needsBarrier(foreachSetting(), NOTHING_BOUND)).isFalse();
    assertThat(analyzer.needsBarrier(foreachRemoving(), NOTHING_BOUND)).isFalse();
  }

  @Test
  void aNestedForeachBodyIsWeighedToo() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node("Person", "n")));

    assertThat(analyzer.needsBarrier(foreachNesting(foreachCreating(node("Person", null))), NOTHING_BOUND)).isTrue();
  }

  @Test
  void anAggregatingWithClosesTheEnumerationsBehindIt() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node("Person", "n")));
    analyzer.observeAggregationBoundary();

    assertThat(analyzer.hasPendingRead()).isFalse();
    assertThat(analyzer.needsBarrier(createOf(node("Person", null)), NOTHING_BOUND)).isFalse();
  }

  @Test
  void aPlantedBarrierSpendsTheReadFootprint() {
    final CypherEagernessAnalyzer analyzer = new CypherEagernessAnalyzer();
    analyzer.observeRead(matchOf(node("Person", "n")));

    assertThat(analyzer.needsBarrier(createOf(node("Person", null)), NOTHING_BOUND)).isTrue();
    analyzer.observeBarrier();
    assertThat(analyzer.needsBarrier(createOf(node("Person", null)), NOTHING_BOUND))
        .as("the barrier already drained what the second write could have fed")
        .isFalse();

    analyzer.observeRead(matchOf(node("Person", "m")));
    assertThat(analyzer.needsBarrier(createOf(node("Person", null)), NOTHING_BOUND))
        .as("a later MATCH re-opens an enumeration")
        .isTrue();
  }

  private static NodePattern node(final String label, final String variable) {
    return new NodePattern(variable, label == null ? List.of() : List.of(label), Map.<String, Object>of());
  }

  private static RelationshipPattern relationship(final String type, final String variable) {
    return new RelationshipPattern(variable, type == null ? List.of() : List.of(type), Direction.OUT,
        Map.<String, Object>of(), null, null);
  }

  private static PathPattern path(final NodePattern from, final RelationshipPattern relationship,
      final NodePattern to) {
    return new PathPattern(from, relationship, to);
  }

  private static MatchClause matchOf(final NodePattern node) {
    return matchOf(new PathPattern(node));
  }

  private static MatchClause matchOf(final PathPattern pathPattern) {
    return new MatchClause(List.of(pathPattern), false);
  }

  private static CreateClause createOf(final NodePattern node) {
    return createOf(new PathPattern(node));
  }

  private static CreateClause createOf(final PathPattern pathPattern) {
    return new CreateClause(List.of(pathPattern));
  }

  private static MergeClause mergeOf(final PathPattern pathPattern) {
    return new MergeClause(pathPattern);
  }

  private static ForeachClause foreachCreating(final NodePattern node) {
    return new ForeachClause("x", new LiteralExpression(List.of(1), "[1]"),
        List.of(new ClauseEntry(ClauseEntry.ClauseType.CREATE, createOf(node), 0)));
  }

  private static ForeachClause foreachNesting(final ForeachClause nested) {
    return new ForeachClause("x", new LiteralExpression(List.of(1), "[1]"),
        List.of(new ClauseEntry(ClauseEntry.ClauseType.FOREACH, nested, 0)));
  }

  private static ForeachClause foreachSetting() {
    final SetClause set = new SetClause(
        List.of(new SetClause.SetItem("n", "touched", new LiteralExpression(true, "true"))));
    return new ForeachClause("x", new LiteralExpression(List.of(1), "[1]"),
        List.of(new ClauseEntry(ClauseEntry.ClauseType.SET, set, 0)));
  }

  private static ForeachClause foreachRemoving() {
    final RemoveClause remove = new RemoveClause(List.of(new RemoveClause.RemoveItem("n", "touched")));
    return new ForeachClause("x", new LiteralExpression(List.of(1), "[1]"),
        List.of(new ClauseEntry(ClauseEntry.ClauseType.REMOVE, remove, 0)));
  }

  private static ForeachClause emptyForeach() {
    return new ForeachClause("x", new LiteralExpression(List.of(), "[]"), List.of());
  }
}
