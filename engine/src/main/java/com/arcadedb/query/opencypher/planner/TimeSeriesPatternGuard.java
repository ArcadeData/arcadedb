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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.schema.Schema;

import java.util.List;

/**
 * Refuses, when a statement is planned, every graph pattern whose node label or relationship type names a TIMESERIES
 * type (issue #10440).
 * <p>
 * A time-series type keeps its samples in its own engine, and they are neither vertices nor edges, so no pattern
 * naming the type can match one. Before this check such a pattern answered zero rows - {@code MATCH (e:Ev) RETURN
 * count(e)} returned 0 over a series SQL counts the samples of - which reads as a correct answer about the data. The
 * patterns are found by {@link CypherExpressionWalker}, which reaches every one a statement holds: those of
 * {@code MATCH}, {@code OPTIONAL MATCH}, {@code CREATE} and {@code MERGE}, a pattern predicate, a pattern
 * comprehension, and the bodies of {@code CALL}, {@code EXISTS}, {@code COUNT} and {@code COLLECT} and of each
 * {@code UNION} branch. Checking them here, rather than where each scan is opened, also refuses the patterns no scan of
 * the type is ever opened for - a neighbour reached by an expansion, an alternative of a disjunction - and refuses
 * them whether or not a row ever reaches them.
 * <p>
 * The schema is the one the plan is built against: a cached plan was checked when it was built, and the schema change
 * that creates a time-series type invalidates the plan cache. A dynamic label ({@code (n:$(expr))}) is only known per
 * row, so the scan refuses it instead ({@link Labels#requireNotTimeSeriesType}).
 * <p>
 * A label check that stays an expression ({@code WHERE NOT n:Ev}, {@code RETURN n:Ev}) is not refused: it asks a question
 * about a node another pattern bound, and the answer - no vertex carries the label - is correct. A plain
 * {@code MATCH (n) WHERE n:Ev} is refused, because {@code LabelPredicateHoister} has already moved the label into the
 * pattern when the statement was parsed, and it is the same query as {@code MATCH (n:Ev)}.
 */
public final class TimeSeriesPatternGuard {

  private TimeSeriesPatternGuard() {
    // utility class
  }

  /**
   * @throws CommandSemanticException when a pattern of {@code statement} names a time-series type
   */
  public static void check(final Schema schema, final CypherStatement statement) {
    CypherExpressionWalker.walk(statement, new CypherExpressionWalker.Visitor() {
      @Override
      public void visit(final Expression expression) {
        // only the patterns are checked
      }

      @Override
      public void visitPattern(final PathPattern pattern) {
        final List<NodePattern> nodes = pattern.getNodes();
        if (nodes != null)
          for (int i = 0; i < nodes.size(); i++) {
            final NodePattern node = nodes.get(i);
            if (node != null)
              checkNames(schema, node.getLabels(), "label");
          }

        final List<RelationshipPattern> relationships = pattern.getRelationships();
        if (relationships != null)
          for (int i = 0; i < relationships.size(); i++) {
            final RelationshipPattern relationship = relationships.get(i);
            if (relationship != null)
              checkNames(schema, relationship.getTypes(), "relationship type");
          }
      }
    });
  }

  private static void checkNames(final Schema schema, final List<String> names, final String kind) {
    if (names == null)
      return;
    for (int i = 0; i < names.size(); i++)
      // a lookup per name: a pattern naming no time-series type, which is every pattern of a graph query, costs only that
      Labels.requireNotTimeSeriesType(schema.getTypeOrNull(names.get(i)), kind);
  }
}
