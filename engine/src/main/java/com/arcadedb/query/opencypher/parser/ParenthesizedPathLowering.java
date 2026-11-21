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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import com.arcadedb.query.opencypher.rewriter.InlineNodeWhereHoister;
import org.antlr.v4.runtime.tree.ParseTree;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Lowers a parenthesized path pattern into what the executor runs: an unquantified one is spliced into the pattern
 * around it, its WHERE carried along, and a quantified one becomes a variable-length relationship where its inner
 * pattern is one the executor can repeat. A shape that cannot be lowered is rejected here.
 * <p>
 * Split out of {@link CypherASTBuilder}, the visitor that builds a statement from its parse tree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ParenthesizedPathLowering {
  private final CypherASTBuilder         ast;
  private final BooleanExpressionBuilder booleans;

  ParenthesizedPathLowering(final CypherASTBuilder ast, final BooleanExpressionBuilder booleans) {
    this.ast = ast;
    this.booleans = booleans;
  }

  /**
   * Absorbs a GQL Quantified Path Pattern (parenthesized inner pattern + optional quantifier) into the
   * surrounding node/relationship lists (ISO/IEC 39075:2024 §15.4).
   *
   * <p>Two lowerings exist, and the shape of the inner pattern picks between them:
   * <ul>
   *   <li><b>Phase A</b> (issue #3365) - a single-relationship inner pattern with anonymous endpoints
   *       and no inner {@code WHERE} is rewritten to a plain variable-length relationship, which the
   *       existing traversal machinery executes. A one-edge group is functionally identical to
   *       {@code -[:R*]->}, so this loses nothing and keeps the faster path.</li>
   *   <li><b>Phase B</b> (issue #4531) - everything else becomes a {@link QuantifiedPathPattern}
   *       occupying one hop of the outer chain, executed by {@code QuantifiedPathStep}: multi-hop
   *       inner patterns, inner {@code WHERE} predicates evaluated per repetition, and inner
   *       variables that surface outside the group as {@code LIST<NODE>} / {@code LIST<RELATIONSHIP>}
   *       group variables.</li>
   * </ul>
   *
   * <p>Quantifier bounds are validated identically for both: an upper bound of zero, or a lower bound
   * above the upper bound, is a syntax error.
   */
  void absorbParenthesizedPath(final Cypher25Parser.ParenthesizedPathContext pCtx,
      final List<NodePattern> outerNodes, final List<RelationshipPattern> outerRels, final boolean nextIsOuterNode) {
    final PathPattern inner = ast.visitPattern(pCtx.pattern());
    if (inner.getRelationshipCount() < 1)
      throw new CommandParsingException(
          "InvalidSyntax: a quantified path pattern must contain at least one relationship");

    final BooleanExpression innerWhere = pCtx.WHERE() != null ? booleans.parsePredicate(pCtx.expression()) : null;

    // Determine quantifier (defaults to 1..1 when absent, which collapses to a plain triplet)
    final Integer min;
    final Integer max;
    if (pCtx.quantifier() != null) {
      final RelationshipPattern dummy = new RelationshipPattern(null, null, Direction.BOTH, null, null, null);
      final RelationshipPattern quantified = applyQuantifier(dummy, pCtx.quantifier());
      min = quantified.getMinHops();
      max = quantified.getMaxHops();
      if (max != null && max == 0)
        throw new CommandParsingException(
            "InvalidSyntax: quantifier upper bound must be greater than zero in quantified path pattern");
      if (min != null && min == 0 && max != null && max == 0)
        throw new CommandParsingException(
            "InvalidSyntax: zero quantifier is not allowed in quantified path pattern");
      if (min != null && max != null && min > max)
        throw new CommandParsingException(
            "InvalidSyntax: quantifier lower bound exceeds upper bound in quantified path pattern");
    } else {
      min = null;
      max = null;
    }

    final NodePattern innerStart = inner.getFirstNode();
    final NodePattern innerEnd = inner.getLastNode();

    if (canLowerToVariableLengthRelationship(inner, innerWhere)) {
      final RelationshipPattern innerRel = inner.getRelationship(0);

      // Splice the inner endpoints into the outer chain. When this is the first child in the
      // pattern, the inner start becomes the leading boundary; otherwise the previous outer
      // node already serves that role.
      if (outerNodes.isEmpty())
        outerNodes.add(innerStart);

      // Build a new variable-length relationship pattern that subsumes the grouped repetition.
      outerRels.add(new RelationshipPattern(
          innerRel.getVariable(),
          innerRel.getTypes(),
          innerRel.getDirection(),
          innerRel.getProperties().isEmpty() ? null : innerRel.getProperties(),
          innerRel.getPropertiesParameterName(),
          min,
          max,
          innerRel.getWhereExpression()));

      // Append the inner end node only when no outer nodePattern follows. If an outer
      // nodePattern comes next, it will play the role of the trailing boundary itself,
      // keeping the (nodes.size == relationships.size + 1) invariant intact.
      if (!nextIsOuterNode)
        outerNodes.add(innerEnd);
      return;
    }

    rejectUnsupportedInnerPattern(inner);

    // Phase B. The group becomes one hop; its boundary nodes carry the inner endpoints' label and
    // property constraints so the enclosing scan stays selective, but never their variables - those
    // bind to per-repetition lists inside the group, not to a single boundary vertex. The executor
    // re-checks the inner endpoint constraints for every repetition regardless.
    if (outerNodes.isEmpty())
      outerNodes.add(toBoundaryNode(innerStart));

    outerRels.add(new QuantifiedPathPattern(inner, innerWhere, min, max));

    if (!nextIsOuterNode)
      outerNodes.add(toBoundaryNode(innerEnd));
  }

  static boolean isUnquantifiedParenthesizedPath(final ParseTree child) {
    return child instanceof Cypher25Parser.ParenthesizedPathContext parenthesized && parenthesized.quantifier() == null;
  }

  /**
   * Splices a parenthesized path pattern that carries no quantifier into the enclosing chain (issue #9940). With no
   * quantifier there is exactly one repetition, so the group is the very path it wraps: its variables bind single elements,
   * not the per-repetition lists of a quantified group, and its {@code WHERE} is an ordinary predicate on them. The
   * predicate rides on the group's last node as an inline {@code WHERE}, which {@link InlineNodeWhereHoister} moves into the
   * clause {@code WHERE} like any other. The group's first node is the node written before it, when there is one
   * (juxtaposition), and {@link CypherASTBuilder#visitPatternElement} merges the node written after it into its last node.
   */
  void spliceParenthesizedPath(final Cypher25Parser.ParenthesizedPathContext pCtx, final List<NodePattern> outerNodes,
      final List<RelationshipPattern> outerRels) {
    final PathPattern inner = ast.visitPattern(pCtx.pattern());
    if (inner instanceof ShortestPathPattern)
      throw new FeatureNotImplementedException(
          "shortestPath() / allShortestPaths() inside a parenthesized path pattern is not supported");
    if (inner.hasPathVariable())
      throw new FeatureNotImplementedException("a path variable inside a parenthesized path pattern is not supported");
    if (inner.getPathMode() != null)
      throw new FeatureNotImplementedException("a path mode inside a parenthesized path pattern is not supported");

    final List<NodePattern> innerNodes = new ArrayList<>(inner.getNodes());
    if (pCtx.WHERE() != null) {
      final int last = innerNodes.size() - 1;
      final NodePattern lastNode = innerNodes.get(last);
      innerNodes.set(last, withWhere(lastNode, and(lastNode.getWhereExpression(), booleans.parsePredicate(pCtx.expression()))));
    }

    if (outerNodes.size() == outerRels.size() + 1)
      outerNodes.set(outerNodes.size() - 1, mergeJuxtaposedNodes(outerNodes.getLast(), innerNodes.getFirst()));
    else
      outerNodes.add(innerNodes.getFirst());
    for (int i = 0; i < inner.getRelationshipCount(); i++) {
      outerRels.add(inner.getRelationship(i));
      outerNodes.add(innerNodes.get(i + 1));
    }
  }

  /**
   * Two node patterns written next to each other, {@code (a)(b)}, are one node that must satisfy both. Their constraints are
   * combined; a shape that cannot be expressed as a single node pattern - two different variables, which would each have to
   * bind it, or a dynamic label, a property-map parameter or a label disjunction next to another constraint - is refused
   * rather than half applied.
   */
  static NodePattern mergeJuxtaposedNodes(final NodePattern left, final NodePattern right) {
    if (isUnconstrained(left) && left.getVariable() == null)
      return right;
    if (isUnconstrained(right) && right.getVariable() == null)
      return left;

    final String variable = left.getVariable() != null ? left.getVariable() : right.getVariable();
    if (left.getVariable() != null && right.getVariable() != null && !left.getVariable().equals(right.getVariable()))
      throw new FeatureNotImplementedException("the node patterns (" + left.getVariable() + ") and ("
          + right.getVariable() + ") written next to each other around a parenthesized path are the same node under two "
          + "variables, which is not supported; use one variable for both");
    if (left.hasDynamicLabels() || right.hasDynamicLabels() || left.getPropertiesParameterName() != null
        || right.getPropertiesParameterName() != null || left.isLabelDisjunction() || right.isLabelDisjunction())
      throw new FeatureNotImplementedException("combining a dynamic label, a property-map parameter or a "
          + "label disjunction with the constraints of the adjacent node of a parenthesized path is not supported");

    final List<String> labels = new ArrayList<>(left.getLabels());
    for (final String label : right.getLabels())
      if (!labels.contains(label))
        labels.add(label);

    final Map<String, Object> properties = new LinkedHashMap<>(left.getProperties());
    for (final Map.Entry<String, Object> entry : right.getProperties().entrySet())
      if (properties.putIfAbsent(entry.getKey(), entry.getValue()) != null)
        throw new FeatureNotImplementedException("the property '" + entry.getKey()
            + "' is constrained by both node patterns written next to each other around a parenthesized path, which is not "
            + "supported; constrain it once");

    return new NodePattern(variable, labels, null,
        left.hasExplicitProperties() || right.hasExplicitProperties() ? properties : null, null, false,
        and(left.getWhereExpression(), right.getWhereExpression()));
  }

  private static boolean isUnconstrained(final NodePattern node) {
    return !node.hasLabels() && !node.hasDynamicLabels() && !node.hasProperties() && !node.hasWhereExpression();
  }

  private static NodePattern withWhere(final NodePattern node, final BooleanExpression where) {
    return new NodePattern(node.getVariable(), node.getLabels(), node.getDynamicLabels(),
        node.hasExplicitProperties() ? node.getProperties() : null, node.getPropertiesParameterName(),
        node.isLabelDisjunction(), where);
  }

  private static BooleanExpression and(final BooleanExpression left, final BooleanExpression right) {
    if (left == null)
      return right;
    if (right == null)
      return left;
    return new LogicalExpression(LogicalExpression.Operator.AND, left, right);
  }

  /**
   * Returns true when the group is the Phase A shape that a plain variable-length relationship
   * already expresses exactly: one relationship, no inner {@code WHERE}, and endpoints that bind
   * nothing and constrain nothing at all.
   * <p>
   * Every endpoint attribute forces Phase B, each for its own reason:
   * <ul>
   *   <li>a <b>variable</b> - GQL binds it to one list element per repetition, which a variable-length
   *       relationship has no way to express;</li>
   *   <li>a <b>label, dynamic label or property map</b> - the constraint holds for every repetition,
   *       including the vertices <i>between</i> two repetitions, and a variable-length relationship
   *       constrains only its two ends. Lowering {@code ((:P)-[:R]->(:P))+} to {@code -[:R*]->} does not
   *       weaken the constraint, it <i>deletes</i> it: the outer boundary nodes take over both ends and
   *       the inner labels are never spliced into the chain at all;</li>
   *   <li>an inline <b>{@code WHERE}</b> - same reasoning as a property map.</li>
   * </ul>
   * What is left for Phase A is the genuinely unconstrained spelling, {@code (()-[:R]->())+}, for which
   * the two forms are interchangeable.
   */
  private static boolean canLowerToVariableLengthRelationship(final PathPattern inner, final BooleanExpression innerWhere) {
    if (innerWhere != null || inner.getRelationshipCount() != 1)
      return false;
    // A group, or a variable-length relationship, is not one hop: lowering it would overwrite its own bounds with the outer
    // quantifier's and drop the inner repetition, so it goes to Phase B, which refuses both shapes (issue #9940)
    final RelationshipPattern relationship = inner.getRelationship(0);
    if (relationship instanceof QuantifiedPathPattern || relationship.isVariableLength())
      return false;
    for (int i = 0; i < inner.getNodeCount(); i++) {
      final NodePattern node = inner.getNode(i);
      if (node.getVariable() != null && !node.getVariable().isEmpty())
        return false;
      if (node.hasWhereExpression())
        return false;
      if (node.hasLabels() || node.hasDynamicLabels() || node.hasProperties())
        return false;
    }
    return true;
  }

  /**
   * Rejects the inner shapes the repeated-subpattern executor cannot run. Both would otherwise be
   * silently mis-executed rather than reported, so they fail at parse time with the feature named.
   */
  private static void rejectUnsupportedInnerPattern(final PathPattern inner) {
    for (int i = 0; i < inner.getRelationshipCount(); i++) {
      final RelationshipPattern rel = inner.getRelationship(i);
      if (rel instanceof QuantifiedPathPattern)
        throw new FeatureNotImplementedException(
            "nesting a quantified path pattern inside another one is not supported (issue #4531)");
      if (rel.isVariableLength())
        throw new FeatureNotImplementedException(
            "a variable-length relationship inside a quantified path pattern is not supported (issue #4531)");
    }
    if (inner.hasPathVariable())
      throw new FeatureNotImplementedException(
          "a path variable inside a quantified path pattern is not supported (issue #4531)");
    // A whole-map parameter, (n $props), is not a set of equality predicates the per-repetition matcher
    // can check, and a dynamic $(expression) label resolves per row rather than per repetition. Both
    // would be silently dropped rather than applied, so name them instead.
    for (int i = 0; i < inner.getNodeCount(); i++) {
      final NodePattern node = inner.getNode(i);
      if (node.getPropertiesParameterName() != null)
        throw new FeatureNotImplementedException(
            "a parameterised property map on a node inside a quantified path pattern is not supported (issue #4531)");
      if (node.hasDynamicLabels())
        throw new FeatureNotImplementedException(
            "dynamic labels on a node inside a quantified path pattern are not supported (issue #4531)");
    }
    for (int i = 0; i < inner.getRelationshipCount(); i++)
      if (inner.getRelationship(i).getPropertiesParameterName() != null)
        throw new FeatureNotImplementedException(
            "a parameterised property map on a relationship inside a quantified path pattern is not supported (issue #4531)");
  }

  /**
   * Derives the outer boundary node for a group endpoint: the endpoint's labels and inline properties
   * are kept so the enclosing scan can use them, its variable and inline {@code WHERE} are dropped so
   * the group variable is not also bound to a single vertex.
   */
  private static NodePattern toBoundaryNode(final NodePattern innerEndpoint) {
    if (innerEndpoint.getVariable() == null && !innerEndpoint.hasWhereExpression())
      return innerEndpoint;
    return new NodePattern(null, innerEndpoint.getLabels(), innerEndpoint.getDynamicLabels(),
        innerEndpoint.hasExplicitProperties() ? innerEndpoint.getProperties() : null,
        innerEndpoint.getPropertiesParameterName(), innerEndpoint.isLabelDisjunction(), null);
  }

  /**
   * Apply a Cypher 25 quantifier ({@code +}, {@code *}, {@code {n}}, {@code {n,m}}) to a
   * relationship pattern, returning a new pattern with minHops/maxHops populated.
   */
  RelationshipPattern applyQuantifier(final RelationshipPattern rel, final Cypher25Parser.QuantifierContext q) {
    final Integer min;
    final Integer max;
    if (q.PLUS() != null) {
      min = 1;
      max = null;
    } else if (q.TIMES() != null) {
      min = 0;
      max = null;
    } else if (q.from != null || q.to != null) {
      min = q.from != null ? Integer.parseInt(q.from.getText()) : 0;
      max = q.to != null ? Integer.parseInt(q.to.getText()) : null;
    } else {
      final int exact = Integer.parseInt(q.UNSIGNED_DECIMAL_INTEGER(0).getText());
      min = exact;
      max = exact;
    }
    return new RelationshipPattern(rel.getVariable(), rel.getTypes().isEmpty() ? null : rel.getTypes(), rel.getDirection(),
        rel.getProperties().isEmpty() ? null : rel.getProperties(), rel.getPropertiesParameterName(), min, max);
  }
}
