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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.executor.steps.QuantifiedPathStep;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.schema.DocumentType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Decides which hops of a MATCH clause could match the same relationship, which is what relationship uniqueness has to
 * keep apart: two hops whose edge types, endpoint labels or endpoint property values can never coincide need no tracking,
 * and a clause none of whose hops can share an edge needs none at all.
 * <p>
 * Split out of {@link CypherExecutionPlan}, whose step builders and count detectors ask it before paying for the tracking.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class HopEdgeTrackingAnalyzer {
  private final DatabaseInternal    database;
  private final Map<String, Object> parameters;

  HopEdgeTrackingAnalyzer(final DatabaseInternal database, final Map<String, Object> parameters) {
    this.database = database;
    this.parameters = parameters;
  }

  /**
   * Determines which hops of one comma-separated pattern part have to bind their edge so that Cypher's
   * relationship uniqueness can be enforced.
   * <p>
   * Uniqueness is scoped to the whole MATCH clause, not to one pattern part: two parts separated by a comma
   * are one pattern and may never bind the same relationship (two separate MATCH clauses may - that is the
   * documented difference between the comma and a second MATCH). The check itself is a comparison against the
   * edges the row already carries, so a hop that is never asked to bind its edge is invisible to it. Scoping
   * this analysis to a single part therefore did not merely miss an optimisation, it dropped the constraint:
   * a part that cannot collide with itself - a single hop, typically - bound nothing, and the hop of a later
   * part had nothing to compare against (issue #6310).
   * <p>
   * A hop is left free - which is what keeps it on the GAV/CSR fast path - only when no other hop in the
   * clause can be the same physical edge:
   * <ul>
   *   <li>the only hop in the clause has nothing to collide with;</li>
   *   <li>two hops whose edge types are disjoint in the schema are always different edges. An edge has
   *   exactly one type, so a hop's type list is a disjunction: the pair is disjoint only when every
   *   combination is. Subtyping counts as overlap - {@code [:BASE]} and {@code [:SUB]} both match a
   *   {@code SUB} edge;</li>
   *   <li>two hops whose OUT or IN endpoint patterns are type-disjoint cannot be the same edge either,
   *   whatever their types.</li>
   * </ul>
   *
   * @param clausePatterns the comma-separated pattern parts of the MATCH clause being planned
   * @param patternIndex   index in {@code clausePatterns} of the part being planned
   *
   * @return boolean array, true at index i if hop i of that part must bind its edge
   */
  boolean[] computeHopEdgeTrackingNeeds(final List<PathPattern> clausePatterns, final int patternIndex) {
    final PathPattern pathPattern = clausePatterns.get(patternIndex);
    final boolean[] needs = new boolean[pathPattern.getRelationshipCount()];

    for (int i = 0; i < needs.length; i++) {
      for (int p = 0; p < clausePatterns.size() && !needs[i]; p++) {
        final PathPattern other = clausePatterns.get(p);
        for (int j = 0; j < other.getRelationshipCount(); j++) {
          if (p == patternIndex && i == j)
            continue;
          if (hopsCanMatchTheSameEdge(pathPattern, i, other, j)) {
            needs[i] = true;
            break;
          }
        }
      }
    }
    return needs;
  }

  /**
   * Whether any two hops of one MATCH clause, comma separated parts included, could bind the same physical edge.
   * <p>
   * The count push-downs multiply adjacency counts, which counts a relationship once per hop that reaches it. openCypher
   * requires the relationships of one MATCH clause to be distinct, so the two only agree when no pair of hops can share an
   * edge: a self loop, or two hops between the same pair of vertices, lets one relationship serve both (issue #9485).
   * Conservative like {@link #hopsCanMatchTheSameEdge}: the schema has to prove two hops apart, by edge types or by
   * the labels of their endpoints, for the push-down to go ahead. Separate MATCH clauses share nothing, so each is asked
   * on its own.
   */
  boolean clauseHopsMayShareAnEdge(final MatchClause matchClause) {
    if (!matchClause.hasPathPatterns())
      return false;
    final List<PathPattern> patterns = matchClause.getPathPatterns();
    for (int p = 0; p < patterns.size(); p++)
      for (final boolean needed : computeHopEdgeTrackingNeeds(patterns, p))
        if (needed)
          return true;
    return false;
  }

  /**
   * Whether two hops of the same MATCH clause could bind the same physical edge, and so have to be compared
   * against each other for relationship uniqueness. Conservative: it answers yes unless the schema proves
   * otherwise, because a wrong no silently returns rows Cypher forbids.
   */
  boolean hopsCanMatchTheSameEdge(final PathPattern patternI, final int hopI, final PathPattern patternJ,
      final int hopJ) {
    final RelationshipPattern relI = patternI.getRelationship(hopI);
    final RelationshipPattern relJ = patternJ.getRelationship(hopJ);

    // A hop capped at zero edges - [*0..0] - binds none, so it has nothing to share with anybody.
    if (bindsNoEdge(relI) || bindsNoEdge(relJ))
      return false;

    // A GQL quantified group (issue #4531) binds whatever its inner pattern's own hops bind, but the
    // synthetic hop standing in for it carries PLACEHOLDER types and direction, not the inner ones -
    // neither test below can say anything true about it. Answer conservatively here rather than let
    // those placeholders decide it indirectly, which is what QuantifiedPathPattern's class javadoc asks
    // of every loop over getRelationships(). This is load-bearing: a "no" would let
    // computeHopEdgeTrackingNeeds drop the sibling hop's relationship variable, and without it on the
    // row QuantifiedPathStep#collectBoundRelationships cannot see that edge and the group would reuse
    // it, breaking relationship isomorphism.
    if (relI instanceof QuantifiedPathPattern || relJ instanceof QuantifiedPathPattern)
      return true;

    if (relI.hasTypes() && relJ.hasTypes() && edgeTypesAreDisjoint(relI.getTypes(), relJ.getTypes()))
      return false;

    return !areHopsDisjointByEndpointLabels(patternI, hopI, patternJ, hopJ);
  }

  /**
   * Whether no edge in the schema can satisfy both type lists. An edge carries exactly one type, so each list
   * is a disjunction and the two are disjoint only when every pair of alternatives is.
   */
  private boolean edgeTypesAreDisjoint(final List<String> typesI, final List<String> typesJ) {
    for (final String typeI : typesI)
      for (final String typeJ : typesJ)
        if (!labelsAreTypeDisjoint(typeI, typeJ))
          return false;
    return true;
  }

  /**
   * Every relationship variable a MATCH clause writes, whether or not this clause is the one that binds it.
   * <p>
   * The other half of the uniqueness scope, alongside the variables the clause binds. A relationship variable
   * an earlier clause already bound - a {@code CALL}'s yield, a {@code WITH} - is still one of this clause's
   * relationship patterns when the clause names it again, so it is a co-participant the clause's other
   * patterns must be distinct from. The freshly-bound set alone cannot say so: it deliberately omits an
   * already-bound name, because {@code OPTIONAL MATCH} nulls what it lists and must not null a carried
   * binding.
   */
  static Set<String> clauseRelationshipVariables(final MatchClause matchClause) {
    if (!matchClause.hasPathPatterns())
      return Set.of();
    Set<String> variables = null;
    for (final PathPattern pathPattern : matchClause.getPathPatterns())
      for (final RelationshipPattern relationship : pathPattern.getRelationships()) {
        final String variable = relationship.getVariable();
        if (variable != null && !variable.isEmpty()) {
          if (variables == null)
            variables = new HashSet<>();
          variables.add(variable);
        }
        if (relationship instanceof QuantifiedPathPattern quantified) {
          if (variables == null)
            variables = new HashSet<>();
          // A group's variables are its node ones as well as its relationship ones, and both are taken, exactly
          // as the clause's own variable set takes them. Nothing is read from a name the row holds a vertex
          // under: every consumer of this set asks whether the value is an Edge first.
          variables.addAll(quantified.getGroupVariables());
        }
      }
    return variables == null ? Set.of() : variables;
  }

  /**
   * Checks if two hops of the same MATCH clause - each one identified by its pattern part and its index
   * inside it, so the pair may straddle a comma - are guaranteed to match different physical edges based
   * on the vertex labels at their edge endpoints.
   * <p>
   * An edge has exactly one OUT vertex and one IN vertex. If we can prove that the OUT vertex
   * (or IN vertex) for hop i must be of a different type than for hop j, the edges are distinct.
   * <p>
   * Uses the pattern direction to map pattern nodes to edge OUT/IN endpoints:
   * <ul>
   *   <li>OUT direction: edge OUT = source node (node[i]), edge IN = target node (node[i+1])</li>
   *   <li>IN direction: edge OUT = target node (node[i+1]), edge IN = source node (node[i])</li>
   *   <li>BOTH direction: cannot determine mapping → conservative (not disjoint)</li>
   *   <li>a hop that can span more than one edge: the mapping describes the whole path, not one edge
   *   → conservative (not disjoint)</li>
   * </ul>
   */
  private boolean areHopsDisjointByEndpointLabels(final PathPattern patternI, final int hopI,
      final PathPattern patternJ, final int hopJ) {
    final RelationshipPattern relI = patternI.getRelationship(hopI);
    final RelationshipPattern relJ = patternJ.getRelationship(hopJ);

    // A variable-length hop's endpoint patterns constrain its first and last edge only: everything in
    // between starts and ends at a node the pattern says nothing about, so proving the declared endpoints
    // disjoint proves nothing about those edges. Only a hop pinned to a single edge can be argued about
    // this way; the type proof above stays valid, since every edge of the hop must match its type list.
    if (spansMoreThanOneEdge(relI) || spansMoreThanOneEdge(relJ))
      return false;

    final Direction dirI = relI.getDirection();
    final Direction dirJ = relJ.getDirection();

    // BOTH direction: can't determine which node is the edge's OUT/IN vertex
    if (dirI == Direction.BOTH || dirJ == Direction.BOTH)
      return false;

    // Map pattern nodes to edge endpoints based on direction
    final NodePattern edgeOutI = dirI == Direction.OUT ? patternI.getNode(hopI) : patternI.getNode(hopI + 1);
    final NodePattern edgeOutJ = dirJ == Direction.OUT ? patternJ.getNode(hopJ) : patternJ.getNode(hopJ + 1);
    final NodePattern edgeInI = dirI == Direction.OUT ? patternI.getNode(hopI + 1) : patternI.getNode(hopI);
    final NodePattern edgeInJ = dirJ == Direction.OUT ? patternJ.getNode(hopJ + 1) : patternJ.getNode(hopJ);

    // If the OUT vertex labels are type-disjoint, or their inline properties are, the edges are different
    if (nodeLabelsAreTypeDisjoint(edgeOutI, edgeOutJ) || nodePropertiesAreDisjoint(edgeOutI, edgeOutJ))
      return true;

    // If the IN vertex labels are type-disjoint, or their inline properties are, the edges are different
    return nodeLabelsAreTypeDisjoint(edgeInI, edgeInJ) || nodePropertiesAreDisjoint(edgeInI, edgeInJ);
  }

  /**
   * Whether no vertex can match both node patterns because their inline property maps give one key two different literal
   * values: {@code (c:Message {kind: 'Comment'})} and {@code (p:Message {kind: 'Post'})} are never one vertex, which their
   * labels cannot prove (issue #9608). Only values whose match is plain equality are compared - strings, booleans and
   * integers - since {@code 1} and {@code 1.0} match the same stored value and a temporal compares by instant. Both
   * literals are matched against the one value a vertex stores, whatever type the schema declares for it, so a value that
   * equals one of them cannot equal the other. Two literals of different kinds ({@code 1} and {@code '1'}) prove nothing:
   * a declared type may convert what is stored, so they are never taken as disjoint.
   * <p>
   * A parameter counts as the value it is bound to: this plan is built for one execution and its bindings, so the proof holds
   * for every row it produces. That keeps the proof for a query whose literals the statement cache extracted into parameters
   * (issue #8307), and extends it to the parameters a caller writes.
   */
  private boolean nodePropertiesAreDisjoint(final NodePattern node1, final NodePattern node2) {
    if (node1.getProperties().isEmpty() || node2.getProperties().isEmpty())
      return false;
    for (final Map.Entry<String, Object> entry : node1.getProperties().entrySet()) {
      final Object other = node2.getProperties().get(entry.getKey());
      if (other != null && literalsNeverMatchTheSameValue(constantValue(entry.getValue()), constantValue(other)))
        return true;
    }
    return false;
  }

  /**
   * The constant an inline property value is - a literal, or a parameter of this execution - and null when it is an expression
   * evaluated at run time.
   */
  private Object constantValue(final Object value) {
    if (value instanceof LiteralExpression literal)
      return literal.getValue();
    if (value instanceof ParameterExpression parameter)
      return parameters != null ? parameters.get(parameter.getParameterName()) : null;
    if (value instanceof CypherASTBuilder.ParameterReference parameter)
      return parameters != null ? parameters.get(parameter.getName()) : null;
    return value instanceof Expression ? null : value;
  }

  /**
   * Whether no stored value can match both literals. Two strings must differ ignoring case: a node seek through a
   * case-insensitive index answers {@code 'A'} and {@code 'a'} with the same vertices, so only a difference no collation
   * folds away is a proof. The index folds the whole key with {@code toLowerCase(Locale.ROOT)}, which differs from the
   * per-character {@code equalsIgnoreCase} on a few characters ({@code 'İ'} lowers to two), so both have to tell the two
   * strings apart.
   */
  private static boolean literalsNeverMatchTheSameValue(final Object a, final Object b) {
    if (a instanceof String sa && b instanceof String sb)
      return !sa.equalsIgnoreCase(sb) && !sa.toLowerCase(Locale.ROOT).equals(sb.toLowerCase(Locale.ROOT));
    if (a instanceof Boolean && b instanceof Boolean)
      return !a.equals(b);
    return isIntegral(a) && isIntegral(b) && ((Number) a).longValue() != ((Number) b).longValue();
  }

  private static boolean isIntegral(final Object value) {
    return value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte;
  }

  /**
   * The patterns with every {@code v.key = literal} (or {@code = $parameter}) conjunct of a {@code WHERE} written into the inline property map of the
   * nodes of {@code v}, for {@link #clauseHopsMayShareAnEdge} to read (issue #9608). The conjunct excludes the vertices the
   * inline entry would, and the proof reads inline maps only. Never handed to an operator, which applies the conjunct itself.
   */
  static List<PathPattern> withWhereEqualities(final List<PathPattern> patterns, final WhereClause where) {
    if (where == null || where.getConditionExpression() == null)
      return patterns;
    // the first equality of a key is kept: a second one with another value matches nothing, and a proof it misses only
    // leaves the shape to the row pipeline
    final Map<String, Map<String, Object>> equalities = new HashMap<>();
    for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(where.getConditionExpression())) {
      if (!(conjunct instanceof ComparisonExpression comparison) || comparison.getOperator() != ComparisonExpression.Operator.EQUALS)
        continue;
      final Expression left = comparison.getLeft();
      final Expression right = comparison.getRight();
      if (left instanceof PropertyAccessExpression property && (right instanceof LiteralExpression || right instanceof ParameterExpression))
        equalities.computeIfAbsent(property.getVariableName(), k -> new LinkedHashMap<>()).putIfAbsent(property.getPropertyName(), right);
      else if (right instanceof PropertyAccessExpression property && (left instanceof LiteralExpression
          || left instanceof ParameterExpression))
        equalities.computeIfAbsent(property.getVariableName(), k -> new LinkedHashMap<>()).putIfAbsent(property.getPropertyName(), left);
    }
    if (equalities.isEmpty())
      return patterns;

    final List<PathPattern> result = new ArrayList<>(patterns.size());
    for (final PathPattern pattern : patterns) {
      final List<NodePattern> nodes = new ArrayList<>(pattern.getNodes().size());
      for (final NodePattern node : pattern.getNodes()) {
        final Map<String, Object> written = node.getVariable() != null ? equalities.get(node.getVariable()) : null;
        // the copy below carries no parameter map nor dynamic label, so a node holding one stays as written
        if (written == null || node.getPropertiesParameterName() != null || node.hasDynamicLabels()) {
          nodes.add(node);
          continue;
        }
        // an entry the node already writes stays as written: a different value matches nothing either way
        final Map<String, Object> properties = new LinkedHashMap<>(written);
        properties.putAll(node.getProperties());
        nodes.add(new NodePattern(node.getVariable(), node.getLabels(), null, properties, null, node.isLabelDisjunction(),
            node.getWhereExpression()));
      }
      result.add(new PathPattern(nodes, pattern.getRelationships()));
    }
    return result;
  }

  /** Whether the hop is pinned to zero edges, and so binds none at all. */
  private static boolean bindsNoEdge(final RelationshipPattern rel) {
    final Integer maxHops = rel.getMaxHops();
    return rel.isVariableLength() && maxHops != null && maxHops == 0;
  }

  /** Whether the hop can bind more than one edge, so its declared endpoints are not one edge's endpoints. */
  private static boolean spansMoreThanOneEdge(final RelationshipPattern rel) {
    if (!rel.isVariableLength())
      return false;
    final Integer maxHops = rel.getMaxHops();
    return maxHops == null || maxHops > 1;
  }

  /**
   * Checks whether two node patterns have labels that are type-disjoint in the schema,
   * meaning no vertex can match both patterns simultaneously.
   * <p>
   * Two labels are disjoint if neither is a supertype/subtype of the other AND no type
   * in the schema is a subtype of both (which would allow a vertex to match both labels).
   * <p>
   * A node can write more than one label, and this is the one place where reading only the first of
   * them can go wrong in <em>either</em> direction, because the answer is a proof rather than a
   * filter: the caller uses it to conclude that two hops cannot be the same edge, so a wrong "yes"
   * drops matches (issue #6322). The two spellings are therefore taken apart:
   * <ul>
   *   <li>{@code (a:A:B)} requires both, so proving <em>any</em> of its labels disjoint from any of
   *   the other node's requirements proves the nodes disjoint;</li>
   *   <li>{@code (a:A|B)} requires either, so the pattern is only disjoint when <em>every</em>
   *   alternative is.</li>
   * </ul>
   * Reading the first label alone answers the conjunction too narrowly, which merely misses an
   * opportunity, and the disjunction too widely, which is the wrong answer.
   */
  private boolean nodeLabelsAreTypeDisjoint(final NodePattern node1, final NodePattern node2) {
    if (!node1.hasLabels() || !node2.hasLabels())
      return false; // Without labels, can't prove disjointness

    // Every alternative of node1 must be disjoint from every alternative of node2. A conjunction is
    // a single alternative listing all its labels; a disjunction is one alternative per label.
    for (final List<String> alternative1 : labelAlternatives(node1))
      for (final List<String> alternative2 : labelAlternatives(node2))
        if (!alternativesAreTypeDisjoint(alternative1, alternative2))
          return false;

    return true;
  }

  /**
   * The label sets a node pattern accepts: one set holding every label for a conjunction, one
   * single-label set per alternative for a disjunction.
   */
  private static List<List<String>> labelAlternatives(final NodePattern node) {
    if (!node.isLabelDisjunction())
      return List.of(node.getLabels());

    final List<List<String>> alternatives = new ArrayList<>(node.getLabels().size());
    for (final String label : node.getLabels())
      alternatives.add(List.of(label));
    return alternatives;
  }

  /**
   * Whether two conjunctions of labels cannot be satisfied by the same vertex, which holds as soon as
   * one label of the first is type-disjoint from one label of the second.
   */
  private boolean alternativesAreTypeDisjoint(final List<String> labels1, final List<String> labels2) {
    for (final String label1 : labels1)
      for (final String label2 : labels2)
        if (labelsAreTypeDisjoint(label1, label2))
          return true;
    return false;
  }

  /** Whether no vertex in the schema can carry both labels. */
  private boolean labelsAreTypeDisjoint(final String label1, final String label2) {
    if (label1.equals(label2))
      return false; // Same label → not disjoint

    if (!database.getSchema().existsType(label1) || !database.getSchema().existsType(label2))
      return false; // Unknown type → conservative

    final DocumentType type1 = database.getSchema().getType(label1);
    final DocumentType type2 = database.getSchema().getType(label2);

    // Direct hierarchy check: if one extends the other, not disjoint
    if (type1.instanceOf(label2) || type2.instanceOf(label1))
      return false;

    // Check for common subtypes: if any type extends both, a vertex could match both labels
    for (final DocumentType schemaType : database.getSchema().getTypes())
      if (schemaType.instanceOf(label1) && schemaType.instanceOf(label2))
        return false;

    return true;
  }
}
