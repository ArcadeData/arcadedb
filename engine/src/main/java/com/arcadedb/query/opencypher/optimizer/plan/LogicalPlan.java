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
package com.arcadedb.query.opencypher.optimizer.plan;

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.WhereClause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Logical plan extracted from Cypher AST.
 * Represents "what" the query does in a normalized form suitable for optimization.
 *
 * Contains:
 * - Nodes (variables, labels, property filters)
 * - Relationships (source, target, types, direction)
 * - WHERE filters
 * - RETURN expressions
 * - ORDER BY, LIMIT, SKIP
 */
public class LogicalPlan {
  private final CypherStatement statement;
  private final Map<String, LogicalNode> nodes;
  /**
   * Every node written in a pattern, anonymous ones included, keyed by the variable the plan binds it
   * to. {@link #nodes} holds only the nodes the query named, because the anchor selector and the
   * count push-down depend on that; the constraints of an anonymous node still have to be applied,
   * and this map is where they are found.
   */
  private final Map<String, LogicalNode> patternNodes;
  private final List<LogicalRelationship> relationships;
  private final List<WhereClause> whereFilters;
  /**
   * The inline property map of every node occurrence, paired with the plan variable it constrains,
   * in the order the patterns wrote them. Kept alongside the merged {@link #patternNodes} because a
   * variable written twice carries two maps and both are predicates on the same vertex.
   */
  private final List<Map.Entry<String, Map<String, Object>>> inlinePropertyOccurrences = new ArrayList<>();
  /** Variables whose merged label set combines a disjunction with a further label - see {@link #hasRepresentableLabelSets()}. */
  private final Set<String> mixedLabelDisjunctions = new HashSet<>();
  private final ReturnClause returnClause;
  private int anonNodeCounter = 0;

  private LogicalPlan(final CypherStatement statement) {
    this.statement = statement;
    this.nodes = new HashMap<>();
    this.patternNodes = new LinkedHashMap<>();
    this.relationships = new ArrayList<>();
    this.whereFilters = new ArrayList<>();
    this.returnClause = statement.getReturnClause();
  }

  /**
   * Package-private constructor for testing.
   * Allows tests to create LogicalPlan with pre-populated nodes.
   *
   * @param nodes nodes to include in the plan
   */
  LogicalPlan(final Map<String, LogicalNode> nodes) {
    this.statement = null;
    this.nodes = new HashMap<>(nodes);
    this.patternNodes = new LinkedHashMap<>(nodes);
    this.relationships = new ArrayList<>();
    this.whereFilters = new ArrayList<>();
    this.returnClause = null;
  }

  /**
   * Creates a LogicalPlan for testing purposes with pre-populated nodes.
   * This factory method is intended for unit tests only.
   *
   * @param nodes nodes to include in the plan
   * @return logical plan
   */
  public static LogicalPlan forTesting(final Map<String, LogicalNode> nodes) {
    return new LogicalPlan(nodes);
  }

  /**
   * Extracts a logical plan from a Cypher AST.
   *
   * @param statement the Cypher statement
   * @return logical plan
   */
  public static LogicalPlan fromAST(final CypherStatement statement) {
    final LogicalPlan plan = new LogicalPlan(statement);
    plan.extractPatterns();
    plan.extractFilters();
    plan.lowerInlinePropertiesToFilters();
    return plan;
  }

  /**
   * Extracts node and relationship patterns from MATCH clauses.
   */
  private void extractPatterns() {
    final List<MatchClause> matchClauses = statement.getMatchClauses();
    if (matchClauses == null || matchClauses.isEmpty()) {
      return;
    }

    for (int clauseIndex = 0; clauseIndex < matchClauses.size(); clauseIndex++) {
      final MatchClause matchClause = matchClauses.get(clauseIndex);
      if (!matchClause.hasPathPatterns()) {
        continue; // Phase 1 queries without parsed patterns
      }

      for (final PathPattern pathPattern : matchClause.getPathPatterns()) {
        extractPathPattern(pathPattern, clauseIndex);
      }
    }
  }

  /**
   * Extracts nodes and relationships from a single path pattern.
   * Anonymous nodes (no variable) receive a synthetic internal name so that
   * consecutive hops can refer to the same intermediate vertex, but they are
   * NOT added to the nodes map. Only named nodes go into the map so that the
   * AnchorSelector keeps its existing behavior (anonymous-only patterns fall
   * back via an empty nodes map, which is relied on by count push-down).
   * The synthetic prefix "  __anon" (two leading spaces) mirrors the convention
   * in CypherExecutionPlan and cannot collide with user-defined variable names.
   * <p>
   * A variable written more than once - across comma-separated patterns or across MATCH clauses -
   * names one vertex that has to satisfy <em>every</em> occurrence, so the occurrences are merged
   * rather than the first one kept (issue #6322).
   */
  private void extractPathPattern(final PathPattern pathPattern, final int clauseIndex) {
    final List<NodePattern> nodePatterns = pathPattern.getNodes();
    final List<RelationshipPattern> relPatterns = pathPattern.getRelationships();

    // Assign a variable name to every node.
    // Named nodes are registered in the nodes map for AnchorSelector.
    // Anonymous nodes receive a synthetic name for relationship tracking only.
    final String[] nodeVars = new String[nodePatterns.size()];
    for (int i = 0; i < nodePatterns.size(); i++) {
      final NodePattern np = nodePatterns.get(i);
      final String variable = np.getVariable();
      nodeVars[i] = variable != null ? variable : "  __anon" + anonNodeCounter++;

      final LogicalNode merged = mergeOccurrence(patternNodes.get(nodeVars[i]), nodeVars[i], np);
      patternNodes.put(nodeVars[i], merged);
      if (variable != null)
        nodes.put(variable, merged);

      // Every occurrence's own property map is lowered, not just the merged one: two occurrences
      // pinning the same property to different values are a contradiction the merged map cannot
      // hold, and dropping either half would turn an empty result into rows.
      if (np.getProperties() != null && !np.getProperties().isEmpty())
        inlinePropertyOccurrences.add(Map.entry(nodeVars[i], np.getProperties()));
    }

    // Extract relationships using the resolved (never-null) variable names.
    for (int i = 0; i < relPatterns.size(); i++) {
      final RelationshipPattern relPattern = relPatterns.get(i);
      final LogicalRelationship logicalRel = new LogicalRelationship(
          relPattern.getVariable(),
          nodeVars[i],
          nodeVars[i + 1],
          relPattern.getTypes(),
          relPattern.getDirection(),
          relPattern.getProperties(),
          relPattern.getMinHops(),
          relPattern.getMaxHops(),
          pathPattern.getEffectivePathMode(),
          clauseIndex,
          relPattern,
          pathPattern.getPathVariable()
      );
      relationships.add(logicalRel);
    }
  }

  /**
   * Folds one more occurrence of a variable into the node the plan already holds for it. Cypher
   * repeats a variable to mean the same vertex, so the constraints accumulate: the labels are ANDed
   * and the inline property maps are unioned.
   * <p>
   * The union of two disjunctions, or of a disjunction with a plain label, is not a shape a single
   * {@link LogicalNode} can express - it carries one flag for the whole list - so the variable is
   * recorded as one {@link #hasRepresentableLabelSets()} rejects, which sends the query to the
   * ordinary pipeline. The merge never drops a constraint, so no caller can under-filter on the
   * strength of it.
   */
  private LogicalNode mergeOccurrence(final LogicalNode existing, final String variable,
      final NodePattern occurrence) {
    if (existing == null)
      return new LogicalNode(variable, occurrence.getLabels(), occurrence.getProperties(),
          occurrence.isLabelDisjunction());

    final List<String> labels;
    final boolean disjunction;
    if (occurrence.getLabels() == null || occurrence.getLabels().isEmpty()) {
      labels = existing.getLabels();
      disjunction = existing.isLabelDisjunction();
    } else if (existing.getLabels().isEmpty()) {
      labels = occurrence.getLabels();
      disjunction = occurrence.isLabelDisjunction();
    } else if (existing.isLabelDisjunction() == occurrence.isLabelDisjunction()
        && sameLabelSet(existing.getLabels(), occurrence.getLabels())) {
      // The same constraint written twice - `(a:A|B)-[:R]->(b), (a:A|B)-[:S]->(c)` - intersects with itself,
      // so there is nothing to fold in and nothing about it a single node cannot express. Taking the union
      // below would reach the same list, but would also record the variable as a mixed disjunction and
      // decline a query the operators can perfectly well run.
      labels = existing.getLabels();
      disjunction = existing.isLabelDisjunction();
    } else {
      final List<String> union = new ArrayList<>(existing.getLabels());
      for (final String label : occurrence.getLabels())
        if (!union.contains(label))
          union.add(label);
      labels = union;
      disjunction = existing.isLabelDisjunction() || occurrence.isLabelDisjunction();
      if (disjunction)
        mixedLabelDisjunctions.add(variable);
    }

    final Map<String, Object> properties;
    if (occurrence.getProperties() == null || occurrence.getProperties().isEmpty()) {
      properties = existing.getProperties();
    } else if (existing.getProperties().isEmpty()) {
      properties = occurrence.getProperties();
    } else {
      // The merged map only steers an index seek and partition pruning; every occurrence's map is
      // lowered into predicates separately, so a key pinned twice keeps both comparisons.
      final Map<String, Object> union = new LinkedHashMap<>(existing.getProperties());
      occurrence.getProperties().forEach(union::putIfAbsent);
      properties = union;
    }

    // The common repeat adds nothing - `(a)-[:R]->(b), (a)-[:S]->(c)` writes `a` twice and the second
    // occurrence is bare - so the node the plan already holds is returned rather than an identical
    // copy of it. Reference comparison, because every branch above either reuses one of the two lists
    // as-is or builds a new one.
    if (labels == existing.getLabels() && properties == existing.getProperties()
        && disjunction == existing.isLabelDisjunction())
      return existing;

    return new LogicalNode(variable, labels, properties, disjunction);
  }

  /**
   * Whether two label lists name the same set. Order is not a constraint a pattern expresses -
   * {@code (a:A|B)} and {@code (a:B|A)} are one disjunction, {@code (a:A:B)} and {@code (a:B:A)} one
   * conjunction - so mutual containment rather than {@link List#equals}, which would miss the no-op and
   * decline a query for the order its two occurrences happened to be written in.
   * <p>
   * Mutual containment rather than a size check plus one direction, because a label repeated inside one
   * list ({@code (a:A:A)}) would let {@code [A, B]} and {@code [A, A]} pass as the same set and drop
   * {@code B}. Both lists are a handful of entries, so the quadratic scan is cheaper than the sets that
   * would replace it.
   */
  private static boolean sameLabelSet(final List<String> first, final List<String> second) {
    return first.containsAll(second) && second.containsAll(first);
  }

  /**
   * Returns true when every node in the pattern carries a label set the physical operators can
   * represent: exactly one label, or a disjunction the anchor scan can enumerate.
   * <p>
   * {@code (a:A:B)} - written that way, or arrived at by merging {@code (a:A)} with a later
   * {@code (a:B)} - names the composite type {@code A~B}, which a vertex labelled {@code A:B:C} does
   * not extend, so a scan of it would miss rows. A disjunction merged with any further label is not
   * expressible at all. Both send the query to the ordinary pipeline, which evaluates the labels one
   * by one (issue #6322).
   */
  public boolean hasRepresentableLabelSets() {
    if (!mixedLabelDisjunctions.isEmpty())
      return false;
    for (final LogicalNode node : patternNodes.values()) {
      if (node.getLabels().isEmpty())
        return false;
      if (node.getLabels().size() > 1 && !node.isLabelDisjunction())
        return false;
    }
    return true;
  }

  /**
   * Extracts WHERE filters from the statement.
   */
  private void extractFilters() {
    // Statement-level WHERE clause
    final WhereClause statementWhere = statement.getWhereClause();
    if (statementWhere != null) {
      whereFilters.add(statementWhere);
    }

    // MATCH-level WHERE clauses
    final List<MatchClause> matchClauses = statement.getMatchClauses();
    if (matchClauses != null) {
      for (final MatchClause matchClause : matchClauses) {
        final WhereClause matchWhere = matchClause.getWhereClause();
        if (matchWhere != null) {
          whereFilters.add(matchWhere);
        }
      }
    }
  }

  /**
   * Turns the inline property map of every pattern node into the equality predicates it stands for,
   * so the rest of the optimizer sees one representation of a filter instead of two.
   * {@code MATCH (n:Person {id: $id})} and {@code MATCH (n:Person) WHERE n.id = $id} mean the same
   * thing and now plan the same way: the anchor selector can seek an index on the property, and
   * whatever is left is applied by the Filter operator above the scan.
   * <p>
   * A node the physical plan never binds - an anonymous node in a pattern with no relationship - is
   * skipped here; the planner refuses such a statement instead, since a predicate on an unbound
   * variable would silently drop the constraint.
   */
  private void lowerInlinePropertiesToFilters() {
    final List<BooleanExpression> predicates = new ArrayList<>();

    for (final Map.Entry<String, Map<String, Object>> entry : inlinePropertyOccurrences) {
      final String variable = entry.getKey();
      final Map<String, Object> properties = entry.getValue();
      if (!isBoundByPlan(variable))
        continue;

      // Sorted so the same query always yields the same predicate order, and so the same plan
      for (final String property : new TreeSet<>(properties.keySet()))
        predicates.add(new ComparisonExpression(new PropertyAccessExpression(variable, property),
            ComparisonExpression.Operator.EQUALS, toExpression(properties.get(property))));
    }

    if (predicates.isEmpty())
      return;

    BooleanExpression conjunction = predicates.get(0);
    for (int i = 1; i < predicates.size(); i++)
      conjunction = new LogicalExpression(LogicalExpression.Operator.AND, conjunction, predicates.get(i));

    whereFilters.add(new WhereClause(conjunction));
  }

  /**
   * Returns true if the physical plan binds this variable: every named node is bound, and an
   * anonymous one only when a relationship expands into it.
   */
  private boolean isBoundByPlan(final String variable) {
    if (nodes.containsKey(variable))
      return true;
    for (final LogicalRelationship relationship : relationships)
      if (variable.equals(relationship.getSourceVariable()) || variable.equals(relationship.getTargetVariable()))
        return true;
    return false;
  }

  /**
   * Wraps an inline property value in the expression the comparison needs. The parser stores literals
   * unwrapped and parameters as a {@link CypherASTBuilder.ParameterReference}, keeping the expression
   * only for values that have to be evaluated per row.
   */
  private static Expression toExpression(final Object value) {
    if (value instanceof Expression expression)
      return expression;
    if (value instanceof CypherASTBuilder.ParameterReference parameter)
      return new ParameterExpression(parameter.getName(), "$" + parameter.getName());
    return new LiteralExpression(value, String.valueOf(value));
  }

  /**
   * Returns all nodes in the logical plan.
   */
  public Map<String, LogicalNode> getNodes() {
    return nodes;
  }

  /**
   * Returns a specific node by variable name. Anonymous nodes are not returned; use
   * {@link #getPatternNode(String)} to reach the constraints of every node the pattern wrote.
   */
  public LogicalNode getNode(final String variable) {
    return nodes.get(variable);
  }

  /**
   * Returns the node the pattern wrote for the given plan variable, whether or not the query named
   * it. Use this wherever a node's own constraints - its labels and its inline property map - have to
   * be applied, since dropping them for an anonymous node silently widens the result.
   */
  public LogicalNode getPatternNode(final String variable) {
    return patternNodes.get(variable);
  }

  /**
   * Returns every node the pattern wrote, anonymous ones included, keyed by the variable the plan
   * binds it to. An anonymous node is a legitimate starting point - {@code (:Person {id: $id})-[...]}
   * is as selective as the named spelling - so anchor selection reads this map, while everything that
   * depends on "did the query name this" keeps reading {@link #getNodes()}.
   */
  public Map<String, LogicalNode> getPatternNodes() {
    return patternNodes;
  }

  /**
   * Returns all relationships in the logical plan.
   */
  public List<LogicalRelationship> getRelationships() {
    return relationships;
  }

  /**
   * Returns all WHERE filters in the logical plan.
   */
  public List<WhereClause> getWhereFilters() {
    return whereFilters;
  }

  /**
   * Returns the RETURN clause.
   */
  public ReturnClause getReturnClause() {
    return returnClause;
  }

  /**
   * Returns the original Cypher statement.
   */
  public CypherStatement getStatement() {
    return statement;
  }

  /**
   * Returns all type names referenced in the query.
   * Used for statistics collection.
   */
  public Set<String> getReferencedTypes() {
    final Set<String> types = new HashSet<>();

    // Collect node labels
    for (final LogicalNode node : nodes.values()) {
      types.addAll(node.getLabels());
    }

    // Collect relationship types
    for (final LogicalRelationship rel : relationships) {
      types.addAll(rel.getTypes());
    }

    return types;
  }

  /**
   * Checks if a node is connected via relationships.
   */
  public boolean isNodeConnected(final String variable) {
    for (final LogicalRelationship rel : relationships) {
      if (variable.equals(rel.getSourceVariable()) || variable.equals(rel.getTargetVariable())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns relationships connected to a specific node.
   */
  public List<LogicalRelationship> getRelationshipsForNode(final String variable) {
    final List<LogicalRelationship> result = new ArrayList<>();
    for (final LogicalRelationship rel : relationships) {
      if (variable.equals(rel.getSourceVariable()) || variable.equals(rel.getTargetVariable())) {
        result.add(rel);
      }
    }
    return result;
  }

  /**
   * Checks if the plan has any graph patterns (nodes/relationships).
   */
  public boolean hasPatterns() {
    return !nodes.isEmpty() || !relationships.isEmpty();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("LogicalPlan{\n");
    sb.append("  nodes=").append(nodes.values()).append("\n");
    sb.append("  relationships=").append(relationships).append("\n");
    sb.append("  filters=").append(whereFilters.size()).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
