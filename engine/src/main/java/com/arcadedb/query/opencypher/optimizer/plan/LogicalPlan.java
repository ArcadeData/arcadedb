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
      if (variable != null) {
        nodeVars[i] = variable;
        if (!nodes.containsKey(variable)) {
          nodes.put(variable, new LogicalNode(variable, np.getLabels(), np.getProperties(), np.isLabelDisjunction()));
        }
      } else {
        nodeVars[i] = "  __anon" + anonNodeCounter++;
      }
      patternNodes.putIfAbsent(nodeVars[i],
          nodes.getOrDefault(nodeVars[i],
              new LogicalNode(nodeVars[i], np.getLabels(), np.getProperties(), np.isLabelDisjunction())));
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
          null,
          clauseIndex
      );
      relationships.add(logicalRel);
    }
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

    for (final Map.Entry<String, LogicalNode> entry : patternNodes.entrySet()) {
      final String variable = entry.getKey();
      final Map<String, Object> properties = entry.getValue().getProperties();
      if (properties == null || properties.isEmpty() || !isBoundByPlan(variable))
        continue;

      // Sorted so the same query always yields the same predicate order, and so the same plan
      for (final String property : new TreeSet<>(properties.keySet()))
        predicates.add(new ComparisonExpression(new PropertyAccessExpression(variable, property),
            ComparisonExpression.Operator.EQUALS, toExpression(properties.get(property))));
    }

    if (predicates.isEmpty())
      return;

    BooleanExpression conjunction = predicates.getFirst();
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
