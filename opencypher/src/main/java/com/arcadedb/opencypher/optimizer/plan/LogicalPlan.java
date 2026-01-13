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
package com.arcadedb.opencypher.optimizer.plan;

import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.opencypher.ast.MatchClause;
import com.arcadedb.opencypher.ast.NodePattern;
import com.arcadedb.opencypher.ast.PathPattern;
import com.arcadedb.opencypher.ast.RelationshipPattern;
import com.arcadedb.opencypher.ast.ReturnClause;
import com.arcadedb.opencypher.ast.WhereClause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private final List<LogicalRelationship> relationships;
  private final List<WhereClause> whereFilters;
  private final ReturnClause returnClause;

  private LogicalPlan(final CypherStatement statement) {
    this.statement = statement;
    this.nodes = new HashMap<>();
    this.relationships = new ArrayList<>();
    this.whereFilters = new ArrayList<>();
    this.returnClause = statement.getReturnClause();
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

    for (final MatchClause matchClause : matchClauses) {
      if (!matchClause.hasPathPatterns()) {
        continue; // Phase 1 queries without parsed patterns
      }

      for (final PathPattern pathPattern : matchClause.getPathPatterns()) {
        extractPathPattern(pathPattern);
      }
    }
  }

  /**
   * Extracts nodes and relationships from a single path pattern.
   */
  private void extractPathPattern(final PathPattern pathPattern) {
    final List<NodePattern> nodePatterns = pathPattern.getNodes();
    final List<RelationshipPattern> relPatterns = pathPattern.getRelationships();

    // Extract nodes
    for (final NodePattern nodePattern : nodePatterns) {
      final String variable = nodePattern.getVariable();
      if (variable != null && !nodes.containsKey(variable)) {
        final LogicalNode logicalNode = new LogicalNode(
            variable,
            nodePattern.getLabels(),
            nodePattern.getProperties()
        );
        nodes.put(variable, logicalNode);
      }
    }

    // Extract relationships (linking nodes)
    for (int i = 0; i < relPatterns.size(); i++) {
      final RelationshipPattern relPattern = relPatterns.get(i);
      final NodePattern sourceNode = nodePatterns.get(i);
      final NodePattern targetNode = nodePatterns.get(i + 1);

      final LogicalRelationship logicalRel = new LogicalRelationship(
          relPattern.getVariable(),
          sourceNode.getVariable(),
          targetNode.getVariable(),
          relPattern.getTypes(),
          relPattern.getDirection(),
          relPattern.getProperties(),
          relPattern.getMinHops(),
          relPattern.getMaxHops()
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
   * Returns all nodes in the logical plan.
   */
  public Map<String, LogicalNode> getNodes() {
    return nodes;
  }

  /**
   * Returns a specific node by variable name.
   */
  public LogicalNode getNode(final String variable) {
    return nodes.get(variable);
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
