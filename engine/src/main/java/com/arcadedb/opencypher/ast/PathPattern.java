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
package com.arcadedb.opencypher.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a complete path pattern in a Cypher query.
 * A path pattern consists of alternating nodes and relationships.
 * <p>
 * Examples:
 * - (a)-[r]->(b) - simple path with two nodes and one relationship
 * - (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_FOR]->(c:Company) - multi-hop path
 * - (a)-[*1..3]->(b) - variable-length path
 */
public class PathPattern {
  private final List<NodePattern> nodes;
  private final List<RelationshipPattern> relationships;
  private final String pathVariable;

  /**
   * Creates a path pattern with nodes and relationships.
   *
   * @param nodes         list of node patterns
   * @param relationships list of relationship patterns
   * @param pathVariable  optional variable name for the entire path
   */
  public PathPattern(final List<NodePattern> nodes, final List<RelationshipPattern> relationships,
      final String pathVariable) {
    if (nodes == null || nodes.isEmpty()) {
      throw new IllegalArgumentException("Path pattern must have at least one node");
    }
    if (relationships != null && relationships.size() != nodes.size() - 1) {
      throw new IllegalArgumentException(
          "Path pattern must have exactly (nodes.size - 1) relationships. Got " + nodes.size() + " nodes and " + relationships
              .size() + " relationships");
    }

    this.nodes = new ArrayList<>(nodes);
    this.relationships = relationships != null ? new ArrayList<>(relationships) : Collections.emptyList();
    this.pathVariable = pathVariable;
  }

  /**
   * Creates a simple path pattern with a single node.
   *
   * @param node single node pattern
   */
  public PathPattern(final NodePattern node) {
    this(Collections.singletonList(node), Collections.emptyList(), null);
  }

  /**
   * Creates a simple path pattern with two nodes and one relationship.
   *
   * @param node1        first node
   * @param relationship relationship between nodes
   * @param node2        second node
   */
  public PathPattern(final NodePattern node1, final RelationshipPattern relationship, final NodePattern node2) {
    this(List.of(node1, node2), List.of(relationship), null);
  }

  /**
   * Creates a path pattern with nodes and relationships (no path variable).
   *
   * @param nodes         list of node patterns
   * @param relationships list of relationship patterns
   */
  public PathPattern(final List<NodePattern> nodes, final List<RelationshipPattern> relationships) {
    this(nodes, relationships, null);
  }

  /**
   * Returns the list of node patterns in this path.
   *
   * @return list of node patterns
   */
  public List<NodePattern> getNodes() {
    return Collections.unmodifiableList(nodes);
  }

  /**
   * Returns the list of relationship patterns in this path.
   *
   * @return list of relationship patterns
   */
  public List<RelationshipPattern> getRelationships() {
    return Collections.unmodifiableList(relationships);
  }

  /**
   * Returns the variable name for the entire path.
   *
   * @return path variable or null
   */
  public String getPathVariable() {
    return pathVariable;
  }

  /**
   * Returns true if this path has a variable name.
   *
   * @return true if path is named
   */
  public boolean hasPathVariable() {
    return pathVariable != null && !pathVariable.isEmpty();
  }

  /**
   * Returns the number of nodes in this path.
   *
   * @return node count
   */
  public int getNodeCount() {
    return nodes.size();
  }

  /**
   * Returns the number of relationships in this path.
   *
   * @return relationship count
   */
  public int getRelationshipCount() {
    return relationships.size();
  }

  /**
   * Returns true if this is a simple single-node pattern.
   *
   * @return true if only one node with no relationships
   */
  public boolean isSingleNode() {
    return nodes.size() == 1 && relationships.isEmpty();
  }

  /**
   * Returns true if this path contains any variable-length relationships.
   *
   * @return true if contains variable-length patterns
   */
  public boolean hasVariableLengthRelationships() {
    return relationships.stream().anyMatch(RelationshipPattern::isVariableLength);
  }

  /**
   * Returns the first node in the path.
   *
   * @return first node pattern
   */
  public NodePattern getFirstNode() {
    return nodes.get(0);
  }

  /**
   * Returns the last node in the path.
   *
   * @return last node pattern
   */
  public NodePattern getLastNode() {
    return nodes.get(nodes.size() - 1);
  }

  /**
   * Returns a node at the specified index.
   *
   * @param index node index
   * @return node pattern at index
   */
  public NodePattern getNode(final int index) {
    return nodes.get(index);
  }

  /**
   * Returns a relationship at the specified index.
   *
   * @param index relationship index
   * @return relationship pattern at index
   */
  public RelationshipPattern getRelationship(final int index) {
    return relationships.get(index);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    if (pathVariable != null) {
      sb.append(pathVariable).append(" = ");
    }

    for (int i = 0; i < nodes.size(); i++) {
      sb.append(nodes.get(i));
      if (i < relationships.size()) {
        sb.append(relationships.get(i));
      }
    }
    return sb.toString();
  }
}
