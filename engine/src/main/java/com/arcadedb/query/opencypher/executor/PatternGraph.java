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

import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The comma-separated path patterns of one MATCH clause read as the graph they describe: a node per variable, wherever it
 * is written, and one per anonymous node, joined by the relationships (issue #9599).
 * <p>
 * The count push-downs read path patterns, and a MATCH can write the same graph in many orders and cuts: LSQB Q2 is a hop
 * and a three-hop chain closing a cycle on it, and the same cycle written as four one-hop patterns starting from the post
 * is the same query. This class serializes the graph again in the shapes the operators read, so that what a detector
 * accepts no longer depends on how the text was cut:
 * <ul>
 *   <li>{@link #cycleSplits()}: a simple cycle as a one-hop probe and the chain between its ends, for every relationship
 *   that can be the probe and in both directions of the chain;</li>
 *   <li>{@link #pathOrientations()}: a simple path as one chain, read from either end.</li>
 * </ul>
 * A variable carries its labels to every position it is written at: {@code (c:Comment)-[:R]->(p), (c)-[:H]->(x)} is the
 * graph where both {@code c} are {@code Comment}. Writing them at every position changes no answer and lets a proof that
 * reads one position at a time (that two hops cannot bind the same edge) see what the variable is.
 * <p>
 * Only plain shapes are modelled: fixed-length relationships, no path variable or mode, no quantified group, and a variable
 * written with one set of labels at most. Anything else answers "no shape" and the detectors read the text as written.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PatternGraph {
  private final List<NodePattern> nodes;
  private final int[]             edgeFrom;
  private final int[]             edgeTo;
  private final RelationshipPattern[] edges;
  private final int[][]           incident;

  private PatternGraph(final List<NodePattern> nodes, final int[] edgeFrom, final int[] edgeTo,
      final RelationshipPattern[] edges) {
    this.nodes = nodes;
    this.edgeFrom = edgeFrom;
    this.edgeTo = edgeTo;
    this.edges = edges;

    final int[] degree = new int[nodes.size()];
    for (int e = 0; e < edges.length; e++) {
      degree[edgeFrom[e]]++;
      degree[edgeTo[e]]++;
    }
    this.incident = new int[nodes.size()][];
    for (int n = 0; n < nodes.size(); n++)
      incident[n] = new int[degree[n]];
    final int[] filled = new int[nodes.size()];
    for (int e = 0; e < edges.length; e++) {
      incident[edgeFrom[e]][filled[edgeFrom[e]]++] = e;
      incident[edgeTo[e]][filled[edgeTo[e]]++] = e;
    }
  }

  /** The graph of the patterns, or null when one of them is a shape this class does not model. */
  static PatternGraph of(final List<PathPattern> patterns) {
    final List<NodePattern> nodes = new ArrayList<>();
    final Map<String, Integer> named = new HashMap<>();
    int edgeCount = 0;
    for (final PathPattern pattern : patterns) {
      if (pattern.hasPathVariable() || pattern.getPathMode() != null)
        return null;
      edgeCount += pattern.getRelationshipCount();
    }

    final int[] edgeFrom = new int[edgeCount];
    final int[] edgeTo = new int[edgeCount];
    final RelationshipPattern[] edges = new RelationshipPattern[edgeCount];
    int e = 0;
    for (final PathPattern pattern : patterns) {
      int previous = -1;
      for (int i = 0; i <= pattern.getRelationshipCount(); i++) {
        final int node = nodeId(pattern.getNode(i), nodes, named);
        if (node < 0)
          return null;
        if (i > 0) {
          final RelationshipPattern relationship = pattern.getRelationship(i - 1);
          if (relationship instanceof QuantifiedPathPattern || relationship.isVariableLength())
            return null;
          edgeFrom[e] = previous;
          edgeTo[e] = node;
          edges[e++] = relationship;
        }
        previous = node;
      }
    }
    return new PatternGraph(nodes, edgeFrom, edgeTo, edges);
  }

  /**
   * The node a position stands for: the variable's, created at its first position and given the labels of whichever
   * position writes them, or a new one for an anonymous node. -1 when two positions of one variable write different labels.
   */
  private static int nodeId(final NodePattern node, final List<NodePattern> nodes, final Map<String, Integer> named) {
    final String variable = node.getVariable();
    if (variable == null || variable.isEmpty()) {
      nodes.add(node);
      return nodes.size() - 1;
    }

    final Integer existing = named.get(variable);
    if (existing == null) {
      nodes.add(node);
      named.put(variable, nodes.size() - 1);
      return nodes.size() - 1;
    }

    final NodePattern known = nodes.get(existing);
    if (node.hasLabels()) {
      if (!known.hasLabels())
        nodes.set(existing, node);
      else if (!known.getLabels().equals(node.getLabels()) || known.isLabelDisjunction() != node.isLabelDisjunction())
        return -1;
    }
    return existing;
  }

  /**
   * Every split of a simple cycle into a one-hop probe between two named nodes and the chain joining them, the chain read
   * from either end. Empty when the graph is not one simple cycle of three relationships or more.
   */
  List<PathPattern[]> cycleSplits() {
    if (edges.length < 3 || edges.length != nodes.size() || !isConnected())
      return Collections.emptyList();
    for (int n = 0; n < nodes.size(); n++)
      if (incident[n].length != 2 || edgeFrom[incident[n][0]] == edgeTo[incident[n][0]])
        return Collections.emptyList();

    final List<PathPattern[]> splits = new ArrayList<>(edges.length * 2);
    for (int probe = 0; probe < edges.length; probe++) {
      final int u = edgeFrom[probe];
      final int v = edgeTo[probe];
      if (!isNamed(u) || !isNamed(v))
        continue;
      final PathPattern chain = walk(u, probe, v);
      splits.add(new PathPattern[] { hop(probe, u), chain });
      splits.add(new PathPattern[] { hop(probe, v), reversed(chain) });
    }
    return splits;
  }

  /** A simple path as one chain from each of its ends. Empty when the graph is not one simple path of two hops or more. */
  List<PathPattern> pathOrientations() {
    if (edges.length < 2 || edges.length != nodes.size() - 1 || !isConnected())
      return Collections.emptyList();
    int end = -1;
    for (int n = 0; n < nodes.size(); n++) {
      if (incident[n].length > 2)
        return Collections.emptyList();
      if (incident[n].length == 1 && end < 0)
        end = n;
    }
    if (end < 0)
      return Collections.emptyList();
    final PathPattern chain = walk(end, -1, -1);
    return List.of(chain, reversed(chain));
  }

  /**
   * The chain from {@code start} along the relationships other than {@code skipped}, up to {@code stop} or, with none, to the
   * other end of a path.
   */
  private PathPattern walk(final int start, final int skipped, final int stop) {
    final List<NodePattern> chainNodes = new ArrayList<>();
    final List<RelationshipPattern> chainRelationships = new ArrayList<>();
    chainNodes.add(nodes.get(start));
    int current = start;
    int arrivedBy = skipped;
    while (current != stop) {
      int next = -1;
      for (final int e : incident[current])
        if (e != arrivedBy && e != skipped) {
          next = e;
          break;
        }
      if (next < 0)
        break;
      final boolean forward = edgeFrom[next] == current;
      chainRelationships.add(forward ? edges[next] : flipped(edges[next]));
      current = forward ? edgeTo[next] : edgeFrom[next];
      chainNodes.add(nodes.get(current));
      arrivedBy = next;
    }
    return new PathPattern(chainNodes, chainRelationships);
  }

  /** The relationship {@code e} as a one-hop pattern read from its end {@code from}. */
  private PathPattern hop(final int e, final int from) {
    final boolean forward = edgeFrom[e] == from;
    return new PathPattern(nodes.get(from), forward ? edges[e] : flipped(edges[e]), nodes.get(forward ? edgeTo[e] : edgeFrom[e]));
  }

  private static PathPattern reversed(final PathPattern chain) {
    final List<NodePattern> chainNodes = new ArrayList<>(chain.getNodes());
    Collections.reverse(chainNodes);
    final List<RelationshipPattern> chainRelationships = new ArrayList<>(chain.getRelationshipCount());
    for (int i = chain.getRelationshipCount() - 1; i >= 0; i--)
      chainRelationships.add(flipped(chain.getRelationship(i)));
    return new PathPattern(chainNodes, chainRelationships);
  }

  /** The same relationship read from its other end. */
  private static RelationshipPattern flipped(final RelationshipPattern relationship) {
    return new RelationshipPattern(relationship.getVariable(), relationship.getTypes(), relationship.getDirection().reverse(),
        relationship.getProperties(), relationship.getPropertiesParameterName(), relationship.getMinHops(),
        relationship.getMaxHops(), relationship.getWhereExpression());
  }

  private boolean isNamed(final int node) {
    final String variable = nodes.get(node).getVariable();
    return variable != null && !variable.isEmpty();
  }

  private boolean isConnected() {
    if (nodes.isEmpty())
      return false;
    final boolean[] reached = new boolean[nodes.size()];
    final int[] stack = new int[nodes.size()];
    int top = 0;
    stack[top++] = 0;
    reached[0] = true;
    int count = 1;
    while (top > 0) {
      final int n = stack[--top];
      for (final int e : incident[n]) {
        final int other = edgeFrom[e] == n ? edgeTo[e] : edgeFrom[e];
        if (!reached[other]) {
          reached[other] = true;
          stack[top++] = other;
          ++count;
        }
      }
    }
    return count == nodes.size();
  }
}
