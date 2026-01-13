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
package com.arcadedb.opencypher.traversal;

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a path through a graph during traversal.
 * Contains the sequence of vertices and edges that form the path.
 */
public class TraversalPath {
  private final List<Vertex> vertices;
  private final List<Edge> edges;

  public TraversalPath() {
    this.vertices = new ArrayList<>();
    this.edges = new ArrayList<>();
  }

  public TraversalPath(final Vertex startVertex) {
    this();
    vertices.add(startVertex);
  }

  /**
   * Creates a path by extending an existing path with a new edge and vertex.
   *
   * @param parent   parent path
   * @param edge     edge to add
   * @param vertex   vertex to add
   */
  public TraversalPath(final TraversalPath parent, final Edge edge, final Vertex vertex) {
    this.vertices = new ArrayList<>(parent.vertices);
    this.edges = new ArrayList<>(parent.edges);
    this.edges.add(edge);
    this.vertices.add(vertex);
  }

  /**
   * Returns the list of vertices in this path.
   *
   * @return unmodifiable list of vertices
   */
  public List<Vertex> getVertices() {
    return Collections.unmodifiableList(vertices);
  }

  /**
   * Returns the list of edges in this path.
   *
   * @return unmodifiable list of edges
   */
  public List<Edge> getEdges() {
    return Collections.unmodifiableList(edges);
  }

  /**
   * Returns the starting vertex of this path.
   *
   * @return first vertex
   */
  public Vertex getStartVertex() {
    return vertices.isEmpty() ? null : vertices.get(0);
  }

  /**
   * Returns the ending vertex of this path.
   *
   * @return last vertex
   */
  public Vertex getEndVertex() {
    return vertices.isEmpty() ? null : vertices.get(vertices.size() - 1);
  }

  /**
   * Returns the length of this path (number of edges).
   *
   * @return path length
   */
  public int length() {
    return edges.size();
  }

  /**
   * Returns true if this path contains the specified vertex.
   *
   * @param vertex vertex to check
   * @return true if path contains vertex
   */
  public boolean containsVertex(final Vertex vertex) {
    return vertices.stream().anyMatch(v -> v.getIdentity().equals(vertex.getIdentity()));
  }

  /**
   * Adds a step to this path (mutates the path).
   *
   * @param edge   edge to add
   * @param vertex vertex to add
   */
  public void addStep(final Edge edge, final Vertex vertex) {
    edges.add(edge);
    vertices.add(vertex);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vertices.size(); i++) {
      sb.append("(").append(vertices.get(i).getIdentity()).append(")");
      if (i < edges.size()) {
        sb.append("-[").append(edges.get(i).getIdentity()).append("]->");
      }
    }
    return sb.toString();
  }
}
