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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * A node that exists only in a query result ({@code create.vNode}, the label nodes of {@code meta.graph}). Its labels are
 * its own list, not a schema type: they are reported as given, sorted and without duplicates as for a stored node. It
 * has no stored edges, so every adjacency question answers empty, and it cannot be modified.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class VirtualVertex extends VirtualElement implements Vertex {
  private final List<String> labels;

  public VirtualVertex(final Database database, final Collection<String> labels, final Map<String, Object> properties) {
    super(database, properties);
    this.labels = labels == null || labels.isEmpty() ? List.of() : List.copyOf(new TreeSet<>(labels));
  }

  /**
   * The node's labels, sorted, without duplicates.
   */
  public List<String> getLabels() {
    return labels;
  }

  /**
   * The labels joined as a stored multi-label node's type would be named, or null for a node without labels.
   */
  @Override
  public String getTypeName() {
    return labels.isEmpty() ? null : String.join("~", labels);
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  @Override
  protected String kind() {
    return "node";
  }

  @Override
  public MutableVertex modify() {
    throw unsupported("modified");
  }

  @Override
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final Object... properties) {
    throw unsupported("connected by a stored relationship");
  }

  @Override
  @Deprecated
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    throw unsupported("connected by a stored relationship");
  }

  @Override
  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex) {
    throw unsupported("connected by a stored relationship");
  }

  @Override
  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    throw unsupported("connected by a stored relationship");
  }

  @Override
  public long countEdges(final DIRECTION direction, final String... edgeTypes) {
    return 0;
  }

  @Override
  public IterableGraph<Edge> getEdges() {
    return GraphEngine.EMPTY_EDGE_LIST;
  }

  @Override
  public IterableGraph<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return GraphEngine.EMPTY_EDGE_LIST;
  }

  @Override
  public IterableGraph<Vertex> getVertices() {
    return GraphEngine.EMPTY_VERTEX_LIST;
  }

  @Override
  public IterableGraph<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
    return GraphEngine.EMPTY_VERTEX_LIST;
  }

  @Override
  public Iterable<RID> getConnectedVertexRIDs(final DIRECTION direction, final String... edgeTypes) {
    return List.of();
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex) {
    return false;
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return false;
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction, final String edgeType) {
    return false;
  }

  @Override
  public RID moveToType(final String targetType) {
    throw unsupported("moved");
  }

  @Override
  public RID moveToBucket(final String targetBucket) {
    throw unsupported("moved");
  }

  @Override
  public Edge asEdge() {
    throw new ClassCastException("A virtual node is not a relationship");
  }

  @Override
  public Edge asEdge(final boolean loadContent) {
    throw new ClassCastException("A virtual node is not a relationship");
  }

  @Override
  public String toString() {
    return "(" + getIdentity() + (labels.isEmpty() ? "" : ":" + String.join(":", labels)) + " " + propertiesAsMap() + ")";
  }
}
