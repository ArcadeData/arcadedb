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
package com.arcadedb.remote;

import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ImmutableLightEdge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;

import java.util.*;

public class RemoteImmutableVertex extends RemoteImmutableDocument implements Vertex {
  private final RemoteVertex internal;

  protected RemoteImmutableVertex(final RemoteDatabase database, final Map<String, Object> properties) {
    super(database, properties);
    this.internal = new RemoteVertex(this, database);
  }

  @Override
  public RemoteMutableVertex modify() {
    return new RemoteMutableVertex(this);
  }

  public RemoteDatabase getRemoteDatabase() {
    return internal.remoteDatabase;
  }

  @Override
  public void delete() {
    internal.delete();
  }

  @Override
  public long countEdges(final DIRECTION direction, final String edgeType) {
    return internal.countEdges(direction, edgeType);
  }

  @Override
  public Iterable<Edge> getEdges() {
    return internal.getEdges(DIRECTION.BOTH);
  }

  @Override
  public Iterable<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return internal.getEdges(DIRECTION.BOTH, edgeTypes);
  }

  @Override
  public Iterable<Vertex> getVertices() {
    return internal.getVertices(DIRECTION.BOTH);
  }

  @Override
  public Iterable<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
    return internal.getVertices(direction, edgeTypes);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex) {
    return internal.isConnectedTo(toVertex);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return internal.isConnectedTo(toVertex, direction);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction, final String edgeType) {
    return internal.isConnectedTo(toVertex, direction, edgeType);
  }

  @Override
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    return internal.newEdge(edgeType, toVertex, bidirectional, properties);
  }

  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    return internal.newLightEdge(edgeType, toVertex, bidirectional);
  }

  @Override
  public Vertex asVertex() {
    return this;
  }

  @Override
  public Vertex asVertex(final boolean loadContent) {
    return this;
  }

}
