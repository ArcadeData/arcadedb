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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;

/**
 * A Vertex represents the main information in a Property Graph. Vertices are connected with edges. Vertices can be Immutable (read-only) and Mutable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 * @see Edge
 */
public interface Vertex extends Document {
  byte RECORD_TYPE = 1;

  enum DIRECTION {
    OUT, IN, BOTH
  }

  MutableVertex modify();

  MutableEdge newEdge(String edgeType, Identifiable toVertex, boolean bidirectional, final Object... properties);

  ImmutableLightEdge newLightEdge(String edgeType, Identifiable toVertex, boolean bidirectional);

  long countEdges(DIRECTION direction, String edgeType);

  Iterable<Edge> getEdges();

  Iterable<Edge> getEdges(DIRECTION direction, String... edgeTypes);

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterable<Vertex> getVertices();

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  Iterable<Vertex> getVertices(DIRECTION direction, String... edgeTypes);

  boolean isConnectedTo(Identifiable toVertex);

  boolean isConnectedTo(Identifiable toVertex, DIRECTION direction);

  @Override
  default Vertex asVertex() {
    return this;
  }

  @Override
  default Vertex asVertex(final boolean loadContent) {
    return this;
  }
}
