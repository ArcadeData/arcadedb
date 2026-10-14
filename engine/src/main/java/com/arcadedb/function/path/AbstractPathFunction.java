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
package com.arcadedb.function.path;

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;

import java.util.List;
import java.util.Map;

/**
 * Abstract base class for path functions.
 * <p>
 * A path reaches these functions in one of three shapes: the {@link TraversalPath} a Cypher pattern binds
 * ({@code MATCH p = ...}), the {@code {_type: 'path', nodes, relationships}} map the path procedures yield, or an
 * alternating {@code [node, rel, node, ...]} list. {@link #asTraversalPath(Object)} reads any of them as a
 * {@link TraversalPath} when its elements are graph elements, which is what a function hands back so that
 * {@code nodes()}, {@code relationships()}, {@code length()} and the drivers see a real path (issue #10017).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractPathFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "path." + getSimpleName();
  }

  protected abstract String getSimpleName();

  /**
   * Checks if an object is a path representation (list of alternating nodes and relationships).
   */
  protected boolean isPath(final Object obj) {
    if (obj instanceof TraversalPath)
      return true;
    if (obj instanceof List) {
      return true; // Assume list is a path
    }
    if (obj instanceof Map) {
      final Map<?, ?> map = (Map<?, ?>) obj;
      return map.containsKey("_type") && "path".equals(map.get("_type"));
    }
    return false;
  }

  /**
   * Reads {@code value} as a graph path: itself when it is a {@link TraversalPath}, or the path a node/relationship map
   * or an alternating node/relationship list describes, when every element of it is a vertex or an edge where one is
   * expected and the counts agree (one more node than relationships).
   *
   * @return the path, or null when {@code value} is not a path of graph elements
   */
  protected static TraversalPath asTraversalPath(final Object value) {
    if (value instanceof TraversalPath path)
      return path;

    if (value instanceof Map<?, ?> map) {
      if (!(map.get("nodes") instanceof List<?> nodes) || nodes.isEmpty())
        return null;
      final Object relsValue = map.get("relationships");
      final List<?> rels = relsValue instanceof List<?> list ? list : List.of();
      if (nodes.size() != rels.size() + 1 || !(nodes.get(0) instanceof Vertex start))
        return null;
      final TraversalPath path = new TraversalPath(start);
      for (int i = 0; i < rels.size(); i++) {
        if (!(rels.get(i) instanceof Edge edge) || !(nodes.get(i + 1) instanceof Vertex next))
          return null;
        path.addStep(edge, next);
      }
      return path;
    }

    if (value instanceof List<?> elements) {
      if (elements.isEmpty() || elements.size() % 2 == 0 || !(elements.get(0) instanceof Vertex start))
        return null;
      final TraversalPath path = new TraversalPath(start);
      for (int i = 1; i < elements.size(); i += 2) {
        if (!(elements.get(i) instanceof Edge edge) || !(elements.get(i + 1) instanceof Vertex next))
          return null;
        path.addStep(edge, next);
      }
      return path;
    }

    return null;
  }
}
