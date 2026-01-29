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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Abstract base class for path expansion procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractPathProcedure implements CypherProcedure {

  protected Vertex extractVertex(final Object arg, final String paramName) {
    if (arg == null) {
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
    }
    if (arg instanceof Vertex v) {
      return v;
    }
    if (arg instanceof Document doc && doc instanceof Vertex v) {
      return v;
    }
    throw new IllegalArgumentException(
        getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());
  }

  @SuppressWarnings("unchecked")
  protected String[] extractRelTypes(final Object arg) {
    if (arg == null) {
      return null;
    }
    if (arg instanceof String s) {
      if (s.isEmpty()) {
        return null;
      }
      // Handle pipe-separated format "REL1|REL2|REL3"
      if (s.contains("|")) {
        return s.split("\\|");
      }
      return new String[] { s };
    }
    if (arg instanceof Collection<?> coll) {
      return coll.stream().map(Object::toString).toArray(String[]::new);
    }
    return new String[] { arg.toString() };
  }

  @SuppressWarnings("unchecked")
  protected String[] extractLabels(final Object arg) {
    if (arg == null) {
      return null;
    }
    if (arg instanceof String s) {
      if (s.isEmpty()) {
        return null;
      }
      // Handle pipe-separated format "Label1|Label2|Label3"
      if (s.contains("|")) {
        return s.split("\\|");
      }
      return new String[] { s };
    }
    if (arg instanceof Collection<?> coll) {
      return coll.stream().map(Object::toString).toArray(String[]::new);
    }
    return new String[] { arg.toString() };
  }

  protected Vertex.DIRECTION parseDirection(final String direction) {
    if (direction == null || direction.isEmpty() || direction.equalsIgnoreCase("BOTH")) {
      return Vertex.DIRECTION.BOTH;
    }
    return Vertex.DIRECTION.valueOf(direction.toUpperCase(Locale.ENGLISH));
  }

  protected Map<String, Object> buildPath(final List<Object> elements) {
    final List<Object> nodes = new ArrayList<>();
    final List<Object> relationships = new ArrayList<>();

    for (final Object element : elements) {
      if (element instanceof Vertex) {
        nodes.add(element);
      } else if (element instanceof Edge) {
        relationships.add(element);
      }
    }

    final Map<String, Object> path = new HashMap<>();
    path.put("_type", "path");
    path.put("nodes", nodes);
    path.put("relationships", relationships);
    path.put("length", relationships.size());
    return path;
  }

  protected boolean matchesLabels(final Vertex vertex, final String[] labels) {
    if (labels == null || labels.length == 0) {
      return true;
    }
    final String vertexType = vertex.getTypeName();
    for (final String label : labels) {
      if (vertexType.equals(label)) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> extractConfig(final Object arg) {
    if (arg == null) {
      return new HashMap<>();
    }
    if (arg instanceof Map) {
      return (Map<String, Object>) arg;
    }
    return new HashMap<>();
  }
}
