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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for algorithm procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractAlgoProcedure implements CypherProcedure {

  protected Vertex extractVertex(final Object arg, final String paramName) {
    if (arg == null) {
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
    }
    if (arg instanceof Vertex) {
      return (Vertex) arg;
    }
    if (arg instanceof Document doc && doc instanceof Vertex v) {
      return v;
    }
    throw new IllegalArgumentException(
        getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());
  }

  protected String extractString(final Object arg, final String paramName) {
    if (arg == null) {
      return null;
    }
    return arg.toString();
  }

  @SuppressWarnings("unchecked")
  protected String[] extractRelTypes(final Object arg) {
    if (arg == null) {
      return null;
    }
    if (arg instanceof String s) {
      return new String[] { s };
    }
    if (arg instanceof Collection<?> coll) {
      return coll.stream().map(Object::toString).toArray(String[]::new);
    }
    return new String[] { arg.toString() };
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> extractMap(final Object arg, final String paramName) {
    if (arg == null) {
      return null;
    }
    if (arg instanceof Map) {
      return (Map<String, Object>) arg;
    }
    throw new IllegalArgumentException(
        getName() + "(): " + paramName + " must be a map, got " + arg.getClass().getSimpleName());
  }

  /**
   * Builds a path representation from a list of RIDs.
   */
  protected Map<String, Object> buildPath(final List<RID> rids, final com.arcadedb.database.Database database) {
    final List<Object> nodes = new ArrayList<>();
    final List<Object> relationships = new ArrayList<>();

    for (final RID rid : rids) {
      final Document doc = database.lookupByRID(rid, true).asDocument();
      if (doc instanceof Vertex) {
        nodes.add(doc);
      } else if (doc instanceof Edge) {
        relationships.add(doc);
      }
    }

    final Map<String, Object> path = new HashMap<>();
    path.put("_type", "path");
    path.put("nodes", nodes);
    path.put("relationships", relationships);
    path.put("length", relationships.size());
    return path;
  }
}
