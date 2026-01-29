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
package com.arcadedb.query.opencypher.functions.node;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.functions.CypherFunction;

/**
 * Abstract base class for node functions.
 *
 * @author ArcadeDB Team
 */
public abstract class AbstractNodeFunction implements CypherFunction {
  @Override
  public String getName() {
    return "node." + getSimpleName();
  }

  protected abstract String getSimpleName();

  /**
   * Converts an input object to a Vertex.
   *
   * @param input the input object
   * @return the Vertex, or null if not a vertex
   */
  protected Vertex toVertex(final Object input) {
    if (input == null)
      return null;

    if (input instanceof Vertex)
      return (Vertex) input;

    if (input instanceof Document) {
      final Document doc = (Document) input;
      if (doc.getRecord() instanceof Vertex)
        return (Vertex) doc.getRecord();
    }

    return null;
  }

  /**
   * Parses direction from string.
   *
   * @param direction the direction string (in, out, both, or null)
   * @return the Vertex.DIRECTION enum value
   */
  protected Vertex.DIRECTION parseDirection(final String direction) {
    if (direction == null)
      return Vertex.DIRECTION.BOTH;

    switch (direction.toLowerCase()) {
    case "in":
    case "incoming":
      return Vertex.DIRECTION.IN;
    case "out":
    case "outgoing":
      return Vertex.DIRECTION.OUT;
    default:
      return Vertex.DIRECTION.BOTH;
    }
  }
}
