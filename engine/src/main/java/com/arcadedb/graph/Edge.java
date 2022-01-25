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
import com.arcadedb.database.RID;

/**
 * An Edge represents the connection between two vertices in a Property Graph. The edge can have properties and point to the same vertex.
 * The direction of the edge goes from the source vertex to the destination vertex. By default edges are bidirectional, that means they can be traversed from
 * both sides. Unidirectional edges can only be traversed from the direction they were created, never backwards. Edges can be Immutable (read-only) and Mutable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 * @see Vertex
 */
public interface Edge extends Document {
  byte RECORD_TYPE = 2;

  MutableEdge modify();

  RID getOut();

  Vertex getOutVertex();

  RID getIn();

  Vertex getInVertex();

  Vertex getVertex(Vertex.DIRECTION iDirection);

  @Override
  default Edge asEdge() {
    return this;
  }

  @Override
  default Edge asEdge(final boolean loadContent) {
    return this;
  }
}
