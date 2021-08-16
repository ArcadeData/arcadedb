/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.graph;

import java.util.Iterator;

/**
 * Created by luigidellaquila on 02/07/16.
 */
public class EdgeToVertexIterator implements Iterator<Vertex> {
  private final Iterator<Edge>   edgeIterator;
  private final Vertex.DIRECTION direction;

  public EdgeToVertexIterator(Iterator<Edge> iterator, Vertex.DIRECTION direction) {
    if (direction == Vertex.DIRECTION.BOTH) {
      throw new IllegalArgumentException("edge to vertex iterator does not support BOTH as direction");
    }
    this.edgeIterator = iterator;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    return edgeIterator.hasNext();
  }

  @Override
  public Vertex next() {
    return edgeIterator.next().getVertex(direction);
  }
}
