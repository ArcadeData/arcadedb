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

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;

/**
 * Asynchronous Task that creates the destination vertex and the edge that connects it to the existent source vertex.
 */
public class CreateDestinationVertexAndEdgeAsyncTask extends CreateEdgeAsyncTask {
  private final String   destinationVertexType;
  private final String[] destinationVertexAttributeNames;
  private final Object[] destinationVertexAttributeValues;

  public CreateDestinationVertexAndEdgeAsyncTask(final RID sourceVertex, final String destinationVertexType, final String[] destinationVertexAttributeNames,
      final Object[] destinationVertexAttributeValues, final String edgeType, final Object[] edgeAttributes, final boolean bidirectional, final boolean light,
      final NewEdgeCallback callback) {
    super(sourceVertex, null, edgeType, edgeAttributes, bidirectional, light, callback);

    this.destinationVertexType = destinationVertexType;
    this.destinationVertexAttributeNames = destinationVertexAttributeNames;
    this.destinationVertexAttributeValues = destinationVertexAttributeValues;
  }

  public void execute(final DatabaseInternal database) {
    final MutableVertex destinationVertex = database.newVertex(destinationVertexType);
    for (int i = 0; i < destinationVertexAttributeNames.length; ++i)
      destinationVertex.set(destinationVertexAttributeNames[i], destinationVertexAttributeValues[i]);
    destinationVertex.save();

    createEdge(database, sourceVertex, destinationVertex, false, true);
  }

  @Override
  public String toString() {
    return "CreateDestinationVertexAndEdgeAsyncTask(" + sourceVertex + "->" + destinationVertexType + ")";
  }
}
