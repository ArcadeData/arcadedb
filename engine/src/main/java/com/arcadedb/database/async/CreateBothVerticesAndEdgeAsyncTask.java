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
package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;

/**
 * Asynchronous Task that creates both vertices and the edge that connects them.
 */
public class CreateBothVerticesAndEdgeAsyncTask extends CreateEdgeAsyncTask {
  private final String   sourceVertexType;
  private final String[] sourceVertexAttributeNames;
  private final Object[] sourceVertexAttributeValues;

  private final String   destinationVertexType;
  private final String[] destinationVertexAttributeNames;
  private final Object[] destinationVertexAttributeValues;

  public CreateBothVerticesAndEdgeAsyncTask(final String sourceVertexType, final String[] sourceVertexAttributeNames,
      final Object[] sourceVertexAttributeValues, final String destinationVertexType, final String[] destinationVertexAttributeNames,
      final Object[] destinationVertexAttributeValues, final String edgeType, final Object[] edgeAttributes, final boolean bidirectional, final boolean light,
      final NewEdgeCallback callback) {
    super(null, null, edgeType, edgeAttributes, bidirectional, light, callback);

    this.sourceVertexType = sourceVertexType;
    this.sourceVertexAttributeNames = sourceVertexAttributeNames;
    this.sourceVertexAttributeValues = sourceVertexAttributeValues;

    this.destinationVertexType = destinationVertexType;
    this.destinationVertexAttributeNames = destinationVertexAttributeNames;
    this.destinationVertexAttributeValues = destinationVertexAttributeValues;
  }

  public void execute(final DatabaseInternal database) {
    final MutableVertex sourceVertex = database.newVertex(sourceVertexType);
    for (int i = 0; i < sourceVertexAttributeNames.length; ++i)
      sourceVertex.set(sourceVertexAttributeNames[i], sourceVertexAttributeValues[i]);
    sourceVertex.save();

    final MutableVertex destinationVertex = database.newVertex(destinationVertexType);
    for (int i = 0; i < destinationVertexAttributeNames.length; ++i)
      destinationVertex.set(destinationVertexAttributeNames[i], destinationVertexAttributeValues[i]);
    destinationVertex.save();

    createEdge(database, sourceVertex, destinationVertex, true, true);
  }

  @Override
  public String toString() {
    return "CreateBothVerticesAndEdgeAsyncTask(" + sourceVertexType + "->" + destinationVertexType + ")";
  }
}
