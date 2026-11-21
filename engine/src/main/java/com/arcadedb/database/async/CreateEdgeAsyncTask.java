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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.VertexInternal;

/**
 * Asynchronous Task that creates the edge that connects 2 vertices.
 */
public class CreateEdgeAsyncTask extends DatabaseAsyncAbstractTask {
  protected final Identifiable sourceVertex;
  protected final Identifiable destinationVertex;

  protected final String   edgeType;
  protected final Object[] edgeAttributes;

  protected final boolean         light;
  protected final boolean         outgoingOnly;
  protected final NewEdgeCallback callback;

  public CreateEdgeAsyncTask(final Identifiable sourceVertex, final Identifiable destinationVertex, final String edgeType,
      final Object[] edgeAttributes, final boolean light, final NewEdgeCallback callback) {
    this(sourceVertex, destinationVertex, edgeType, edgeAttributes, light, false, callback);
  }

  /**
   * @param outgoingOnly {@code true} wires only the OUT side of the edge, because the caller has the IN side connected afterwards by
   *                     a {@link CreateIncomingEdgeAsyncTask} in the slot of the vertex the edge ends at: wiring it here too appended
   *                     the edge twice to the IN list of the destination (issue #12602). Meaningless for a unidirectional type,
   *                     which has no IN side.
   */
  public CreateEdgeAsyncTask(final Identifiable sourceVertex, final Identifiable destinationVertex, final String edgeType,
      final Object[] edgeAttributes, final boolean light, final boolean outgoingOnly, final NewEdgeCallback callback) {
    this.sourceVertex = sourceVertex;
    this.destinationVertex = destinationVertex;

    this.edgeType = edgeType;
    this.edgeAttributes = edgeAttributes;

    this.light = light;
    this.outgoingOnly = outgoingOnly;
    this.callback = callback;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    createEdge(database, sourceVertex, destinationVertex, false, false);
  }

  protected void createEdge(final DatabaseInternal database, final Identifiable sourceVertex, final Identifiable destinationVertex,
      final boolean createdSourceVertex, final boolean createdDestinationVertex) {

    final GraphEngine graphEngine = database.getGraphEngine();
    final VertexInternal source = (VertexInternal) sourceVertex.getRecord();
    final RID destination = destinationVertex.getIdentity();

    final Edge edge;
    if (light)
      edge = outgoingOnly ?
          graphEngine.newOutgoingOnlyLightEdge(source, edgeType, destination) :
          graphEngine.newLightEdge(source, edgeType, destination);
    else
      edge = outgoingOnly ?
          graphEngine.newOutgoingOnlyEdgeReportingVeto(source, edgeType, destination, edgeAttributes) :
          graphEngine.newEdgeReportingVeto(source, edgeType, destination, edgeAttributes);

    // AN EDGE A BeforeRecordCreateListener VETOED IS NOT AN ERROR AND CREATED NOTHING: NO CALLBACK, AS FOR A VETOED DOCUMENT (issue #10085)
    if (edge != null && callback != null)
      callback.call(edge, createdSourceVertex, createdDestinationVertex);
  }

  @Override
  public String toString() {
    return "CreateEdgeAsyncTask(" + sourceVertex + "->" + destinationVertex + ")";
  }
}
