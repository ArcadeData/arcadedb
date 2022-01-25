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

/**
 * Asynchronous Task that creates the relationship between the destinationVertex and the sourceVertex as incoming.
 */
public class CreateIncomingEdgeAsyncTask implements DatabaseAsyncTask {
  protected final RID          sourceVertexRID;
  protected final Identifiable destinationVertex;
  protected final Identifiable edge;

  protected final NewEdgeCallback callback;

  public CreateIncomingEdgeAsyncTask(final RID sourceVertex, final Identifiable destinationVertex, final Identifiable edge,
      final NewEdgeCallback callback) {
    this.sourceVertexRID = sourceVertex;
    this.destinationVertex = destinationVertex;
    this.edge = edge;
    this.callback = callback;
  }

  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    database.getGraphEngine().connectIncomingEdge(destinationVertex, sourceVertexRID, edge.getIdentity());

    if (callback != null)
      callback.call((Edge) edge.getRecord(), false, false);
  }

  @Override
  public String toString() {
    return "CreateIncomingEdgeAsyncTask(" + sourceVertexRID + "<-" + destinationVertex + ")";
  }
}
