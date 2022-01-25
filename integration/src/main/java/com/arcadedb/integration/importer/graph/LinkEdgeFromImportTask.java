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
package com.arcadedb.integration.importer.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.async.DatabaseAsyncTask;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.Pair;

import java.util.List;

/**
 * Asynchronous Task that links the destination vertex back to the edges/vertices.
 */
public class LinkEdgeFromImportTask implements DatabaseAsyncTask {
  private final Identifiable                           destinationVertex;
  private final List<Pair<Identifiable, Identifiable>> connections;
  private final EdgeLinkedCallback                     callback;

  public LinkEdgeFromImportTask(final Identifiable destinationVertex, final List<Pair<Identifiable, Identifiable>> connections,
      final EdgeLinkedCallback callback) {
    this.destinationVertex = destinationVertex;
    this.connections = connections;
    this.callback = callback;
  }

  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {

    final MutableVertex toVertexRecord = ((Vertex) destinationVertex.getRecord()).modify();

    final EdgeSegment inChunk = database.getGraphEngine().createInEdgeChunk(toVertexRecord);

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.addAll(connections);

    if (callback != null)
      callback.onLinked(connections.size());
  }

  @Override
  public String toString() {
    return "LinkEdgeFromImportTask(" + destinationVertex.getIdentity() + "<-" + connections.size() + ")";
  }
}
