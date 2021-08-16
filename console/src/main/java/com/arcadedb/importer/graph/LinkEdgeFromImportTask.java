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

package com.arcadedb.importer.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.async.DatabaseAsyncAbstractTask;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.Pair;

import java.util.List;

/**
 * Asynchronous Task that links the destination vertex back to the edges/vertices.
 */
public class LinkEdgeFromImportTask extends DatabaseAsyncAbstractTask {
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

    final EdgeSegment inChunk = database.getGraphEngine().createInEdgeChunk(database, toVertexRecord);

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
