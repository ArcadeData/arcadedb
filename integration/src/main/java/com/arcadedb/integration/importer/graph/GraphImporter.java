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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.async.NewRecordCallback;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.CompressedAny2RIDIndex;
import com.arcadedb.index.CompressedRID2RIDsIndex;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.logging.*;

public class GraphImporter {
  private final CompressedAny2RIDIndex verticesIndex;
  private final DatabaseInternal       database;
  private final GraphImporterThreadContext[] threadContexts;

  enum STATUS {IMPORTING_VERTEX, IMPORTING_EDGE, CLOSED}

  private STATUS status = STATUS.IMPORTING_VERTEX;

  public class GraphImporterThreadContext {
    Binary                  vertexIndexThreadBuffer;
    CompressedRID2RIDsIndex incomingConnectionsIndexThread;

    Long                                  lastSourceKey    = null;
    VertexInternal                        lastSourceVertex = null;
    List<GraphEngine.CreateEdgeOperation> connections      = new ArrayList<>();
    int                                   importedEdges    = 0;

    public GraphImporterThreadContext(final int expectedVertices, final int expectedEdges) {
      incomingConnectionsIndexThread = new CompressedRID2RIDsIndex(database, expectedVertices, expectedEdges);
    }
  }

  public GraphImporter(final DatabaseInternal database, final int expectedVertices, final int expectedEdges, Type idType) {
    this.database = database;

    final int parallel = database.async().getParallelLevel();

    this.verticesIndex = new CompressedAny2RIDIndex(database, idType, expectedVertices);

    final int expectedEdgesPerThread = expectedEdges / parallel;

    threadContexts = new GraphImporterThreadContext[parallel];
    for (int i = 0; i < parallel; ++i)
      threadContexts[i] = new GraphImporterThreadContext(expectedVertices, expectedEdgesPerThread);
  }

  public void close() {
    close(null);
  }

  public void close(final EdgeLinkedCallback callback) {
    database.commit();

    database.async().waitCompletion();

    for (int i = 0; i < threadContexts.length; ++i)
      threadContexts[i].incomingConnectionsIndexThread.setReadOnly();

    createIncomingEdges(database, callback);

    database.async().waitCompletion();

    for (int i = 0; i < threadContexts.length; ++i)
      threadContexts[i] = null;

    status = STATUS.CLOSED;
  }

  public RID getVertex(final Binary vertexIndexThreadBuffer, final long vertexId) {
    return verticesIndex.get(vertexIndexThreadBuffer, vertexId);
  }

  public RID getVertex(final long vertexId) {
    return verticesIndex.get(vertexId);
  }

  public void createVertex(final String vertexTypeName, final String vertexId, final Object[] vertexProperties) {
    final Object transformedVertexId = verticesIndex.getKeyBinaryType().newInstance(vertexId);

    final MutableVertex sourceVertex;
    RID sourceVertexRID = verticesIndex.get(transformedVertexId);
    if (sourceVertexRID == null) {
      // CREATE THE VERTEX
      sourceVertex = database.newVertex(vertexTypeName);
      sourceVertex.set(vertexProperties);

      database.async().createRecord(sourceVertex, newDocument -> {
        // PRE-CREATE OUT/IN CHUNKS TO SPEEDUP EDGE CREATION
        final DatabaseInternal db = database;
        db.getGraphEngine().createOutEdgeChunk(sourceVertex);
        db.getGraphEngine().createInEdgeChunk(sourceVertex);

        verticesIndex.put(transformedVertexId, newDocument.getIdentity());
      });
    }
  }

  //TODO SUPPORT NOT ONLY LONGS AS VERTICES KEYS
  public void createEdge(final long sourceVertexKey, final String edgeTypeName, final long destinationVertexKey, final Object[] edgeProperties,
      final ImporterContext context, final ImporterSettings settings) {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
    final int slot = async.getSlot((int) sourceVertexKey);

    async.scheduleTask(slot,
        new CreateEdgeFromImportTask(threadContexts[slot], edgeTypeName, sourceVertexKey, destinationVertexKey, edgeProperties, context, settings), true, 70);
  }

  public void startImportingEdges() {
    if (status != STATUS.IMPORTING_VERTEX)
      throw new IllegalStateException("Cannot import edges on current status " + status);

    status = STATUS.IMPORTING_EDGE;

    for (int i = 0; i < threadContexts.length; ++i)
      threadContexts[i].vertexIndexThreadBuffer = verticesIndex.getInternalBuffer().slice();
  }

  public CompressedAny2RIDIndex<Object> getVerticesIndex() {
    return verticesIndex;
  }

  protected void createIncomingEdges(final DatabaseInternal database, final EdgeLinkedCallback callback) {
    List<Pair<Identifiable, Identifiable>> connections = new ArrayList<>();

    long browsedVertices = 0;
    long browsedEdges = 0;
    long verticesWithNoEdges = 0;
    long verticesWithEdges = 0;

    LogManager.instance().log(this, Level.INFO, "Linking back edges for %d vertices...", null, verticesIndex.size());

    // BROWSE ALL THE VERTICES AND COLLECT ALL THE EDGES FROM THE OTHER IN RAM INDEXES
    for (final CompressedAny2RIDIndex.EntryIterator it = verticesIndex.vertexIterator(); it.hasNext(); ) {
      final RID destinationVertex = it.next();

      ++browsedVertices;

      for (int t = 0; t < threadContexts.length; ++t) {
        final List<Pair<RID, RID>> edges = threadContexts[t].incomingConnectionsIndexThread.get(destinationVertex);
        if (edges != null) {
          for (int e = 0; e < edges.size(); ++e) {
            final Pair<RID, RID> edge = edges.get(e);
            connections.add(new Pair<>(edge.getFirst(), edge.getSecond()));
            ++browsedEdges;
          }
        }
      }

      if (!connections.isEmpty()) {
        final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
        final int slot = async.getSlot(destinationVertex.getBucketId());
        async.scheduleTask(slot, new LinkEdgeFromImportTask(destinationVertex, connections, callback), true, 70);
        connections = new ArrayList<>();
        ++verticesWithEdges;
      } else
        ++verticesWithNoEdges;
    }

    LogManager.instance()
        .log(this, Level.INFO, "Linking back edges completed: browsedVertices=%d browsedEdges=%d verticesWithEdges=%d verticesWithNoEdges=%d", null,
            browsedVertices, browsedEdges, verticesWithEdges, verticesWithNoEdges);
  }
}
