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
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.graph.Edge;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

public class GraphImporter {
  private final CompressedAny2RIDIndex       verticesIndex;
  private final DatabaseInternal             database;
  private final GraphImporterThreadContext[] threadContexts;

  enum Status {IMPORTING_VERTEX, IMPORTING_EDGE, CLOSED}

  private Status status = Status.IMPORTING_VERTEX;

  public class GraphImporterThreadContext {
    Binary                                vertexIndexThreadBuffer;
    CompressedRID2RIDsIndex               incomingConnectionsIndexThread;
    Object                                lastSourceKey    = null;
    VertexInternal                        lastSourceVertex = null;
    List<GraphEngine.CreateEdgeOperation> connections      = new ArrayList<>();
    int                                   importedEdges    = 0;

    public GraphImporterThreadContext(final int expectedVertices, final int expectedEdges) throws ClassNotFoundException {
      incomingConnectionsIndexThread = new CompressedRID2RIDsIndex(database, expectedVertices, expectedEdges);
    }
  }

  public GraphImporter(final DatabaseInternal database, final int expectedVertices, final int expectedEdges, final Type idType)
      throws ClassNotFoundException {
    this.database = database;

    final int parallel = database.async().getParallelLevel();

    this.verticesIndex = new CompressedAny2RIDIndex(database, idType, expectedVertices);

    final int expectedEdgesPerThread = expectedEdges / parallel;

    threadContexts = new GraphImporterThreadContext[parallel];
    for (int i = 0; i < parallel; ++i)
      threadContexts[i] = new GraphImporterThreadContext(expectedVertices, expectedEdgesPerThread);
  }

  public void close() {
    close(null, null);
  }

  public void close(final EdgeLinkedCallback callback) {
    close(callback, null);
  }

  public void close(final EdgeLinkedCallback callback, final ImporterContext context) {
    if (database.isTransactionActive())
      database.commit();

    database.async().waitCompletion();

    // Flush any remaining connections from all thread contexts
    // This fixes the issue where the last batch of edges for each thread was never created
    flushRemainingConnections(context);

    for (GraphImporterThreadContext threadContext : threadContexts)
      threadContext.incomingConnectionsIndexThread.setReadOnly();

    createIncomingEdges(database, callback);

    database.async().waitCompletion();

    Arrays.fill(threadContexts, null);

    status = Status.CLOSED;
  }

  /**
   * Flushes any remaining edge connections from all thread contexts.
   * This is called at the end of edge import to ensure the last batch of edges
   * for each thread is properly created (fixes GitHub issue #1198).
   */
  private void flushRemainingConnections(final ImporterContext context) {
    // Check if there are any remaining connections to flush
    boolean hasRemainingConnections = false;
    for (final GraphImporterThreadContext threadContext : threadContexts) {
      if (!threadContext.connections.isEmpty() && threadContext.lastSourceVertex != null) {
        hasRemainingConnections = true;
        break;
      }
    }

    if (!hasRemainingConnections)
      return;

    // Begin transaction for the flush operation
    database.begin();

    try {
      for (final GraphImporterThreadContext threadContext : threadContexts) {
        if (!threadContext.connections.isEmpty() && threadContext.lastSourceVertex != null) {
          // Reload vertex if chunks are not loaded
          if (threadContext.lastSourceVertex.getOutEdgesHeadChunk() == null)
            threadContext.lastSourceVertex = (VertexInternal) threadContext.lastSourceVertex.getIdentity().asVertex();

          // Create edges for the remaining connections
          final List<Edge> newEdges = database.getGraphEngine().newEdges(threadContext.lastSourceVertex, threadContext.connections, false);

          // Update statistics
          if (context != null)
            context.createdEdges.addAndGet(newEdges.size());

          // Add to incoming connections index for back-linking
          for (final Edge e : newEdges)
            threadContext.incomingConnectionsIndexThread.put(e.getIn(), e.getIdentity(), threadContext.lastSourceVertex.getIdentity());

          threadContext.connections.clear();
        }
      }

      database.commit();
    } catch (final Exception e) {
      database.rollback();
      throw e;
    }
  }

  public RID getVertex(final Binary vertexIndexThreadBuffer, final long vertexId) {
    return verticesIndex.get(vertexIndexThreadBuffer, vertexId);
  }

  public RID getVertex(final long vertexId) {
    return verticesIndex.get(vertexId);
  }

  public RID getVertex(final Binary vertexIndexThreadBuffer, final Object vertexId) {
    return verticesIndex.get(vertexIndexThreadBuffer, vertexId);
  }

  public RID getVertex(final Object vertexId) {
    return verticesIndex.get(vertexId);
  }

  public void createVertex(final String vertexTypeName, final String vertexId, final Object[] vertexProperties) {
    final Object transformedVertexId = verticesIndex.getKeyBinaryType().newInstance(vertexId);

    final MutableVertex sourceVertex;
    final RID sourceVertexRID = verticesIndex.get(transformedVertexId);
    if (sourceVertexRID == null) {
      // CREATE THE VERTEX
      sourceVertex = database.newVertex(vertexTypeName);
      sourceVertex.set(vertexProperties);

      database.async().createRecord(sourceVertex, newDocument -> {
        // v1: Edge chunks will be created with correct bucket IDs when edges are added
        verticesIndex.put(transformedVertexId, newDocument.getIdentity());
      });
    }
  }

  /**
   * Creates an edge with long vertex keys (legacy method for backward compatibility).
   */
  public void createEdge(final long sourceVertexKey,
      final String edgeTypeName,
      final long destinationVertexKey,
      final Object[] edgeProperties,
      final ImporterContext context,
      final ImporterSettings settings) {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
    final int slot = async.getSlot((int) sourceVertexKey);

    async.scheduleTask(slot,
        new CreateEdgeFromImportTask(threadContexts[slot], edgeTypeName, sourceVertexKey, destinationVertexKey, edgeProperties,
            context, settings), true, 70);
  }

  /**
   * Creates an edge with Object vertex keys (supports any ID type including String).
   * This method was added to fix GitHub issue #1552.
   */
  public void createEdge(final Object sourceVertexKey,
      final String edgeTypeName,
      final Object destinationVertexKey,
      final Object[] edgeProperties,
      final ImporterContext context,
      final ImporterSettings settings) {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
    final int slot = async.getSlot(sourceVertexKey.hashCode());

    async.scheduleTask(slot,
        new CreateEdgeFromImportTask(threadContexts[slot], edgeTypeName, sourceVertexKey, destinationVertexKey, edgeProperties,
            context, settings), true, 70);
  }

  public void startImportingEdges() {
    if (status != Status.IMPORTING_VERTEX)
      throw new IllegalStateException("Cannot import edges on current status " + status);

    status = Status.IMPORTING_EDGE;

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

    // Check if vertices index is empty (happens when edges are imported separately from vertices)
    // In this case, we need to iterate through the incoming connections index instead
    // This fixes GitHub issue #2267
    if (verticesIndex.size() == 0) {
      LogManager.instance()
          .log(this, Level.INFO,
              "Vertices index is empty, iterating through incoming connections index instead (separate edge import scenario)");

      // Begin transaction for linking incoming edges
      database.begin();

      try {
        // Iterate through all thread contexts and process their incoming connections
        for (int t = 0; t < threadContexts.length; ++t) {
          final CompressedRID2RIDsIndex incomingIndex = threadContexts[t].incomingConnectionsIndexThread;

          // Use the existing method in CreateEdgeFromImportTask to process this index
          CreateEdgeFromImportTask.createIncomingEdgesInBatch(database, incomingIndex, callback);
        }

        database.commit();
      } catch (final Exception e) {
        database.rollback();
        throw e;
      }

      LogManager.instance()
          .log(this, Level.INFO,
              "Linking back edges completed (separate edge import scenario): browsedVertices=%d browsedEdges=%d verticesWithEdges=%d verticesWithNoEdges=%d",
              null,
              browsedVertices, browsedEdges, verticesWithEdges, verticesWithNoEdges);
      return;
    }

    // BROWSE ALL THE VERTICES AND COLLECT ALL THE EDGES FROM THE OTHER IN RAM INDEXES
    for (final CompressedAny2RIDIndex.EntryIterator it = verticesIndex.vertexIterator(); it.hasNext(); ) {
      final RID destinationVertex = it.next();

      ++browsedVertices;

      for (int t = 0; t < threadContexts.length; ++t) {
        final List<Pair<RID, RID>> edges = threadContexts[t].incomingConnectionsIndexThread.get(destinationVertex);
        if (edges != null) {
          for (final Pair<RID, RID> edge : edges) {
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
        .log(this, Level.INFO,
            "Linking back edges completed: browsedVertices=%d browsedEdges=%d verticesWithEdges=%d verticesWithNoEdges=%d", null,
            browsedVertices, browsedEdges, verticesWithEdges, verticesWithNoEdges);
  }
}
