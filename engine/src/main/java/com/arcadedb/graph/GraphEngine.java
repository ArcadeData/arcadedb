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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Central class to work with graphs. This is not intended to be used by the end user, but rather from Vertex and
 * Edge classes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 */
public class GraphEngine {
  public static final String OUT_EDGES_SUFFIX = "_out_edges";
  public static final String IN_EDGES_SUFFIX  = "_in_edges";

  public static final IterableGraph<Vertex> EMPTY_VERTEX_LIST = new IterableGraph<>() {
    @Override
    public Iterator<Vertex> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public Class<? extends Document> getEntryType() {
      return Vertex.class;
    }
  };

  public static final IterableGraph<Edge> EMPTY_EDGE_LIST = new IterableGraph<>() {
    @Override
    public Iterator<Edge> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public Class<? extends Document> getEntryType() {
      return Edge.class;
    }
  };

  private final DatabaseInternal database;

  public GraphEngine(final DatabaseInternal database) {
    this.database = database;
  }

  public static class CreateEdgeOperation {
    final String       edgeTypeName;
    final Identifiable destinationVertex;
    final Object[]     edgeProperties;

    public CreateEdgeOperation(final String edgeTypeName, final Identifiable destinationVertex,
                               final Object[] edgeProperties) {
      this.edgeTypeName = edgeTypeName;
      this.destinationVertex = destinationVertex;
      this.edgeProperties = edgeProperties;
    }
  }

  public List<Bucket> createVertexAdditionalBuckets(final LocalBucket b) {
    final Bucket[] outInBuckets = new Bucket[2];
    if (database.getSchema().existsBucket(b.getName() + OUT_EDGES_SUFFIX))
      outInBuckets[0] = database.getSchema().getBucketByName(b.getName() + OUT_EDGES_SUFFIX);
    else
      outInBuckets[0] = database.getSchema().createBucket(b.getName() + OUT_EDGES_SUFFIX);

    if (database.getSchema().existsBucket(b.getName() + IN_EDGES_SUFFIX))
      outInBuckets[1] = database.getSchema().getBucketByName(b.getName() + IN_EDGES_SUFFIX);
    else
      outInBuckets[1] = database.getSchema().createBucket(b.getName() + IN_EDGES_SUFFIX);
    return List.of(outInBuckets);
  }

  public void dropVertexType(final VertexType type) {
    for (final Bucket b : type.getBuckets(false)) {
      if (database.getSchema().existsBucket(b.getName() + OUT_EDGES_SUFFIX))
        database.getSchema().dropBucket(b.getName() + OUT_EDGES_SUFFIX);
      if (database.getSchema().existsBucket(b.getName() + IN_EDGES_SUFFIX))
        database.getSchema().dropBucket(b.getName() + IN_EDGES_SUFFIX);
    }
  }

  public ImmutableLightEdge newLightEdge(final VertexInternal fromVertex, final String edgeTypeName,
                                         final Identifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID fromVertexRID = fromVertex.getIdentity();
    if (fromVertexRID == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof MutableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) fromVertex.getDatabase();

    final EdgeType edgeType = (EdgeType) database.getSchema().getType(edgeTypeName);

    final RID edgeRID = new RID(database, edgeType.getFirstBucketId(), -1l);

    final ImmutableLightEdge edge = new ImmutableLightEdge(database, database.getSchema().getType(edgeTypeName),
        edgeRID,
        fromVertexRID, toVertex.getIdentity());

    connectOutgoingEdge(fromVertex, toVertex, edge);
    if (edgeType.isBidirectional())
      connectIncomingEdge(toVertex, fromVertex.getIdentity(), edge.getIdentity());

    return edge;
  }

  public MutableEdge newEdge(final VertexInternal fromVertex, String edgeTypeName, final Identifiable toVertex,
                             final Object... edgeProperties) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID fromVertexRID = fromVertex.getIdentity();
    if (fromVertexRID == null)
      throw new IllegalArgumentException("Current vertex is not persistent. Call save() on vertex first");

    if (toVertex instanceof MutableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent. Call save() on vertex first");

    final DatabaseInternal database = (DatabaseInternal) fromVertex.getDatabase();

    final String bucketName;
    if (edgeTypeName.startsWith("bucket:")) {
      bucketName = edgeTypeName.substring("bucket:".length());
      final DocumentType type = database.getSchema().getTypeByBucketName(bucketName);
      if (type == null)
        edgeTypeName = null;
      else
        edgeTypeName = type.getName();
    } else
      bucketName = null;

    final EdgeType type = (EdgeType) database.getSchema().getType(edgeTypeName);

    final MutableEdge edge = new MutableEdge(database, type, fromVertexRID, toVertex.getIdentity());
    if (edgeProperties != null && edgeProperties.length > 0)
      setProperties(edge, edgeProperties);

    if (bucketName != null)
      edge.save(bucketName);
    else
      edge.save();

    connectOutgoingEdge(fromVertex, toVertex, edge);
    if (type.isBidirectional())
      connectIncomingEdge(toVertex, fromVertex.getIdentity(), edge.getIdentity());

    return edge;
  }

  public void connectOutgoingEdge(VertexInternal fromVertex, final Identifiable toVertex, final Edge edge) {
    fromVertex = fromVertex.modify();

    final int edgeBucketId = edge.getIdentity().getBucketId();
    final EdgeSegment outChunk = createOutEdgeChunk((MutableVertex) fromVertex, edgeBucketId);

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(fromVertex, Vertex.DIRECTION.OUT, outChunk);

    outLinkedList.add(edge.getIdentity(), toVertex.getIdentity());
  }

  public List<Edge> newEdges(VertexInternal sourceVertex, final List<CreateEdgeOperation> connections,
                             final boolean bidirectional) {

    if (connections == null || connections.isEmpty())
      return Collections.emptyList();

    final RID sourceVertexRID = sourceVertex.getIdentity();

    final List<Edge> edges = new ArrayList<>(connections.size());
    final List<Pair<Identifiable, Identifiable>> outEdgePairs = new ArrayList<>();

    for (final CreateEdgeOperation connection : connections) {
      final MutableEdge edge;

      final Identifiable destinationVertex = connection.destinationVertex;

      final EdgeType edgeType = (EdgeType) database.getSchema().getType(connection.edgeTypeName);

      edge = new MutableEdge(database, edgeType, sourceVertexRID, destinationVertex.getIdentity());

      if (connection.edgeProperties != null && connection.edgeProperties.length > 0)
        setProperties(edge, connection.edgeProperties);

      edge.save();

      outEdgePairs.add(new Pair<>(edge, destinationVertex));

      edges.add(edge);
    }

    sourceVertex = sourceVertex.modify();

    final EdgeSegment outChunk = createOutEdgeChunk((MutableVertex) sourceVertex);

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(sourceVertex, Vertex.DIRECTION.OUT, outChunk);
    outLinkedList.addAll(outEdgePairs);

    if (bidirectional) {
      for (int i = 0; i < outEdgePairs.size(); ++i) {
        final Pair<Identifiable, Identifiable> edge = outEdgePairs.get(i);
        connectIncomingEdge(edge.getSecond(), edge.getFirst().getIdentity(), sourceVertexRID);
      }
    }

    return edges;
  }

  public void connectIncomingEdge(final Identifiable toVertex, final RID fromVertexRID, final RID edgeRID) {
    final MutableVertex toVertexRecord = toVertex.asVertex().modify();

    final int edgeBucketId = edgeRID.getBucketId();
    final EdgeSegment inChunk = createInEdgeChunk(toVertexRecord, edgeBucketId);

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.add(edgeRID, fromVertexRID);
  }

  public EdgeSegment createInEdgeChunk(final MutableVertex toVertex) {
    return createInEdgeChunk(toVertex, -1);
  }

  public EdgeSegment createInEdgeChunk(final MutableVertex toVertex, final int edgeBucketId) {
    // v1: Get head chunk for specific edge bucket
    RID inEdgesHeadChunk = toVertex.getInEdgesHeadChunk(edgeBucketId);

    EdgeSegment inChunk = null;
    if (inEdgesHeadChunk != null)
      try {
        inChunk = (EdgeSegment) database.lookupByRID(inEdgesHeadChunk, true);
      } catch (final RecordNotFoundException e) {
        LogManager.instance()
            .log(this, Level.SEVERE, "Record %s (inEdgesHeadChunk) not found on vertex %s. Creating a new one",
                inEdgesHeadChunk,
                toVertex);
        inEdgesHeadChunk = null;
      }

    if (inEdgesHeadChunk == null) {
      inChunk = new MutableEdgeSegment(database, LocalDatabase.getNewEdgeListSize(0));
      database.createRecord(inChunk, getEdgesBucketName(toVertex.getIdentity().getBucketId(), Vertex.DIRECTION.IN));
      inEdgesHeadChunk = inChunk.getIdentity();

      // v1: Set head chunk for specific edge bucket
      toVertex.setInEdgesHeadChunk(edgeBucketId, inEdgesHeadChunk);
      database.updateRecord(toVertex);
    }

    return inChunk;
  }

  public EdgeSegment createOutEdgeChunk(final MutableVertex fromVertex) {
    return createOutEdgeChunk(fromVertex, -1);
  }

  public EdgeSegment createOutEdgeChunk(final MutableVertex fromVertex, final int edgeBucketId) {
    // v1: Get head chunk for specific edge bucket
    RID outEdgesHeadChunk = fromVertex.getOutEdgesHeadChunk(edgeBucketId);

    EdgeSegment outChunk = null;
    if (outEdgesHeadChunk != null)
      try {
        outChunk = (EdgeSegment) database.lookupByRID(outEdgesHeadChunk, true);
      } catch (final RecordNotFoundException e) {
        LogManager.instance()
            .log(this, Level.SEVERE, "Record %s (outEdgesHeadChunk) not found on vertex %s. Creating a new one",
                outEdgesHeadChunk,
                fromVertex.getIdentity());
        outEdgesHeadChunk = null;
      }

    if (outEdgesHeadChunk == null) {
      outChunk = new MutableEdgeSegment(database, LocalDatabase.getNewEdgeListSize(0));
      database.createRecord(outChunk, getEdgesBucketName(fromVertex.getIdentity().getBucketId(), Vertex.DIRECTION.OUT));
      outEdgesHeadChunk = outChunk.getIdentity();

      // v1: Set head chunk for specific edge bucket
      fromVertex.setOutEdgesHeadChunk(edgeBucketId, outEdgesHeadChunk);
      database.updateRecord(fromVertex);
    }

    return outChunk;
  }

  public long countEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    // Get the most updated vertex from transaction cache
    final VertexInternal mostUpdatedVertex = getMostUpdatedVertex(vertex);

    // v1 optimization: if edgeTypes are specified, extract bucket IDs upfront
    // and only query edge lists for those specific buckets (no filtering needed in EdgeLinkedList)
    final Set<Integer> targetBucketIds;
    if (edgeTypes != null && edgeTypes.length > 0) {
      targetBucketIds = new HashSet<>();
      for (final String edgeType : edgeTypes)
        targetBucketIds.addAll(database.getSchema().getType(edgeType).getBucketIds(true));
    } else
      targetBucketIds = null;

    // v1: Iterate over per-type edge lists
    long total = 0;

    switch (direction) {
      case BOTH: {
        for (final Integer bucketId : mostUpdatedVertex.getOutEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList outEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.OUT, bucketId);
          total += (outEdges != null) ? outEdges.count() : 0L;
        }
        for (final Integer bucketId : mostUpdatedVertex.getInEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList inEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.IN, bucketId);
          total += (inEdges != null) ? inEdges.count() : 0L;
        }
        break;
      }

      case OUT: {
        for (final Integer bucketId : mostUpdatedVertex.getOutEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList outEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.OUT, bucketId);
          total += (outEdges != null) ? outEdges.count() : 0L;
        }
        break;
      }

      case IN: {
        for (final Integer bucketId : mostUpdatedVertex.getInEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList inEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.IN, bucketId);
          total += (inEdges != null) ? inEdges.count() : 0L;
        }
        break;
      }

      default:
        throw new IllegalArgumentException("Invalid direction " + direction);
    }

    return total;
  }

  public void deleteEdge(final Edge edge) {
    final Database database = edge.getDatabase();

    // v1: Use edge bucket ID to find the correct edge list
    final int edgeBucketId = edge.getIdentity().getBucketId();

    try {
      if (database.existsRecord(edge.getOut())) {
        final VertexInternal vOut = (VertexInternal) edge.getOutVertex();
        if (vOut != null) {
          final EdgeLinkedList outEdges = getEdgeHeadChunk(vOut, Vertex.DIRECTION.OUT, edgeBucketId);
          if (outEdges != null)
            outEdges.removeEdge(edge);
        }
      }
    } catch (final SchemaException | RecordNotFoundException e) {
      LogManager.instance()
          .log(this, Level.FINE, "Error on loading outgoing vertex %s from edge %s", e, edge.getOut(),
              edge.getIdentity());
    }

    try {
      if (database.existsRecord(edge.getIn())) {
        final VertexInternal vIn = (VertexInternal) edge.getInVertex();
        if (vIn != null) {
          final EdgeLinkedList inEdges = getEdgeHeadChunk(vIn, Vertex.DIRECTION.IN, edgeBucketId);
          if (inEdges != null)
            inEdges.removeEdge(edge);
        }
      }
    } catch (final SchemaException | RecordNotFoundException e) {
      LogManager.instance()
          .log(this, Level.FINE, "Error on loading incoming vertex %s from edge %s", e, edge.getIn(),
              edge.getIdentity());
    }

    final RID edgeRID = edge.getIdentity();
    if (edgeRID != null && !(edge instanceof LightEdge))
      // DELETE EDGE RECORD TOO
      try {
        // Use the database's delete method to ensure proper index cleanup
        // instead of directly calling bucket.deleteRecord()
        final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(edge.getIdentity().getBucketId());
        bucket.deleteRecord(edge.getIdentity());
      } catch (final RecordNotFoundException e) {
        // ALREADY DELETED: IGNORE IT
      }
  }

  public void deleteVertex(final VertexInternal vertex) {
    // RETRIEVE ALL THE EDGES TO DELETE AT THE END
    final List<Identifiable> edgesToDelete = new ArrayList<>();

    // v1: Iterate over all per-type edge lists
    final List<EdgeLinkedList> outEdgeLists = new ArrayList<>();
    for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
      try {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
        if (outEdges != null) {
          outEdgeLists.add(outEdges);
          final EdgeIterator outIterator = (EdgeIterator) outEdges.edgeIterator();

          while (outIterator.hasNext()) {
            RID inV = null;
            try {
              final Edge nextEdge = outIterator.next();
              inV = nextEdge.getIn();
              edgesToDelete.add(nextEdge);
            } catch (final RecordNotFoundException e) {
              // ALREADY DELETED, IGNORE THIS
              LogManager.instance()
                      .log(this, Level.FINE, "Error on deleting outgoing vertex %s connected from vertex %s (record not found)", inV,
                              vertex.getIdentity());
            }
          }
        }
      } catch (Exception e) {
        // LINKED LIST COULD BE BROKEN
        LogManager.instance()
                .log(this, Level.WARNING, "Error on deleting outgoing edges (bucket %d) connected to vertex %s", bucketId, vertex.getIdentity());
      }
    }

    final List<EdgeLinkedList> inEdgeLists = new ArrayList<>();
    for (final Integer bucketId : vertex.getInEdgeBuckets()) {
      try {
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
        if (inEdges != null) {
          inEdgeLists.add(inEdges);
          final EdgeIterator inIterator = (EdgeIterator) inEdges.edgeIterator();

          while (inIterator.hasNext()) {
            RID outV = null;
            try {
              final Edge nextEdge = inIterator.next();
              outV = nextEdge.getOut();
              edgesToDelete.add(nextEdge);
            } catch (final RecordNotFoundException e) {
              // ALREADY DELETED, IGNORE THIS
              LogManager.instance()
                      .log(this, Level.FINE, "Error on deleting incoming vertex %s connected to vertex %s", outV, vertex.getIdentity());
            }
          }
        }
      } catch (Exception e) {
        // LINKED LIST COULD BE BROKEN
        LogManager.instance()
                .log(this, Level.WARNING, "Error on deleting incoming edges (bucket %d) connected to vertex %s", bucketId, vertex.getIdentity());
      }
    }

    for (Identifiable edge : edgesToDelete)
      try {
        edge.asEdge().delete();
      } catch (RecordNotFoundException e) {
        // ALREADY DELETED, IGNORE IT
      }


    // RELOAD AND DELETE OUTGOING EDGE LINKED LISTS
    if (!outEdgeLists.isEmpty())
      for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
        final EdgeLinkedList reloaded = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
        if (reloaded != null)
          try {
            reloaded.deleteAll();
          } catch (Exception e) {
            LogManager.instance()
                .log(this, Level.WARNING, "Error on deleting outgoing edges (bucket %d) connected to vertex %s",
                    bucketId, vertex.getIdentity());
          }
      }

    // RELOAD AND DELETE INCOMING EDGE LINKED LISTS
    if (!inEdgeLists.isEmpty())
      for (final Integer bucketId : vertex.getInEdgeBuckets()) {
        final EdgeLinkedList reloaded = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
        if (reloaded != null)
          try {
            reloaded.deleteAll();
          } catch (Exception e) {
            LogManager.instance()
                .log(this, Level.WARNING, "Error on deleting incoming edges (bucket %d) connected to vertex %s",
                    bucketId, vertex.getIdentity());
          }
      }

    // DELETE VERTEX RECORD
    vertex.getDatabase().getSchema().getBucketById(vertex.getIdentity().getBucketId()).deleteRecord(vertex.getIdentity());
  }

  public IterableGraph<Edge> getEdges(final VertexInternal vertex) {
    final MultiIterator<Edge> result = new MultiIterator<>();

    // v1: Iterate over all per-type edge lists
    for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
      if (outEdges != null)
        result.addIterator(outEdges.edgeIterator());
    }

    for (final Integer bucketId : vertex.getInEdgeBuckets()) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
      if (inEdges != null)
        result.addIterator(inEdges.edgeIterator());
    }

    return result;
  }

  public IterableGraph<Edge> getEdges(final VertexInternal vertex, final Vertex.DIRECTION direction,
                                      final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    // Get the most updated vertex from transaction cache
    final VertexInternal mostUpdatedVertex = getMostUpdatedVertex(vertex);

    // v1 optimization: if edgeTypes are specified, extract bucket IDs upfront
    // and only query edge lists for those specific buckets (no filtering needed in EdgeLinkedList)
    final Set<Integer> targetBucketIds;
    if (edgeTypes != null && edgeTypes.length > 0) {
      targetBucketIds = new HashSet<>();
      for (final String edgeType : edgeTypes)
        targetBucketIds.addAll(database.getSchema().getType(edgeType).getBucketIds(true));
    } else
      targetBucketIds = null;

    // v1: Combine iterators from all per-type edge lists
    switch (direction) {
      case BOTH: {
        final MultiIterator<Edge> result = new MultiIterator<>();

        for (final Integer bucketId : mostUpdatedVertex.getOutEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList outEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.OUT, bucketId);
          if (outEdges != null)
            result.addIterator(outEdges.edgeIterator());
        }
        for (final Integer bucketId : mostUpdatedVertex.getInEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList inEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.IN, bucketId);
          if (inEdges != null)
            result.addIterator(inEdges.edgeIterator());
        }
        return result;
      }

      case OUT: {
        final MultiIterator<Edge> result = new MultiIterator<>();
        for (final Integer bucketId : mostUpdatedVertex.getOutEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList outEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.OUT, bucketId);
          if (outEdges != null)
            result.addIterator(outEdges.edgeIterator());
        }
        return result;
      }

      case IN: {
        final MultiIterator<Edge> result = new MultiIterator<>();
        for (final Integer bucketId : mostUpdatedVertex.getInEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList inEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.IN, bucketId);
          if (inEdges != null)
            result.addIterator(inEdges.edgeIterator());
        }
        return result;
      }

      default:
        throw new IllegalArgumentException("Invalid direction " + direction);
    }

  }

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PVertex instances
   */
  public IterableGraph<Vertex> getVertices(final VertexInternal vertex) {
    final MultiIterator<Vertex> result = new MultiIterator<>();

    // v1: Iterate over all per-type edge lists
    for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
      if (outEdges != null)
        result.addIterator(outEdges.vertexIterator());
    }

    for (final Integer bucketId : vertex.getInEdgeBuckets()) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
      if (inEdges != null)
        result.addIterator(inEdges.vertexIterator());
    }

    return result;
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   * @param edgeTypes Edge type names to filter
   * @return An iterator of PVertex instances
   */
  public IterableGraph<Vertex> getVertices(final VertexInternal vertex, final Vertex.DIRECTION direction,
                                           final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    // Get the most updated vertex from transaction cache
    final VertexInternal mostUpdatedVertex = getMostUpdatedVertex(vertex);

    // v1 optimization: if edgeTypes are specified, extract bucket IDs upfront
    // and only query edge lists for those specific buckets (no filtering needed in EdgeLinkedList)
    final Set<Integer> targetBucketIds;
    if (edgeTypes != null && edgeTypes.length > 0) {
      targetBucketIds = new HashSet<>();
      for (final String edgeType : edgeTypes)
        targetBucketIds.addAll(database.getSchema().getType(edgeType).getBucketIds(true));
    } else
      targetBucketIds = null;

    // v1: Combine iterators from all per-type edge lists
    switch (direction) {
      case BOTH: {
        final MultiIterator<Vertex> result = new MultiIterator<>();

        for (final Integer bucketId : mostUpdatedVertex.getOutEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList outEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.OUT, bucketId);
          if (outEdges != null)
            result.addIterator(outEdges.vertexIterator());
        }
        for (final Integer bucketId : mostUpdatedVertex.getInEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList inEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.IN, bucketId);
          if (inEdges != null)
            result.addIterator(inEdges.vertexIterator());
        }
        return result;
      }

      case OUT: {
        final MultiIterator<Vertex> result = new MultiIterator<>();
        for (final Integer bucketId : mostUpdatedVertex.getOutEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList outEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.OUT, bucketId);
          if (outEdges != null)
            result.addIterator(outEdges.vertexIterator());
        }
        return result;
      }

      case IN: {
        final MultiIterator<Vertex> result = new MultiIterator<>();
        for (final Integer bucketId : mostUpdatedVertex.getInEdgeBuckets()) {
          if (targetBucketIds != null && !targetBucketIds.contains(bucketId))
            continue; // Skip edge lists not matching the requested types
          final EdgeLinkedList inEdges = getEdgeHeadChunk(mostUpdatedVertex, Vertex.DIRECTION.IN, bucketId);
          if (inEdges != null)
            result.addIterator(inEdges.vertexIterator());
        }
        return result;
      }

      default:
        throw new IllegalArgumentException("Invalid direction " + direction);
    }

  }

  public RID getFirstEdgeConnectedToVertex(final VertexInternal vertex, final Identifiable toVertex,
                                           final int[] edgeBucketFilter) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    // v1: Iterate over all per-type edge lists
    for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
      if (outEdges != null) {
        final RID edgeRID = outEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
        if (edgeRID != null)
          return edgeRID;
      }
    }

    for (final Integer bucketId : vertex.getInEdgeBuckets()) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
      if (inEdges != null) {
        final RID edgeRID = inEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
        if (edgeRID != null)
          return edgeRID;
      }
    }

    return null;
  }

  public RID getFirstEdgeConnectedToVertex(final VertexInternal vertex, final Identifiable toVertex,
                                           final Vertex.DIRECTION direction, final int[] edgeBucketFilter) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    // v1: Iterate over all per-type edge lists
    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
        if (outEdges != null) {
          final RID edgeRID = outEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
          if (edgeRID != null)
            return edgeRID;
        }
      }
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      for (final Integer bucketId : vertex.getInEdgeBuckets()) {
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
        if (inEdges != null) {
          final RID edgeRID = inEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
          if (edgeRID != null)
            return edgeRID;
        }
      }
    }

    return null;
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    // v1: Iterate over all per-type edge lists
    for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
      if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), null))
        return true;
    }

    for (final Integer bucketId : vertex.getInEdgeBuckets()) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
      if (inEdges != null && inEdges.containsVertex(toVertex.getIdentity(), null))
        return true;
    }

    return false;
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex,
                                     final Vertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    // v1: Iterate over all per-type edge lists
    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
        if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), null))
          return true;
      }
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      for (final Integer bucketId : vertex.getInEdgeBuckets()) {
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
        if (inEdges != null && inEdges.containsVertex(toVertex.getIdentity(), null))
          return true;
      }
    }

    return false;
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex,
                                     final Vertex.DIRECTION direction,
                                     final String edgeType) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (edgeType == null)
      throw new IllegalArgumentException("Edge type is null");

    final int[] bucketFilter = vertex.getDatabase().getSchema().getType(edgeType).getBuckets(true).stream()
        .mapToInt(x -> x.getFileId()).toArray();

    // v1: Iterate over all per-type edge lists
    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      for (final Integer bucketId : vertex.getOutEdgeBuckets()) {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT, bucketId);
        if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), bucketFilter))
          return true;
      }
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      for (final Integer bucketId : vertex.getInEdgeBuckets()) {
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN, bucketId);
        if (inEdges != null && inEdges.containsVertex(toVertex.getIdentity(), bucketFilter))
          return true;
      }
    }

    return false;
  }

  public String getEdgesBucketName(final int bucketId, final Vertex.DIRECTION direction) {
    final Bucket vertexBucket = database.getSchema().getBucketById(bucketId);

    if (direction == Vertex.DIRECTION.OUT)
      return vertexBucket.getName() + OUT_EDGES_SUFFIX;
    else if (direction == Vertex.DIRECTION.IN)
      return vertexBucket.getName() + IN_EDGES_SUFFIX;

    throw new IllegalArgumentException("Invalid direction");
  }

  public static void setProperties(final MutableEdge edge, final Object[] properties) {
    if (properties != null)
      if (properties.length == 1 && properties[0] instanceof Map) {
        // GET PROPERTIES FROM THE MAP
        final Map<String, Object> map = (Map<String, Object>) properties[0];
        for (final Map.Entry<String, Object> entry : map.entrySet())
          edge.set(entry.getKey(), entry.getValue());
      } else {
        if (properties.length % 2 != 0)
          throw new IllegalArgumentException("Properties must be an even number as pairs of name, value");
        for (int i = 0; i < properties.length; i += 2)
          edge.set((String) properties[i], properties[i + 1]);
      }
  }

  /**
   * Get edge list without bucket ID (returns first bucket only).
   *
   * @deprecated In v1 format, vertices have per-type edge maps. This method only returns the first bucket's
   * edge list for backward compatibility. Callers should iterate over all buckets using
   * {@link #getEdgeHeadChunk(VertexInternal, Vertex.DIRECTION, int)} instead:
   * <pre>
   * for (Integer bucketId : vertex.getOutEdgeBuckets()) {
   *   EdgeLinkedList edges = getEdgeHeadChunk(vertex, DIRECTION.OUT, bucketId);
   *   // process edges
   * }
   * </pre>
   */
  @Deprecated
  public EdgeLinkedList getEdgeHeadChunk(final VertexInternal vertex, final Vertex.DIRECTION direction) {
    if (direction == Vertex.DIRECTION.OUT) {
      RID rid = null;
      try {
        rid = vertex.getOutEdgesHeadChunk();
        if (rid != null) {
          return new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(rid,
              true));
        }
      } catch (final RecordNotFoundException e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Cannot load OUT edge list chunk (%s) for vertex %s", e, rid,
                vertex.getIdentity());
      }
    } else if (direction == Vertex.DIRECTION.IN) {
      RID rid = null;
      try {
        rid = vertex.getInEdgesHeadChunk();
        if (rid != null) {
          return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(rid,
              true));
        }
      } catch (final RecordNotFoundException e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Cannot load IN edge list chunk (%s) for vertex %s", e, rid,
                vertex.getIdentity());
      }
    }

    return null;
  }

  /**
   * Get edge list for a specific edge bucket (v1 format).
   */
  public EdgeLinkedList getEdgeHeadChunk(final VertexInternal vertex, final Vertex.DIRECTION direction, final int bucketId) {
    if (direction == Vertex.DIRECTION.OUT) {
      RID rid = null;
      try {
        rid = vertex.getOutEdgesHeadChunk(bucketId);
        if (rid != null) {
          return new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(rid, true));
        }
      } catch (final RecordNotFoundException e) {
        LogManager.instance()
                .log(this, Level.WARNING, "Cannot load OUT edge list chunk (%s) for vertex %s bucket %d", e, rid, vertex.getIdentity(), bucketId);
      }
    } else if (direction == Vertex.DIRECTION.IN) {
      RID rid = null;
      try {
        rid = vertex.getInEdgesHeadChunk(bucketId);
        if (rid != null) {
          return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(rid, true));
        }
      } catch (final RecordNotFoundException e) {
        LogManager.instance()
                .log(this, Level.WARNING, "Cannot load IN edge list chunk (%s) for vertex %s bucket %d", e, rid, vertex.getIdentity(), bucketId);
      }
    }

    return null;
  }

  protected RID moveToType(final Vertex vertex, final String typeName) {
    return moveTo(vertex, typeName, null);
  }

  protected RID moveToBucket(final Vertex vertex, final String bucketName) {
    return moveTo(vertex, vertex.getTypeName(), bucketName);
  }

  protected RID moveTo(final Vertex vertex, final String typeName, final String bucketName) {
    final Database db = vertex.getDatabase();
    boolean moveTx = !db.isTransactionActive();
    try {
      if (moveTx)
        db.begin();

      // SAVE OLD VERTEX PROPERTIES AND EDGES
      final Map<String, Object> properties = vertex.propertiesAsMap();
      final List<Edge> outEdges = new ArrayList<>();
      for (Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
        outEdges.add(edge.asEdge(true));
      final List<Edge> inEdges = new ArrayList<>();
      for (Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
        inEdges.add(edge.asEdge(true));

      // DELETE THE OLD RECORD FIRST TO AVOID ISSUES WITH UNIQUE CONSTRAINTS
      vertex.delete();

      final MutableVertex newVertex = (MutableVertex) db.newVertex(typeName).set(properties);
      if (bucketName != null)
        newVertex.save(bucketName);
      else
        newVertex.save();
      final RID newIdentity = newVertex.getIdentity();

      for (Edge oe : outEdges) {
        final RID inV = oe.getIn();
        if (oe instanceof LightEdge)
          newVertex.newLightEdge(oe.getTypeName(), inV);
        else {
          final MutableEdge e = newVertex.newEdge(oe.getTypeName(), inV);
          final Map<String, Object> edgeProperties = oe.propertiesAsMap();
          if (!edgeProperties.isEmpty())
            e.set(edgeProperties).save();
        }
      }

      for (Edge ie : inEdges) {
        final RID outV = ie.getOut();
        if (ie instanceof LightEdge)
          newVertex.newLightEdge(ie.getTypeName(), outV);
        else {
          final MutableEdge e = newVertex.newEdge(ie.getTypeName(), outV);
          final Map<String, Object> edgeProperties = ie.propertiesAsMap();
          if (!edgeProperties.isEmpty())
            e.set(edgeProperties).save();
        }
      }

      if (moveTx)
        db.commit();

      return newIdentity;

    } catch (RuntimeException ex) {
      if (moveTx)
        db.rollback();

      throw ex;
    }
  }

  /**
   * Get the most updated version of a vertex from the transaction cache.
   * If the vertex is in the cache (modified in current transaction), return the cached version.
   * Otherwise, return the vertex as-is.
   * This is important for v1 format vertices to ensure we access the edge maps from MutableVertex.
   */
  private VertexInternal getMostUpdatedVertex(final VertexInternal vertex) {
    if (!database.isTransactionActive())
      return vertex;

    final VertexInternal cached = (VertexInternal) database.getTransaction().getRecordFromCache(vertex.getIdentity());
    if (cached != null)
      return cached;
    return vertex;
  }
}
