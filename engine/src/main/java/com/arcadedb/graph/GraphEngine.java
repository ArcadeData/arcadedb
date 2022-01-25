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

import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Central class to work with graphs. This is not intended to be used by the end user, but rather from Vertex and Edge classes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 */
public class GraphEngine {
  public static final String           OUT_EDGES_SUFFIX = "_out_edges";
  public static final String           IN_EDGES_SUFFIX  = "_in_edges";
  private final       DatabaseInternal database;

  public GraphEngine(final DatabaseInternal database) {
    this.database = database;
  }

  public static class CreateEdgeOperation {
    final String       edgeTypeName;
    final Identifiable destinationVertex;
    final Object[]     edgeProperties;

    public CreateEdgeOperation(final String edgeTypeName, final Identifiable destinationVertex, final Object[] edgeProperties) {
      this.edgeTypeName = edgeTypeName;
      this.destinationVertex = destinationVertex;
      this.edgeProperties = edgeProperties;
    }
  }

  public void createVertexType(final VertexType type) {
    for (Bucket b : type.getBuckets(false)) {
      if (!database.getSchema().existsBucket(b.getName() + OUT_EDGES_SUFFIX))
        database.getSchema().createBucket(b.getName() + OUT_EDGES_SUFFIX);
      if (!database.getSchema().existsBucket(b.getName() + IN_EDGES_SUFFIX))
        database.getSchema().createBucket(b.getName() + IN_EDGES_SUFFIX);
    }
  }

  public void dropVertexType(final VertexType type) {
    for (Bucket b : type.getBuckets(false)) {
      if (database.getSchema().existsBucket(b.getName() + OUT_EDGES_SUFFIX))
        database.getSchema().dropBucket(b.getName() + OUT_EDGES_SUFFIX);
      if (database.getSchema().existsBucket(b.getName() + IN_EDGES_SUFFIX))
        database.getSchema().dropBucket(b.getName() + IN_EDGES_SUFFIX);
    }
  }

  public ImmutableLightEdge newLightEdge(VertexInternal fromVertex, final String edgeTypeName, Identifiable toVertex, final boolean bidirectional) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID fromVertexRID = fromVertex.getIdentity();
    if (fromVertexRID == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof MutableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) fromVertex.getDatabase();

    final RID edgeRID = new RID(database, database.getSchema().getType(edgeTypeName).getFirstBucketId(), -1l);

    final ImmutableLightEdge edge = new ImmutableLightEdge(database, database.getSchema().getType(edgeTypeName), edgeRID, fromVertexRID,
        toVertex.getIdentity());

    connectEdge(fromVertex, toVertex, edge, bidirectional);

    return edge;
  }

  public MutableEdge newEdge(final VertexInternal fromVertex, final String edgeTypeName, Identifiable toVertex, final boolean bidirectional,
      final Object... edgeProperties) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final RID fromVertexRID = fromVertex.getIdentity();
    if (fromVertexRID == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof MutableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final DatabaseInternal database = (DatabaseInternal) fromVertex.getDatabase();

    final MutableEdge edge = new MutableEdge(database, database.getSchema().getType(edgeTypeName), fromVertexRID, toVertex.getIdentity());
    if (edgeProperties != null && edgeProperties.length > 0)
      setProperties(edge, edgeProperties);

    edge.save();

    connectEdge(fromVertex, toVertex, edge, bidirectional);

    return edge;
  }

  public void connectEdge(VertexInternal fromVertex, final Identifiable toVertex, final Edge edge, final boolean bidirectional) {
    fromVertex = fromVertex.modify();

    final EdgeSegment outChunk = createOutEdgeChunk((MutableVertex) fromVertex);

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(fromVertex, Vertex.DIRECTION.OUT, outChunk);

    outLinkedList.add(edge.getIdentity(), toVertex.getIdentity());

    if (bidirectional)
      connectIncomingEdge(toVertex, fromVertex.getIdentity(), edge.getIdentity());
  }

  public void upgradeEdge(VertexInternal fromVertex, final Identifiable toVertex, final MutableEdge edge, final boolean bidirectional) {
    fromVertex = fromVertex.modify();
    final EdgeSegment outChunk = createOutEdgeChunk((MutableVertex) fromVertex);

    final EdgeLinkedList outLinkedList = new EdgeLinkedList(fromVertex, Vertex.DIRECTION.OUT, outChunk);

    outLinkedList.upgrade(edge.getIdentity(), toVertex.getIdentity());

    if (bidirectional)
      upgradeIncomingEdge(toVertex, fromVertex.getIdentity(), edge.getIdentity());
  }

  public void upgradeIncomingEdge(final Identifiable toVertex, final RID fromVertexRID, final RID edgeRID) {
    final MutableVertex toVertexRecord = toVertex.asVertex().modify();

    final EdgeSegment inChunk = createInEdgeChunk(toVertexRecord);

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.upgrade(edgeRID, fromVertexRID);
  }

  public List<Edge> newEdges(VertexInternal sourceVertex, final List<CreateEdgeOperation> connections, final boolean bidirectional) {

    if (connections == null || connections.isEmpty())
      return Collections.EMPTY_LIST;

    final RID sourceVertexRID = sourceVertex.getIdentity();

    final List<Edge> edges = new ArrayList<>(connections.size());
    final List<Pair<Identifiable, Identifiable>> outEdgePairs = new ArrayList<>();

    for (int i = 0; i < connections.size(); ++i) {
      final CreateEdgeOperation connection = connections.get(i);

      final MutableEdge edge;

      final Identifiable destinationVertex = connection.destinationVertex;

      edge = new MutableEdge(database, database.getSchema().getType(connection.edgeTypeName), sourceVertexRID, destinationVertex.getIdentity());

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

    final EdgeSegment inChunk = createInEdgeChunk(toVertexRecord);

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.add(edgeRID, fromVertexRID);
  }

  public EdgeSegment createInEdgeChunk(final MutableVertex toVertex) {
    RID inEdgesHeadChunk = toVertex.getInEdgesHeadChunk();

    EdgeSegment inChunk = null;
    if (inEdgesHeadChunk != null)
      try {
        inChunk = (EdgeSegment) database.lookupByRID(inEdgesHeadChunk, true);
      } catch (RecordNotFoundException e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Record %s (inEdgesHeadChunk) not found on vertex %s. Creating a new one", inEdgesHeadChunk, toVertex);
        inEdgesHeadChunk = null;
      }

    if (inEdgesHeadChunk == null) {
      inChunk = new MutableEdgeSegment(database, database.getNewEdgeListSize(0));
      database.createRecord(inChunk, getEdgesBucketName(toVertex.getIdentity().getBucketId(), Vertex.DIRECTION.IN));
      inEdgesHeadChunk = inChunk.getIdentity();

      toVertex.setInEdgesHeadChunk(inEdgesHeadChunk);
      database.updateRecord(toVertex);
    }

    return inChunk;
  }

  public EdgeSegment createOutEdgeChunk(final MutableVertex fromVertex) {
    RID outEdgesHeadChunk = fromVertex.getOutEdgesHeadChunk();

    EdgeSegment outChunk = null;
    if (outEdgesHeadChunk != null)
      try {
        outChunk = (EdgeSegment) database.lookupByRID(outEdgesHeadChunk, true);
      } catch (RecordNotFoundException e) {
        LogManager.instance().log(this, Level.WARNING, "Record %s (outEdgesHeadChunk) not found on vertex %s. Creating a new one", outEdgesHeadChunk,
            fromVertex.getIdentity());
        outEdgesHeadChunk = null;
      }

    if (outEdgesHeadChunk == null) {
      outChunk = new MutableEdgeSegment(database, database.getNewEdgeListSize(0));
      database.createRecord(outChunk, getEdgesBucketName(fromVertex.getIdentity().getBucketId(), Vertex.DIRECTION.OUT));
      outEdgesHeadChunk = outChunk.getIdentity();

      fromVertex.setOutEdgesHeadChunk(outEdgesHeadChunk);
      database.updateRecord(fromVertex);
    }

    return outChunk;
  }

  public long countEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    long total = 0;

    switch (direction) {
    case BOTH: {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      total += (outEdges != null) ? outEdges.count(edgeType) : 0L;

      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      total += (inEdges != null) ? inEdges.count(edgeType) : 0L;
      break;
    }

    case OUT: {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      total += (outEdges != null) ? outEdges.count(edgeType) : 0L;
      break;
    }

    case IN: {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      total += (inEdges != null) ? inEdges.count(edgeType) : 0L;
      break;
    }

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }

    return total;
  }

  public void deleteEdge(final Edge edge) {
    final Database database = edge.getDatabase();

    try {
      final VertexInternal vOut = (VertexInternal) edge.getOutVertex();
      if (vOut != null) {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vOut, Vertex.DIRECTION.OUT);
        if (outEdges != null)
          outEdges.removeEdge(edge);
      }
    } catch (SchemaException | RecordNotFoundException e) {
      LogManager.instance().log(this, Level.FINE, "Error on loading outgoing vertex %s from edge %s", e, edge.getOut(), edge.getIdentity());
    }

    try {
      final VertexInternal vIn = (VertexInternal) edge.getInVertex();
      if (vIn != null) {
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vIn, Vertex.DIRECTION.IN);
        if (inEdges != null)
          inEdges.removeEdge(edge);
      }
    } catch (SchemaException | RecordNotFoundException e) {
      LogManager.instance().log(this, Level.FINE, "Error on loading incoming vertex %s from edge %s", e, edge.getIn(), edge.getIdentity());
    }

    final RID edgeRID = edge.getIdentity();
    if (edgeRID != null && !(edge instanceof LightEdge))
      // DELETE EDGE RECORD TOO
      try {
        database.getSchema().getBucketById(edge.getIdentity().getBucketId()).deleteRecord(edge.getIdentity());
      } catch (RecordNotFoundException e) {
        // ALREADY DELETED: IGNORE IT
      }
  }

  public void deleteVertex(final VertexInternal vertex) {
    final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
    if (outEdges != null) {
      final Iterator<Edge> outIterator = outEdges.edgeIterator();

      while (outIterator.hasNext()) {
        RID inV = null;
        try {
          final Edge nextEdge = outIterator.next();

          inV = nextEdge.getIn();

          VertexInternal nextVertex = (VertexInternal) nextEdge.getInVertex();

          final EdgeLinkedList inEdges2 = getEdgeHeadChunk(nextVertex, Vertex.DIRECTION.IN);
          if (inEdges2 != null) {
            inEdges2.removeEdge(nextEdge);

            if (nextEdge.getIdentity().getPosition() > -1)
              // NON LIGHTWEIGHT
              nextEdge.delete();
          }
        } catch (RecordNotFoundException e) {
          // ALREADY DELETED, IGNORE THIS
          LogManager.instance()
              .log(this, Level.FINE, "Error on deleting outgoing vertex %s connected from vertex %s (record not found)", inV, vertex.getIdentity());
        }
      }

      final RID outRID = vertex.getOutEdgesHeadChunk();
      outRID.getRecord(false).delete();
    }

    final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
    if (inEdges != null) {
      final Iterator<Edge> inIterator = inEdges.edgeIterator();

      while (inIterator.hasNext()) {
        RID outV = null;
        try {
          final Edge nextEdge = inIterator.next();

          outV = nextEdge.getOut();

          VertexInternal nextVertex = (VertexInternal) nextEdge.getOutVertex();

          final EdgeLinkedList outEdges2 = getEdgeHeadChunk(nextVertex, Vertex.DIRECTION.OUT);
          if (outEdges2 != null) {
            outEdges2.removeEdge(nextEdge);

            if (nextEdge.getIdentity().getPosition() > -1)
              // NON LIGHTWEIGHT
              nextEdge.delete();
          }
        } catch (RecordNotFoundException e) {
          // ALREADY DELETED, IGNORE THIS
          LogManager.instance().log(this, Level.WARNING, "Error on deleting incoming vertex %s connected to vertex %s", outV, vertex.getIdentity());
        }
      }

      final RID inRID = vertex.getInEdgesHeadChunk();
      inRID.getRecord(false).delete();
    }

    // DELETE VERTEX RECORD
    vertex.getDatabase().getSchema().getBucketById(vertex.getIdentity().getBucketId()).deleteRecord(vertex.getIdentity());
  }

  public Iterable<Edge> getEdges(final VertexInternal vertex) {
    final MultiIterator<Edge> result = new MultiIterator<>();

    final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
    if (outEdges != null)
      result.addIterator(outEdges.edgeIterator());

    final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
    if (inEdges != null)
      result.addIterator(inEdges.edgeIterator());

    return result;
  }

  public Iterable<Edge> getEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
    case BOTH: {
      final MultiIterator<Edge> result = new MultiIterator<>();

      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null)
        result.addIterator(outEdges.edgeIterator(edgeTypes));

      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      if (inEdges != null)
        result.addIterator(inEdges.edgeIterator(edgeTypes));
      return result;
    }

    case OUT:
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null)
        return (Iterable<Edge>) outEdges.edgeIterator(edgeTypes);
      break;

    case IN:
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      if (inEdges != null)
        return (Iterable<Edge>) inEdges.edgeIterator(edgeTypes);
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return Collections.EMPTY_LIST;
  }

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PVertex instances
   */
  public Iterable<Vertex> getVertices(final VertexInternal vertex) {
    final MultiIterator<Vertex> result = new MultiIterator<>();

    final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
    if (outEdges != null)
      result.addIterator(outEdges.vertexIterator());

    final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
    if (inEdges != null)
      result.addIterator(inEdges.vertexIterator());

    return result;
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   * @param edgeTypes Edge type names to filter
   *
   * @return An iterator of PVertex instances
   */
  public Iterable<Vertex> getVertices(final VertexInternal vertex, final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
    case BOTH: {
      final MultiIterator<Vertex> result = new MultiIterator<>();

      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null)
        result.addIterator(outEdges.vertexIterator(edgeTypes));

      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      if (inEdges != null)
        result.addIterator(inEdges.vertexIterator(edgeTypes));
      return result;
    }

    case OUT:
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null)
        return (Iterable<Vertex>) outEdges.vertexIterator(edgeTypes);
      break;

    case IN:
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      if (inEdges != null)
        return (Iterable<Vertex>) inEdges.vertexIterator(edgeTypes);
      break;

    default:
      throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return Collections.EMPTY_LIST;
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
    if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), null))
      return true;

    final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
    return inEdges != null && inEdges.containsVertex(toVertex.getIdentity(), null);
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex, final Vertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), null))
        return true;
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      return inEdges != null && inEdges.containsVertex(toVertex.getIdentity(), null);
    }

    return false;
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex, final Vertex.DIRECTION direction, final String edgeType) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (edgeType == null)
      throw new IllegalArgumentException("Edge type is null");

    final int[] bucketFilter = vertex.getDatabase().getSchema().getType(edgeType).getBuckets(true).stream().mapToInt(x -> x.getId()).toArray();

    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), bucketFilter))
        return true;
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      return inEdges != null && inEdges.containsVertex(toVertex.getIdentity(), bucketFilter);
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

  public static void setProperties(final MutableDocument edge, final Object[] properties) {
    if (properties != null)
      if (properties.length == 1 && properties[0] instanceof Map) {
        // GET PROPERTIES FROM THE MAP
        final Map<String, Object> map = (Map<String, Object>) properties[0];
        for (Map.Entry<String, Object> entry : map.entrySet())
          edge.set(entry.getKey(), entry.getValue());
      } else {
        if (properties.length % 2 != 0)
          throw new IllegalArgumentException("Properties must be an even number as pairs of name, value");
        for (int i = 0; i < properties.length; i += 2)
          edge.set((String) properties[i], properties[i + 1]);
      }
  }

  public EdgeLinkedList getEdgeHeadChunk(final VertexInternal vertex, final Vertex.DIRECTION direction) {
    if (direction == Vertex.DIRECTION.OUT) {
      final RID rid = vertex.getOutEdgesHeadChunk();
      if (rid != null) {
        try {
          return new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT, (EdgeSegment) vertex.getDatabase().lookupByRID(rid, true));
        } catch (RecordNotFoundException e) {
          LogManager.instance().log(this, Level.WARNING, "Cannot load OUT edge list chunk (%s) for vertex %s", e, rid, vertex.getIdentity());
        }
      }
    } else if (direction == Vertex.DIRECTION.IN) {
      final RID rid = vertex.getInEdgesHeadChunk();
      if (rid != null) {
        try {
          return new EdgeLinkedList(vertex, Vertex.DIRECTION.IN, (EdgeSegment) vertex.getDatabase().lookupByRID(rid, true));
        } catch (RecordNotFoundException e) {
          LogManager.instance().log(this, Level.WARNING, "Cannot load IN edge list chunk (%s) for vertex %s", e, rid, vertex.getIdentity());
        }
      }
    }

    return null;
  }

  public Map<String, Object> checkVertices(final String typeName, final boolean fix, final int verboseLevel) {
    final AtomicLong autoFix = new AtomicLong();
    final AtomicLong invalidLinks = new AtomicLong();
    final LinkedHashSet<RID> corruptedRecords = new LinkedHashSet<>();
    final List<String> warnings = new ArrayList<>();

    final Map<String, Object> stats = new HashMap<>();

    database.begin();

    try {
      database.scanType(typeName, false, (record) -> {
        try {
          final Vertex vertex = record.asVertex(true);

          final EdgeLinkedList outEdges = getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.OUT);
          if (outEdges != null) {
            final Iterator<Pair<RID, RID>> out = outEdges.entryIterator();
            while (out.hasNext()) {
              final Pair<RID, RID> current = out.next();
              final RID edgeRID = current.getFirst();
              final RID vertexRID = current.getSecond();

              boolean removeEntry = false;

              if (edgeRID == null) {
                warnings.add("outgoing edge null from vertex " + vertex.getIdentity());
                removeEntry = true;
                invalidLinks.incrementAndGet();
              } else if (vertexRID == null) {
                warnings.add("outgoing vertex null from vertex " + vertex.getIdentity());
                corruptedRecords.add(edgeRID);
                removeEntry = true;
                invalidLinks.incrementAndGet();
              } else {
                try {
                  if (edgeRID.getPosition() < 0)
                    // LIGHTWEIGHT EDGE
                    continue;

                  final Edge edge = edgeRID.asEdge(true);

                  if (edge.getIn() == null || !edge.getIn().isValid()) {
                    warnings.add("edge " + edgeRID + " has an invalid incoming link " + edge.getIn());
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  } else {
                    try {
                      edge.getInVertex().asVertex(true);
                    } catch (RecordNotFoundException e) {
                      warnings.add("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " that is not found (deleted?)");
                      corruptedRecords.add(edgeRID);
                      removeEntry = true;
                      corruptedRecords.add(edge.getIn());
                      invalidLinks.incrementAndGet();
                    } catch (Exception e) {
                      // UNKNOWN ERROR ON LOADING
                      warnings.add(
                          "edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " which cannot be loaded (error: " + e.getMessage() + ")");
                      corruptedRecords.add(edgeRID);
                      removeEntry = true;
                      corruptedRecords.add(edge.getIn());
                    }
                  }

                  if (!edge.getOut().equals(vertex.getIdentity())) {
                    warnings.add("edge " + edgeRID + " has an outgoing link " + edge.getOut() + " different from expected " + vertex.getIdentity());
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  } else if (!edge.getIn().equals(vertexRID)) {
                    warnings.add("edge " + edgeRID + " has an incoming link " + edge.getIn() + " different from expected " + vertexRID);
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  }

                } catch (RecordNotFoundException e) {
                  warnings.add("edge " + edgeRID + " not found");
                  corruptedRecords.add(edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
                } catch (Exception e) {
                  // UNKNOWN ERROR ON LOADING
                  warnings.add("edge " + edgeRID + " error on loading (error: " + e.getMessage() + ")");
                  corruptedRecords.add(edgeRID);
                  removeEntry = true;
                }
              }

              if (fix && removeEntry)
                out.remove();
            }
          }

          final EdgeLinkedList inEdges = getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.IN);
          if (inEdges != null) {
            final Iterator<Pair<RID, RID>> in = inEdges.entryIterator();
            while (in.hasNext()) {
              final Pair<RID, RID> current = in.next();
              final RID edgeRID = current.getFirst();
              final RID vertexRID = current.getSecond();

              boolean removeEntry = false;

              if (edgeRID == null) {
                warnings.add("outgoing edge null from vertex " + vertex.getIdentity());
                removeEntry = true;
                invalidLinks.incrementAndGet();
              } else if (vertexRID == null) {
                warnings.add("outgoing vertex null from vertex " + vertex.getIdentity());
                corruptedRecords.add(edgeRID);
                removeEntry = true;
                invalidLinks.incrementAndGet();
              } else {
                if (edgeRID.getPosition() < 0)
                  // LIGHTWEIGHT EDGE
                  continue;

                try {
                  final Edge edge = edgeRID.asEdge(true);

                  if (edge.getOut() == null || !edge.getOut().isValid()) {
                    warnings.add("edge " + edgeRID + " has an invalid outgoing link " + edge.getIn());
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  } else {
                    try {
                      edge.getOutVertex().asVertex(true);
                    } catch (RecordNotFoundException e) {
                      warnings.add("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " that is not found (deleted?)");
                      corruptedRecords.add(edgeRID);
                      removeEntry = true;
                      corruptedRecords.add(edge.getOut());
                      invalidLinks.incrementAndGet();
                    } catch (Exception e) {
                      // UNKNOWN ERROR ON LOADING
                      warnings.add(
                          "edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " which cannot be loaded (error: " + e.getMessage() + ")");
                      corruptedRecords.add(edgeRID);
                      removeEntry = true;
                      corruptedRecords.add(edge.getOut());
                    }
                  }

                  if (!edge.getIn().equals(vertex.getIdentity())) {
                    warnings.add("edge " + edgeRID + " has an incoming link " + edge.getIn() + " different from expected " + vertex.getIdentity());
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  } else if (!edge.getOut().equals(vertexRID)) {
                    warnings.add("edge " + edgeRID + " has an outgoing link " + edge.getOut() + " different from expected " + vertexRID);
                    corruptedRecords.add(edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  }
                } catch (RecordNotFoundException e) {
                  warnings.add("edge " + edgeRID + " not found");
                  corruptedRecords.add(edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
                } catch (Exception e) {
                  // UNKNOWN ERROR ON LOADING
                  warnings.add("edge " + edgeRID + " error on loading (error: " + e.getMessage() + ")");
                  corruptedRecords.add(edgeRID);
                  removeEntry = true;
                }
              }

              if (fix && removeEntry)
                in.remove();
            }
          }

        } catch (Throwable e) {
          warnings.add("vertex " + record.getIdentity() + " cannot be loaded (error: " + e.getMessage() + ")");
          corruptedRecords.add(record.getIdentity());
        }

        return true;
      }, (rid, exception) -> {
        warnings.add("vertex " + rid + " cannot be loaded (error: " + exception.getMessage() + ")");
        corruptedRecords.add(rid);
        return true;
      });

      if (fix) {
        for (RID rid : corruptedRecords) {
          autoFix.incrementAndGet();
          try {
            database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
          } catch (RecordNotFoundException e) {
            // IGNORE IT
          } catch (Throwable e) {
            warnings.add("Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
        }
      }

      if (verboseLevel > 0)
        for (String warning : warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      stats.put("autoFix", autoFix.get());
      stats.put("corruptedRecords", corruptedRecords);
      stats.put("invalidLinks", invalidLinks.get());
      stats.put("warnings", warnings);
    }

    return stats;
  }

  public Map<String, Object> checkEdges(final String typeName, final boolean fix, final int verboseLevel) {
    final AtomicLong autoFix = new AtomicLong();
    final AtomicLong invalidLinks = new AtomicLong();
    final AtomicLong missingReferenceBack = new AtomicLong();
    final List<RID> corruptedRecords = new ArrayList<>();
    final List<String> warnings = new ArrayList<>();

    final Map<String, Object> stats = new HashMap<>();

    database.begin();

    try {
      database.scanType(typeName, false, (record) -> {
        final RID edgeRID = record.getIdentity();

        try {
          final Edge edge = record.asEdge(true);

          if (edge == null) {
            warnings.add("edge " + edgeRID + " cannot be loaded");
            corruptedRecords.add(edgeRID);

          } else if (edge.getIn() == null || !edge.getIn().isValid()) {
            warnings.add("edge " + edgeRID + " has an invalid incoming link " + edge.getIn());
            corruptedRecords.add(edgeRID);
            invalidLinks.incrementAndGet();

          } else if (edge.getOut() == null || !edge.getOut().isValid()) {
            warnings.add("edge " + edgeRID + " has an invalid outgoing link " + edge.getOut());
            corruptedRecords.add(edgeRID);
            invalidLinks.incrementAndGet();

          } else {
            try {
              final Vertex vertex = edge.getInVertex().asVertex(true);

              final EdgeLinkedList inEdges = getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.IN);
              if (inEdges == null || !inEdges.containsEdge(edgeRID))
                // UNI DIRECTIONAL EDGE
                missingReferenceBack.incrementAndGet();

            } catch (RecordNotFoundException e) {
              warnings.add("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " that is not found (deleted?)");
              corruptedRecords.add(edgeRID);
              corruptedRecords.add(edge.getIn());
              invalidLinks.incrementAndGet();
            } catch (Exception e) {
              // UNKNOWN ERROR ON LOADING
              warnings.add("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " which cannot be loaded (error: " + e.getMessage() + ")");
              corruptedRecords.add(edgeRID);
              corruptedRecords.add(edge.getIn());
            }

            try {
              final Vertex vertex = edge.getOutVertex().asVertex(true);

              final EdgeLinkedList outEdges = getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.OUT);
              if (outEdges == null || !outEdges.containsEdge(edgeRID))
                // UNI DIRECTIONAL EDGE
                missingReferenceBack.incrementAndGet();

            } catch (RecordNotFoundException e) {
              warnings.add("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " that is not found (deleted?)");
              corruptedRecords.add(edgeRID);
              invalidLinks.incrementAndGet();
            } catch (Exception e) {
              // UNKNOWN ERROR ON LOADING
              warnings.add("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " which cannot be loaded (error: " + e.getMessage() + ")");
              corruptedRecords.add(edgeRID);
              corruptedRecords.add(edge.getOut());
            }
          }

        } catch (Throwable e) {
          warnings.add("edge " + record.getIdentity() + " cannot be loaded (error: " + e.getMessage() + ")");
          corruptedRecords.add(edgeRID);
        }

        return true;
      }, (rid, exception) -> {
        warnings.add("edge " + rid + " cannot be loaded (error: " + exception.getMessage() + ")");
        corruptedRecords.add(rid);
        return true;
      });

      if (fix) {
        for (RID rid : corruptedRecords) {
          autoFix.incrementAndGet();
          try {
            database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
          } catch (RecordNotFoundException e) {
            // IGNORE IT
          } catch (Throwable e) {
            warnings.add("Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
        }
      }

      if (verboseLevel > 0)
        for (String warning : warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      stats.put("autoFix", autoFix.get());
      stats.put("corruptedRecords", corruptedRecords);
      stats.put("invalidLinks", invalidLinks.get());
      stats.put("missingReferenceBack", missingReferenceBack.get());
      stats.put("warnings", warnings);
    }

    return stats;
  }

}
