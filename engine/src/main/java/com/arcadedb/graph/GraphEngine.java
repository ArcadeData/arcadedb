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
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    // DROP THE SUPER-NODE STRIPE POOL, IF THE TYPE EVER PROMOTED A VERTEX (#5156)
    for (int i = 0; database.getSchema().existsBucket(StripedEdgeList.stripeBucketName(type.getName(), i)); i++)
      database.getSchema().dropBucket(StripedEdgeList.stripeBucketName(type.getName(), i));
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

    final EdgeType edgeType = getEdgeType(database, edgeTypeName);

    final RID edgeRID = new RID(edgeType.getFirstBucketId(), -1l);

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

    final EdgeType type = getEdgeType(database, edgeTypeName);

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

  /**
   * Resolves an edge type by name validating its kind. Vertex and edge types share the same schema namespace: using a
   * vertex or document type name where an edge type is required must surface as a clean schema error, not as an
   * internal {@link ClassCastException} (issue #5194).
   */
  private static EdgeType getEdgeType(final DatabaseInternal database, final String edgeTypeName) {
    final DocumentType type = database.getSchema().getType(edgeTypeName);
    if (!(type instanceof EdgeType edgeType))
      throw new SchemaException("Type '" + edgeTypeName + "' is not an edge type (found " +
          (type instanceof VertexType ? "a vertex" : "a document") + " type with the same name)");
    return edgeType;
  }

  public void connectOutgoingEdge(final VertexInternal fromVertex, final Identifiable toVertex, final Edge edge) {
    // No eager modify(): materialising the MutableVertex anchors the vertex page in the transaction, putting
    // the vertex FILE into the commit lock set of EVERY append and serialising all writers on a hot vertex
    // across the whole replication round. The rare paths that really rewrite the vertex record (first chunk,
    // head flip, super-node promotion) call modify() themselves, re-validating the head at that point.
    getOrCreateEdgeList(fromVertex, Vertex.DIRECTION.OUT).add(edge.getIdentity(), toVertex.getIdentity());
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

      final EdgeType edgeType = getEdgeType(database, connection.edgeTypeName);

      edge = new MutableEdge(database, edgeType, sourceVertexRID, destinationVertex.getIdentity());

      if (connection.edgeProperties != null && connection.edgeProperties.length > 0)
        setProperties(edge, connection.edgeProperties);

      edge.save();

      outEdgePairs.add(new Pair<>(edge, destinationVertex));

      edges.add(edge);
    }

    // No eager modify() - see connectOutgoingEdge.
    getOrCreateEdgeList(sourceVertex, Vertex.DIRECTION.OUT).addAll(outEdgePairs);

    if (bidirectional) {
      for (int i = 0; i < outEdgePairs.size(); ++i) {
        final Pair<Identifiable, Identifiable> edge = outEdgePairs.get(i);
        connectIncomingEdge(edge.getSecond(), edge.getFirst().getIdentity(), sourceVertexRID);
      }
    }

    return edges;
  }

  public void connectIncomingEdge(final Identifiable toVertex, final RID fromVertexRID, final RID edgeRID) {
    // No eager modify() - see connectOutgoingEdge. On a super-node target the eager MutableVertex would
    // serialise every concurrent append on the target vertex file's commit lock, which measured as THE
    // bottleneck (all writers queueing one replication round each) regardless of the edge-list layout.
    getOrCreateEdgeList((VertexInternal) toVertex.asVertex(), Vertex.DIRECTION.IN).add(edgeRID, fromVertexRID);
  }

  /**
   * Loads (or lazily creates) the edge list of a vertex for a WRITE operation, dispatching on the head record's
   * type: a classic {@link EdgeSegment} chain yields an {@link EdgeLinkedList}, a {@link StripeDirectory}
   * (super-node promoted vertex, #5156) yields a {@link StripedEdgeList}. The head page is anchored in the
   * transaction at read time (#5147/#5153).
   */
  public EdgeLinkedList getOrCreateEdgeList(VertexInternal vertex, final Vertex.DIRECTION direction) {
    // Resolve the transaction's own WRITTEN copy of the vertex first (a cache lookup, NO page anchoring):
    // head-pointer updates are DEFERRED, so the freshest head may live only in the transaction's
    // updated-records. Reading a stale head from an older immutable copy would re-create or re-flip the chain
    // and orphan chunks appended earlier in this same transaction (lost edges). Read-only cached copies are
    // deliberately NOT consulted (see TransactionContext.getWrittenRecord).
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null) {
      final Record inTx = tx.getWrittenRecord(vertex.getIdentity());
      if (inTx instanceof VertexInternal inTxVertex && inTx != vertex)
        vertex = inTxVertex;
    }

    RID headRID = direction == Vertex.DIRECTION.OUT ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();

    if (headRID != null)
      try {
        // The transaction's own WRITTEN copy of the head (multi-append transaction) is authoritative: its
        // pending appends live only in that object until commit, and its page is already anchored.
        final TransactionContext headTx = database.getTransactionIfExists();
        if (headTx != null && headTx.getWrittenRecord(headRID) instanceof EdgeSegment written)
          return new EdgeLinkedList(vertex, direction, written);

        final Record head = database.lookupByRID(headRID, true);
        if (head instanceof StripeDirectory directory)
          // DO NOT anchor the directory page here: an anchored-but-unmodified page still contributes its FILE
          // to the commit lock set, serialising every striped append through the directory's file across the
          // replication round. StripedEdgeList anchors it only on slot writes (~1/1000 appends).
          return new StripedEdgeList(vertex, direction, directory);
        // CLASSIC list: anchor the head chunk page at read time (#5147) - it is about to be appended to -
        // and RE-READ the chunk THROUGH the anchored page, bypassing the record cache: the dispatch read
        // above happened BEFORE the anchor, so a concurrent commit publishing in between would leave a
        // one-version-older buffer paired with the fresh page version, and writing that stale buffer back
        // at commit would silently erase the concurrent append (no MVCC conflict - the version matches).
        anchorHeadChunkPage(headRID);
        final LocalBucket headBucket = (LocalBucket) database.getSchema().getBucketById(headRID.getBucketId());
        return new EdgeLinkedList(vertex, direction,
            new MutableEdgeSegment(database, headRID, headBucket.getRecord(headRID).copyOfContent()));
      } catch (final RecordNotFoundException e) {
        // TRANSIENT by construction: a concurrent commit publishes its pages one file at a time and this
        // reader takes no commit lock, so the vertex page can expose the new head RID a moment before the
        // head chunk's page is visible. Surface a retryable conflict so the transaction retry re-reads a
        // consistent view. The previous behaviour - resetting the head to a fresh chunk - ORPHANED the whole
        // existing list here (silent edge loss) whenever this window was hit.
        throw new ConcurrentModificationException(
            "Edge list " + direction + " head chunk " + headRID + " of vertex " + vertex.getIdentity()
                + " not visible yet (concurrent commit in flight)");
      }

    // FIRST EDGE IN THIS DIRECTION: the vertex record itself is rewritten (head pointer), so materialise the
    // mutable copy only now - anchoring the vertex page is correct here because the vertex is in the write set.
    // modify() reloads the record when it is not already part of the transaction: re-check the head, a
    // concurrent transaction may have created it in the meantime.
    final MutableVertex mutable = vertex.modify();
    final RID reloadedHead = direction == Vertex.DIRECTION.OUT ? mutable.getOutEdgesHeadChunk() : mutable.getInEdgesHeadChunk();
    if (reloadedHead != null)
      return getOrCreateEdgeList(mutable, direction);

    final MutableEdgeSegment chunk = new MutableEdgeSegment(database, LocalDatabase.getNewEdgeListSize(0));
    database.createRecord(chunk, getEdgesBucketName(mutable.getIdentity().getBucketId(), direction));

    if (direction == Vertex.DIRECTION.OUT)
      mutable.setOutEdgesHeadChunk(chunk.getIdentity());
    else
      mutable.setInEdgesHeadChunk(chunk.getIdentity());
    database.updateRecord(mutable);

    return new EdgeLinkedList(mutable, direction, chunk);
  }

  public EdgeSegment createInEdgeChunk(final MutableVertex toVertex) {
    RID inEdgesHeadChunk = toVertex.getInEdgesHeadChunk();

    EdgeSegment inChunk = null;
    if (inEdgesHeadChunk != null)
      try {
        anchorHeadChunkPage(inEdgesHeadChunk);
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
        anchorHeadChunkPage(outEdgesHeadChunk);
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

      fromVertex.setOutEdgesHeadChunk(outEdgesHeadChunk);
      database.updateRecord(fromVertex);
    }

    return outChunk;
  }

  /**
   * #5147: brings the head chunk's page into the transaction NOW, at the version it is read, so an append is
   * anchored to that version for the commit-time MVCC check. Without this the chunk is read via an immutable
   * lookup (which, under READ_COMMITTED, does not retain the page) and the page is only captured later by the
   * deferred {@code updateRecord} - at the newer version if a concurrent transaction appended to the same
   * chunk in between. The version check would then compare the newer version against itself, find no conflict,
   * and the stale chunk buffer would silently overwrite the concurrent append (a lost update / dropped edge).
   * Anchoring here makes the conflict visible so the transaction retries and re-reads the current chunk.
   */
  private void anchorHeadChunkPage(final RID headChunkRID) {
    try {
      ((LocalBucket) database.getSchema().getBucketById(headChunkRID.getBucketId())).fetchPageInTransaction(headChunkRID);
    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on loading edge chunk page " + headChunkRID, e);
    }
  }

  public long countEdges(final VertexInternal vertex, final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    long total = 0;

    switch (direction) {
      case BOTH: {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
        total += outEdges != null ? outEdges.count(edgeTypes) : 0L;

        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
        total += inEdges != null ? inEdges.count(edgeTypes) : 0L;
        break;
      }

      case OUT: {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
        total += outEdges != null ? outEdges.count(edgeTypes) : 0L;
        break;
      }

      case IN: {
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
        total += inEdges != null ? inEdges.count(edgeTypes) : 0L;
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
      if (database.existsRecord(edge.getOut())) {
        final VertexInternal vOut = (VertexInternal) edge.getOutVertex();
        if (vOut != null) {
          final EdgeLinkedList outEdges = getEdgeHeadChunk(vOut, Vertex.DIRECTION.OUT);
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
          final EdgeLinkedList inEdges = getEdgeHeadChunk(vIn, Vertex.DIRECTION.IN);
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

  public void moveEdge(final MutableEdge edge, final Vertex.DIRECTION direction, final RID newVertexRID) {
    if (direction != Vertex.DIRECTION.IN && direction != Vertex.DIRECTION.OUT)
      throw new IllegalArgumentException("Unsupported direction for moveEdge: " + direction);

    final String typeName = edge.getTypeName();
    final Map<String, Object> properties = edge.propertiesAsMap();
    final RID newOut = direction == Vertex.DIRECTION.OUT ? newVertexRID : edge.getOut();
    final RID newIn = direction == Vertex.DIRECTION.IN ? newVertexRID : edge.getIn();

    deleteEdge(edge);

    final EdgeType edgeType = (EdgeType) database.getSchema().getType(typeName);
    final VertexInternal fromVertex = (VertexInternal) database.lookupByRID(newOut, false);
    final Identifiable toVertex = database.lookupByRID(newIn, false);

    final MutableEdge newEdge = new MutableEdge(database, edgeType, newOut, newIn);
    if (!properties.isEmpty())
      newEdge.set(properties);
    newEdge.save();

    connectOutgoingEdge(fromVertex, toVertex, newEdge);
    if (edgeType.isBidirectional())
      connectIncomingEdge(toVertex, newOut, newEdge.getIdentity());

    edge.updateIdentity(newEdge.getIdentity(), newOut, newIn);
  }

  public void deleteVertex(final VertexInternal vertex) {
    // RETRIEVE ALL THE EDGES TO DELETE AT THE END
    final List<Identifiable> edgesToDelete = new ArrayList<>();

    EdgeLinkedList outEdges = null;
    try {
      outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null) {
        final Iterator<Edge> outIterator = outEdges.edgeIterator();

        while (outIterator.hasNext()) {
          RID inV = null;
          try {
            final Edge nextEdge = outIterator.next();
            inV = nextEdge.getIn();
            edgesToDelete.add(nextEdge);
          } catch (final RecordNotFoundException e) {
            // ALREADY DELETED, IGNORE THIS
            LogManager.instance()
                .log(this, Level.FINE, """
                        Error on deleting outgoing vertex %s connected from vertex %s (record not \
                        found)""", inV,
                    vertex.getIdentity());
          }
        }
      }
    } catch (Exception e) {
      // LINKED LIST COULD BE BROKEN
      LogManager.instance()
          .log(this, Level.WARNING, "Error on deleting outgoing edges connected to vertex %s", e, vertex.getIdentity());
    }

    EdgeLinkedList inEdges = null;
    try {
      inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      if (inEdges != null) {
        final Iterator<Edge> inIterator = inEdges.edgeIterator();

        while (inIterator.hasNext()) {
          RID outV = null;
          try {
            final Edge nextEdge = inIterator.next();
            outV = nextEdge.getOut();
            edgesToDelete.add(nextEdge);
          } catch (final RecordNotFoundException e) {
            // ALREADY DELETED, IGNORE THIS
            LogManager.instance()
                .log(this, Level.FINE, "Error on deleting incoming vertex %s connected to vertex %s", outV,
                    vertex.getIdentity());
          }
        }
      }
    } catch (Exception e) {
      // LINKED LIST COULD BE BROKEN
      LogManager.instance()
          .log(this, Level.WARNING, "Error on deleting incoming edges connected to vertex %s", e, vertex.getIdentity());
    }

    for (Identifiable edge : edgesToDelete)
      try {
        edge.asEdge().delete();
      } catch (RecordNotFoundException e) {
        // ALREADY DELETED, IGNORE IT
      }

    if (outEdges != null) {
      // RELOAD LINKED LISTS
      outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null)
        try {
          outEdges.deleteAll();
        } catch (Exception e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error on deleting outgoing edges connected to vertex %s", e,
                  vertex.getIdentity());
        }
    }

    if (inEdges != null) {
      inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      if (inEdges != null)
        try {
          inEdges.deleteAll();
        } catch (Exception e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error on deleting incoming edges connected to vertex %s", e,
                  vertex.getIdentity());
        }
    }

    // DELETE VERTEX RECORD
    vertex.getDatabase().getSchema().getBucketById(vertex.getIdentity().getBucketId()).deleteRecord(vertex.getIdentity());
  }

  public IterableGraph<Edge> getEdges(final VertexInternal vertex) {
    final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
    final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
    final MultiIterator<Edge> result = new MultiIterator<>() {
      @Override
      public long countEntries() {
        // Efficient: count entries directly from edge segments without materializing Edge objects
        return (outEdges != null ? outEdges.count() : 0L) + (inEdges != null ? inEdges.count() : 0L);
      }
    };
    if (outEdges != null)
      result.addIterator(outEdges.edgeIterator());
    if (inEdges != null)
      result.addIterator(inEdges.edgeIterator());
    return result;
  }

  public IterableGraph<Edge> getEdges(final VertexInternal vertex, final Vertex.DIRECTION direction,
                                      final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
      case BOTH: {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
        final MultiIterator<Edge> result = new MultiIterator<>() {
          @Override
          public long countEntries() {
            // Efficient: count entries directly from edge segments without materializing Edge objects
            return (outEdges != null ? outEdges.count(edgeTypes) : 0L) +
                (inEdges != null ? inEdges.count(edgeTypes) : 0L);
          }
        };
        if (outEdges != null)
          result.addIterator(outEdges.edgeIterator(edgeTypes));
        if (inEdges != null)
          result.addIterator(inEdges.edgeIterator(edgeTypes));
        return result;
      }

      case OUT:
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
        if (outEdges != null) {
          return new IterableGraph<>() {
            @Override
            public Iterator<Edge> iterator() {
              return outEdges.edgeIterator(edgeTypes);
            }

            @Override
            public int size() {
              // Efficient: count entries directly from edge segments without materializing Edge objects
              return (int) outEdges.count(edgeTypes);
            }

            @Override
            public Class<? extends Document> getEntryType() {
              return Edge.class;
            }
          };
        }
        break;

      case IN:
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
        if (inEdges != null)
          return new IterableGraph<>() {
            @Override
            public Iterator<Edge> iterator() {
              return inEdges.edgeIterator(edgeTypes);
            }

            @Override
            public int size() {
              // Efficient: count entries directly from edge segments without materializing Edge objects
              return (int) inEdges.count(edgeTypes);
            }

            @Override
            public Class<? extends Document> getEntryType() {
              return Edge.class;
            }
          };
        break;

      default:
        throw new IllegalArgumentException("Invalid direction " + direction);
    }

    return EMPTY_EDGE_LIST;
  }

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PVertex instances
   */
  public IterableGraph<Vertex> getVertices(final VertexInternal vertex) {
    final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
    final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
    final MultiIterator<Vertex> result = new MultiIterator<>() {
      @Override
      public long countEntries() {
        return (outEdges != null ? outEdges.count() : 0L) + (inEdges != null ? inEdges.count() : 0L);
      }
    };
    if (outEdges != null)
      result.addIterator(outEdges.vertexIterator());
    if (inEdges != null)
      result.addIterator(inEdges.vertexIterator());
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

    switch (direction) {
      case BOTH: {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
        final MultiIterator<Vertex> result = new MultiIterator<>() {
          @Override
          public long countEntries() {
            return (outEdges != null ? outEdges.count(edgeTypes) : 0L) +
                (inEdges != null ? inEdges.count(edgeTypes) : 0L);
          }
        };
        if (outEdges != null)
          result.addIterator(outEdges.vertexIterator(edgeTypes));
        if (inEdges != null)
          result.addIterator(inEdges.vertexIterator(edgeTypes));
        return result;
      }

      case OUT:
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
        if (outEdges != null) {
          return new IterableGraph<>() {
            @Override
            public Iterator<Vertex> iterator() {
              return outEdges.vertexIterator(edgeTypes);
            }

            @Override
            public int size() {
              return (int) outEdges.count(edgeTypes);
            }

            @Override
            public Class<? extends Document> getEntryType() {
              return Vertex.class;
            }
          };
        }
        break;

      case IN:
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
        if (inEdges != null)
          return new IterableGraph<>() {
            @Override
            public Iterator<Vertex> iterator() {
              return inEdges.vertexIterator(edgeTypes);
            }

            @Override
            public int size() {
              return (int) inEdges.count(edgeTypes);
            }

            @Override
            public Class<? extends Document> getEntryType() {
              return Vertex.class;
            }
          };
        break;

      default:
        throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return EMPTY_VERTEX_LIST;
  }

  /**
   * Returns connected vertex RIDs without loading vertex records from disk.
   * This is significantly faster than {@link #getVertices} when only RIDs are needed
   * (e.g., for hash-join neighbor maps, anti-join set construction, connectivity checks).
   */
  public Iterable<RID> getConnectedVertexRIDs(final VertexInternal vertex, final Vertex.DIRECTION direction,
                                               final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    switch (direction) {
      case BOTH: {
        final MultiIterator<RID> result = new MultiIterator<>();
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
        if (outEdges != null)
          result.addIterator(outEdges.ridIterator(edgeTypes));
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
        if (inEdges != null)
          result.addIterator(inEdges.ridIterator(edgeTypes));
        return result;
      }

      case OUT: {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
        if (outEdges != null)
          return () -> outEdges.ridIterator(edgeTypes);
        break;
      }

      case IN: {
        final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
        if (inEdges != null)
          return () -> inEdges.ridIterator(edgeTypes);
        break;
      }

      default:
        throw new IllegalArgumentException("Invalid direction " + direction);
    }
    return Collections.emptyList();
  }

  public RID getFirstEdgeConnectedToVertex(final VertexInternal vertex, final Identifiable toVertex,
                                           final int[] edgeBucketFilter) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
    if (outEdges != null) {
      final RID edgeRID = outEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
      if (edgeRID != null)
        return edgeRID;
    }

    final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
    if (inEdges != null)
      return inEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);

    return null;
  }

  public RID getFirstEdgeConnectedToVertex(final VertexInternal vertex, final Identifiable toVertex,
                                           final Vertex.DIRECTION direction, final int[] edgeBucketFilter) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      if (outEdges != null) {
        final RID edgeRID = outEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
        if (edgeRID != null)
          return edgeRID;
      }
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
      if (inEdges != null)
        return inEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
    }

    return null;
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

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex,
                                     final Vertex.DIRECTION direction) {
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

  public EdgeLinkedList getEdgeHeadChunk(final VertexInternal vertex, final Vertex.DIRECTION direction) {
    if (direction != Vertex.DIRECTION.OUT && direction != Vertex.DIRECTION.IN)
      return null;

    RID rid = null;
    try {
      rid = direction == Vertex.DIRECTION.OUT ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();
      if (rid != null) {
        final Record head = vertex.getDatabase().lookupByRID(rid, true);
        if (head instanceof StripeDirectory directory)
          // SUPER-NODE PROMOTED VERTEX (#5156): THE EDGE LIST IS STRIPED OVER MULTIPLE CHAINS
          return new StripedEdgeList(vertex, direction, directory);
        return new EdgeLinkedList(vertex, direction, (EdgeSegment) head);
      }
    } catch (final RecordNotFoundException e) {
      // rid == null: the vertex itself was deleted concurrently (stale reference from a
      //   prior traversal step). Expected under concurrent writes — log at FINE.
      // rid != null: the edge chunk segment is orphaned — possible corruption, keep WARNING.
      final Level level = rid == null ? Level.FINE : Level.WARNING;
      LogManager.instance()
          .log(this, level, "Cannot load %s edge list chunk (%s) for vertex %s", e, direction, rid,
              vertex.getIdentity());
    }

    return null;
  }

  // -------------------------------------------------------------------------
  // Graph algorithm utilities (used by algo procedures and SQL functions)
  // -------------------------------------------------------------------------

  /**
   * Returns a lazy iterator over all vertices in the database, optionally filtered by node labels.
   * Uses a MultiIterator to compose per-type iterators without loading all vertices into RAM.
   *
   * @param db         the database to query
   * @param nodeLabels optional array of vertex type names to filter (null or empty means all)
   * @return lazy iterator over all matching vertices
   */
  @SuppressWarnings("unchecked")
  public static Iterator<Vertex> getAllVertices(final Database db, final String[] nodeLabels) {
    final MultiIterator<Vertex> multiIter = new MultiIterator<>();
    for (final DocumentType type : db.getSchema().getTypes()) {
      if (!(type instanceof VertexType))
        continue;
      if (nodeLabels != null && nodeLabels.length > 0 && !Arrays.asList(nodeLabels).contains(type.getName()))
        continue;
      multiIter.addIterator((Iterator<Vertex>) (Iterator<?>) db.iterateType(type.getName(), false));
    }
    return multiIter;
  }

  /**
   * Builds a mapping from vertex RID to array index for O(1) lookups.
   */
  public static Map<RID, Integer> buildRidIndex(final List<Vertex> vertices) {
    final int n = vertices.size();
    final Map<RID, Integer> map = new HashMap<>(n * 2);
    for (int i = 0; i < n; i++)
      map.put(vertices.get(i).getIdentity(), i);
    return map;
  }

  /**
   * Returns the neighbor RID for an edge given source RID and traversal direction.
   */
  public static RID neighborRid(final Edge edge, final RID sourceRid, final Vertex.DIRECTION dir) {
    return switch (dir) {
      case OUT -> edge.getIn();
      case IN -> edge.getOut();
      default -> edge.getOut().equals(sourceRid) ? edge.getIn() : edge.getOut();
    };
  }

  /**
   * Parses a direction string ("OUT", "IN", "BOTH") into a {@link Vertex.DIRECTION} enum value.
   * Returns {@code BOTH} for null or unknown values.
   */
  public static Vertex.DIRECTION parseDirection(final String dir) {
    if (dir == null)
      return Vertex.DIRECTION.BOTH;
    return switch (dir.toUpperCase()) {
      case "OUT" -> Vertex.DIRECTION.OUT;
      case "IN" -> Vertex.DIRECTION.IN;
      default -> Vertex.DIRECTION.BOTH;
    };
  }

  /**
   * Builds an unweighted adjacency list as {@code int[][]} (GC-friendly, no boxing).
   * Two-pass approach: first counts neighbors per vertex, then fills — no reallocation.
   *
   * @param vertices  ordered list of vertices
   * @param ridToIdx  RID → index mapping (from {@link #buildRidIndex})
   * @param dir       traversal direction
   * @param relTypes  optional edge type filter (null or empty means all)
   * @return adj[i] = array of neighbor indices for vertex i
   */
  public static int[][] buildAdjacencyList(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx,
      final Vertex.DIRECTION dir, final String[] relTypes) {
    final int n = vertices.size();
    final int[] counts = new int[n];
    for (int i = 0; i < n; i++) {
      final Vertex v = vertices.get(i);
      final RID vid = v.getIdentity();
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          v.getEdges(dir, relTypes) : v.getEdges(dir);
      for (final Edge e : edges) {
        try {
          if (ridToIdx.containsKey(neighborRid(e, vid, dir)))
            counts[i]++;
        } catch (final RecordNotFoundException rnf) {  // 'rnf' not 'e' here: 'e' is the Edge loop variable in this scope
          // Ghost edge: dangling segment pointer to a missing edge/target record. Skip it (the fill
          // pass below skips it identically, so counts and adjacency stay consistent). This holds on two
          // invariants of a read query: (1) an edge record is only deleted, never resurrected, so a pass-1
          // ghost is still a ghost in pass 2; (2) the two getEdges() calls per vertex iterate the same
          // edges in the same order, so the i-th live edge counted here is the i-th live edge filled below.
          GhostEdgeReporter.reportSkipped(rnf);
        }
      }
    }
    final int[][] adj = new int[n][];
    for (int i = 0; i < n; i++)
      adj[i] = new int[counts[i]];
    final int[] pos = new int[n];
    for (int i = 0; i < n; i++) {
      final Vertex v = vertices.get(i);
      final RID vid = v.getIdentity();
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0 ?
          v.getEdges(dir, relTypes) : v.getEdges(dir);
      for (final Edge e : edges) {
        try {
          final RID nid = neighborRid(e, vid, dir);
          final Integer j = ridToIdx.get(nid);
          if (j != null)
            adj[i][pos[i]++] = j;
        } catch (final RecordNotFoundException rnf) {  // 'rnf' not 'e' here: 'e' is the Edge loop variable in this scope
          // Ghost edge: skipped identically to the counting pass above, so pos[i] never exceeds counts[i].
          GhostEdgeReporter.reportSkipped(rnf);
        }
      }
    }
    return adj;
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
}
