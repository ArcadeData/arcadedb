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

import com.arcadedb.GlobalConfiguration;
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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    // DROP THE SUPER-NODE STRIPE POOL, IF THE TYPE EVER PROMOTED A VERTEX (#5156)
    // EXACT best-effort sweep: pool buckets are created contiguously (createStripePool loops 0..stripes-1),
    // so walking until the first gap AT OR PAST the configured count covers a pool of ANY size, past or
    // present - no fixed cap to leak beyond. Gaps below the configured count (a partially-created pool) are
    // stepped over; a failed dropBucket is logged and does not abort the sweep (drops are individually
    // durable, not transactional as a group), and the loop still advances past it.
    final int configuredStripes = database.getConfiguration().getValueAsInteger(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES);
    for (int i = 0; ; i++) {
      final String stripeBucketName = StripedEdgeList.stripeBucketName(type.getName(), i);
      if (!database.getSchema().existsBucket(stripeBucketName)) {
        if (i == 0)
          // POOLS ARE CREATED FROM SLOT 0: no slot 0 = this type never promoted, skip the whole sweep
          break;
        if (i >= configuredStripes)
          break;
        continue;
      }
      try {
        database.getSchema().dropBucket(stripeBucketName);
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error dropping super-node stripe bucket '%s' of type '%s'", e, stripeBucketName, type.getName());
      }
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

    final EdgeType edgeType = getEdgeType(database, edgeTypeName);

    checkLightEdgeUniqueness(fromVertex, edgeType, toVertex.getIdentity());

    final ImmutableLightEdge edge = new ImmutableLightEdge(database, edgeType, edgeType.getFirstBucketId(),
        fromVertexRID, toVertex.getIdentity());

    connectOutgoingEdge(fromVertex, toVertex, edge);
    if (edgeType.isBidirectional())
      connectIncomingEdge(toVertex, fromVertex.getIdentity(), edge.getIdentity());

    return edge;
  }

  /**
   * Rejects a second lightweight edge of the same type between the same ordered pair, when the type asks for it.
   * <p>
   * There is no index to consult - a lightweight edge has no record - so this walks the source vertex's outgoing
   * list, which is O(degree). That is exactly why {@link EdgeType#isUnique()} is off by default: turning it on for a
   * super-node type makes every insert scan the whole chain. A type that needs the guarantee at scale is better
   * modelled as a regular edge type, where the constraint is a unique index on {@code (@out, @in)} and the check is
   * a O(log n) probe.
   */
  public void checkLightEdgeUniqueness(final VertexInternal fromVertex, final EdgeType edgeType,
                                        final RID toVertexRID) {
    if (!edgeType.isUnique())
      return;

    final EdgeLinkedList outEdges = getEdgeHeadChunk(fromVertex, Vertex.DIRECTION.OUT);
    if (outEdges == null)
      return;

    if (outEdges.containsLightEdge(edgeType.getFirstBucketId(), toVertexRID))
      throw new DuplicatedKeyException(edgeType.getName() + "[@out,@in]",
          "[" + fromVertex.getIdentity() + ", " + toVertexRID + "]", fromVertex.getIdentity());
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

    if (type.isLightweight()) {
      // The storage shape is a property of the type, not of the call: on a LIGHTWEIGHT type every edge is stored
      // inside the two vertices, so there is no record to create, save or place in a bucket.
      if (edgeProperties != null && edgeProperties.length > 0)
        throw new IllegalArgumentException("Edge type '" + type.getName()
            + "' is declared LIGHTWEIGHT, so its edges cannot have properties. Use a regular edge type if the edge "
            + "needs to carry data");
      if (bucketName != null)
        throw new IllegalArgumentException("Edge type '" + type.getName()
            + "' is declared LIGHTWEIGHT, so its edges have no record and cannot be targeted at bucket '" + bucketName
            + "'");

      checkLightEdgeUniqueness(fromVertex, type, toVertex.getIdentity());

      final MutableLightEdge lightEdge = new MutableLightEdge(database, type, fromVertexRID, toVertex.getIdentity());
      connectOutgoingEdge(fromVertex, toVertex, lightEdge);
      if (type.isBidirectional())
        connectIncomingEdge(toVertex, fromVertexRID, lightEdge.getIdentity());

      return lightEdge;
    }

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
    getOrCreateEdgeList(asVertexInternal(toVertex), Vertex.DIRECTION.IN).add(edgeRID, fromVertexRID);
  }

  /**
   * Resolves an {@link Identifiable} edge endpoint to a {@link VertexInternal}, unwrapping any {@link Vertex} that is
   * not itself a {@code VertexInternal}. Notably {@link SynchronizedVertex} - the wrapper used to share a cached
   * vertex across threads - is a {@code Vertex} whose {@code asVertex()} returns the wrapper itself, so a direct
   * {@code (VertexInternal) toVertex.asVertex()} threw {@link ClassCastException} when a cached vertex was passed as an
   * edge target (e.g. {@code from.newEdge(type, cachedVertex)}). {@link Identifiable#getRecord()} delegates to the real
   * underlying record, giving back the {@code VertexInternal} the edge-list machinery needs.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static VertexInternal asVertexInternal(final Identifiable identifiable) {
    final Vertex vertex = identifiable.asVertex();
    return vertex instanceof VertexInternal vertexInternal ? vertexInternal : (VertexInternal) vertex.getRecord();
  }

  /**
   * Rebuilds the OUT and/or IN adjacency list of each given vertex from scratch, by scanning every edge type in the
   * schema for edges whose {@code out}/{@code in} endpoint names one of the given vertex RIDs, and re-adding each
   * match found. Used by an emergency vertex restore (a freshly recreated vertex shell has no edges of its own;
   * this finds every surviving edge that still names it) - see {@link #restoreVertexAt}.
   * <p>
   * {@code GraphDatabaseChecker}'s own rebuild-from-surviving-edges repair keeps a separate, near-identical
   * implementation rather than calling this: it additionally excludes edges the same FIX pass is about to delete
   * and warns per unreadable edge, both of which need its {@code CheckReport}. Keep the two in sync by hand if the
   * scan-and-rewire mechanics change here.
   * <p>
   * A rebuilt vertex ends up with a brand-new chain; any old, now-orphaned chunks it used to point at are left
   * untouched on disk (harmless - reclaimed by compaction). An edge this transaction cannot read is skipped, not
   * fatal: the surviving, readable edges still reconnect.
   *
   * @return the number of outgoing and incoming edge entries reconnected, respectively (index 0 = out, 1 = in)
   */
  public long[] reconnectEdgesFromSurvivors(final Set<RID> reconnectOutVertices, final Set<RID> reconnectInVertices) {
    final List<EdgeType> edgeTypes = new ArrayList<>();
    for (final DocumentType schemaType : database.getSchema().getTypes())
      if (schemaType instanceof EdgeType t)
        edgeTypes.add(t);

    final List<Edge> outEdgesToReconnect = new ArrayList<>();
    final List<Edge> inEdgesToReconnect = new ArrayList<>();

    for (final EdgeType edgeType : edgeTypes) {
      final boolean bidirectional = edgeType.isBidirectional();
      database.scanType(edgeType.getName(), false, record -> {
        try {
          final Edge e = record.asEdge(true);
          if (reconnectOutVertices.contains(e.getOut()))
            outEdgesToReconnect.add(e);
          // A unidirectional edge is never stored in the target's IN list: rebuilding it there would invent
          // adjacency that never existed.
          if (bidirectional && reconnectInVertices.contains(e.getIn()))
            inEdgesToReconnect.add(e);
        } catch (final Exception ignore) {
          // Unreadable edge record: skip it, the surviving edges still reconnect.
        }
        return true;
      }, (rid, exception) -> true);
    }

    for (final Edge e : outEdgesToReconnect) {
      final MutableVertex vertex = e.getOutVertex().modify();
      getOrCreateEdgeList(vertex, Vertex.DIRECTION.OUT).add(e.getIdentity(), e.getIn());
    }

    for (final Edge e : inEdgesToReconnect) {
      final MutableVertex vertex = e.getInVertex().modify();
      getOrCreateEdgeList(vertex, Vertex.DIRECTION.IN).add(e.getIdentity(), e.getOut());
    }

    return new long[] { outEdgesToReconnect.size(), inEdgesToReconnect.size() };
  }

  /**
   * Emergency repair: recreates an empty vertex shell of type {@code typeName} at the exact RID {@code targetRid}
   * used to hold, then immediately rebuilds its adjacency from surviving edge records via
   * {@link #reconnectEdgesFromSurvivors}, unconditionally - it does not rely on {@code CHECK DATABASE}'s
   * corruption heuristics, which never flag a freshly created, legitimately edge-less vertex as needing a rebuild.
   * <p>
   * Restores graph STRUCTURE only: {@code targetRid}'s original property values are not recoverable from its
   * edges and are not set here (the caller may set them afterward if known from another source). Refuses if the
   * slot is occupied (see {@link LocalBucket#restoreRecordAtPosition}) or if {@code typeName} does not own
   * {@code targetRid}'s bucket.
   */
  public Vertex restoreVertexAt(final RID targetRid, final String typeName) {
    final DocumentType type = database.getSchema().getType(typeName);
    if (!(type instanceof VertexType))
      throw new IllegalArgumentException("Type '" + typeName + "' is not a vertex type");
    if (type.getBucketIds(true).stream().noneMatch(id -> id == targetRid.getBucketId()))
      throw new IllegalArgumentException(
          "Type '" + typeName + "' does not own bucket " + targetRid.getBucketId() + ", cannot restore " + targetRid + " as this type");

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(targetRid.getBucketId());
    final MutableVertex shell = database.newVertex(typeName);
    bucket.restoreRecordAtPosition(targetRid.getPosition(), shell);

    final Set<RID> asSet = Set.of(targetRid);
    final long[] counts = reconnectEdgesFromSurvivors(asSet, asSet);

    LogManager.instance()
            .log(this, Level.WARNING, "Restored vertex %s (type=%s): reconnected %d outgoing and %d incoming edge(s) from surviving records",
                    null, targetRid, typeName, counts[0], counts[1]);

    return targetRid.asVertex(true);
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
        if (tx != null && tx.getWrittenRecord(headRID) instanceof EdgeSegment written)
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
                + " not visible yet (concurrent commit in flight)", e);
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
    anchorRecordPage(headChunkRID);
  }

  /**
   * Brings a record's page into the transaction at its current version, so a write this transaction makes to that
   * page later is version-checked against the state it decided on rather than against whatever a concurrent commit
   * left there in the meantime. {@link #anchorHeadChunkPage} is the edge-list case (#5147);
   * {@link #checkEdgeListHeadsUnchanged} uses it on the vertex record itself (#5725).
   */
  private void anchorRecordPage(final RID rid) {
    try {
      ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).fetchPageInTransaction(rid);
    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on loading page of record " + rid, e);
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

  /**
   * Disconnects an edge from both its endpoints and deletes the edge record.
   * <p>
   * #5670: the two endpoint blocks tolerate a VANISHED ENDPOINT VERTEX (nothing left to disconnect) but NOT a
   * vanished piece of a still-existing vertex's edge list. Resolving the vertex and walking its chain used to sit
   * inside one {@code catch (SchemaException | RecordNotFoundException)}, so a chunk that was merely not visible
   * YET - a concurrent commit publishes the vertex page carrying the new head RID before the head chunk's own page,
   * the exact transient {@link #getOrCreateEdgeList} already turns into a retryable conflict on the append path -
   * silently skipped the removal while the edge record below was deleted anyway. The back-reference survived its
   * edge: one edge too many in the endpoint's degree and one integrity error, under concurrency only. Splitting the
   * resolution from the mutation, and reading the head through the strict {@link #getEdgeHeadChunkForWrite}, makes
   * that window a retry instead.
   */
  public void deleteEdge(final Edge edge) {
    deleteEdge(edge, null);
  }

  /**
   * {@link #deleteEdge(Edge)} for a caller that is about to drop one endpoint's edge list WHOLESALE, so the removal
   * of this edge from that list would be pure waste (#5760).
   * <p>
   * NOT a general-purpose "delete an edge but keep one side attached" entry point, and it cannot become one: the
   * only reason skipping is sound is that the caller destroys the skipped list immediately afterwards. Pass a
   * {@code skipEndpoint} whose list SURVIVES and the result is a back-reference to a deleted edge - precisely the
   * corruption #5670 exists to prevent. It is public only because {@code LocalDatabase} dispatches to it from
   * another package; the sole legitimate caller is {@link #deleteVertex}, and a new one has to satisfy that same
   * "the skipped list is about to be dropped" precondition.
   * <p>
   * {@code skipEndpoint} is the RID of a vertex whose edge list must NOT be touched. One of an edge's two
   * endpoints is always the vertex being deleted, and disconnecting it means walking the chain from the head
   * probing each chunk for the entry, anchoring the chunk that holds it, compacting it, and writing it back -
   * per edge, over a list that
   * {@link #deleteRemainingChunks} deletes in its entirety a moment later. Skipping it removes the work rather than
   * making it cheaper.
   * <p>
   * Matched by RID against the endpoint recorded ON THE EDGE, not by which list the edge was found in: an entry
   * whose {@code out}/{@code in} does NOT name the vertex being deleted is a reference into somebody else's list
   * and is disconnected normally.
   * <p>
   * A SELF-LOOP therefore skips BOTH sides, which is correct for the same reason as one side: {@code A -> A} is
   * reachable from both of A's lists and both are dropped. What is NOT skipped is the self-side READ - the strict
   * collection walk in {@link #deleteEdgesOf} that refuses to delete a vertex whose own list is not fully readable
   * (#5670/#5680) - because that walk is what decides which edges exist to disconnect at all.
   */
  public void deleteEdge(final Edge edge, final RID skipEndpoint) {
    final Database database = edge.getDatabase();

    disconnectEndpoint(edge, Vertex.DIRECTION.OUT, skipEndpoint);
    disconnectEndpoint(edge, Vertex.DIRECTION.IN, skipEndpoint);

    final RID edgeRID = edge.getIdentity();
    if (edgeRID != null && !(edge instanceof LightEdge))
      // DELETE EDGE RECORD TOO
      try {
        // The physical removal only: on the DELETE PATH the index cleanup has already happened. This method is
        // normally reached through LocalDatabase.deleteRecordNoLock, which cleans the record's index entries and
        // fires the delete events BEFORE dispatching an Edge here - so going back through the database would
        // repeat that work, not add it. (The comment previously here said the opposite of what the line below
        // does; verified by deleting an edge carrying an indexed property and watching the index drop from 1
        // entry to 0.)
        //
        // "Normally" is load-bearing: moveEdge calls the public deleteEdge(Edge) DIRECTLY, and there the
        // precondition does NOT hold - the old edge record's index entries are never cleaned. Measured rather
        // than assumed before writing this down, and it is benign TODAY for a reason that is an allocation
        // coincidence rather than a guarantee: moveEdge re-creates the edge with the same properties, and the
        // bucket hands the just-freed slot straight back, so the stale entry ends up on the new record with the
        // same key (checked on a multi-bucket edge type - 41 records, 41 index entries, identical RID). Anything
        // that breaks that coincidence - a different bucket, a concurrent allocation taking the slot - leaves an
        // index entry naming a record that is gone. A new caller of the public deleteEdge(Edge) must therefore
        // either arrive through deleteRecordNoLock or clean up after itself. Tracked as #5779, which carries the
        // measurement above and the ways the coincidence breaks - the comment is where you are, the issue is
        // where the fix gets scheduled.
        final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(edge.getIdentity().getBucketId());
        bucket.deleteRecord(edge.getIdentity());
      } catch (final RecordNotFoundException e) {
        // ALREADY DELETED: IGNORE IT
      }
  }

  /**
   * Removes {@code edge} from the edge list of its endpoint in {@code direction}, unless that endpoint is the one
   * the caller is dropping wholesale (see {@link #deleteEdge(Edge, RID)}).
   */
  private void disconnectEndpoint(final Edge edge, final Vertex.DIRECTION direction, final RID skipEndpoint) {
    if (skipEndpoint != null && skipEndpoint.equals(
        direction == Vertex.DIRECTION.OUT ? edge.getOut() : edge.getIn()))
      return;

    final VertexInternal endpoint = resolveEndpointToDisconnect(edge, direction);
    if (endpoint == null)
      return;

    final EdgeLinkedList list = getEdgeHeadChunkForWrite(endpoint, direction);
    if (list != null)
      list.removeEdge(edge);
  }

  /**
   * The endpoint vertex of {@code edge} in {@code direction} whose edge list still has to be disconnected, or null
   * when there is nothing to disconnect because the vertex itself is gone (deleted concurrently, or never existed -
   * a dangling endpoint reference). ONLY the vertex resolution is tolerated here: every failure further in, on the
   * edge list of a vertex that does exist, must reach the caller (see {@link #deleteEdge}).
   */
  private VertexInternal resolveEndpointToDisconnect(final Edge edge, final Vertex.DIRECTION direction) {
    final RID endpointRID = direction == Vertex.DIRECTION.OUT ? edge.getOut() : edge.getIn();
    if (endpointRID == null)
      // No endpoint recorded on this side: nothing to disconnect, same answer as a vertex that is gone. An edge
      // always carries both, so this is a guard rather than a case - but existsRecord raises IllegalArgumentException
      // on a null RID, which the catch below does not cover and which is not retryable, so it would escape as a hard
      // failure from the one method whose job is to decide what is tolerable.
      return null;
    try {
      // NOT redundant with the resolution below, however much it looks it: Edge.getOutVertex/getInVertex load with
      // loadContent=false, which hands back a LAZY handle without touching the bucket. A deleted endpoint therefore
      // does not surface here at all - it surfaces later, inside getEdgeHeadChunkForWrite, which maps it to a
      // retryable conflict. This check is what keeps the two apart: vertex gone = nothing to disconnect (tolerated),
      // vertex present but its list unreadable = a conflict to retry. Removing it turns the first into the second.
      if (!edge.getDatabase().existsRecord(endpointRID))
        return null;
      final Vertex endpoint = direction == Vertex.DIRECTION.OUT ? edge.getOutVertex() : edge.getInVertex();
      if (endpoint == null)
        return null;
      // asVertexInternal, not a direct cast: a cached vertex arrives wrapped in a SynchronizedVertex, which is a
      // Vertex but not a VertexInternal. Then getMostUpdatedVertex, because the caller is about to read this
      // vertex's edge-list HEAD and #5660 requires every such read to come from the instance the transaction holds -
      // a handle predating an append still points at the previous head. The lookup above happens to consult the same
      // cache today; going through the one method that owns the rule keeps that an invariant rather than a
      // coincidence.
      return getMostUpdatedVertex(asVertexInternal(endpoint));
    } catch (final SchemaException | RecordNotFoundException e) {
      LogManager.instance()
          .log(this, Level.FINE, "Error on loading %s vertex %s from edge %s", e, direction, endpointRID,
              edge.getIdentity());
      return null;
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
    deleteVertex(vertex, false);
  }

  /**
   * Deletes a vertex, optionally forcing removal of its record even when its edge list - or the multi-page chunk
   * chain of its own record - is structurally broken. With {@code force=true} the final record removal uses
   * {@link Bucket#deleteRecord(RID, boolean)} so a vertex whose body cannot be assembled can still be deleted.
   * <p>
   * #5680: {@code force} is now the ONLY thing that makes edge disconnection best-effort, and it makes it
   * best-effort END TO END. An ordinary delete used to collect its edges through {@link #getEdgeHeadChunk} with the
   * whole walk wrapped in {@code catch (Exception)}, so a chunk that could not be read was taken for "nothing to
   * remove here" and the vertex record was deleted on top of that empty (or partial) view - the edges outlived
   * their endpoint. That is not a fact about the graph, it is a fact about timing: a concurrent commit publishes
   * its pages one at a time and this reader holds no commit lock, so a head RID can be visible before the chunk it
   * names, and a chunk emptied by another transaction is relinked out from under a walker. Exactly the window
   * {@link #deleteEdge} answers with a retry since #5670, and it is answered the same way here.
   * <p>
   * So:
   * <ul>
   *   <li>{@code force == false} - the removal walk is strict: an unreadable head or hop raises a retryable
   *   {@link ConcurrentModificationException}, the transaction rolls back whole and re-reads a consistent view.
   *   A GENUINELY broken chain is indistinguishable from the transient one and therefore fails the delete after
   *   the retries are spent; {@code CHECK DATABASE} is the repair path - it rebuilds an unloadable chain from the
   *   surviving edge records (see {@code GraphDatabaseCheckerChainRebuildTest}), after which the delete succeeds
   *   normally.</li>
   *   <li>{@code force == true} - every one of those conflicts is absorbed and logged, INCLUDING the one raised
   *   while disconnecting an edge from the vertex at its OTHER end. Before #5680 that last one escaped:
   *   a broken NEIGHBOUR blocked a forced delete just as it blocked an ordinary one, which is precisely what
   *   {@code force} exists to override.</li>
   * </ul>
   * Draining the chunk records at the end stays best-effort in BOTH modes on purpose: by then every edge the walk
   * saw has been disconnected from the vertex at its far end and the vertex record is about to go, so a chunk that
   * cannot be read there costs orphaned chunk records - garbage {@code CHECK DATABASE} reclaims - never a
   * surviving reference.
   * <p>
   * #5725 closes the other half of the same defect, the one #5680 could not reach: an edge that did not EXIST when
   * the removal walk ran, because a concurrent transaction appended it a moment later. Strictness in the read
   * cannot see what is not there; what makes it safe is that the delete now leaves an MVCC footprint on
   * everything the list could grow through, so an append that raced it turns the delete into a retry rather than
   * into a vertex deleted with an edge it never saw still pointing at it. That is two checks, both skipped under
   * {@code force}: {@link EdgeLinkedList#anchorForFullRemoval()} pins every page of the list at the version the
   * walk reads it at, and {@link #checkEdgeListHeadsUnchanged} re-reads the vertex's head pointers, for the append
   * that lands in a BRAND NEW chunk and so touches none of those pages.
   * <p>
   * #5764: every recovery hint this method emits names {@code CHECK DATABASE RECORD}, the scope #5710 added for
   * exactly this case - see {@link #scopedRepairAdvice} and {@link #danglingRepairAdvice} for which of the two
   * each outcome deserves.
   * <p>
   * #5760 removes the two costs this method used to pay for the walk, both of which fall out of ONE observation:
   * the vertex's own lists are dropped wholesale at the end, so nothing this method does to them is worth doing.
   * <ul>
   *   <li>Each edge is disconnected from its FAR endpoint only ({@link #deleteEdge(Edge, RID)}). The self side was
   *   a chain walk, a chunk anchor, a compaction and a write-back per edge, over a list
   *   {@link #deleteRemainingChunks} deletes entirely moments later.</li>
   *   <li>The walk STREAMS. It used to materialise every edge into an {@code ArrayList} first, and had to: the
   *   self-side removals relinked and deleted chunks underneath the iterator, so a single pass would have been a
   *   walk over a list being restructured - the shape #5155, #5670 and #5680 were all about. With the self side
   *   skipped, this method no longer writes the list it is reading AT ALL, so the iterator is stable and the list
   *   of every edge (an object per edge, retained for the whole delete) simply disappears.</li>
   * </ul>
   * The self-side READ is untouched: it is what decides which edges exist, and its strictness is the whole of
   * #5670/#5680.
   */
  public void deleteVertex(final VertexInternal vertex, final boolean force) {
    // #5660: the edge-list heads are pointers INSIDE the vertex record, so they must be read from the instance this
    // transaction holds - a handle obtained before an append in the same transaction still names the previous head
    // and would hide the newest edges, which is the same "see nothing, delete anyway" defect by another route.
    final VertexInternal mostUpdatedVertex = getMostUpdatedVertex(vertex);

    // The heads this delete is about to walk, kept for checkEdgeListHeadsUnchanged below.
    final RID[] headsAtWalkStart = readEdgeListHeads(mostUpdatedVertex);

    final boolean hadOutList = deleteEdgesOf(mostUpdatedVertex, Vertex.DIRECTION.OUT, force);
    final boolean hadInList = deleteEdgesOf(mostUpdatedVertex, Vertex.DIRECTION.IN, force);

    if (hadOutList)
      deleteRemainingChunks(mostUpdatedVertex, Vertex.DIRECTION.OUT);
    if (hadInList)
      deleteRemainingChunks(mostUpdatedVertex, Vertex.DIRECTION.IN);

    if (!force && headsAtWalkStart != null)
      checkEdgeListHeadsUnchanged(mostUpdatedVertex, headsAtWalkStart[0], headsAtWalkStart[1]);

    // DELETE VERTEX RECORD
    mostUpdatedVertex.getDatabase().getSchema().getBucketById(mostUpdatedVertex.getIdentity().getBucketId())
        .deleteRecord(mostUpdatedVertex.getIdentity(), force);
  }

  /**
   * The {OUT, IN} edge-list head pointers of a vertex about to be deleted, or {@code null} if they cannot be read.
   * <p>
   * Purely ADVISORY, and the blanket catch is the point rather than an oversight. This runs FIRST, before
   * {@link #deleteEdgesOf}, only to capture a value for the optional {@link #checkEdgeListHeadsUnchanged}
   * at the end - so it must not be what decides whether the delete proceeds. On a not-yet-materialised
   * {@code ImmutableVertex} the head read lazy-loads the record, which puts every way that load can fail in front
   * of a delete that used to meet them further in, where they are each already owned and answered:
   * <ul>
   *   <li>a corrupt or truncated buffer ({@link SerializationException} and the rest of the decode family) is
   *   tolerated by {@code deleteEdgesOf}, because such a vertex reaches here with {@code force == false} and
   *   failing would make it undeletable - the complaint #4420 and #4432 fixed;</li>
   *   <li>a vanished record, or a multi-page body a concurrent commit is rewriting, is a retryable conflict
   *   {@link #getEdgeHeadChunkForWrite} raises as a {@link ConcurrentModificationException} - and one that
   *   {@code force} then absorbs, which is how {@code LocalDatabase.deleteRecordNoLock} deletes a record whose own
   *   chunk chain is broken.</li>
   * </ul>
   * Re-raising any of them from here would replace a handled outcome with a raw failure the force policy never
   * gets to see. Nothing is hidden by swallowing them either: the very next thing the delete does is read the same
   * heads again through the method that owns the answer.
   */
  private static RID[] readEdgeListHeads(final VertexInternal vertex) {
    try {
      return new RID[] { vertex.getOutEdgesHeadChunk(), vertex.getInEdgesHeadChunk() };
    } catch (final RuntimeException e) {
      return null;
    }
  }

  /**
   * #5764: the repair for a vertex whose edge list cannot be walked, named as a command that can be pasted into a
   * console rather than as a category of command.
   * <p>
   * This is the outcome {@code CHECK DATABASE RECORD} (#5710) was added for: the delete was REFUSED, so the vertex
   * is still there, and the operator holds the one piece of information that makes the repair cheap - the RID -
   * while every message here used to point at a whole-database or whole-type run costing two full passes over the
   * vertex type. Stated in the same breath because the scope does not bound everything: rebuilding an adjacency
   * means finding every surviving edge that points at the vertex, and no index maps an endpoint back to its edges,
   * so the edge sweep still runs once per distinct vertex type named.
   *
   * @see #danglingRepairAdvice() for the other outcome - the delete went THROUGH and left references behind.
   */
  private static String scopedRepairAdvice(final RID vertexRID) {
    return "run `CHECK DATABASE RECORD " + vertexRID + " FIX` to rebuild its edge list from the surviving edge "
        + "records, then retry the delete (the scope saves the vertex passes, not the edge sweep the rebuild needs)";
  }

  /**
   * #5764: the repair for the OTHER outcome - the vertex record is gone and the references the delete could not
   * remove now dangle. {@code CHECK DATABASE RECORD} cannot help there: the record it would be aimed at no longer
   * exists, and the survivors are edges nobody can enumerate without a scan. So this one stays whole-database, and
   * says what the scoped form would have bought had it been run BEFORE the delete.
   */
  private static String danglingRepairAdvice() {
    return "run `CHECK DATABASE FIX` to drop the references that now dangle - rebuilding the list first with "
        + "`CHECK DATABASE RECORD <vertex> FIX` and deleting without force is what keeps the edges";
  }

  /**
   * #5764: the same retryable conflict, carrying the repair command for the vertex whose list could not be read.
   * <p>
   * A conflict is normally absorbed by the transaction retry and never seen, so the one run that DOES surface this
   * message is the retry-exhausted one - which, by the design spelled out on {@link #deleteEdgesOf}, is the
   * run where the list is genuinely broken rather than transiently invisible. That is precisely the run whose
   * message has to say how to recover, and it used to arrive carrying only {@code getEdgeHeadChunkForWrite}'s
   * "concurrent commit in flight".
   * <p>
   * The class is preserved rather than re-typed: a retryable that is NOT a conflict (a lock timeout, replication
   * back-pressure) means something else entirely, and rewriting it into a {@link ConcurrentModificationException}
   * to improve a message would throw that distinction away.
   */
  private static NeedRetryException withRepairAdvice(final NeedRetryException e, final RID vertexRID) {
    if (!(e instanceof ConcurrentModificationException))
      return e;
    // toString() rather than getMessage(): every conflict raised on this path carries a message today, but a
    // message-less one would render the advice as "null. If it persists...", which reads as a bug in the advice
    // rather than as a missing diagnosis. toString() degrades to the class name instead.
    final String diagnosis = e.getMessage() != null ? e.getMessage() : e.toString();
    return new ConcurrentModificationException(
        diagnosis + ". If it persists once the retries are spent the list is genuinely broken: "
            + scopedRepairAdvice(vertexRID), e);
  }

  /**
   * The endpoint of {@code edge} that is not {@code vertexRID}, or {@code null} when it cannot be resolved.
   * <p>
   * Best-effort on purpose, and only ever used to enrich a message: this is called from the handler for an edge
   * whose disconnection just failed, so the edge record itself may well be unreadable. A failure to name the
   * neighbour must degrade the advice, never replace the original failure.
   * <p>
   * ONE case would answer {@code vertexRID} itself despite the name, and it is the right answer rather than a leak
   * to be closed: a self-loop ({@code out == in == vertexRID}) has no other end, and the list that failed to
   * disconnect it IS the vertex's own - so that is the RID whose repair the caller must name. Stated so the
   * equality is not "simplified" away, not because it can happen.
   * <p>
   * It cannot. The reason CHANGED with #5760 and the old one no longer holds, so it is worth being exact: this
   * used to be unreachable because the only caller ran after BOTH of the vertex's lists had been collected, and a
   * self-loop is read out of those same two lists - a list broken enough to fail the disconnection failed the
   * collection first. That argument died with the two-phase walk, which now streams, so an OUT edge is deleted
   * before the IN list has been read at all. What replaces it is stronger: #5760 skips the disconnection at BOTH
   * endpoints of a self-loop, since both are the vertex being deleted, so a self-loop never reads an edge list
   * here and therefore never raises the conflict that reaches this method.
   */
  private static RID otherEndOf(final Identifiable edge, final RID vertexRID) {
    try {
      final Edge resolved = edge.asEdge();
      final RID out = resolved.getOut();
      return vertexRID.equals(out) ? resolved.getIn() : out;
    } catch (final RuntimeException e) {
      return null;
    }
  }

  /**
   * #5725: the second half of "the list must not grow behind this delete", covering the growth that does NOT touch
   * any page {@link EdgeLinkedList#anchorForFullRemoval()} pinned.
   * <p>
   * An append that finds the head chunk FULL does not write that chunk at all: it creates a new one and records it
   * as the new head IN THE VERTEX RECORD (or, on a super-node promotion, replaces the head with a stripe
   * directory). The pinned chunk pages see nothing, this delete walks the chain hanging off the head it read at
   * the start, misses the whole new chunk, and then deletes the vertex - leaving the appended edge naming a record
   * that is gone. So the vertex record itself is re-read here, THROUGH its anchored page, and the delete is
   * refused as a retryable conflict if either head moved since the collection.
   * <p>
   * The anchor is what makes the check binding rather than advisory: the page it pins is the page the record
   * delete right after this writes, so a flip that commits between the two fails the commit-time version check
   * instead of slipping in behind the comparison. Doing it HERE, at the end, rather than up front keeps that
   * window to a few instructions - pinning the vertex page for the whole collection would put every unrelated
   * record sharing that bucket page in conflict with the delete for its entire duration.
   * <p>
   * A vertex this transaction has WRITTEN itself needs no check: its own copy is authoritative, and a concurrent
   * commit over it cannot pass the version check on that write.
   */
  private void checkEdgeListHeadsUnchanged(final VertexInternal vertex, final RID walkedOutHead,
      final RID walkedInHead) {
    final RID vertexRID = vertex.getIdentity();

    final TransactionContext tx = database.getTransactionIfExists();
    if (tx == null || tx.getWrittenRecord(vertexRID) != null)
      return;

    anchorRecordPage(vertexRID);

    final VertexInternal committed;
    try {
      // Read AFTER the anchor, so the heads compared here are the ones on the pinned page. A read never POPULATES
      // the tx record cache - only createRecord/updateRecord do - so a vertex this transaction has merely read is
      // resolved through that page rather than through a copy taken before it. It can still be answered FROM the
      // cache if this transaction wrote the record earlier, which is exactly the case the guard above returns on.
      committed = (VertexInternal) database.lookupByRID(vertexRID, true);
    } catch (final RecordNotFoundException e) {
      // The vertex is already gone: a concurrent transaction deleted it while this one was disconnecting its
      // edges. Retry, and let the re-read decide there is nothing left to delete.
      throw new ConcurrentModificationException(
          "Vertex " + vertexRID + " was deleted by a concurrent transaction while its edges were being removed", e);
    } catch (final ClassCastException e) {
      // The RID no longer names a vertex: the slot was reused after a concurrent delete. Same answer as above.
      throw new ConcurrentModificationException(
          "Vertex " + vertexRID + " no longer names a vertex record (concurrent commit in flight)", e);
    }

    final RID[] committedHeads = readEdgeListHeads(committed);
    if (committedHeads == null)
      // The committed buffer cannot be decoded: there is nothing to compare against, and refusing here would make
      // a corrupt vertex undeletable for the reason spelled out on readEdgeListHeads.
      return;

    final RID committedOutHead = committedHeads[0];
    final RID committedInHead = committedHeads[1];

    if (!Objects.equals(walkedOutHead, committedOutHead) || !Objects.equals(walkedInHead, committedInHead))
      throw new ConcurrentModificationException(
          "Edge list head of vertex " + vertexRID + " changed while it was being deleted (OUT " + walkedOutHead
              + " -> " + committedOutHead + ", IN " + walkedInHead + " -> " + committedInHead
              + "): a concurrent transaction appended an edge this delete did not see");
  }

  /**
   * Deletes every edge reachable from {@code vertex} in {@code direction}, reading the list the way a removal must:
   * the head through {@link #getEdgeHeadChunkForWrite}, every page of it pinned by
   * {@link EdgeLinkedList#anchorForFullRemoval()} (#5725), the walk through
   * {@link EdgeLinkedList#edgeIteratorForRemoval} (which, on a promoted super-node, refuses to skip a stripe chain
   * it cannot load), and the chain hops through {@link #hasNextEdgeToDelete}. See {@link #deleteVertex} for why.
   * Only {@code force} turns a conflict into a logged warning.
   * <p>
   * #5760: the walk STREAMS - each edge is deleted as it is yielded, instead of being appended to a list of every
   * edge the vertex has and deleted in a second pass. The second pass was not a style choice: while the removals
   * still disconnected each edge from THIS vertex too, they relinked and deleted chunks underneath the iterator,
   * and a single pass would have been a walk over a list being restructured. It is
   * {@link #deleteEdge(Edge, RID)} skipping this vertex that makes one pass legal - the deletions below now write
   * the FAR endpoints' lists and the edge records, never this list - and the accumulator, one live object per edge
   * held for the whole delete, goes away with it.
   * <p>
   * A single edge whose RECORD cannot be resolved is a different matter from a list that cannot be read, and stays
   * tolerated in both modes: the walk keeps every other entry, so the cost is that one already-dangling pointer
   * rather than the whole remaining list, and {@code EdgeIteratorFilter} and {@code CHECK DATABASE} treat such an
   * entry the same way.
   * <p>
   * The per-edge deletion sits OUTSIDE the try that reads the list, deliberately. The two tolerances below are
   * about THIS vertex's list; an exception raised while disconnecting an edge from the vertex at its other end
   * carries a different meaning entirely, has its own policy in {@link #deleteEdgeOfDeletedVertex}, and must not
   * be mistaken for "this vertex's list is corrupt, delete it anyway".
   *
   * @return whether the vertex has an edge list in this direction at all, i.e. whether there are chunk records left
   * to drain afterwards.
   */
  private boolean deleteEdgesOf(final VertexInternal vertex, final Vertex.DIRECTION direction, final boolean force) {
    EdgeLinkedList edges = null;
    Iterator<Edge> iterator = null;
    try {
      edges = getEdgeHeadChunkForWrite(vertex, direction);
      if (edges != null) {
        // #5725: pin every page this list can grow through BEFORE walking it, so an edge appended behind the walk
        // fails this transaction's commit-time version check instead of being deleted along with the chunk that
        // holds it. Deliberately not folded into getEdgeHeadChunkForWrite: deleteEdge calls that once per endpoint
        // per edge, and pinning a whole neighbour list there would retain a page copy per visited chunk for every
        // edge removed. Pinning earns its cost exactly where the transaction writes those pages anyway - here.
        //
        // AFTER the assignment above, not before it. A pin that fails jumps straight to the catch below, so the
        // walk is skipped either way and nothing is deleted from this direction; what the ordering buys is that
        // `edges` is non-null by then, so under force this method still reports the list as present and the chunk
        // drain still runs. Pinning before the assignment would leave it null and orphan the chunks as well.
        edges.anchorForFullRemoval();

        iterator = edges.edgeIteratorForRemoval();
      }
    } catch (final NeedRetryException e) {
      tolerateUnreadableEdgeList(e, vertex, direction, force);
    } catch (final SerializationException | NegativeArraySizeException | BufferUnderflowException
                   | IndexOutOfBoundsException | IllegalArgumentException | ClassCastException | SchemaException e) {
      tolerateUndecodableEdgeList(e, vertex, direction);
    }

    // Nothing to walk: no list in this direction, or a failure the block above tolerated. Returned here rather
    // than folded into the loop condition, which then read as a disguised while(true) - the reference is never
    // reassigned and every exit below is a break.
    if (iterator == null)
      return edges != null;

    while (true) {
      final Edge edge;
      try {
        if (!hasNextEdgeToDelete(iterator, vertex, direction))
          break;
        edge = iterator.next();
      } catch (final RecordNotFoundException e) {
        // DANGLING ENTRY: THE EDGE RECORD IS ALREADY GONE, KEEP WALKING
        LogManager.instance()
            .log(this, Level.FINE, "Error on deleting %s edge connected to vertex %s (record not found)", direction,
                vertex.getIdentity());
        continue;
      } catch (final NeedRetryException e) {
        tolerateUnreadableEdgeList(e, vertex, direction, force);
        break;
      } catch (final SerializationException | NegativeArraySizeException | BufferUnderflowException
                     | IndexOutOfBoundsException | IllegalArgumentException | ClassCastException | SchemaException e) {
        tolerateUndecodableEdgeList(e, vertex, direction);
        break;
      }

      // A SELF-LOOP ARRIVES HERE TWICE, once from each of the vertex's two lists, and that is expected rather than
      // guarded against: the second call runs the delete pipeline over a record the first already removed (the
      // iterator does not filter it out - it resolves the edge with loadContent=false, and a lazy handle to a
      // record deleted earlier in this transaction still resolves), where the disconnection is skipped on both
      // sides and bucket.deleteRecord absorbs the RecordNotFoundException. The visible consequence is that
      // onBeforeDelete fires TWICE for a self-loop, which is what this path did before #5760 as well - the
      // two-phase walk collected it from each list and called delete() on it twice. Pinned as an exact count by
      // Issue5760VertexDeleteSelfSideSkipTest.aSelfLoopIsWalkedFromBothListsSoItsDeleteEventFiresTwice, so a
      // non-idempotent listener meets a documented number rather than a surprise.
      deleteEdgeOfDeletedVertex(edge, vertex, force);
    }

    return edges != null;
  }

  /**
   * The vertex's own edge list could not be READ. Retryable, and only {@code force} absorbs it.
   * <p>
   * The split between this and {@link #tolerateUndecodableEdgeList} is NOT "transient versus permanent", and
   * reading it that way is the one mistake to avoid here. It is "what does a miss cost". A chunk that cannot be
   * FOUND is treated as retryable because that is the only answer that is safe when the alternative - deleting the
   * vertex on a short list - loses references; it is deliberately applied to a chunk that is genuinely LOST as
   * well, which no retry can bring back, so that case now fails the delete once the retries are spent instead of
   * quietly completing it. A chunk that cannot be DECODED takes the other path for the reason spelled out there,
   * not because it is less permanent.
   */
  private void tolerateUnreadableEdgeList(final NeedRetryException e, final VertexInternal vertex,
      final Vertex.DIRECTION direction, final boolean force) {
    if (!force)
      // #5764: NOT a bare rethrow. What the operator saw was getEdgeHeadChunkForWrite's "concurrent commit in
      // flight", which is the right diagnosis for the transient case that never reaches a human and says nothing
      // about the permanent one that does. See withRepairAdvice.
      throw withRepairAdvice(e, vertex.getIdentity());
    LogManager.instance()
        .log(this, Level.WARNING, """
                Cannot read the %s edge list of vertex %s while force-deleting it: its edges survive, %s""", e,
            direction, vertex.getIdentity(), danglingRepairAdvice());
  }

  /**
   * The vertex's own edge list could not be DECODED. Not an oversight and not the case above: this is what is left
   * once the TRANSIENT window has been split off into the retryable branch. What reaches here is a buffer that
   * cannot be decoded - a corrupted chunk body or vertex prefix raising {@link SerializationException},
   * {@link BufferUnderflowException}, {@link NegativeArraySizeException} and friends - which no retry can fix, and
   * which is tolerated on purpose EVEN when force is false. That looks inconsistent with the strictness above
   * until you follow where such a record comes from: {@code LocalDatabase.deleteRecordNoLock} catches exactly this
   * exception family around the index cleanup and proceeds WITHOUT setting its force flag (it raises that flag
   * only for a confirmed broken chunk chain), so a vertex whose buffer is corrupt reaches the delete with
   * {@code force == false}. Failing here would therefore make it undeletable - precisely the "records that can't
   * be deleted" complaint issues #4420 and #4432 fixed. The cost is real and larger than the tolerated single
   * dangling entry (everything behind the corrupt chunk is dropped, and the vertex is still deleted), which is why
   * it is logged at WARNING: {@code CHECK DATABASE RECORD <rid> FIX} rebuilds the chain from the surviving edge
   * records and is the way to delete such a vertex without losing its edges - run BEFORE the delete, since by the
   * time this fires the vertex is on its way out (#5764).
   * <p>
   * The caught list is CLOSED, and deliberately not a blanket {@code catch (Exception)}: "tolerate and delete
   * anyway" is the behaviour the surrounding strictness exists to take away from conditions that do not deserve
   * it, so it must not be handed to an exception nobody has reasoned about. The first five are the decode family
   * {@code LocalDatabase} uses for the same purpose; {@link ClassCastException} and {@link SchemaException} are
   * the two further shapes a CORRUPT edge list adds on top of it (a head RID naming a record that is not an edge
   * segment, an edge bucket whose type is gone). {@link IllegalArgumentException} is the loosest member and the
   * one to re-examine first if this ever starts firing in the field: it earns its place because {@code Binary}
   * raises it ("Invalid position") for a content offset that decodes past the end of the buffer, which is a
   * genuine corruption shape and not a caller error. Anything else - an NPE or an IllegalStateException from a
   * future change, an I/O failure surfacing as {@link DatabaseOperationException} - is a bug or an environment
   * fault, not a broken graph, and propagates so it is seen rather than silently paid for with the vertex's edges.
   * If a genuine corruption shape ever escapes, add it to the caught list with the reason; do not widen the catch.
   */
  private void tolerateUndecodableEdgeList(final RuntimeException e, final VertexInternal vertex,
      final Vertex.DIRECTION direction) {
    LogManager.instance()
        .log(this, Level.WARNING, """
                Cannot decode the %s edge list of vertex %s (corrupted chunk): deleting it anyway, edges behind the \
                damage survive - %s""", e, direction, vertex.getIdentity(), danglingRepairAdvice());
  }

  /**
   * Deletes one edge of a vertex that is itself being deleted: the full record deletion (index cleanup, external
   * values, delete events) with the disconnection from {@code vertex} skipped - see
   * {@link #deleteEdge(Edge, RID)} for why, and {@link #deleteEdgesOf} for why this is not inside the walk's
   * {@code try}.
   * <p>
   * Routed through the edge's OWN database handle rather than this engine's, so a wrapped instance
   * (server, replicated) still sees the delete go through its own pipeline - exactly what {@code Record.delete()}
   * did when this was a plain {@code edge.delete()}.
   */
  private void deleteEdgeOfDeletedVertex(final Edge edge, final VertexInternal vertex, final boolean force) {
    try {
      ((DatabaseInternal) edge.getDatabase()).deleteEdgeSkippingEndpoint(edge, vertex.getIdentity());
    } catch (final RecordNotFoundException e) {
      // ALREADY DELETED, IGNORE IT
    } catch (final NeedRetryException e) {
      // THE EDGE LIST OF THE VERTEX AT THE OTHER END IS NOT READABLE (SEE getEdgeHeadChunkForWrite). #5764: the
      // list that needs rebuilding belongs to the NEIGHBOUR, not to the vertex being deleted, so that is the RID
      // the advice names - resolved best-effort, since reading the edge is what just failed.
      final RID otherEnd = otherEndOf(edge, vertex.getIdentity());
      if (!force)
        throw otherEnd != null ? withRepairAdvice(e, otherEnd) : e;
      LogManager.instance()
          .log(this, Level.WARNING, """
                  Cannot disconnect edge %s from the vertex at its other end while force-deleting vertex %s: \
                  the reference survives, %s""", e, edge.getIdentity(), vertex.getIdentity(),
              otherEnd != null ?
                  "rebuild that vertex's list with `CHECK DATABASE RECORD " + otherEnd + " FIX`, then "
                      + danglingRepairAdvice() :
                  danglingRepairAdvice());
    }
  }

  /**
   * {@link Iterator#hasNext()} on a removal walk, where a chunk hop that cannot be read is a retryable conflict and
   * never a reason to stop walking - the same line {@code EdgeLinkedList.readChunk} draws for the removal walks it
   * serves. Abandoning the walk there ends the collection having seen only part of the list, and the caller goes on
   * to delete the vertex record anyway, so every edge behind the hole outlives its endpoint.
   * <p>
   * Only the CHAIN HOP surfaces here: an entry whose edge record cannot be read is skipped inside {@code hasNext}
   * itself (a dangling pointer, deliberately tolerated), so a {@link RecordNotFoundException} reaching this point
   * is always a chunk that could not be loaded.
   * <p>
   * "Reaching this point" is not the whole story on a promoted super-node, and the difference is only in WHERE the
   * conflict is raised, never in whether it is: {@link StripedEdgeList#edgeIteratorForRemoval} resolves every stripe
   * head eagerly while BUILDING the iterator, so a stripe chain that cannot be loaded raises its
   * {@link ConcurrentModificationException} there rather than on a hop through here. Both sit inside the same
   * {@code try} in {@link #deleteEdgesOf} and are handled identically.
   */
  private boolean hasNextEdgeToDelete(final Iterator<Edge> iterator, final VertexInternal vertex,
      final Vertex.DIRECTION direction) {
    try {
      return iterator.hasNext();
    } catch (final RecordNotFoundException e) {
      // Interpolated AND kept as the cause, for the reason spelled out on getEdgeHeadChunkForWrite: the chunk that
      // could not be loaded is named only inside the record-not-found message, and the top-level message is what
      // reaches a log line (#5764).
      throw new ConcurrentModificationException(
          "Edge list " + direction + " of vertex " + vertex.getIdentity()
              + " is not fully readable (concurrent commit in flight): " + e.getMessage(), e);
    }
  }

  /**
   * Drains the chunk records of a list whose owner is being deleted. Best-effort by design, in both modes: nothing
   * references these chunks any more and the vertex record is about to be deleted, so failing here would abort a
   * delete that has already done everything that could dangle a reference - see {@link #deleteVertex}.
   * <p>
   * The chunks still hold their entries: since #5760 the walk does not remove them one at a time, precisely
   * because this drops all of them at once. What matters is that the far-end back-references are gone and the
   * edge records with them, which {@link #deleteEdgesOf} has already done.
   */
  private void deleteRemainingChunks(final VertexInternal vertex, final Vertex.DIRECTION direction) {
    try {
      // RE-READ THE HEAD rather than reuse the walk's list: under force the walk may not have produced one at all.
      final EdgeLinkedList edges = getEdgeHeadChunk(vertex, direction);
      if (edges != null)
        edges.deleteAll();
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.WARNING, "Error on deleting the %s edge list chunks of vertex %s", e, direction,
              vertex.getIdentity());
    }
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
   * Returns the edges between two known vertices, without materialising the ones that do not reach the target.
   * <p>
   * This is the iterating counterpart of {@link #isVertexConnectedTo(VertexInternal, Identifiable, Vertex.DIRECTION, String)}:
   * use it when the far end of the pattern is already pinned and the edges still have to be inspected - typically to
   * apply a filter on the edge's own properties. Compared to walking {@link #getEdges(VertexInternal, Vertex.DIRECTION,
   * String...)} and comparing endpoints afterwards, the rejection happens on the pointers held in the edge segment, so
   * a non-matching edge costs a pointer comparison instead of a record load and a property deserialization. The walk
   * itself is not free - the segment still yields an RID per entry, as it always did - but on a super-node dropping
   * the per-edge record load is the difference between the probe being unusable and being cheap.
   *
   * Under {@link Vertex.DIRECTION#BOTH} a self-loop is returned twice, once from each list, exactly as
   * {@link #getEdges(VertexInternal, Vertex.DIRECTION, String...)} does. A caller that cares must de-duplicate by
   * edge identity, as the Cypher expansion does.
   *
   * @param target the vertex the returned edges must reach; must not be null
   */
  public Iterator<Edge> getEdgesConnectedTo(final VertexInternal vertex, final Vertex.DIRECTION direction,
                                            final Identifiable target, final String... edgeTypes) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");
    if (target == null)
      throw new IllegalArgumentException("Target vertex is null");

    final RID targetRID = target.getIdentity();
    // The edge-list head pointers live in the vertex record, so they have to be read from the instance the
    // running transaction has, not from whatever snapshot the caller happens to hold: a vertex loaded before
    // an edge was appended still points at the previous head and would hide the newest edges. The Vertex-level
    // accessors reach the engine already holding that instance - ImmutableVertex resolves it explicitly and a
    // MutableVertex is one - but a caller reaching this method directly hands over whatever it has, so the
    // resolution belongs here.
    final VertexInternal source = getMostUpdatedVertex(vertex);

    switch (direction) {
      case BOTH: {
        // No countEntries override here, unlike the BOTH branch of getEdges: that one delegates to
        // EdgeLinkedList.count, which filters by edge type only. There is no neighbour-keyed count, so an
        // override could do nothing cheaper than the walk MultiIterator already falls back to.
        final MultiIterator<Edge> result = new MultiIterator<>();
        final EdgeLinkedList outEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.OUT);
        if (outEdges != null)
          result.addIterator(outEdges.edgeIteratorConnectedTo(targetRID, edgeTypes));
        final EdgeLinkedList inEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.IN);
        if (inEdges != null)
          result.addIterator(inEdges.edgeIteratorConnectedTo(targetRID, edgeTypes));
        return result;
      }

      case OUT: {
        final EdgeLinkedList outEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.OUT);
        return outEdges != null ? outEdges.edgeIteratorConnectedTo(targetRID, edgeTypes) : Collections.emptyIterator();
      }

      case IN: {
        final EdgeLinkedList inEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.IN);
        return inEdges != null ? inEdges.edgeIteratorConnectedTo(targetRID, edgeTypes) : Collections.emptyIterator();
      }

      default:
        throw new IllegalArgumentException("Invalid direction " + direction);
    }
  }

  /**
   * Returns the instance of the vertex the running transaction has already loaded, or the given one.
   * <p>
   * Every read of an edge-list head has to go through this: the head is a pointer inside the vertex record, so a
   * handle obtained before an edge was appended still points at the previous head and would hide the newest edges.
   * The {@link Vertex} accessors call it before reaching the engine; a caller reaching the engine directly gets it
   * from the entry point it uses.
   * <p>
   * The cache is keyed by RID over every record shape, so the entry can legitimately be absent (the vertex was never
   * touched in this transaction) or not a vertex at all (a record loaded through a path that did not resolve its
   * type). Neither is an error: the caller's own handle is then the best available.
   */
  VertexInternal getMostUpdatedVertex(final VertexInternal vertex) {
    if (!database.isTransactionActive())
      return vertex;
    return database.getTransaction().getRecordFromCache(vertex.getIdentity()) instanceof VertexInternal cached ?
        cached :
        vertex;
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

    // Read the edge-list head from the instance this transaction holds; see getMostUpdatedVertex.
    final VertexInternal source = getMostUpdatedVertex(vertex);

    final EdgeLinkedList outEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.OUT);
    if (outEdges != null) {
      final RID edgeRID = outEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
      if (edgeRID != null)
        return edgeRID;
    }

    final EdgeLinkedList inEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.IN);
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

    // Read the edge-list head from the instance this transaction holds; see getMostUpdatedVertex.
    final VertexInternal source = getMostUpdatedVertex(vertex);

    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.OUT);
      if (outEdges != null) {
        final RID edgeRID = outEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
        if (edgeRID != null)
          return edgeRID;
      }
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.IN);
      if (inEdges != null)
        return inEdges.getFirstEdgeConnectedToVertex(toVertex.getIdentity(), edgeBucketFilter);
    }

    return null;
  }

  /**
   * Tells whether the two vertices are joined, rejecting on the neighbour pointer held in the edge segment so no
   * edge record is loaded. {@link #getEdgesConnectedTo(VertexInternal, Vertex.DIRECTION, Identifiable, String...)}
   * is the iterating counterpart, for when the matching edges themselves have to be inspected.
   * <p>
   * Like that method, and like every other probe here that walks an edge list, it resolves the vertex to the instance
   * the running transaction holds before reading the head pointer - see {@link #getMostUpdatedVertex}.
   */
  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    // Read the edge-list head from the instance this transaction holds; see getMostUpdatedVertex.
    final VertexInternal source = getMostUpdatedVertex(vertex);

    final EdgeLinkedList outEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.OUT);
    if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), null))
      return true;

    final EdgeLinkedList inEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.IN);
    return inEdges != null && inEdges.containsVertex(toVertex.getIdentity(), null);
  }

  public boolean isVertexConnectedTo(final VertexInternal vertex, final Identifiable toVertex,
                                     final Vertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    // Read the edge-list head from the instance this transaction holds; see getMostUpdatedVertex.
    final VertexInternal source = getMostUpdatedVertex(vertex);

    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.OUT);
      if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), null))
        return true;
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.IN);
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

    // Read the edge-list head from the instance this transaction holds; see getMostUpdatedVertex.
    final VertexInternal source = getMostUpdatedVertex(vertex);

    final int[] bucketFilter = source.getDatabase().getSchema().getType(edgeType).getBuckets(true).stream()
        .mapToInt(x -> x.getFileId()).toArray();

    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList outEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.OUT);
      if (outEdges != null && outEdges.containsVertex(toVertex.getIdentity(), bucketFilter))
        return true;
    }

    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final EdgeLinkedList inEdges = getEdgeHeadChunk(source, Vertex.DIRECTION.IN);
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

  /**
   * READ-side edge list of a vertex: best-effort, so a head chunk that cannot be loaded costs the caller the list
   * (null) rather than an exception. Callers that are about to MUTATE the list must use
   * {@link #getEdgeHeadChunkForWrite} instead - see the contract stated there.
   */
  public EdgeLinkedList getEdgeHeadChunk(final VertexInternal vertex, final Vertex.DIRECTION direction) {
    if (direction != Vertex.DIRECTION.OUT && direction != Vertex.DIRECTION.IN)
      return null;

    RID rid = null;
    try {
      rid = direction == Vertex.DIRECTION.OUT ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();
      if (rid != null)
        return buildEdgeList(vertex, direction, rid);
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

  /**
   * WRITE-side edge list of a vertex, for a caller about to remove entries from it (#5670). Identical to
   * {@link #getEdgeHeadChunk} except that a head RID which is present but cannot be READ raises a retryable
   * {@link ConcurrentModificationException} instead of answering null.
   * <p>
   * Null means one thing only here: the vertex has NO edge list in this direction, so there is genuinely nothing
   * to remove. Anything else is not a fact about the graph, it is a fact about timing - a concurrent commit
   * publishes its pages one at a time and this reader holds no commit lock, so the vertex page can expose a head
   * RID a moment before that head's own page becomes visible. Answering null there let the caller "successfully"
   * skip a removal it had to perform. {@link #getOrCreateEdgeList} draws the same line on the append path, and
   * {@link StripedEdgeList#addChain} on the per-stripe chains.
   * <p>
   * The price, taken deliberately: a chunk that is not transiently invisible but GENUINELY lost is indistinguishable
   * from one here, so it now fails the removal on every attempt instead of completing it best-effort and leaving the
   * back-reference. Repair belongs to {@code CHECK DATABASE}, which rebuilds an unloadable chain from the surviving
   * edge records and drops the references into it, after which the removal goes through normally.
   * <p>
   * That price reaches {@link #deleteVertex} too, from both sides. Disconnecting an edge touches the vertex at the
   * OTHER end, so deleting a healthy vertex whose NEIGHBOUR's list cannot be read reports a conflict; and #5680
   * routes the collection of the vertex's OWN edges through here as well, so a delete can no longer remove the
   * record while its edges stay behind. Both are deliberate - succeeding in either case leaves a reference to a
   * record that is gone - and {@code force} is the single documented escape hatch from both. Pinned by
   * {@code Issue5670EdgeDeleteDanglingBackRefTest.deletingAVertexWhoseNeighbourListIsUnreadableReportsAConflictRatherThanDanglingTheReference}
   * and by {@code Issue5680VertexDeleteEdgeCollectionTest}.
   */
  public EdgeLinkedList getEdgeHeadChunkForWrite(final VertexInternal vertex, final Vertex.DIRECTION direction) {
    if (direction != Vertex.DIRECTION.OUT && direction != Vertex.DIRECTION.IN)
      return null;

    // The head-RID read stays INSIDE the try: on a not-yet-materialised ImmutableVertex it lazy-loads the record,
    // so a vertex deleted since the caller resolved it raises RecordNotFoundException here rather than at the chunk
    // lookup. That is the same transient this method exists to convert - letting it escape raw would fail the
    // transaction with an exception that is NOT retryable.
    try {
      final RID rid = direction == Vertex.DIRECTION.OUT ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();
      if (rid == null)
        return null;
      return buildEdgeList(vertex, direction, rid);
    } catch (final RecordNotFoundException e) {
      // The cause and the interpolated e.getMessage() are NOT redundant, though they read that way (#5764). This
      // message is the only place the missing CHUNK's RID appears - the text above names the vertex, not the chunk,
      // which is inside the record-not-found message - and the top-level message is what a log line and an HTTP
      // error body carry, while the cause chain surfaces only in a full trace or a development-mode detail field.
      // Pinned by Issue5670EdgeDeleteDanglingBackRefTest, which asserts the head chunk's RID is in this message.
      throw new ConcurrentModificationException(
          "Edge list " + direction + " of vertex " + vertex.getIdentity()
              + " is not fully visible yet (concurrent commit in flight): " + e.getMessage(), e);
    }
  }

  /** Wraps the head record of an edge list in the view matching its layout: striped (#5156) or classic. */
  private EdgeLinkedList buildEdgeList(final VertexInternal vertex, final Vertex.DIRECTION direction, final RID headRID) {
    final Record head = vertex.getDatabase().lookupByRID(headRID, true);
    if (head instanceof StripeDirectory directory)
      // SUPER-NODE PROMOTED VERTEX (#5156): THE EDGE LIST IS STRIPED OVER MULTIPLE CHAINS
      return new StripedEdgeList(vertex, direction, directory);
    return new EdgeLinkedList(vertex, direction, (EdgeSegment) head);
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

  // newLightEdge() is deprecated in favour of declaring LIGHTWEIGHT on the edge type, but this method must
  // reproduce each edge with the shape it ALREADY has, which is not necessarily the shape its type declares:
  // a database written before the flag existed can hold lightweight edges on a type that declares nothing.
  // Routing those through newEdge() would silently materialise a record and change the storage of edges the
  // caller only asked to move. Do not "fix" this warning by switching to newEdge().
  @SuppressWarnings("deprecation")
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
