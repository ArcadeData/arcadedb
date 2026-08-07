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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

/**
 * Linked list uses to manage edges in vertex. The edges are stored in reverse order from insertion. The last item is the first in the list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeLinkedList {
  protected final Vertex vertex;
  protected final Vertex.DIRECTION direction;
  private EdgeSegment lastSegment;

  public EdgeLinkedList(final Vertex vertex, final Vertex.DIRECTION direction, final EdgeSegment lastSegment) {
    this.vertex = vertex;
    this.direction = direction;
    this.lastSegment = lastSegment;
  }

  public Iterator<Pair<RID, RID>> entryIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new EdgeVertexIterator(lastSegment, vertex.getIdentity(), direction);
    return new EdgeVertexIteratorFilter((DatabaseInternal) vertex.getDatabase(), lastSegment, edgeTypes);
  }

  public Iterator<Edge> edgeIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new EdgeIterator(lastSegment, vertex.getIdentity(), direction);
    return new EdgeIteratorFilter((DatabaseInternal) vertex.getDatabase(), vertex, direction, lastSegment, edgeTypes);
  }

  /**
   * #5680: {@link #edgeIterator} for a caller that is about to REMOVE every edge it yields (today,
   * {@code GraphEngine.deleteVertex}), so a part of the list that cannot be read must surface rather than be
   * skipped - the caller deletes the vertex record on top of whatever this walk returned, and an entry silently
   * dropped here outlives its endpoint.
   * <p>
   * On the classic layout the walk is a single chain and the two are the same iterator: an unreadable hop already
   * escapes, and the caller maps it to a retryable conflict. The distinction exists for the striped layout, where
   * {@link StripedEdgeList} composes one iterator per stripe chain and a chain whose head cannot be read is
   * legitimately skipped on a READ - see {@code StripedEdgeList.addChain}.
   */
  public Iterator<Edge> edgeIteratorForRemoval() {
    return edgeIterator();
  }

  /**
   * #5725: brings EVERY page of this list into the transaction, at the version the caller is about to read the
   * list at, for a caller that is going to remove the whole list and then delete the vertex that owns it (today,
   * {@code GraphEngine.deleteVertex}).
   * <p>
   * Without this the collection walk leaves no MVCC footprint at all - it reads the chunks through plain
   * {@code lookupByRID}, which under READ_COMMITTED does not retain their pages - while the writes that follow
   * capture each page only LATER, at whatever version it has by then. An edge appended between the walk and those
   * writes is therefore already IN the page the removal rebuilds and the drain deletes: the commit-time check
   * compares the newer version against itself, finds no conflict, and the vertex goes away with an edge it never
   * collected. The edge record survives naming a vertex that no longer exists - the same silent graph corruption
   * #5670/#5680 fixed from the removal side, arriving from the append side. This is the read-modify-write gap
   * #5147 closed on the append path ({@code GraphEngine.anchorHeadChunkPage}), on the walk that removes.
   * <p>
   * Every chunk, not just the head: an appender resolves the head from its own handle of the vertex, so a handle
   * that predates a head flip appends into a chunk that is no longer the head but is still in the chain. Anchoring
   * the whole chain costs nothing at the peak either - {@link #deleteAll} deletes every one of these chunks, so
   * their pages end up in the transaction regardless; this only brings them in EARLY ENOUGH to be worth something.
   * <p>
   * Reading through {@link #loadChunkForWrite} is what makes the anchor mean something: it anchors the page and
   * then re-reads the chunk THROUGH it, so the {@code previous} pointer this walk follows comes from the version
   * it just pinned rather than from one a concurrent commit may have replaced in between.
   */
  public void anchorForFullRemoval() {
    lastSegment = anchorChain(lastSegment.getIdentity());
  }

  /**
   * Anchors every page of the chunk chain starting at {@code headRID}, returning the head re-read through its
   * anchored page. Shared with {@link StripedEdgeList}, which applies it to one stripe chain at a time.
   * <p>
   * A hop this walk cannot follow STOPS the pinning instead of failing it - the pages up to the break stay pinned
   * and the caller carries on. The reason is that this pass must not become the thing that reports a broken chain:
   * the collection walk that runs immediately after follows the same pointers in the same order, so it meets the
   * same wall, and the policy for what to do about it already lives there and is carefully split (a chunk that
   * cannot be FOUND is retryable and only {@code force} absorbs it; a chunk that cannot be DECODED is tolerated
   * even without {@code force}, so a vertex whose list is corrupt stays deletable - #4420/#4432). Raising from
   * here would pre-empt all of that with a failure BEFORE a single edge has been collected, so a delete that used
   * to disconnect everything in front of the break from its neighbours would now disconnect nothing and leave those
   * far-end pointers dangling. Nothing is hidden by stopping quietly: the next walk raises what this one saw.
   * <p>
   * The HEAD load is deliberately outside that tolerance. It is where an append lands, and unlike the hops it is
   * not re-read by the collection walk - {@code lastSegment} is already materialised - so continuing past a head
   * this walk could not pin would leave the collection reading a page it never pinned, which is the exact window
   * this whole mechanism exists to close.
   * <p>
   * The self-reference guard matches every other walk in this class: a chunk pointing at itself ends the chain
   * instead of looping forever. A longer cycle would hang here exactly as it already hangs {@link #deleteAll},
   * which walks the same chain immediately afterwards, so this adds no exposure that was not there.
   */
  protected final EdgeSegment anchorChain(final RID headRID) {
    final EdgeSegment head = loadChunkForWrite(headRID);
    EdgeSegment current = head;
    while (true) {
      try {
        final RID previousRID = current.getPreviousRID();
        if (previousRID == null || previousRID.equals(current.getIdentity()))
          return head;
        current = loadChunkForWrite(previousRID);
      } catch (final ConcurrentModificationException | SerializationException | NegativeArraySizeException
                     | BufferUnderflowException | IndexOutOfBoundsException | IllegalArgumentException
                     | ClassCastException | SchemaException e) {
        // "This chunk cannot be read", in the two shapes the chain can produce it: loadChunkForWrite maps a
        // vanished record to a retryable conflict, and a corrupted body fails to decode. Both stop the pinning
        // here and are re-raised by the collection walk. Anything else - an I/O fault surfacing as
        // DatabaseOperationException - is not a broken chain and must not be mistaken for one, because the
        // collection walk might then read a chunk this pass failed to pin.
        return head;
      }
    }
  }

  /**
   * Same as {@link #edgeIterator(String...)} but yielding only the edges that reach the given
   * neighbour vertex.
   * <p>
   * The filter is applied to the neighbour pointer stored beside each edge pointer in the segment,
   * so a non-matching edge is discarded without its record being loaded. Use this instead of
   * iterating every edge and comparing endpoints whenever the far end is already known.
   */
  public Iterator<Edge> edgeIteratorConnectedTo(final RID neighbor, final String... edgeTypes) {
    final ResettableIteratorBase<Edge> iterator;
    if (edgeTypes == null || edgeTypes.length == 0)
      iterator = new EdgeIterator(lastSegment, vertex.getIdentity(), direction);
    else
      iterator = new EdgeIteratorFilter((DatabaseInternal) vertex.getDatabase(), vertex, direction, lastSegment, edgeTypes);
    iterator.setNeighborVertexFilter(neighbor);
    return iterator;
  }

  public Iterator<Vertex> vertexIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new VertexIterator((DatabaseInternal) vertex.getDatabase(), lastSegment);
    return new VertexIteratorFilter((DatabaseInternal) vertex.getDatabase(), lastSegment, edgeTypes);
  }

  public Iterator<RID> ridIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new RIDIterator(lastSegment);
    return new RIDIteratorFilter((DatabaseInternal) vertex.getDatabase(), lastSegment, edgeTypes);
  }

  /**
   * True if the list already holds a lightweight edge of the given type reaching the given vertex. Backs the
   * {@link com.arcadedb.schema.EdgeType#isUnique()} check, which has no index to consult and therefore walks the
   * whole chain: O(degree).
   */
  public boolean containsLightEdge(final int edgeTypeBucketId, final RID vertexRID) {
    EdgeSegment current = lastSegment;
    while (current != null) {
      if (current.containsLightEdge(edgeTypeBucketId, vertexRID))
        return true;

      final EdgeSegment prev = current.getPrevious();
      if (prev != null && prev.getIdentity().equals(current.getIdentity()))
        // CURRENT POINT TO ITSELF, AVOID LOOPS
        break;

      current = prev;
    }

    return false;
  }

  public boolean containsEdge(final RID rid) {
    EdgeSegment current = lastSegment;
    while (current != null) {
      if (current.containsEdge(rid))
        return true;

      final EdgeSegment prev = current.getPrevious();
      if (prev != null && prev.getIdentity().equals(current.getIdentity()))
        // CURRENT POINT TO ITSELF, AVOID LOOPS
        break;

      current = prev;
    }

    return false;
  }

  public JSONArray toJSON() {
    final JSONArray array = new JSONArray();

    EdgeSegment current = lastSegment;
    while (current != null) {
      final JSONObject j = current.toJSON(false);
      if (j.has("array")) {
        final JSONArray a = j.getJSONArray("array");
        for (int i = 0; i < a.length(); ++i)
          array.put(a.getString(i));
      }
      current = current.getPrevious();
    }

    return array;
  }

  public RID getFirstEdgeConnectedToVertex(final RID ridVertex, final int[] edgeBucketFilter) {
    EdgeSegment current = lastSegment;
    while (current != null) {
      final RID edgeConnectedToVertex = current.getFirstEdgeConnectedToVertex(ridVertex, edgeBucketFilter);
      if (edgeConnectedToVertex != null)
        return edgeConnectedToVertex;

      final EdgeSegment prev = current.getPrevious();
      if (prev != null && prev.getIdentity().equals(current.getIdentity()))
        // CURRENT POINT TO ITSELF, AVOID LOOPS
        break;

      current = prev;
    }

    return null;
  }


  public boolean containsVertex(final RID rid, final int[] edgeBucketFilter) {
    EdgeSegment current = lastSegment;
    while (current != null) {
      final RID edgeConnectedToVertex = current.getFirstEdgeConnectedToVertex(rid, edgeBucketFilter);
      if (edgeConnectedToVertex != null)
        return true;

      final EdgeSegment prev = current.getPrevious();
      if (prev != null && prev.getIdentity().equals(current.getIdentity()))
        // CURRENT POINT TO ITSELF, AVOID LOOPS
        break;

      current = prev;
    }

    return false;
  }

  /**
   * Counts the items in the linked list.
   *
   * @param edgeTypes Types of edges to filter for the counting. If null or empty, any type is counted.
   *                  Non-existent edge type names are skipped (matching the behaviour of
   *                  {@link IteratorFilterBase}), so callers - e.g. range evaluation through
   *                  {@code MultiValue.getSize} - do not see a {@code SchemaException}.
   */
  public long count(final String... edgeTypes) {
    long total = 0;

    final Set<Integer> fileIdToFilter;
    if (edgeTypes != null && edgeTypes.length > 0) {
      fileIdToFilter = new HashSet<>();
      final Schema schema = vertex.getDatabase().getSchema();
      for (final String edgeType : edgeTypes) {
        if (!schema.existsType(edgeType))
          continue;
        fileIdToFilter.addAll(schema.getType(edgeType).getBucketIds(true));
      }
      if (fileIdToFilter.isEmpty())
        return 0;
    } else
      fileIdToFilter = null;

    EdgeSegment current = lastSegment;
    while (current != null) {
      total += current.count(fileIdToFilter);
      current = current.getPrevious();
    }

    return total;
  }

  public void add(final RID edgeRID, final RID vertexRID) {
    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();
    if (lastSegment.add(edgeRID, vertexRID)) {
      database.updateRecord(lastSegment);
      // Record the in-chunk append as commutative: at commit, a page-version conflict on this chunk caused only
      // by concurrent appends can be resolved by replaying the append instead of retrying the whole transaction.
      final TransactionContext tx = database.getTransactionIfExists();
      if (tx != null)
        tx.trackEdgeAppend(lastSegment.getIdentity(), edgeRID, vertexRID);
    } else {
      // CHUNK FULL: the one place that already rewrites the vertex record, so promotion to the super-node
      // striped layout (#5156) costs no extra write here.
      if (tryPromoteToSuperNode(database, edgeRID, vertexRID))
        return;

      // HEAD FLIP: the vertex record is rewritten, so materialise the mutable copy only here - keeping plain
      // appends off the vertex file's commit lock (the hot-vertex serialisation point). modify() reloads the
      // record when it is not in the transaction: if the head moved in the meantime, flipping it to our chunk
      // would orphan the concurrent chunk (lost edges) - surface a retryable conflict instead.
      final MutableVertex modifiableV = vertex.modify();
      final RID reloadedHead = direction == Vertex.DIRECTION.OUT ?
          modifiableV.getOutEdgesHeadChunk() :
          modifiableV.getInEdgesHeadChunk();
      if (reloadedHead != null && !reloadedHead.equals(lastSegment.getIdentity()))
        throw new ConcurrentModificationException(
            "Edge list " + direction + " head of vertex " + vertex.getIdentity() + " changed by a concurrent transaction");

      // ALLOCATE A NEW, BIGGER CHUNK
      final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, computeBestSize());

      newChunk.add(edgeRID, vertexRID);
      newChunk.setPrevious(lastSegment);

      // createRecord poisons the new chunk's page for the append-merge (a new chunk cannot be rebased).
      database.createRecord(newChunk, database.getSchema().getBucketById(lastSegment.getIdentity().getBucketId()).getName());

      if (direction == Vertex.DIRECTION.OUT)
        modifiableV.setOutEdgesHeadChunk(newChunk.getIdentity());
      else
        modifiableV.setInEdgesHeadChunk(newChunk.getIdentity());

      lastSegment = newChunk;

      modifiableV.save();
    }
  }

  public void addAll(final List<Pair<Identifiable, Identifiable>> entries) {
    final Set<Record> recordsToUpdate = new HashSet<>();

    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();

    Vertex currentVertex = vertex;

    for (int i = 0; i < entries.size(); ++i) {
      final Pair<Identifiable, Identifiable> entry = entries.get(i);

      final RID edgeRID = entry.getFirst() != null ? entry.getFirst().getIdentity() : null;
      final RID vertexRID = entry.getSecond().getIdentity();

      if (lastSegment.add(edgeRID, vertexRID))
        recordsToUpdate.add(lastSegment);
      else {
        // CHUNK FULL, ALLOCATE A NEW ONE
        final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, computeBestSize());

        newChunk.add(edgeRID, vertexRID);
        newChunk.setPrevious(lastSegment);

        database.createRecord(newChunk, database.getSchema().getBucketById(lastSegment.getIdentity().getBucketId()).getName());

        final MutableVertex modifiableV = currentVertex.modify();
        if (currentVertex == vertex) {
          // First flip of this batch: same stale-head guard as add() (modify() may have reloaded the record).
          final RID reloadedHead = direction == Vertex.DIRECTION.OUT ?
              modifiableV.getOutEdgesHeadChunk() :
              modifiableV.getInEdgesHeadChunk();
          if (reloadedHead != null && !reloadedHead.equals(lastSegment.getIdentity()))
            throw new ConcurrentModificationException(
                "Edge list " + direction + " head of vertex " + vertex.getIdentity() + " changed by a concurrent transaction");
        }
        currentVertex = modifiableV;

        if (direction == Vertex.DIRECTION.OUT)
          modifiableV.setOutEdgesHeadChunk(newChunk.getIdentity());
        else
          modifiableV.setInEdgesHeadChunk(newChunk.getIdentity());

        lastSegment = newChunk;

        recordsToUpdate.add(modifiableV);
      }
    }

    // addAll batches its updateRecord calls and does not register individual appends: exclude every touched
    // edge page from the append-merge so a concurrent-append rebase can never lose these edges.
    final TransactionContext tx = database.getTransactionIfExists();
    for (final Record r : recordsToUpdate) {
      database.updateRecord(r);
      if (tx != null && r instanceof MutableEdgeSegment segment)
        tx.poisonEdgeAppendPage(segment.getIdentity());
    }
  }

  public void removeEdge(final Edge edge) {
    final RID rid = edge.getIdentity();
    final boolean byEdgeRID = rid.getPosition() > -1;
    // A lightweight edge has no record to point at, so it is located by (edge type bucket, far endpoint) instead -
    // never by the far endpoint alone, which would unlink whichever other edge happens to reach the same neighbour
    // first, of any type, regular ones included.
    final int edgeTypeBucketId = rid.getBucketId();
    final RID targetVertexRID = byEdgeRID ? null : (direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut());

    RID prevBrowsedRID = null;
    EdgeSegment current = lastSegment;
    while (current != null) {
      // #5155: walk the chain with unanchored reads. A chunk that does not hold the target is read-only, so
      // anchoring it (loadChunkForWrite -> fetchPageInTransaction -> page.modify()) would copy its whole page
      // buffer into the tx for nothing (churn/GC on wide super-nodes; the copy is dropped again at commit
      // because an unwritten page is pruned before the version check). Anchor a chunk ONLY once a read-only
      // probe proves it holds the target, right before the mutating removeEdge/removeLightEdge.
      final boolean present = byEdgeRID ?
          current.containsEdge(rid) :
          current.containsLightEdge(edgeTypeBucketId, targetVertexRID);

      if (present) {
        final EdgeSegment modifiable = loadChunkForWrite(current.getIdentity());
        final int deleted = byEdgeRID ?
            modifiable.removeEdge(rid) :
            modifiable.removeLightEdge(edgeTypeBucketId, targetVertexRID);
        if (deleted > 0) {
          updateSegment(modifiable, prevBrowsedRID);
          break;
        }
      }

      prevBrowsedRID = current.getIdentity();
      final RID prevRID = current.getPreviousRID();
      current = prevRID == null ? null : readChunk(prevRID);
    }
  }

  public void removeEdgeRID(final RID edge) {
    RID prevBrowsedRID = null;
    EdgeSegment current = lastSegment;
    while (current != null) {
      // #5155: probe read-only, anchor only the chunk that actually holds the edge (see removeEdge).
      if (current.containsEdge(edge)) {
        final EdgeSegment modifiable = loadChunkForWrite(current.getIdentity());
        if (modifiable.removeEdge(edge) > 0) {
          updateSegment(modifiable, prevBrowsedRID);
          break;
        }
      }
      prevBrowsedRID = current.getIdentity();
      final RID prevRID = current.getPreviousRID();
      current = prevRID == null ? null : readChunk(prevRID);
    }
  }

  public void removeVertex(final RID vertexRID) {
    RID prevBrowsedRID = null;
    EdgeSegment current = lastSegment;
    while (current != null) {
      final RID nextRID = current.getPreviousRID();
      // #5155: a chunk with no edge to the vertex is read-only during this removal - probe unanchored and skip
      // anchoring it. Only when the chunk holds at least one matching edge do we anchor and drain it.
      if (current.getFirstEdgeConnectedToVertex(vertexRID, null) != null) {
        final EdgeSegment modifiable = loadChunkForWrite(current.getIdentity());
        boolean deleted = false;
        while (modifiable.removeVertex(vertexRID) > 0)
          deleted = true;
        if (deleted) {
          final boolean segmentWillBeDeleted = prevBrowsedRID != null && modifiable.isEmpty() && nextRID != null;
          updateSegment(modifiable, prevBrowsedRID);
          if (!segmentWillBeDeleted)
            prevBrowsedRID = current.getIdentity();
        } else
          prevBrowsedRID = current.getIdentity();
      } else
        prevBrowsedRID = current.getIdentity();
      current = nextRID == null ? null : readChunk(nextRID);
    }
  }

  /**
   * #5147/#5153: loads an edge-list chunk for a WRITE (remove/relink), anchoring its page in the transaction at
   * the version it is read - and reading its content from that anchored page. Without the anchor the chunk is
   * read via an immutable lookup that (under READ_COMMITTED) does not retain the page, and the deferred
   * updateRecord captures the page only later, at the newer version if a concurrent transaction modified the
   * same chunk in between. The commit-time MVCC check would then compare matching versions, miss the conflict,
   * and let the stale chunk buffer silently overwrite the concurrent change (a lost update / dropped edge).
   * <p>
   * A chunk that is not readable at all maps to a retryable {@link ConcurrentModificationException}: the directory
   * page and the chunk page of a concurrent commit are published one page at a time (readers take no commit lock),
   * so a freshly-read head RID can momentarily point to a record whose page is not visible yet. That is transient
   * by construction - surfacing it as a conflict lets the transaction retry loop re-read a consistent view instead
   * of raising a "record not found" that reads like a fact about the graph, and that a caller further up was
   * entitled to take for "there was nothing here to remove" (#5670).
   */
  protected EdgeSegment loadChunkForWrite(final RID chunkRID) {
    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();
    // The transaction's own WRITTEN copy carries its pending (deferred) appends: it is the only correct base
    // for a further write, and its page is already anchored from the first write.
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null && tx.getWrittenRecord(chunkRID) instanceof EdgeSegment written)
      return written;
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(chunkRID.getBucketId());
    try {
      bucket.fetchPageInTransaction(chunkRID);
      // Read THROUGH the anchored page, bypassing the record cache: a cached copy read before the anchor can be
      // one version older than the page just anchored - writing that stale buffer back at commit would pass the
      // MVCC check (the version matches) and silently erase a concurrent append.
      return new MutableEdgeSegment(database, chunkRID, bucket.getRecord(chunkRID).copyOfContent());
    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on loading edge chunk page " + chunkRID, e);
    } catch (final RecordNotFoundException e) {
      throw new ConcurrentModificationException(
          "Edge list " + direction + " chunk " + chunkRID + " of vertex " + vertex.getIdentity()
              + " not visible yet (concurrent commit in flight)", e);
    }
  }

  /**
   * Super-node promotion (#5156): when this vertex's approximate degree crosses
   * {@link GlobalConfiguration#GRAPH_SUPERNODE_THRESHOLD}, its edge list is converted to the striped layout - a
   * {@link StripeDirectory} listing N per-stripe chains hosted in a per-type pool of dedicated buckets, so
   * concurrent appends land on different files (different commit locks) instead of serialising on this one
   * head chunk. The existing chain is untouched (it becomes generation 0), the vertex head pointer flips to the
   * directory, and the pending edge is appended through the striped path. Called ONLY from the chunk-full
   * branch, which already rewrites the vertex record, so the check costs nothing on the append hot path.
   *
   * @return true if the vertex was promoted and the pending edge appended, false to continue on the classic path.
   */
  private boolean tryPromoteToSuperNode(final DatabaseInternal database, final RID edgeRID, final RID vertexRID) {
    final int threshold = database.getConfiguration().getValueAsInteger(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD);
    if (threshold < 1)
      return false;

    // Approximate degree derived from the geometric chunk-size schedule (64, 128, ... doubling): the cumulative
    // bytes so far ~= 2 x the current chunk size, and an entry (2 compressed RIDs) averages ~8 bytes. No degree
    // counter and no chain walk on the common path - NOTE: only while the estimate can reach the threshold;
    // once chunks hit the size cap the estimate saturates (~2K edges), so for larger thresholds (incl. the
    // 4096 default) every cap-chunk-full falls into the bounded walk below until the vertex promotes - a
    // handful of walks over a short chain, then never again.
    final int currentChunkSize = lastSegment.getRecordSize();
    long estimatedEdges = (2L * currentChunkSize) / 8;
    if (estimatedEdges < threshold) {
      if (currentChunkSize < LocalDatabase.MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE)
        return false;
      // At the chunk-size cap the geometric estimate is only a lower bound: walk the chain once (this runs at
      // most once per ~1000 appends) to honour thresholds larger than the cap estimate. The walk is BOUNDED:
      // a corrupted cyclic chain longer than a self-loop must not hang the thread, and past the bound the
      // accumulated bytes already exceed any practical threshold anyway (4096 cap-size chunks ~= 4M edges).
      long totalBytes = 0;
      EdgeSegment segment = lastSegment;
      for (int walked = 0; segment != null && walked < 4096; ++walked) {
        totalBytes += segment.getRecordSize();
        final EdgeSegment prev = segment.getPrevious();
        if (prev != null && prev.getIdentity().equals(segment.getIdentity()))
          // CURRENT POINT TO ITSELF, AVOID LOOPS
          break;
        segment = prev;
      }
      estimatedEdges = totalBytes / 8;
      if (estimatedEdges < threshold)
        return false;
    }

    final int stripes = database.getConfiguration().getValueAsInteger(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES);
    if (stripes < 2)
      return false;

    if (!StripedEdgeList.ensureStripePool(database, vertex.getTypeName(), stripes))
      // POOL NOT READY (ON SERVER/HA IT IS CREATED OUTSIDE THIS TRANSACTION): PROMOTE AT A LATER CHUNK-FULL
      return false;

    // The promotion rewrites the vertex record: materialise the mutable copy and re-validate the head (same
    // stale-head guard as the flip path above) BEFORE creating the directory and stripe chunks.
    final MutableVertex modifiableV = vertex.modify();
    final RID reloadedHead = direction == Vertex.DIRECTION.OUT ?
        modifiableV.getOutEdgesHeadChunk() :
        modifiableV.getInEdgesHeadChunk();
    if (reloadedHead != null && !reloadedHead.equals(lastSegment.getIdentity()))
      throw new ConcurrentModificationException(
          "Edge list " + direction + " head of vertex " + vertex.getIdentity() + " changed by a concurrent transaction");

    // THE DIRECTORY LIVES IN THE SAME BUCKET AS THE (NOW GENERATION-0) CLASSIC CHAIN. Every stripe gets its
    // first (tiny) chunk EAGERLY here: lazy per-stripe initialisation would rewrite the directory once per
    // stripe, each rewrite serialising concurrent appenders on the directory file's commit lock for a full
    // replication round - a thundering herd right after promotion. Pre-warming collapses that to this single
    // transaction; after it, appends touch the directory again only at a stripe chunk-full (~1000 appends).
    final StripeDirectory directory = new StripeDirectory(database, lastSegment.getIdentity(), stripes);
    final String typeName = vertex.getTypeName();
    for (int i = 0; i < stripes; i++) {
      final MutableEdgeSegment stripeChunk = new MutableEdgeSegment(database, LocalDatabase.getNewEdgeListSize(0));
      database.createRecord(stripeChunk, StripedEdgeList.stripeBucketName(typeName, i));
      directory.setHead(1, i, stripeChunk.getIdentity());
    }
    database.createRecord(directory, database.getSchema().getBucketById(lastSegment.getIdentity().getBucketId()).getName());

    if (direction == Vertex.DIRECTION.OUT)
      modifiableV.setOutEdgesHeadChunk(directory.getIdentity());
    else
      modifiableV.setInEdgesHeadChunk(directory.getIdentity());
    modifiableV.save();

    // APPEND THE PENDING EDGE THROUGH THE STRIPED LAYOUT (ALLOCATES ITS STRIPE'S FIRST CHUNK LAZILY)
    new StripedEdgeList(modifiableV, direction, directory).add(edgeRID, vertexRID);
    return true;
  }

  /**
   * #5155: reads an edge-list chunk for a read-only walk hop, WITHOUT anchoring its page in the transaction.
   * Used while scanning the chain for the chunk to modify; the modified chunk (and, on an empty-chunk relink,
   * the previous-browsed chunk) is re-loaded through {@link #loadChunkForWrite} before being mutated.
   * <p>
   * #5670: called ONLY from the three removal walks, so a chunk that cannot be read is a retryable conflict, never
   * a reason to stop walking. The hop pointer this transaction is following was read before the walk reached it,
   * and a concurrent commit can have relinked that chunk out of the chain (an emptied chunk is deleted, see
   * {@link #updateSegment}) or not published its page yet. Abandoning the walk there would end the removal without
   * having removed anything, while the caller goes on to delete the edge record - leaving a back-reference to a
   * record that no longer exists. Retrying re-reads the chain from the vertex's current head instead.
   */
  private EdgeSegment readChunk(final RID chunkRID) {
    try {
      return (EdgeSegment) ((DatabaseInternal) vertex.getDatabase()).lookupByRID(chunkRID, true);
    } catch (final RecordNotFoundException e) {
      throw new ConcurrentModificationException(
          "Edge list " + direction + " chunk " + chunkRID + " of vertex " + vertex.getIdentity()
              + " is no longer readable (concurrent commit in flight)", e);
    }
  }

  private int computeBestSize() {
    return LocalDatabase.getNewEdgeListSize(lastSegment.getRecordSize());
  }

  private void updateSegment(final EdgeSegment current, final RID prevBrowsedRID) {
    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();
    // Edge removal/relink does not commute with a concurrent append: exclude the touched pages from the merge.
    final TransactionContext tx = database.getTransactionIfExists();
    if (prevBrowsedRID != null && current.isEmpty() && current.getPrevious() != null) {
      // SEGMENT EMPTY: DELETE ONLY IF IT IS NOT THE FIRST SEGMENT. DELETE CURRENT SEGMENT AND REATTACH THE LINKED LIST.
      // #5155: the previous-browsed chunk was only read unanchored during the walk; anchor it now, before its
      // relink write, so the modification lands on a tx-retained page and is MVCC-version-checked at commit.
      final EdgeSegment prevBrowsed = loadChunkForWrite(prevBrowsedRID);
      prevBrowsed.setPrevious(current.getPrevious());
      database.updateRecord(prevBrowsed);
      if (tx != null) {
        tx.poisonEdgeAppendPage(prevBrowsed.getIdentity());
        tx.poisonEdgeAppendPage(current.getIdentity());
      }
      current.delete();
    } else {
      database.updateRecord(current);
      if (tx != null)
        tx.poisonEdgeAppendPage(current.getIdentity());
    }
  }

  public void deleteAll() {
    final TransactionContext tx = ((DatabaseInternal) vertex.getDatabase()).getTransactionIfExists();
    EdgeSegment current = lastSegment;
    while (current != null) {
      final EdgeSegment prev = current.getPrevious();
      // Deleting a chunk does not commute with a concurrent append on its page: exclude the page from the
      // append-merge so a rebase can never re-derive it from committed-state + appends and lose the deletion
      // (uniformly enforces the "every non-append edge-list write poisons its page" invariant).
      if (tx != null)
        tx.poisonEdgeAppendPage(current.getIdentity());
      current.delete();
      current = prev;
    }
  }
}
