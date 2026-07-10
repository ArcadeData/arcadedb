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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Linked list uses to manage edges in vertex. The edges are stored in reverse order from insertion. The last item is the first in the list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeLinkedList {
  private final Vertex vertex;
  private final Vertex.DIRECTION direction;
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
      // CHUNK FULL, ALLOCATE A NEW ONE
      final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, computeBestSize());

      newChunk.add(edgeRID, vertexRID);
      newChunk.setPrevious(lastSegment);

      // createRecord poisons the new chunk's page for the append-merge (a new chunk cannot be rebased).
      database.createRecord(newChunk, database.getSchema().getBucketById(lastSegment.getIdentity().getBucketId()).getName());

      final MutableVertex modifiableV = vertex.modify();

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
    // DELETE BY VERTEX RID: resolve the target vertex once, outside the walk.
    final RID targetVertexRID = byEdgeRID ? null : (direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut());

    RID prevBrowsedRID = null;
    EdgeSegment current = lastSegment;
    while (current != null) {
      // #5155: walk the chain with unanchored reads. A chunk that does not hold the target is read-only, so
      // anchoring it (loadChunkForWrite -> fetchPageInTransaction -> page.modify()) would copy its whole page
      // buffer into the tx for nothing (churn/GC on wide super-nodes; the copy is dropped again at commit
      // because an unwritten page is pruned before the version check). Anchor a chunk ONLY once a read-only
      // probe proves it holds the target, right before the mutating removeEdge/removeVertex.
      final boolean present = byEdgeRID ?
          current.containsEdge(rid) :
          current.getFirstEdgeConnectedToVertex(targetVertexRID, null) != null;

      if (present) {
        final EdgeSegment modifiable = loadChunkForWrite(current.getIdentity());
        final int deleted = byEdgeRID ? modifiable.removeEdge(rid) : modifiable.removeVertex(targetVertexRID);
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
   */
  private EdgeSegment loadChunkForWrite(final RID chunkRID) {
    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();
    try {
      ((LocalBucket) database.getSchema().getBucketById(chunkRID.getBucketId())).fetchPageInTransaction(chunkRID);
    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on loading edge chunk page " + chunkRID, e);
    }
    return (EdgeSegment) database.lookupByRID(chunkRID, true);
  }

  /**
   * #5155: reads an edge-list chunk for a read-only walk hop, WITHOUT anchoring its page in the transaction.
   * Used while scanning the chain for the chunk to modify; the modified chunk (and, on an empty-chunk relink,
   * the previous-browsed chunk) is re-loaded through {@link #loadChunkForWrite} before being mutated.
   */
  private EdgeSegment readChunk(final RID chunkRID) {
    return (EdgeSegment) ((DatabaseInternal) vertex.getDatabase()).lookupByRID(chunkRID, true);
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
