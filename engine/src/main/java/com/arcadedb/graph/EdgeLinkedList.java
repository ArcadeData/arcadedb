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
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;

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
    EdgeSegment prevBrowsed = null;
    EdgeSegment current = lastSegment;
    while (current != null) {
      final RID rid = edge.getIdentity();

      int deleted = 0;
      if (rid.getPosition() > -1)
        // DELETE BY EDGE RID
        deleted = current.removeEdge(rid);
      else
        // DELETE BY VERTEX RID
        deleted = current.removeVertex(direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut());

      if (deleted > 0) {
        updateSegment(current, prevBrowsed);
        break;
      }

      prevBrowsed = current;
      current = current.getPrevious();
    }
  }

  public void removeEdgeRID(final RID edge) {
    EdgeSegment prevBrowsed = null;
    EdgeSegment current = lastSegment;
    while (current != null) {
      final int deleted = current.removeEdge(edge);
      if (deleted > 0) {
        updateSegment(current, prevBrowsed);
        break;
      }
      prevBrowsed = current;
      current = current.getPrevious();
    }
  }

  public void removeVertex(final RID vertexRID) {
    EdgeSegment prevBrowsed = null;
    EdgeSegment current = lastSegment;
    while (current != null) {
      final EdgeSegment next = current.getPrevious();
      boolean deleted = false;
      while (current.removeVertex(vertexRID) > 0)
        deleted = true;
      if (deleted) {
        final boolean segmentWillBeDeleted = prevBrowsed != null && current.isEmpty() && next != null;
        updateSegment(current, prevBrowsed);
        if (!segmentWillBeDeleted)
          prevBrowsed = current;
      } else
        prevBrowsed = current;
      current = next;
    }
  }

  private int computeBestSize() {
    return LocalDatabase.getNewEdgeListSize(lastSegment.getRecordSize());
  }

  private void updateSegment(final EdgeSegment current, final EdgeSegment prevBrowsed) {
    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();
    // Edge removal/relink does not commute with a concurrent append: exclude the touched pages from the merge.
    final TransactionContext tx = database.getTransactionIfExists();
    if (prevBrowsed != null && current.isEmpty() && current.getPrevious() != null) {
      // SEGMENT EMPTY: DELETE ONLY IF IT IS NOT THE FIRST SEGMENT. DELETE CURRENT SEGMENT AND REATTACH THE LINKED LIST
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
    EdgeSegment current = lastSegment;
    while (current != null) {
      final EdgeSegment prev = current.getPrevious();
      current.delete();
      current = prev;
    }
  }
}
