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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;

import java.util.*;

/**
 * Linked list uses to manage edges in vertex. The edges are stored in reverse order from insertion. The last item is the first in the list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeLinkedList {
  private final Vertex           vertex;
  private final Vertex.DIRECTION direction;
  private       EdgeSegment      lastSegment;

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
      return new VertexIterator(lastSegment);
    return new VertexIteratorFilter((DatabaseInternal) vertex.getDatabase(), lastSegment, edgeTypes);
  }

  public boolean containsEdge(final RID rid) {
    EdgeSegment current = lastSegment;
    while (current != null) {
      if (current.containsEdge(rid))
        return true;

      current = current.getPrevious();
    }

    return false;
  }

  public JSONArray toJSON() {
    final JSONArray array = new JSONArray();

    EdgeSegment current = lastSegment;
    while (current != null) {
      final JSONObject j = current.toJSON();
      if (j.has("array")) {
        final JSONArray a = j.getJSONArray("array");
        for (int i = 0; i < a.length(); ++i)
          array.put(a.getString(i));
      }
      current = current.getPrevious();
    }

    return array;
  }

  public boolean containsVertex(final RID rid, final int[] edgeBucketFilter) {
    EdgeSegment current = lastSegment;
    while (current != null) {
      if (current.containsVertex(rid, edgeBucketFilter))
        return true;

      current = current.getPrevious();
    }

    return false;
  }

  /**
   * Counts the items in the linked list.
   *
   * @param edgeType Type of edge to filter for the counting. If it is null, any type is counted.
   *
   * @return
   */
  public long count(final String edgeType) {
    long total = 0;

    final Set<Integer> fileIdToFilter;
    if (edgeType != null)
      fileIdToFilter = new HashSet<>(vertex.getDatabase().getSchema().getType(edgeType).getBucketIds(true));
    else
      fileIdToFilter = null;

    EdgeSegment current = lastSegment;
    while (current != null) {
      total += current.count(fileIdToFilter);
      current = current.getPrevious();
    }

    return total;
  }

  public void add(final RID edgeRID, final RID vertexRID) {
    if (lastSegment.add(edgeRID, vertexRID))
      ((DatabaseInternal) vertex.getDatabase()).updateRecord(lastSegment);
    else {
      // CHUNK FULL, ALLOCATE A NEW ONE
      final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();

      final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, computeBestSize());

      newChunk.add(edgeRID, vertexRID);
      newChunk.setPrevious(lastSegment);

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

    for (final Record r : recordsToUpdate)
      database.updateRecord(r);
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
      if (current.removeVertex(vertexRID) > 0) {
        updateSegment(current, prevBrowsed);
        break;
      }
      prevBrowsed = current;
      current = current.getPrevious();
    }
  }

  private int computeBestSize() {
    return ((DatabaseInternal) vertex.getDatabase()).getNewEdgeListSize(lastSegment.getRecordSize());
  }

  private void updateSegment(final EdgeSegment current, final EdgeSegment prevBrowsed) {
    if (prevBrowsed != null && current.isEmpty() && current.getPrevious() != null) {
      // SEGMENT EMPTY: DELETE ONLY IF IT IS NOT THE FIRST SEGMENT. DELETE CURRENT SEGMENT AND REATTACH THE LINKED LIST
      prevBrowsed.setPrevious(current.getPrevious());
      ((DatabaseInternal) vertex.getDatabase()).updateRecord(prevBrowsed);
      current.delete();
    } else
      ((DatabaseInternal) vertex.getDatabase()).updateRecord(current);
  }
}
