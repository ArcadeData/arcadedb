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
 */
package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EdgeLinkedList {
  private final Vertex           vertex;
  private final Vertex.DIRECTION direction;
  private       EdgeSegment      first;

  public EdgeLinkedList(final Vertex vertex, final Vertex.DIRECTION direction, final EdgeSegment first) {
    this.vertex = vertex;
    this.direction = direction;
    this.first = first;
  }

  public Iterator<Pair<RID, RID>> entryIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new EdgeVertexIterator(first, vertex.getIdentity(), direction);
    return new EdgeVertexIteratorFilter((DatabaseInternal) vertex.getDatabase(), first, edgeTypes);
  }

  public Iterator<Edge> edgeIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new EdgeIterator(first, vertex.getIdentity(), direction);
    return new EdgeIteratorFilter((DatabaseInternal) vertex.getDatabase(), vertex, direction, first, edgeTypes);
  }

  public Iterator<Vertex> vertexIterator(final String... edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return new VertexIterator(first);
    return new VertexIteratorFilter((DatabaseInternal) vertex.getDatabase(), first, edgeTypes);
  }

  public boolean containsEdge(final RID rid) {
    EdgeSegment current = first;
    while (current != null) {
      if (current.containsEdge(rid))
        return true;

      current = current.getNext();
    }

    return false;
  }

  public JSONArray toJSON() {
    final JSONArray array = new JSONArray();

    EdgeSegment current = first;
    while (current != null) {
      final JSONObject j = current.toJSON();
      if (j.has("array")) {
        final JSONArray a = j.getJSONArray("array");
        for (int i = 0; i < a.length(); ++i)
          array.put(a.getString(i));
      }
      current = current.getNext();
    }

    return array;
  }

  public boolean containsVertex(final RID rid, final int[] edgeBucketFilter) {
    EdgeSegment current = first;
    while (current != null) {
      if (current.containsVertex(rid, edgeBucketFilter))
        return true;

      current = current.getNext();
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
    if (edgeType != null) {
      final DocumentType type = vertex.getDatabase().getSchema().getType(edgeType);
      final List<Bucket> buckets = type.getBuckets(true);
      fileIdToFilter = new HashSet<>(buckets.size());
      for (Bucket b : buckets)
        fileIdToFilter.add(b.getId());
    } else
      fileIdToFilter = null;

    EdgeSegment current = first;
    while (current != null) {
      total += current.count(fileIdToFilter);
      current = current.getNext();
    }

    return total;
  }

  public void upgrade(final RID newEdgeRID, final RID vertexRID) {
    // TODO: ???HOW TO MANAGE THE NEW SPACE REQUESTED FOR THE COMPRESSED RIDS??????

  }

  public void add(final RID edgeRID, final RID vertexRID) {
    if (first.add(edgeRID, vertexRID))
      ((DatabaseInternal) vertex.getDatabase()).updateRecord(first);
    else {
      // CHUNK FULL, ALLOCATE A NEW ONE
      DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();

      final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, computeBestSize());

      newChunk.add(edgeRID, vertexRID);
      newChunk.setNext(first);

      database.createRecord(newChunk, database.getSchema().getBucketById(first.getIdentity().getBucketId()).getName());

      final MutableVertex modifiableV = vertex.modify();

      if (direction == Vertex.DIRECTION.OUT)
        modifiableV.setOutEdgesHeadChunk(newChunk.getIdentity());
      else
        modifiableV.setInEdgesHeadChunk(newChunk.getIdentity());

      first = newChunk;

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

      if (first.add(edgeRID, vertexRID))
        recordsToUpdate.add(first);
      else {
        // CHUNK FULL, ALLOCATE A NEW ONE
        final MutableEdgeSegment newChunk = new MutableEdgeSegment(database, computeBestSize());

        newChunk.add(edgeRID, vertexRID);
        newChunk.setNext(first);

        database.createRecord(newChunk, database.getSchema().getBucketById(first.getIdentity().getBucketId()).getName());

        final MutableVertex modifiableV = currentVertex.modify();
        currentVertex = modifiableV;

        if (direction == Vertex.DIRECTION.OUT)
          modifiableV.setOutEdgesHeadChunk(newChunk.getIdentity());
        else
          modifiableV.setInEdgesHeadChunk(newChunk.getIdentity());

        first = newChunk;

        recordsToUpdate.add(modifiableV);
      }
    }

    for (Record r : recordsToUpdate)
      database.updateRecord(r);
  }

  public void removeEdge(final Edge edge) {
    EdgeSegment current = first;
    while (current != null) {
      RID rid = edge.getIdentity();

      int deleted = 0;
      if (rid.getPosition() > -1)
        // DELETE BY EDGE RID
        deleted = current.removeEdge(rid);
      else
        // DELETE BY VERTEX RID
        deleted = current.removeVertex(direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut());

      if (deleted > 0)
        ((DatabaseInternal) vertex.getDatabase()).updateRecord(current);

      current = current.getNext();
    }
  }

  public void removeEdgeRID(final RID edge) {
    EdgeSegment current = first;
    while (current != null) {
      final int deleted = current.removeEdge(edge);
      if (deleted > 0)
        ((DatabaseInternal) vertex.getDatabase()).updateRecord(current);
      current = current.getNext();
    }
  }

  public void removeVertex(final RID vertexRID) {
    EdgeSegment current = first;
    while (current != null) {
      if (current.removeVertex(vertexRID) > 0)
        ((DatabaseInternal) vertex.getDatabase()).updateRecord(current);

      current = current.getNext();
    }
  }

  private int computeBestSize() {
    return ((DatabaseInternal) vertex.getDatabase()).getNewEdgeListSize(first.getRecordSize());
  }
}
