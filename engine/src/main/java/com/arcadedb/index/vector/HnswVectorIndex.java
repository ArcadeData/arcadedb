/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.index.vector;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.vector.distance.DistanceFunctionFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VectorIndexBuilder;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import com.github.jelmerk.knn.DistanceFunction;
import com.github.jelmerk.knn.Index;
import com.github.jelmerk.knn.Item;
import com.github.jelmerk.knn.SearchResult;
import com.github.jelmerk.knn.util.Murmur3;
import org.eclipse.collections.api.list.primitive.MutableIntList;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;
import java.util.stream.*;

import static com.arcadedb.serializer.BinaryTypes.TYPE_BINARY;
import static com.arcadedb.serializer.BinaryTypes.TYPE_BOOLEAN;
import static com.arcadedb.serializer.BinaryTypes.TYPE_COMPRESSED_RID;
import static com.arcadedb.serializer.BinaryTypes.TYPE_INT;
import static com.arcadedb.serializer.BinaryTypes.TYPE_RID;

/**
 * This work is derived from the excellent work made by Jelmer Kuperus on https://github.com/jelmerk/hnswlib.
 * <p>
 * Implementation of {@link Index} that implements the hnsw algorithm.
 * TODO: Check if the global lock interferes with ArcadeDB's tx approach
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://arxiv.org/abs/1603.09320">
 * Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
 */
public class HnswVectorIndex<TId, TVector, TDistance> extends Bucket implements com.arcadedb.index.Index, IndexInternal {
  public static final String FILE_EXT        = "hnswidx";
  public static final int    CURRENT_VERSION = 0;

  private final   DistanceFunction<TVector, TDistance> distanceFunction;
  private final   Comparator<TDistance>                distanceComparator;
  private final   MaxValueComparator<TDistance>        maxValueDistanceComparator;
  private final   int                                  dimensions;
  private final   int                                  maxItemCount;
  private final   int                                  m;
  private final   int                                  maxM;
  private final   int                                  maxM0;
  private final   double                               levelLambda;
  private final   int                                  ef;
  private final   int                                  efConstruction;
  private final   ReentrantLock                        globalLock;
  private final   Set<RID>                             excludedCandidates  = new HashSet<>();
  private final   String                               recordType;
  private final   String                               recordSchmaType;
  private final   String                               vectorPropertyName;
  private final   String                               indexName;
  public volatile int                                  entryPoint          = -1;
  private         int                                  lastSavedEntryPoint = -1;
  private final   int                                  maxRecordsInPage;
  private final   BinarySerializer                     serializer;
  private         LSMTreeIndexAbstract.NULL_STRATEGY   nullStrategy;
  private final   RID                                  structureRecordRID;

  private class Node {
    public final int     id;
    public final RID     rid;
    public final int     maxLevel;
    public final boolean deleted;
    public final TVector vector;

    private Node(final int id, final RID rid, final int maxLevel, final boolean deleted, final TVector vector) {
      this.id = id;
      this.rid = rid;
      this.maxLevel = maxLevel;
      this.deleted = deleted;
      this.vector = vector;
    }
  }

  public interface IgnoreVertexCallback {
    boolean ignoreVertex(Vertex v);
  }

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
      if (!(builder instanceof VectorIndexBuilder))
        throw new IndexException("Expected VectorIndexBuilder but received " + builder);

      try {
        return new HnswVectorIndex<>((VectorIndexBuilder) builder);
      } catch (IOException e) {
        throw new IndexException("Error on creating HSNW index " + builder.getIndexName(), e);
      }
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public Component createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new HnswVectorIndex<>(database, name, filePath, id, mode, pageSize, version);
    }
  }

  protected HnswVectorIndex(final VectorIndexBuilder builder) throws IOException {
    super(builder.getDatabase(), builder.getIndexName(),
        builder.getDatabase().getDatabasePath() + File.separator + builder.getIndexName(), ComponentFile.MODE.READ_WRITE,
        builder.getPageSize(), Bucket.CURRENT_VERSION, FILE_EXT);
    this.structureRecordRID = new RID(getDatabase(), getFileId(), 0);
    this.maxRecordsInPage = getMaxRecordsInPage();
    this.serializer = database.getSerializer();

    this.dimensions = builder.getDimensions();
    this.maxItemCount = builder.getMaxItemCount();
    this.distanceFunction = builder.getDistanceFunction();
    this.distanceComparator = builder.getDistanceComparator();
    this.maxValueDistanceComparator = new MaxValueComparator<>(this.distanceComparator);

    this.m = builder.getM();
    this.maxM = m;
    this.maxM0 = m * 2;
    this.levelLambda = 1 / Math.log(this.m);
    this.efConstruction = Math.max(builder.getEfConstruction(), m);
    this.ef = builder.getEf();

    this.recordType = builder.getVertexType();
    this.vectorPropertyName = builder.getVectorPropertyName();

    this.globalLock = new ReentrantLock();
    this.indexName = builder.getIndexName() != null ? builder.getIndexName() : recordType + "[" + vectorPropertyName + "]";
  }

  /**
   * Load time.
   */
  protected HnswVectorIndex(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);

    this.structureRecordRID = new RID(getDatabase(), getFileId(), 0);
    this.maxRecordsInPage = getMaxRecordsInPage();
    this.serializer = database.getSerializer();

    // LOAD THE INDEX SETTINGS FROM RECORD 0
    final String fileContent = getRecord(new RID(database, id, 0)).getString();

    final JSONObject json = new JSONObject(fileContent);

    this.distanceFunction = DistanceFunctionFactory.getImplementationByClassName(json.getString("distanceFunction"));
    if (distanceFunction == null)
      throw new IllegalArgumentException("distance function '" + json.getString("distanceFunction") + "' not supported");

    this.dimensions = json.getInt("dimensions");
    this.distanceComparator = (Comparator<TDistance>) Comparator.naturalOrder();
    this.maxValueDistanceComparator = new MaxValueComparator<>(this.distanceComparator);
    this.maxItemCount = json.getInt("maxItemCount");
    this.m = json.getInt("m");
    this.maxM = json.getInt("maxM");
    this.maxM0 = json.getInt("maxM0");
    this.levelLambda = json.getDouble("levelLambda");
    this.ef = json.getInt("ef");
    this.efConstruction = json.getInt("efConstruction");
    this.entryPoint = json.getInt("entryPoint");
    this.recordType = json.getString("vertexType");
    this.vectorPropertyName = json.getString("vectorPropertyName");
    this.indexName = json.getString("indexName");

    this.globalLock = new ReentrantLock();
    this.lastSavedEntryPoint = entryPoint;
  }

  @Override
  public void onAfterCommit() {
    if (entryPoint > -1 && entryPoint != lastSavedEntryPoint) {
      // ENTRY POINT IS CHANGED: SAVE THE NEW CONFIGURATION TO DISK
      save();
    }
  }

  @Override
  public String getName() {
    return indexName;
  }

  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromVertex(final Vertex start, final int k,
      final IgnoreVertexCallback ignoreVertexCallback) {
    final RID startRID = start.getIdentity();
    final TVector vector = getVectorFromNode(start);

    final List<SearchResult<Vertex, TDistance>> neighbors = findNearest(vector, k + 1, ignoreVertexCallback).stream()//
        .filter(result -> !result.item().getIdentity().equals(startRID))//
        .limit(k)//
        .collect(Collectors.toList());

    final List<Pair<Identifiable, ? extends Number>> result = new ArrayList<>(neighbors.size());
    for (SearchResult<Vertex, TDistance> neighbor : neighbors)
      result.add(new Pair(neighbor.item(), neighbor.distance()));
    return result;
  }

  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromVector(final TVector vector, final int k) {
    return findNeighborsFromVector(vector, k, null);
  }

  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromVector(final TVector vector, final int k,
      final IgnoreVertexCallback ignoreVertexCallback) {
    final List<SearchResult<Vertex, TDistance>> neighbors = findNearest(vector, k + 1, ignoreVertexCallback).stream().limit(k)
        .collect(Collectors.toList());

    final List<Pair<Identifiable, ? extends Number>> result = new ArrayList<>(neighbors.size());
    for (SearchResult<Vertex, TDistance> neighbor : neighbors)
      result.add(new Pair(neighbor.item(), neighbor.distance()));
    return result;
  }

  public void addAll(final List<Item<TId, TVector>> embeddings) {
    int indexed = 0;
    for (Item<TId, TVector> embedding : embeddings) {
      final IndexCursor existent = underlyingIndex.get(new Object[] { embedding.id() });
      MutableVertex vertex;
      if (existent.hasNext()) {
        vertex = existent.next().asVertex().modify();
        final Boolean deleted = vertex.getBoolean(deletedPropertyName);
        if (deleted != null && deleted)
          vertex.remove(deletedPropertyName);
      } else
        vertex = database.newVertex(recordType);

      vertex.set(idPropertyName, embedding.id()).set(vectorPropertyName, embedding.vector()).save();

      add(vertex);
    }
  }

  public boolean add(final Document record) {
    final TVector vertexVector = getVectorFromNode(record);
    if (Array.getLength(vertexVector) != dimensions)
      throw new IllegalArgumentException(
          "Item has dimensionality of " + Array.getLength(vertexVector) + " but the index was defined with " + dimensions
              + " dimensions");

    final int vertexMaxLevel = loadNodeFromId(record).maxLevel;

    final int randomLevel = assignLevel(vertexId, this.levelLambda);

    globalLock.lock();
    try {
      final Boolean deleted = record.getBoolean(deletedPropertyName);
      if (deleted != null && deleted) {
        record = record.modify();
        ((MutableVertex) record).remove(deletedPropertyName);
        ((MutableVertex) record).save();
      }

      final long totalEdges = record.countEdges(Vertex.DIRECTION.OUT, getEdgeType(0));
      if (totalEdges > 0)
        // ALREADY INSERTED
        return true;

      record = record.modify().set("vectorMaxLevel", randomLevel).save();

      final RID vertexRID = record.getIdentity();
      synchronized (excludedCandidates) {
        excludedCandidates.add(vertexRID);
      }

      final int entryPointCopy = entryPoint;
      try {
        if (entryPoint > -1 && randomLevel <= getMaxLevelFromNode(entryPoint)) {
          globalLock.unlock();
        }

        Vertex currObj = entryPointCopy;
        final int entryPointCopyMaxLevel = getMaxLevelFromNode(entryPointCopy);

        if (currObj != null) {
          if (vertexMaxLevel < entryPointCopyMaxLevel) {
            final TVector vector = getVectorFromNode(currObj);
            if (vector == null) {
              LogManager.instance().log(this, Level.WARNING, "Vector not found in vertex %s", currObj);
              throw new IndexException("Embeddings not found in object " + currObj);
            }

            TDistance curDist = distanceFunction.distance(vertexVector, vector);
            for (int activeLevel = entryPointCopyMaxLevel; activeLevel > vertexMaxLevel; activeLevel--) {
              boolean changed = true;

              while (changed) {
                changed = false;

                synchronized (currObj) {
                  final Iterator<Vertex> candidateConnections = getConnectionsFromVertex(currObj, activeLevel);
                  while (candidateConnections.hasNext()) {
                    final Vertex candidateNode = candidateConnections.next();

                    final TVector candidateNodeVector = getVectorFromNode(candidateNode);
                    if (candidateNodeVector == null) {
                      // INVALID
                      LogManager.instance().log(this, Level.WARNING, "Vector not found in vertex %s", candidateNode);
                      continue;
                    }

                    final TDistance candidateDistance = distanceFunction.distance(vertexVector, candidateNodeVector);

                    if (lt(candidateDistance, curDist)) {
                      curDist = candidateDistance;
                      currObj = candidateNode;
                      changed = true;
                    }
                  }
                }
              }
            }
          }

          for (int level = Math.min(randomLevel, entryPointCopyMaxLevel); level >= 0; level--) {
            final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, vertexVector, efConstruction,
                level, null);

            final boolean entryPointDeleted = isNodeDeleted(entryPointCopy);
            if (entryPointDeleted) {
              TDistance distance = distanceFunction.distance(vertexVector, getVectorFromNode(entryPointCopy));
              topCandidates.add(new NodeIdAndDistance<>(entryPointCopy.getIdentity(), distance, maxValueDistanceComparator));

              if (topCandidates.size() > efConstruction)
                topCandidates.poll();
            }

            mutuallyConnectNewElement(record, topCandidates, level);
          }
        }

        // zoom out to the highest level
        if (entryPoint == null || vertexMaxLevel > entryPointCopyMaxLevel)
          // this is thread safe because we get the global lock when we add a level
          this.entryPoint = record;

        return true;

      } finally {
        synchronized (excludedCandidates) {
          excludedCandidates.remove(vertexRID);
        }
      }
    } finally {
      if (globalLock.isHeldByCurrentThread()) {
        globalLock.unlock();
      }
    }
  }

  private Iterator<Vertex> getConnectionsFromVertex(final Vertex vertex, final int level) {
    return vertex.getVertices(Vertex.DIRECTION.OUT, edgeType + level).iterator();
  }

  private int countConnectionsFromVertex(final Vertex vertex, final int level) {
    return (int) vertex.countEdges(Vertex.DIRECTION.OUT, edgeType + level);
  }

  private int getMaxLevelFromNode(final int nodeId) {
    if (nodeId < 0)
      return 0;

    final Binary record = loadNodeFromId(nodeId);
    serializer.deserializeValue(database, record, TYPE_COMPRESSED_RID, null);
    return (int) serializer.deserializeValue(database, record, TYPE_INT, null);
  }

  private Node loadNodeFromId(final int nodeId) {
    if (nodeId < 0)
      throw new IllegalArgumentException("Invalid node id " + nodeId);

    final Binary record = getRecord(new RID(database, fileId, nodeId));
    final RID rid = (RID) serializer.deserializeValue(database, record, TYPE_COMPRESSED_RID, null);
    final int maxLevel = (int) serializer.deserializeValue(database, record, TYPE_INT, null);
    final boolean deleted = (boolean) serializer.deserializeValue(database, record, TYPE_BOOLEAN, null);
    return new Node(nodeId, rid, maxLevel, deleted, null);
  }

  private void mutuallyConnectNewElement(final Vertex newNode, final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates,
      final int level) {
    final int bestN = level == 0 ? this.maxM0 : this.maxM;
    final RID newNodeId = newNode.getIdentity();
    final TVector newItemVector = getVectorFromNode(newNode);

    getNeighborsByHeuristic2(topCandidates, m);

    while (!topCandidates.isEmpty()) {
      final RID selectedNeighbourId = topCandidates.poll().nodeId;
      synchronized (excludedCandidates) {
        if (excludedCandidates.contains(selectedNeighbourId)) {
          continue;
        }
      }

      // CREATE THE EDGE TYPE IF NOT PRESENT
      final String edgeTypeName = getEdgeType(level);
      database.getSchema().getOrCreateEdgeType(edgeTypeName);

      newNode.newEdge(edgeTypeName, selectedNeighbourId, false);

      final Vertex neighbourNode = loadNodeFromId(selectedNeighbourId);
      final TVector neighbourVector = getVectorFromNode(neighbourNode);
      final int neighbourConnectionsAtLevelTotal = countConnectionsFromVertex(neighbourNode, level);
      final Iterator<Vertex> neighbourConnectionsAtLevel = getConnectionsFromVertex(neighbourNode, level);

      if (neighbourConnectionsAtLevelTotal < bestN) {
        neighbourNode.newEdge(edgeTypeName, newNode, false);
      } else {
        // finding the "weakest" element to replace it with the new one
        final TDistance dMax = distanceFunction.distance(newItemVector, neighbourVector);
        final Comparator<NodeIdAndDistance<TDistance>> comparator = Comparator.<NodeIdAndDistance<TDistance>>naturalOrder()
            .reversed();
        final PriorityQueue<NodeIdAndDistance<TDistance>> candidates = new PriorityQueue<>(comparator);
        candidates.add(new NodeIdAndDistance<>(newNodeId, dMax, maxValueDistanceComparator));

        neighbourConnectionsAtLevel.forEachRemaining(neighbourConnection -> {
          final TDistance dist = distanceFunction.distance(neighbourVector, getVectorFromNode(neighbourConnection));
          candidates.add(new NodeIdAndDistance<>(neighbourConnection.getIdentity(), dist, maxValueDistanceComparator));
        });

        getNeighborsByHeuristic2(candidates, bestN);

        while (!candidates.isEmpty()) {
          neighbourNode.newEdge(edgeTypeName, candidates.poll().nodeId, false);
        }
      }
    }
  }

  public List<SearchResult<Vertex, TDistance>> findNearest(final TVector destination, final int k,
      final IgnoreVertexCallback ignoreVertexCallback) {
    if (entryPoint == null)
      return Collections.emptyList();

    final Vertex entryPointCopy = entryPoint;
    Vertex currObj = entryPointCopy;

    final TVector vector = getVectorFromNode(currObj);
    if (vector == null) {
      LogManager.instance().log(this, Level.WARNING, "Vector not found in vertex %s", currObj);
      return Collections.emptyList();
    }

    TDistance curDist = distanceFunction.distance(destination, vector);

    for (int activeLevel = getMaxLevelFromNode(entryPointCopy); activeLevel > 0; activeLevel--) {
      boolean changed = true;
      while (changed) {
        changed = false;

        final Iterator<Vertex> candidateConnections = getConnectionsFromVertex(currObj, activeLevel);

        while (candidateConnections.hasNext()) {
          final Vertex candidateNode = candidateConnections.next();

          TDistance candidateDistance = distanceFunction.distance(destination, getVectorFromNode(candidateNode));
          if (lt(candidateDistance, curDist)) {
            curDist = candidateDistance;
            currObj = candidateNode;
            changed = true;
          }
        }

      }
    }

    final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, destination, Math.max(ef, k), 0,
        ignoreVertexCallback);

    while (topCandidates.size() > k) {
      topCandidates.poll();
    }

    List<SearchResult<Vertex, TDistance>> results = new ArrayList<>(topCandidates.size());
    while (!topCandidates.isEmpty()) {
      NodeIdAndDistance<TDistance> pair = topCandidates.poll();
      results.add(0, new SearchResult<>(loadNodeFromId(pair.nodeId), pair.distance, maxValueDistanceComparator));
    }

    return results;
  }

  private PriorityQueue<NodeIdAndDistance<TDistance>> searchBaseLayer(final Vertex entryPointNode, final TVector destination,
      final int k, final int layer, final IgnoreVertexCallback ignoreVertexCallback) {
    final Set<RID> visitedNodes = new HashSet<>();

    final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = new PriorityQueue<>(
        Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed());
    final PriorityQueue<NodeIdAndDistance<TDistance>> candidateSet = new PriorityQueue<>();

    TDistance lowerBound;

    if (!ignoreVertex(entryPointNode, ignoreVertexCallback)) {
      final TVector entryPointVector = getVectorFromNode(entryPointNode);
      final TDistance distance = distanceFunction.distance(destination, entryPointVector);
      final NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.getIdentity(), distance,
          maxValueDistanceComparator);

      topCandidates.add(pair);
      lowerBound = distance;
      candidateSet.add(pair);
    } else {
      lowerBound = MaxValueComparator.maxValue();
      NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.getIdentity(), lowerBound,
          maxValueDistanceComparator);
      candidateSet.add(pair);
    }

    visitedNodes.add(entryPointNode.getIdentity());

    while (!candidateSet.isEmpty()) {
      final NodeIdAndDistance<TDistance> currentPair = candidateSet.poll();

      if (gt(currentPair.distance, lowerBound))
        break;

      final Vertex node = loadNodeFromId(currentPair.nodeId);

      final Iterator<Vertex> candidates = getConnectionsFromVertex(node, layer);
      while (candidates.hasNext()) {
        final Vertex candidateNode = candidates.next();

        if (!visitedNodes.contains(candidateNode.getIdentity())) {
          visitedNodes.add(candidateNode.getIdentity());

          final TVector vector = getVectorFromNode(candidateNode);
          if (vector == null) {
            // INVALID
            LogManager.instance().log(this, Level.WARNING, "Vector not found in vertex %s", candidateNode);
            continue;
          }

          final TDistance candidateDistance = distanceFunction.distance(destination, vector);
          if (topCandidates.size() < k || gt(lowerBound, candidateDistance)) {
            final NodeIdAndDistance<TDistance> candidatePair = new NodeIdAndDistance<>(candidateNode.getIdentity(),
                candidateDistance, maxValueDistanceComparator);

            candidateSet.add(candidatePair);

            if (!ignoreVertex(candidateNode, ignoreVertexCallback))
              topCandidates.add(candidatePair);

            if (topCandidates.size() > k)
              topCandidates.poll();

            if (!topCandidates.isEmpty())
              lowerBound = topCandidates.peek().distance;
          }
        }
      }
    }

    return topCandidates;
  }

  /**
   * Returns the dimensionality of the items stored in this index.
   *
   * @return the dimensionality of the items stored in this index
   */
  public int getDimensions() {
    return dimensions;
  }

  /**
   * Returns the number of bi-directional links created for every new element during construction.
   *
   * @return the number of bi-directional links created for every new element during construction
   */
  public int getM() {
    return m;
  }

  /**
   * The size of the dynamic list for the nearest neighbors (used during the search)
   *
   * @return The size of the dynamic list for the nearest neighbors
   */
  public int getEf() {
    return ef;
  }

  /**
   * Returns the parameter has the same meaning as ef, but controls the index time / index precision.
   *
   * @return the parameter has the same meaning as ef, but controls the index time / index precision
   */
  public int getEfConstruction() {
    return efConstruction;
  }

  /**
   * Returns the distance function.
   *
   * @return the distance function
   */
  public DistanceFunction<TVector, TDistance> getDistanceFunction() {
    return distanceFunction;
  }

  /**
   * Returns the comparator used to compare distances.
   *
   * @return the comparator used to compare distance
   */
  public Comparator<TDistance> getDistanceComparator() {
    return distanceComparator;
  }

  /**
   * Returns the maximum number of items the index can hold.
   *
   * @return the maximum number of items the index can hold
   */
  public int getMaxItemCount() {
    return maxItemCount;
  }

  private int assignLevel(final TId value, final double lambda) {
    // by relying on the external id to come up with the level, the graph construction should be a lot more stable
    // see : https://github.com/nmslib/hnswlib/issues/28
    final int hashCode = value.hashCode();
    final byte[] bytes = new byte[] { (byte) (hashCode >> 24), (byte) (hashCode >> 16), (byte) (hashCode >> 8), (byte) hashCode };
    final double random = Math.abs((double) Murmur3.hash32(bytes) / (double) Integer.MAX_VALUE);
    final double r = -Math.log(random) * lambda;
    return (int) r;
  }

  private boolean lt(final TDistance x, final TDistance y) {
    return maxValueDistanceComparator.compare(x, y) < 0;
  }

  private boolean gt(final TDistance x, final TDistance y) {
    return maxValueDistanceComparator.compare(x, y) > 0;
  }

  public <TVector> TVector getVectorFromNode(final Document record) {
    return (TVector) record.get(vectorPropertyName);
  }

  public boolean isNodeDeleted(final int nodeId) {
    final Binary buffer = loadNodeFromId(nodeId);
    serializer.deserializeValue(database, buffer, RID);
    final Boolean deleted = nodeId.getBoolean(deletedPropertyName);
    return deleted != null && deleted;
  }

  public boolean ignoreVertex(final Vertex vertex, final IgnoreVertexCallback ignoreVertexCallback) {
    if (isNodeDeleted(vertex))
      return true;
    if (ignoreVertexCallback != null)
      return ignoreVertexCallback.ignoreVertex(vertex);
    return false;
  }

  public int getDimensionFromVertex(final Vertex vertex) {
    return Array.getLength(getVectorFromNode(vertex));
  }

  public String getEdgeType(final int level) {
    return edgeType + level;
  }

  static class NodeIdAndDistance<TDistance> implements Comparable<NodeIdAndDistance<TDistance>> {
    final int                   nodeId;
    final TDistance             distance;
    final Comparator<TDistance> distanceComparator;

    NodeIdAndDistance(final int nodeId, final TDistance distance, final Comparator<TDistance> distanceComparator) {
      this.nodeId = nodeId;
      this.distance = distance;
      this.distanceComparator = distanceComparator;
    }

    @Override
    public int compareTo(NodeIdAndDistance<TDistance> o) {
      return distanceComparator.compare(distance, o.distance);
    }
  }

  static class MaxValueComparator<TDistance> implements Comparator<TDistance>, Serializable {

    private static final long serialVersionUID = 1L;

    private final Comparator<TDistance> delegate;

    MaxValueComparator(Comparator<TDistance> delegate) {
      this.delegate = delegate;
    }

    @Override
    public int compare(final TDistance o1, final TDistance o2) {
      return o1 == null ? o2 == null ? 0 : 1 : o2 == null ? -1 : delegate.compare(o1, o2);
    }

    static <TDistance> TDistance maxValue() {
      return null;
    }
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("indexName", getName());
    json.put("version", CURRENT_VERSION);
    json.put("dimensions", dimensions);
    json.put("distanceFunction", distanceFunction.getClass().getSimpleName());
    json.put("distanceComparator", distanceComparator.getClass().getSimpleName());
    json.put("maxItemCount", maxItemCount);
    json.put("m", m);
    json.put("maxM", maxM);
    json.put("maxM0", maxM0);
    json.put("levelLambda", levelLambda);
    json.put("ef", ef);
    json.put("efConstruction", efConstruction);
    json.put("levelLambda", levelLambda);
    json.put("entryPoint", entryPoint == null ? -1 : entryPoint);
    json.put("recordType", recordType);
    json.put("vectorPropertyName", vectorPropertyName);
    return json;
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return null;
  }

  @Override
  public void drop() {
    database.getSchema().dropBucket(getName());
  }

  @Override
  public Map<String, Long> getStats() {
    return new HashMap<>();
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return nullStrategy;
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    this.nullStrategy = nullStrategy;
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  public long build(final HnswVectorIndexRAM origin, final int buildIndexBatchSize) {
    if (origin != null) {
      // IMPORT FROM RAM Index
      final int[] pointersMapping = new int[origin.nodeCount];

      database.begin();

      /// SAVE INDEX STRUCTURE INTO THE RECORD 0 OF THE BUCKET. SAVE WITH INITIAL 1024 BYTES TO AVOID PLACEHOLDERS
      final BinaryRecord structureRecord = new BinaryRecord(database);
      byte[] empty = new byte[1024];
      structureRecord.getBuffer().putBytes(empty);
      if (!createRecord(structureRecord, true).equals(structureRecordRID))
        throw new IllegalArgumentException("Index is not empty");

      // CREATE EMPTY RECORDS FIRST TO ASSIGN THE RID TO NODE NUMBER. THE RECORDS WILL BE STORED WITH AN APPROXIMATED RIGHT SIZE TO
      // REDUCE THE CHANCE TO CREATING PLACEHOLDERS
      int maxLevel = 0;
      HnswVectorIndexRAM.ItemIterator iter = origin.iterateNodes();
      for (int nodeId = 0; iter.hasNext(); ++nodeId) {
        final HnswVectorIndexRAM.Node node = iter.next();

        final int nodeMaxLevel = node.maxLevel();
        if (nodeMaxLevel > maxLevel)
          maxLevel = nodeMaxLevel;

        final BinaryRecord nodeRecord = new BinaryRecord(database);
        serializer.serializeValue(database, nodeRecord.getBuffer(), TYPE_RID, node.item.id());
        serializer.serializeValue(database, nodeRecord.getBuffer(), TYPE_INT, nodeMaxLevel);

        final MutableIntList[] connections = node.connections();
        empty = new byte[4 * connections.length];
        serializer.serializeValue(database, nodeRecord.getBuffer(), TYPE_BINARY, empty);

        pointersMapping[nodeId] = rid2nodeId(createRecord(nodeRecord, true));

        if (nodeId % buildIndexBatchSize == 0) {
          database.commit();
          database.begin();
        }
      }

      database.commit();

      final Integer entryPoint = origin.getEntryPoint();
      if (entryPoint != null)
        this.entryPoint = pointersMapping[entryPoint];

      database.begin();

      iter = origin.iterateNodes();
      for (int txCounter = 0; iter.hasNext(); ++txCounter) {
        final HnswVectorIndexRAM.Node node = iter.next();

        final int persistentId = pointersMapping[node.id];
        final RID rid = nodeId2RID(persistentId);

        final Binary record = getRecord(rid);
        record.clear();
        serializer.serializeValue(database, record, TYPE_RID, node.item.id());
        serializer.deserializeValue(database, record, TYPE_INT, null);

        final MutableIntList[] connections = node.connections();

        serializer.serializeValue(database, record, TYPE_INT, connections.length);

        for (int level = 0; level < connections.length; level++) {
          final MutableIntList pointers = connections[level];

          serializer.serializeValue(database, record, TYPE_INT, pointers.size());
          for (int i = 0; i < pointers.size(); i++) {
            final int pointer = pointers.get(i);
            final int destination = pointersMapping[pointer];
            serializer.serializeValue(database, record, TYPE_INT, destination);
          }
        }

        if (txCounter % buildIndexBatchSize == 0) {
          database.commit();
          database.begin();
        }
      }

      save();

      database.commit();

      return origin.nodeCount;
    }

    // TODO: NOT SUPPORTED WITHOUT RAM INDEX
    return 0L;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof HnswVectorIndex))
      return false;
    return componentName.equals(((HnswVectorIndex) obj).componentName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(componentName);
  }

  @Override
  public String toString() {
    return indexName;
  }

  @Override
  public void setMetadata(final String name, final String[] propertyNames, final int associatedBucketId) {
  }

  @Override
  public Component getComponent() {
    return this;
  }

  @Override
  public Type[] getKeyTypes() {
    return new Type[] { Type.ARRAY_OF_FLOATS };
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return underlyingIndex.getBinaryKeyTypes();
  }

  @Override
  public List<Integer> getFileIds() {
    if (underlyingIndex == null)
      // NOT PROPERLY BUILT YET
      return Collections.emptyList();
    return underlyingIndex.getFileIds();
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    throw new UnsupportedOperationException("setTypeIndex");
  }

  @Override
  public TypeIndex getTypeIndex() {
    return null;
  }

  @Override
  public int getAssociatedBucketId() {
    return -1;
  }

  public void addIndexOnBucket(final IndexInternal index) {
    underlyingIndex.addIndexOnBucket(index);
  }

  public void removeIndexOnBucket(final IndexInternal index) {
    underlyingIndex.removeIndexOnBucket(index);
  }

  public IndexInternal[] getIndexesOnBuckets() {
    return underlyingIndex.getIndexesOnBuckets();
  }

  public List<? extends com.arcadedb.index.Index> getIndexesByKeys(final Object[] keys) {
    return underlyingIndex.getIndexesByKeys(keys);
  }

  public IndexCursor iterator(final boolean ascendingOrder) {
    return underlyingIndex.iterator(ascendingOrder);
  }

  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    return underlyingIndex.iterator(ascendingOrder, fromKeys, inclusive);
  }

  public IndexCursor range(final boolean ascending, final Object[] beginKeys, final boolean beginKeysInclusive,
      final Object[] endKeys, boolean endKeysInclusive) {
    return underlyingIndex.range(ascending, beginKeys, beginKeysInclusive, endKeys, endKeysInclusive);
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return underlyingIndex.get(keys);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    return underlyingIndex.get(keys, limit);
  }

  @Override
  public void put(final Object[] keys, RID[] rid) {
    underlyingIndex.put(keys, rid);
  }

  @Override
  public void remove(final Object[] keys) {
    globalLock.lock();
    try {
      final IndexCursor cursor = underlyingIndex.get(new Object[] { keys[0] });
      if (!cursor.hasNext())
        return;

      final Vertex vertex = loadNodeFromId(cursor.next());
      vertex.modify().set(deletedPropertyName, true).save();
      //underlyingIndex.remove(keys);
    } finally {
      globalLock.unlock();
    }
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    globalLock.lock();
    try {
      final IndexCursor cursor = underlyingIndex.get(new Object[] { keys[0] });
      if (!cursor.hasNext())
        return;

      final Identifiable itemRID = cursor.next();
      if (!itemRID.equals(rid))
        return;

      final Vertex vertex = loadNodeFromId(itemRID);
      vertex.modify().set(deletedPropertyName, true).save();
      // underlyingIndex.remove(keys, rid);

    } finally {
      globalLock.unlock();
    }
  }

  @Override
  public long countEntries() {
    return underlyingIndex.countEntries();
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    return underlyingIndex.compact();
  }

  @Override
  public boolean isCompacting() {
    return underlyingIndex.isCompacting();
  }

  @Override
  public boolean isValid() {
    return underlyingIndex.isValid();
  }

  @Override
  public boolean scheduleCompaction() {
    return underlyingIndex.scheduleCompaction();
  }

  @Override
  public String getMostRecentFileName() {
    return underlyingIndex.getMostRecentFileName();
  }

  @Override
  public EmbeddedSchema.INDEX_TYPE getType() {
    return EmbeddedSchema.INDEX_TYPE.HSNW;
  }

  @Override
  public String getTypeName() {
    return recordType;
  }

  @Override
  public List<String> getPropertyNames() {
    return List.of(vectorPropertyName);
  }

  @Override
  public void close() {
    underlyingIndex.close();
  }

  private Vertex get(final Object id) {
    globalLock.lock();
    try {
      final IndexCursor cursor = underlyingIndex.get(new Object[] { id });
      if (!cursor.hasNext())
        return null;

      return loadNodeFromId(cursor.next());
    } finally {
      globalLock.unlock();
    }
  }

  private void getNeighborsByHeuristic2(final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates, final int m) {
    if (topCandidates.size() < m)
      return;

    final PriorityQueue<NodeIdAndDistance<TDistance>> queueClosest = new PriorityQueue<>();
    final List<NodeIdAndDistance<TDistance>> returnList = new ArrayList<>();

    while (!topCandidates.isEmpty())
      queueClosest.add(topCandidates.poll());

    while (!queueClosest.isEmpty()) {
      if (returnList.size() >= m)
        break;

      final NodeIdAndDistance<TDistance> currentPair = queueClosest.poll();
      final TDistance distToQuery = currentPair.distance;

      boolean good = true;
      for (NodeIdAndDistance<TDistance> secondPair : returnList) {

        final TDistance curdist = distanceFunction.distance(//
            getVectorFromNode(loadNodeFromId(secondPair.nodeId)),//
            getVectorFromNode(loadNodeFromId(currentPair.nodeId)));

        if (lt(curdist, distToQuery)) {
          good = false;
          break;
        }

      }
      if (good) {
        returnList.add(currentPair);
      }
    }

    topCandidates.addAll(returnList);
  }

  protected RID nodeId2RID(final int nodeId) {
    return new RID(database, fileId, nodeId / maxRecordsInPage);
  }

  protected int rid2nodeId(final RID nodeRID) {
    return (int) (nodeRID.getPosition() / maxRecordsInPage);
  }

  /**
   * Saves the index structure into the first record of the underlying bucket. The RID is the property `structureRecordRID`.
   */
  private void save() {
    final Binary structureRecord = getRecord(structureRecordRID);
    structureRecord.clear();
    structureRecord.putString(toJSON().toString());
    updateRecord(new BinaryRecord(database, structureRecord), true);
    lastSavedEntryPoint = entryPoint;
  }
}
