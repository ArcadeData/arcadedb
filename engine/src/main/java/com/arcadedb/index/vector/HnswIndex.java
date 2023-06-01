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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.github.jelmerk.knn.DistanceFunction;
import com.github.jelmerk.knn.Index;
import com.github.jelmerk.knn.SearchResult;
import com.github.jelmerk.knn.util.Murmur3;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * This work is derived from the excellent work made by Jelmer Kuperus on https://github.com/jelmerk/hnswlib.
 * <p>
 * Implementation of {@link Index} that implements the hnsw algorithm.
 *
 * @see <a href="https://arxiv.org/abs/1603.09320">
 * Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
 */
public class HnswIndex<TId, TVector, TDistance> {
  private          DistanceFunction<TVector, TDistance> distanceFunction;
  private          Comparator<TDistance>                distanceComparator;
  private          MaxValueComparator<TDistance>        maxValueDistanceComparator;
  private          int                                  dimensions;
  private          int                                  maxItemCount;
  private          int                                  m;
  private          int                                  maxM;
  private          int                                  maxM0;
  private          double                               levelLambda;
  private          int                                  ef;
  private          int                                  efConstruction;
  private volatile Vertex                               entryPoint;
  private          TypeIndex                            lookup;
  private          MutableObjectLongMap<TId>            deletedItemVersions;
  private          Map<TId, Object>                     locks;
  private          ReentrantLock                        globalLock;
  private          Set<RID>                             excludedCandidates = new HashSet<>();
  private          String                               edgeType;
  private          String                               vectorPropertyName;
  private          String                               idPropertyName;

  private HnswIndex(final DatabaseInternal database, final String typeName, final String vectorPropertyName, BuilderBase builder) {
    this.dimensions = builder.dimensions;
    this.maxItemCount = builder.maxItemCount;
    this.distanceFunction = builder.distanceFunction;
    this.distanceComparator = builder.distanceComparator;
    this.maxValueDistanceComparator = new MaxValueComparator<>(this.distanceComparator);

    this.m = builder.m;
    this.maxM = builder.m;
    this.maxM0 = builder.m * 2;
    this.levelLambda = 1 / Math.log(this.m);
    this.efConstruction = Math.max(builder.efConstruction, m);
    this.ef = builder.ef;

    this.lookup = database.getSchema().buildTypeIndex(typeName, new String[] { vectorPropertyName }).withUnique(true).withIgnoreIfExists(true)
        .withType(Schema.INDEX_TYPE.LSM_TREE).create();

    this.deletedItemVersions = new ObjectLongHashMap<>();
    this.locks = new HashMap<>();
    this.globalLock = new ReentrantLock();
  }

  public Vertex get(Object id) {
    globalLock.lock();
    try {
      final IndexCursor cursor = lookup.get(new Object[] { id });
      if (!cursor.hasNext())
        return null;

      return cursor.next().asVertex();
    } finally {
      globalLock.unlock();
    }
  }

  public boolean remove(final Object id) {
    globalLock.lock();
    try {
      final IndexCursor cursor = lookup.get(new Object[] { id });
      if (!cursor.hasNext())
        return false;

      final Vertex vertex = cursor.next().asVertex();
      if (vertex.equals(entryPoint)) {
        // TODO: CHANGE THE ENTRYPOINT
      }

      vertex.delete();

      return true;
    } finally {
      globalLock.unlock();
    }
  }

  public boolean add(final Vertex vertex) {
    if (getDimensionFromVertex(vertex) != dimensions)
      throw new IllegalArgumentException("Item does not have dimensionality of " + dimensions);

    final TId vertexId = getIdFromVertex(vertex);
    final TVector vertexVector = getVectorFromVertex(vertex);
    final int vertexMaxLevel = getMaxLevelFromVertex(vertex);

    final int randomLevel = assignLevel(vertexId, this.levelLambda);

    final ArrayList<RID>[] connections = new ArrayList[randomLevel + 1];

    for (int level = 0; level <= randomLevel; level++) {
      final int levelM = randomLevel == 0 ? maxM0 : maxM;
      connections[level] = new ArrayList<>(levelM);
    }

    globalLock.lock();
    try {

      final IndexCursor cursor = lookup.get(new Object[] { vertexId });
      if (cursor.hasNext()) {
        final Vertex node = cursor.next().asVertex();
        if (Objects.deepEquals(getVectorFromVertex(node), vertexVector)) {
          // ALREADY INSERTED
          return true;
        } else {
          // DIFFERENT VERTEX, REMOVE CURRENT AND ADD THE NEW ONE
          remove(vertexId);
        }
      }

      if (vertex.getIdentity() == null && vertex instanceof MutableVertex)
        ((MutableVertex) vertex).save();

      final RID vertexRID = vertex.getIdentity();
      synchronized (excludedCandidates) {
        excludedCandidates.add(vertexRID);
      }

      lookup.put(new Object[] { vertexId }, new RID[] { vertexRID });
      deletedItemVersions.remove(vertexId);

      Object lock = locks.computeIfAbsent(vertexId, k -> new Object());

      final Vertex entryPointCopy = entryPoint;

      try {
        synchronized (lock) {

          if (entryPoint != null && randomLevel <= getMaxLevelFromVertex(entryPoint)) {
            globalLock.unlock();
          }

          Vertex currObj = entryPointCopy;
          final int entryPointCopyMaxLevel = getMaxLevelFromVertex(entryPointCopy);

          if (currObj != null) {
            if (vertexMaxLevel < entryPointCopyMaxLevel) {
              TDistance curDist = distanceFunction.distance(vertexVector, getVectorFromVertex(currObj));
              for (int activeLevel = entryPointCopyMaxLevel; activeLevel > vertexMaxLevel; activeLevel--) {
                boolean changed = true;

                while (changed) {
                  changed = false;

                  synchronized (currObj) {
                    final Iterator<Vertex> candidateConnections = getConnectionsFromVertex(currObj, activeLevel);
                    while (candidateConnections.hasNext()) {
                      final Vertex candidateNode = candidateConnections.next();
                      final TDistance candidateDistance = distanceFunction.distance(vertexVector, getVectorFromVertex(candidateNode));

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
              final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, vertexVector, efConstruction, level);

              // TODO: MANAGE DELETE OF ENTRYPOINT
//              if (entryPointCopy.deleted) {
//                TDistance distance = distanceFunction.distance(vertex.vector(), entryPointCopy.item.vector());
//                topCandidates.add(new NodeIdAndDistance<>(entryPointCopy.id, distance, maxValueDistanceComparator));
//
//                if (topCandidates.size() > efConstruction) {
//                  topCandidates.poll();
//                }
//              }

              mutuallyConnectNewElement(vertex, topCandidates, level);

            }
          }

          // zoom out to the highest level
          if (entryPoint == null || vertexMaxLevel > entryPointCopyMaxLevel)
            // this is thread safe because we get the global lock when we add a level
            this.entryPoint = vertex;

          return true;
        }
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

  private int getMaxLevelFromVertex(final Vertex vertex) {
    return vertex.getInteger("vectorMaxLevel");
  }

  private void mutuallyConnectNewElement(final Vertex newNode, final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates, final int level) {
    final int bestN = level == 0 ? this.maxM0 : this.maxM;
    final RID newNodeId = newNode.getIdentity();
    final TVector newItemVector = getVectorFromVertex(newNode);
    final Iterator<Vertex> newItemConnections = getConnectionsFromVertex(newNode, level);

    getNeighborsByHeuristic2(topCandidates, m);

    while (!topCandidates.isEmpty()) {
      final RID selectedNeighbourId = topCandidates.poll().nodeId;
      synchronized (excludedCandidates) {
        if (excludedCandidates.contains(selectedNeighbourId)) {
          continue;
        }
      }

      newNode.newEdge(getEdgeType(level), selectedNeighbourId, false);

      final Vertex neighbourNode = selectedNeighbourId.asVertex();
      final TVector neighbourVector = getVectorFromVertex(neighbourNode);
      final int neighbourConnectionsAtLevelTotal = countConnectionsFromVertex(neighbourNode, level);
      final Iterator<Vertex> neighbourConnectionsAtLevel = getConnectionsFromVertex(neighbourNode, level);

      if (neighbourConnectionsAtLevelTotal < bestN) {
        neighbourNode.newEdge(getEdgeType(level), newNode, false);
      } else {
        // finding the "weakest" element to replace it with the new one
        final TDistance dMax = distanceFunction.distance(newItemVector, neighbourVector);
        final Comparator<NodeIdAndDistance<TDistance>> comparator = Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed();
        final PriorityQueue<NodeIdAndDistance<TDistance>> candidates = new PriorityQueue<>(comparator);
        candidates.add(new NodeIdAndDistance<>(newNodeId, dMax, maxValueDistanceComparator));

        neighbourConnectionsAtLevel.forEachRemaining(neighbourConnection -> {
          final TDistance dist = distanceFunction.distance(neighbourVector, getVectorFromVertex(neighbourConnection));
          candidates.add(new NodeIdAndDistance<>(neighbourConnection.getIdentity(), dist, maxValueDistanceComparator));
        });

        getNeighborsByHeuristic2(candidates, bestN);

        while (!candidates.isEmpty()) {
          neighbourNode.newEdge(getEdgeType(level), candidates.poll().nodeId, false);
        }
      }
    }
  }

  private void getNeighborsByHeuristic2(final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates, final int m) {
    if (topCandidates.size() < m)
      return;

    final PriorityQueue<NodeIdAndDistance<TDistance>> queueClosest = new PriorityQueue<>();
    final List<NodeIdAndDistance<TDistance>> returnList = new ArrayList<>();

    while (!topCandidates.isEmpty()) {
      queueClosest.add(topCandidates.poll());
    }

    while (!queueClosest.isEmpty()) {
      if (returnList.size() >= m)
        break;

      final NodeIdAndDistance<TDistance> currentPair = queueClosest.poll();
      final TDistance distToQuery = currentPair.distance;

      boolean good = true;
      for (NodeIdAndDistance<TDistance> secondPair : returnList) {

        final TDistance curdist = distanceFunction.distance(//
            getVectorFromVertex(secondPair.nodeId.asVertex()),//
            getVectorFromVertex(currentPair.nodeId.asVertex()));

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

  public List<SearchResult<Vertex, TDistance>> findNearest(final TVector destination, final int k) {
    if (entryPoint == null)
      return Collections.emptyList();

    final Vertex entryPointCopy = entryPoint;
    Vertex currObj = entryPointCopy;

    TDistance curDist = distanceFunction.distance(destination, getVectorFromVertex(currObj));

    for (int activeLevel = getMaxLevelFromVertex(entryPointCopy); activeLevel > 0; activeLevel--) {
      boolean changed = true;
      while (changed) {
        changed = false;

        final Iterator<Vertex> candidateConnections = getConnectionsFromVertex(currObj, activeLevel);

        while (candidateConnections.hasNext()) {
          final Vertex candidateNode = candidateConnections.next();

          TDistance candidateDistance = distanceFunction.distance(destination, getVectorFromVertex(candidateNode));
          if (lt(candidateDistance, curDist)) {
            curDist = candidateDistance;
            currObj = candidateNode;
            changed = true;
          }
        }

      }
    }

    final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, destination, Math.max(ef, k), 0);

    while (topCandidates.size() > k) {
      topCandidates.poll();
    }

    List<SearchResult<Vertex, TDistance>> results = new ArrayList<>(topCandidates.size());
    while (!topCandidates.isEmpty()) {
      NodeIdAndDistance<TDistance> pair = topCandidates.poll();
      results.add(0, new SearchResult<>(pair.nodeId.asVertex(), pair.distance, maxValueDistanceComparator));
    }

    return results;
  }

  private PriorityQueue<NodeIdAndDistance<TDistance>> searchBaseLayer(final Vertex entryPointNode, final TVector destination, final int k, final int layer) {
    final Set<RID> visitedNodes = new HashSet<>();

    final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = new PriorityQueue<>(Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed());
    final PriorityQueue<NodeIdAndDistance<TDistance>> candidateSet = new PriorityQueue<>();

    TDistance lowerBound;

    final TVector entryPointVector = getVectorFromVertex(entryPointNode);

    final TDistance distance = distanceFunction.distance(destination, entryPointVector);
    final NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.getIdentity(), distance, maxValueDistanceComparator);

    topCandidates.add(pair);
    lowerBound = distance;
    candidateSet.add(pair);

    // TODO: MANAGE WHEN ENTRY POINT WAS DELETED
//      lowerBound = MaxValueComparator.maxValue();
//      NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.id, lowerBound, maxValueDistanceComparator);
//      candidateSet.add(pair);

    visitedNodes.add(entryPointNode.getIdentity());

    while (!candidateSet.isEmpty()) {
      final NodeIdAndDistance<TDistance> currentPair = candidateSet.poll();

      if (gt(currentPair.distance, lowerBound))
        break;

      final Vertex node = currentPair.nodeId.asVertex();

      final Iterator<Vertex> candidates = getConnectionsFromVertex(node, layer);
      while (candidates.hasNext()) {
        final Vertex candidateNode = candidates.next();

        if (!visitedNodes.contains(candidateNode.getIdentity())) {
          visitedNodes.add(candidateNode.getIdentity());

          final TDistance candidateDistance = distanceFunction.distance(destination, getVectorFromVertex(candidateNode));
          if (topCandidates.size() < k || gt(lowerBound, candidateDistance)) {
            final NodeIdAndDistance<TDistance> candidatePair = new NodeIdAndDistance<>(candidateNode.getIdentity(), candidateDistance,
                maxValueDistanceComparator);

            candidateSet.add(candidatePair);
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
   * Set the size of the dynamic list for the nearest neighbors (used during the search)
   *
   * @param ef The size of the dynamic list for the nearest neighbors
   */
  public void setEf(int ef) {
    this.ef = ef;
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

  public void save(OutputStream out) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
      oos.writeObject(this);
    }
  }

  /**
   * Start the process of building a new HNSW index.
   *
   * @param dimensions       the dimensionality of the vectors stored in the index
   * @param distanceFunction the distance function
   * @param maxItemCount     maximum number of items the index can hold
   * @param <TVector>        Type of the vector to perform distance calculation on
   * @param <TDistance>      Type of distance between items (expect any numeric type: float, double, int, ..)
   *
   * @return a builder
   */
  public static <TVector, TDistance extends Comparable<TDistance>> Builder<TVector, TDistance> newBuilder(int dimensions,
      DistanceFunction<TVector, TDistance> distanceFunction, int maxItemCount) {

    Comparator<TDistance> distanceComparator = Comparator.naturalOrder();

    return new Builder<>(dimensions, distanceFunction, distanceComparator, maxItemCount);
  }

  /**
   * Start the process of building a new HNSW index.
   *
   * @param dimensions         the dimensionality of the vectors stored in the index
   * @param distanceFunction   the distance function
   * @param distanceComparator used to compare distances
   * @param maxItemCount       maximum number of items the index can hold
   * @param <TVector>          Type of the vector to perform distance calculation on
   * @param <TDistance>        Type of distance between items (expect any numeric type: float, double, int, ..)
   *
   * @return a builder
   */
  public static <TVector, TDistance> Builder<TVector, TDistance> newBuilder(int dimensions, DistanceFunction<TVector, TDistance> distanceFunction,
      Comparator<TDistance> distanceComparator, int maxItemCount) {

    return new Builder<>(dimensions, distanceFunction, distanceComparator, maxItemCount);
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

  public <TId> TId getIdFromVertex(final Vertex vertex) {
    return (TId) vertex.get(idPropertyName);
  }

  public <TVector> TVector getVectorFromVertex(final Vertex vertex) {
    return (TVector) vertex.get(vectorPropertyName);
  }

  public int getDimensionFromVertex(final Vertex vertex) {
    return Array.getLength(getVectorFromVertex(vertex));
  }

  public String getEdgeType(final int level) {
    return edgeType + level;
  }

  static class NodeIdAndDistance<TDistance> implements Comparable<NodeIdAndDistance<TDistance>> {

    final RID                   nodeId;
    final TDistance             distance;
    final Comparator<TDistance> distanceComparator;

    NodeIdAndDistance(final RID nodeId, final TDistance distance, final Comparator<TDistance> distanceComparator) {
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

  /**
   * Base class for HNSW index builders.
   *
   * @param <TBuilder>  Concrete class that extends from this builder
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TDistance> Type of items stored in the index
   */
  public static abstract class BuilderBase<TBuilder extends BuilderBase<TBuilder, TVector, TDistance>, TVector, TDistance> {

    public static final int     DEFAULT_M               = 10;
    public static final int     DEFAULT_EF              = 10;
    public static final int     DEFAULT_EF_CONSTRUCTION = 200;
    public static final boolean DEFAULT_REMOVE_ENABLED  = false;

    int                                  dimensions;
    DistanceFunction<TVector, TDistance> distanceFunction;
    Comparator<TDistance>                distanceComparator;

    int maxItemCount;

    int m              = DEFAULT_M;
    int ef             = DEFAULT_EF;
    int efConstruction = DEFAULT_EF_CONSTRUCTION;

    BuilderBase(int dimensions, DistanceFunction<TVector, TDistance> distanceFunction, Comparator<TDistance> distanceComparator, int maxItemCount) {

      this.dimensions = dimensions;
      this.distanceFunction = distanceFunction;
      this.distanceComparator = distanceComparator;
      this.maxItemCount = maxItemCount;
    }

    abstract TBuilder self();

    /**
     * Sets the number of bi-directional links created for every new element during construction. Reasonable range
     * for m is 2-100. Higher m work better on datasets with high intrinsic dimensionality and/or high recall,
     * while low m work better for datasets with low intrinsic dimensionality and/or low recalls. The parameter
     * also determines the algorithm's memory consumption.
     * As an example for d = 4 random vectors optimal m for search is somewhere around 6, while for high dimensional
     * datasets (word embeddings, good face descriptors), higher M are required (e.g. m = 48, 64) for optimal
     * performance at high recall. The range mM = 12-48 is ok for the most of the use cases. When m is changed one
     * has to update the other parameters. Nonetheless, ef and efConstruction parameters can be roughly estimated by
     * assuming that m  efConstruction is a constant.
     *
     * @param m the number of bi-directional links created for every new element during construction
     *
     * @return the builder.
     */
    public TBuilder withM(int m) {
      this.m = m;
      return self();
    }

    /**
     * `
     * The parameter has the same meaning as ef, but controls the index time / index precision. Bigger efConstruction
     * leads to longer construction, but better index quality. At some point, increasing efConstruction does not
     * improve the quality of the index. One way to check if the selection of ef_construction was ok is to measure
     * a recall for M nearest neighbor search when ef = efConstruction: if the recall is lower than 0.9, then
     * there is room for improvement.
     *
     * @param efConstruction controls the index time / index precision
     *
     * @return the builder
     */
    public TBuilder withEfConstruction(int efConstruction) {
      this.efConstruction = efConstruction;
      return self();
    }

    /**
     * The size of the dynamic list for the nearest neighbors (used during the search). Higher ef leads to more
     * accurate but slower search. The value ef of can be anything between k and the size of the dataset.
     *
     * @param ef size of the dynamic list for the nearest neighbors
     *
     * @return the builder
     */
    public TBuilder withEf(int ef) {
      this.ef = ef;
      return self();
    }
  }

  /**
   * Builder for initializing an {@link HnswIndex} instance.
   *
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ..)
   */
  public static class Builder<TVector, TDistance> extends BuilderBase<Builder<TVector, TDistance>, TVector, TDistance> {

    /**
     * Constructs a new {@link Builder} instance.
     *
     * @param dimensions       the dimensionality of the vectors stored in the index
     * @param distanceFunction the distance function
     * @param maxItemCount     the maximum number of elements in the index
     */
    Builder(int dimensions, DistanceFunction<TVector, TDistance> distanceFunction, Comparator<TDistance> distanceComparator, int maxItemCount) {

      super(dimensions, distanceFunction, distanceComparator, maxItemCount);
    }

    @Override
    Builder<TVector, TDistance> self() {
      return this;
    }
  }
}
