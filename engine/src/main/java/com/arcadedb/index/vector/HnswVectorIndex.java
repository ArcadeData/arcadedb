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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VectorIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
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
public class HnswVectorIndex<TId, TVector, TDistance> extends Component implements com.arcadedb.index.Index, IndexInternal {
  public interface BuildVectorIndexCallback<TId, TVector> {
    void onVertexIndexed(Vertex document, Item<TId, TVector> item, long totalIndexed);
  }

  public interface IgnoreVertexCallback {
    boolean ignoreVertex(Vertex v);
  }

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
  private final   Set<RID>                             excludedCandidates = new HashSet<>();
  private final   String                               vertexType;
  private final   String                               edgeType;
  private final   String                               vectorPropertyName;
  private final   String                               idPropertyName;
  private final   String                               deletedPropertyName;
  private final   Map<RID, Vertex>                     cache;
  private final   String                               indexName;
  private         TypeIndex                            underlyingIndex;
  public volatile RID                                  entryPointRIDToLoad;
  public volatile Vertex                               entryPoint;

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
      if (!(builder instanceof VectorIndexBuilder))
        throw new IndexException("Expected VectorIndexBuilder but received " + builder);

      return new HnswVectorIndex<>((VectorIndexBuilder) builder);
    }
  }

  public static class PaginatedComponentFactoryHandlerUnique implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public Component createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id, final ComponentFile.MODE mode,
        final int pageSize, final int version) throws IOException {
      return new HnswVectorIndex(database, name, filePath, id, version);
    }
  }

  protected HnswVectorIndex(final VectorIndexBuilder builder) {
    super(builder.getDatabase(), builder.getFilePath(), builder.getDatabase().getFileManager().newFileId(), CURRENT_VERSION, builder.getFilePath());

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

    this.vertexType = builder.getVertexType();
    this.edgeType = builder.getEdgeType();
    this.vectorPropertyName = builder.getVectorPropertyName();
    this.idPropertyName = builder.getIdPropertyName();
    this.deletedPropertyName = builder.getDeletedPropertyName();

    this.cache = builder.getCache();

    this.underlyingIndex = builder.getDatabase().getSchema().buildTypeIndex(builder.getVertexType(), new String[] { idPropertyName }).withUnique(true)
        .withIgnoreIfExists(true).withType(Schema.INDEX_TYPE.LSM_TREE).create();

    this.underlyingIndex.setAssociatedIndex(this);

    this.globalLock = new ReentrantLock();
    this.indexName = builder.getIndexName() != null ? builder.getIndexName() : vertexType + "[" + idPropertyName + "," + vectorPropertyName + "]";
  }

  /**
   * Load time.
   */
  protected HnswVectorIndex(final DatabaseInternal database, final String indexName, final String filePath, final int id, final int version)
      throws IOException {
    super(database, indexName, id, version, filePath);

    final String fileContent = FileUtils.readFileAsString(new File(filePath));

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

    if (!json.getString("entryPoint").isEmpty()) {
      this.entryPointRIDToLoad = new RID(database, json.getString("entryPoint"));
    } else
      this.entryPointRIDToLoad = null;

    this.vertexType = json.getString("vertexType");
    this.edgeType = json.getString("edgeType");
    this.idPropertyName = json.getString("idPropertyName");
    this.vectorPropertyName = json.getString("vectorPropertyName");
    this.deletedPropertyName = json.has("deletedPropertyName") ? json.getString("deletedPropertyName") : "deleted";

    this.globalLock = new ReentrantLock();
    this.cache = null;
    this.indexName = json.getString("indexName");
  }

  @Override
  public void onAfterSchemaLoad() {
    try {
      this.underlyingIndex = database.getSchema().buildTypeIndex(vertexType, new String[] { idPropertyName }).withIgnoreIfExists(true).withUnique(true)
          .withType(Schema.INDEX_TYPE.LSM_TREE).create();

      this.underlyingIndex.setAssociatedIndex(this);

      // AFTER THE WHOLE SCHEMA IS LOADED INITIALIZE THE INDEX
      if (this.entryPointRIDToLoad != null) {
        try {
          this.entryPoint = this.entryPointRIDToLoad.asVertex();
        } catch (RecordNotFoundException e) {
          // ENTRYPOINT DELETED, DROP THE INDEX
          LogManager.instance().log(this, Level.WARNING, "HNSW index '" + indexName + "' has an invalid entrypoint. The index will be removed");
          this.entryPointRIDToLoad = null;
          database.getSchema().dropIndex(indexName);
        }
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on loading of HNSW index '" + indexName + "'", e);
    }
  }

  @Override
  public void onAfterCommit() {
    if (entryPoint != null && !entryPoint.getIdentity().equals(entryPointRIDToLoad)) {
      // ENTRY POINT IS CHANGED: SAVE THE NEW CONFIGURATION TO DISK
      save();
      entryPointRIDToLoad = entryPoint.getIdentity();
    }
  }

  @Override
  public String getName() {
    return indexName;
  }

  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromId(final TId id, final int k) {
    return findNeighborsFromId(id, k, null);
  }

  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromId(final TId id, final int k, IgnoreVertexCallback ignoreVertexCallback) {
    final Vertex start = get(id);
    if (start == null)
      return Collections.emptyList();

    return findNeighborsFromVertex(start, k, ignoreVertexCallback);
  }

  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromVertex(final Vertex start, final int k, final IgnoreVertexCallback ignoreVertexCallback) {
    final RID startRID = start.getIdentity();
    final TVector vector = getVectorFromVertex(start);

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
    final List<SearchResult<Vertex, TDistance>> neighbors = findNearest(vector, k + 1, ignoreVertexCallback).stream().limit(k).collect(Collectors.toList());

    final List<Pair<Identifiable, ? extends Number>> result = new ArrayList<>(neighbors.size());
    for (SearchResult<Vertex, TDistance> neighbor : neighbors)
      result.add(new Pair(neighbor.item(), neighbor.distance()));
    return result;
  }

  public void addAll(final List<Item<TId, TVector>> embeddings, final BuildVectorIndexCallback callback) {
    int indexed = 0;
    for (Item<TId, TVector> embedding : embeddings) {
      final IndexCursor existent = underlyingIndex.get(new Object[] { embedding.id() });
      Vertex vertex;
      if (existent.hasNext())
        vertex = existent.next().asVertex();
      else
        vertex = database.newVertex(vertexType).set(idPropertyName, embedding.id()).set(vectorPropertyName, embedding.vector()).save();

      add(vertex);

      callback.onVertexIndexed(vertex, embedding, ++indexed);
    }
  }

  public boolean add(Vertex vertex) {
    final TVector vertexVector = getVectorFromVertex(vertex);
    if (Array.getLength(vertexVector) != dimensions)
      throw new IllegalArgumentException(
          "Item has dimensionality of " + Array.getLength(vertexVector) + " but the index was defined with " + dimensions + " dimensions");

    final TId vertexId = getIdFromVertex(vertex);
    final int vertexMaxLevel = getMaxLevelFromVertex(vertex);

    final int randomLevel = assignLevel(vertexId, this.levelLambda);

    final ArrayList<RID>[] connections = new ArrayList[randomLevel + 1];

    for (int level = 0; level <= randomLevel; level++) {
      final int levelM = level == 0 ? maxM0 : maxM;
      connections[level] = new ArrayList<>(levelM);
    }

    globalLock.lock();
    try {

      final long totalEdges = vertex.countEdges(Vertex.DIRECTION.OUT, getEdgeType(0));
      if (totalEdges > 0)
        // ALREADY INSERTED
        return true;

      vertex = vertex.modify().set("vectorMaxLevel", randomLevel).save();
      if (cache != null)
        cache.put(vertex.getIdentity(), vertex);

      final RID vertexRID = vertex.getIdentity();
      synchronized (excludedCandidates) {
        excludedCandidates.add(vertexRID);
      }

      final Vertex entryPointCopy = entryPoint;
      try {
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
            final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, vertexVector, efConstruction, level, null);

            final boolean entryPointDeleted = isDeletedFromVertex(entryPointCopy);
            if (entryPointDeleted) {
              TDistance distance = distanceFunction.distance(vertexVector, getVectorFromVertex(entryPointCopy));
              topCandidates.add(new NodeIdAndDistance<>(entryPointCopy.getIdentity(), distance, maxValueDistanceComparator));

              if (topCandidates.size() > efConstruction)
                topCandidates.poll();
            }

            mutuallyConnectNewElement(vertex, topCandidates, level);
          }
        }

        // zoom out to the highest level
        if (entryPoint == null || vertexMaxLevel > entryPointCopyMaxLevel)
          // this is thread safe because we get the global lock when we add a level
          this.entryPoint = vertex;

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

  private int getMaxLevelFromVertex(final Vertex vertex) {
    if (vertex == null)
      return 0;
    final Integer vectorMaxLevel = vertex.getInteger("vectorMaxLevel");
    return vectorMaxLevel != null ? vectorMaxLevel : 0;
  }

  private Vertex loadVertexFromRID(final Identifiable rid) {
    if (rid instanceof Vertex)
      return (Vertex) rid;

    Vertex vertex = null;
    if (cache != null)
      vertex = cache.get(rid);
    if (vertex == null)
      vertex = rid.asVertex();
    return vertex;
  }

  private void mutuallyConnectNewElement(final Vertex newNode, final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates, final int level) {
    final int bestN = level == 0 ? this.maxM0 : this.maxM;
    final RID newNodeId = newNode.getIdentity();
    final TVector newItemVector = getVectorFromVertex(newNode);

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

      final Vertex neighbourNode = loadVertexFromRID(selectedNeighbourId);
      final TVector neighbourVector = getVectorFromVertex(neighbourNode);
      final int neighbourConnectionsAtLevelTotal = countConnectionsFromVertex(neighbourNode, level);
      final Iterator<Vertex> neighbourConnectionsAtLevel = getConnectionsFromVertex(neighbourNode, level);

      if (neighbourConnectionsAtLevelTotal < bestN) {
        neighbourNode.newEdge(edgeTypeName, newNode, false);
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
          neighbourNode.newEdge(edgeTypeName, candidates.poll().nodeId, false);
        }
      }
    }
  }

  public TypeIndex getUnderlyingIndex() {
    return underlyingIndex;
  }

  public List<SearchResult<Vertex, TDistance>> findNearest(final TVector destination, final int k, final IgnoreVertexCallback ignoreVertexCallback) {
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

    final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, destination, Math.max(ef, k), 0, ignoreVertexCallback);

    while (topCandidates.size() > k) {
      topCandidates.poll();
    }

    List<SearchResult<Vertex, TDistance>> results = new ArrayList<>(topCandidates.size());
    while (!topCandidates.isEmpty()) {
      NodeIdAndDistance<TDistance> pair = topCandidates.poll();
      results.add(0, new SearchResult<>(loadVertexFromRID(pair.nodeId), pair.distance, maxValueDistanceComparator));
    }

    return results;
  }

  private PriorityQueue<NodeIdAndDistance<TDistance>> searchBaseLayer(final Vertex entryPointNode, final TVector destination, final int k, final int layer,
      final IgnoreVertexCallback ignoreVertexCallback) {
    final Set<RID> visitedNodes = new HashSet<>();

    final PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = new PriorityQueue<>(Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed());
    final PriorityQueue<NodeIdAndDistance<TDistance>> candidateSet = new PriorityQueue<>();

    TDistance lowerBound;

    if (!ignoreVertex(entryPointNode, ignoreVertexCallback)) {
      final TVector entryPointVector = getVectorFromVertex(entryPointNode);
      final TDistance distance = distanceFunction.distance(destination, entryPointVector);
      final NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.getIdentity(), distance, maxValueDistanceComparator);

      topCandidates.add(pair);
      lowerBound = distance;
      candidateSet.add(pair);
    } else {
      lowerBound = MaxValueComparator.maxValue();
      NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.getIdentity(), lowerBound, maxValueDistanceComparator);
      candidateSet.add(pair);
    }

    visitedNodes.add(entryPointNode.getIdentity());

    while (!candidateSet.isEmpty()) {
      final NodeIdAndDistance<TDistance> currentPair = candidateSet.poll();

      if (gt(currentPair.distance, lowerBound))
        break;

      final Vertex node = loadVertexFromRID(currentPair.nodeId);

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

  public void save(OutputStream out) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
      oos.writeObject(this);
    }
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

  public boolean isDeletedFromVertex(final Vertex vertex) {
    final Boolean deleted = vertex.getBoolean(deletedPropertyName);
    return deleted != null && deleted;
  }

  public boolean ignoreVertex(final Vertex vertex, final IgnoreVertexCallback ignoreVertexCallback) {
    if (isDeletedFromVertex(vertex))
      return true;
    if (ignoreVertexCallback != null)
      return ignoreVertexCallback.ignoreVertex(vertex);
    return false;
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

  public void save() {
    try {
      FileUtils.writeFile(new File(filePath), toJSON().toString());
    } catch (IOException e) {
      throw new IndexException("Error on saving HNSW index '" + indexName + "'", e);
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
    json.put("entryPoint", entryPoint == null ? "" : entryPoint.getIdentity().toString());

    json.put("vertexType", vertexType);
    json.put("edgeType", edgeType);
    json.put("idPropertyName", idPropertyName);
    json.put("vectorPropertyName", vectorPropertyName);
    return json;
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return null;
  }

  @Override
  public void drop() {
// KEEP THE UNDERLYING INDEX ALIVE TO ALLOW THE REBUILD WITHOUT CALCULATING THE EMBEDDINGS
//    if (underlyingIndex != null)
//      database.getSchema().dropIndex(underlyingIndex.getName());
    try {
      if (underlyingIndex != null) {
        database.transaction(() -> {
          final IndexCursor it = underlyingIndex.iterator(true);
          while (it.hasNext()) {
            try {
              final Identifiable next = it.next();
              if (next != null) {
                final Vertex vertex = next.asVertex();
                for (int level = 0; level < getMaxLevelFromVertex(vertex); level++) {
                  for (Edge e : vertex.getEdges(Vertex.DIRECTION.BOTH, getEdgeType(level)))
                    e.delete();
                }
              }
            } catch (RecordNotFoundException e) {
              // IGNORE IT
            }
          }
        });
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on scanning the vector index to delete edges", e);
    }

    final File cfg = new File(filePath);
    if (cfg.exists())
      cfg.delete();
  }

  @Override
  public Map<String, Long> getStats() {
    return underlyingIndex.getStats();
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return underlyingIndex.getNullStrategy();
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    underlyingIndex.setNullStrategy(nullStrategy);
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return underlyingIndex.supportsOrderedIterations();
  }

  @Override
  public boolean isAutomatic() {
    return underlyingIndex.isAutomatic();
  }

  @Override
  public int getPageSize() {
    return underlyingIndex.getPageSize();
  }

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    return underlyingIndex.build(buildIndexBatchSize, callback);
  }

  public long build(final HnswVectorIndexRAM origin, final int buildIndexBatchSize, final BuildVectorIndexCallback vertexCreationCallback,
      final BuildIndexCallback edgeCallback) {
    if (origin != null) {
      // IMPORT FROM RAM Index
      final RID[] pointersToRIDMapping = new RID[origin.nodeCount];

      database.begin();

      // SAVE ALL THE NODES AS VERTICES AND KEEP AN ARRAY OF RIDS TO BUILD EDGES LATER
      int maxLevel = 0;
      HnswVectorIndexRAM.ItemIterator iter = origin.iterateNodes();
      for (int totalVertices = 0; iter.hasNext(); ++totalVertices) {
        final HnswVectorIndexRAM.Node node = iter.next();

        final int nodeMaxLevel = node.maxLevel();
        if (nodeMaxLevel > maxLevel)
          maxLevel = nodeMaxLevel;

        final MutableVertex vertex;
        final IndexCursor existent = underlyingIndex.get(new Object[] { node.item.id() });
        if (existent.hasNext())
          vertex = existent.next().asVertex().modify();
        else
          vertex = database.newVertex(vertexType).set(idPropertyName, node.item.id()).set(vectorPropertyName, node.item.vector());

        if (nodeMaxLevel > 0)
          // SAVE MAX LEVEL INTO THE VERTEX. IF NOT PRESENT, MEANS 0
          vertex.set("vectorMaxLevel", nodeMaxLevel);

        vertex.save();

        if (vertexCreationCallback != null)
          vertexCreationCallback.onVertexIndexed(vertex, node.item, totalVertices);

        pointersToRIDMapping[node.id] = vertex.getIdentity();

        if (totalVertices % buildIndexBatchSize == 0) {
          database.commit();
          database.begin();
        }
      }

      database.commit();

      final Integer entryPoint = origin.getEntryPoint();
      if (entryPoint != null)
        this.entryPoint = pointersToRIDMapping[entryPoint].asVertex();

      // BUILD ALL EDGE TYPES (ONE PER LEVEL)
      for (int level = 0; level <= maxLevel; level++) {
        // ASSURE THE EDGE TYPE IS CREATED IN THE DATABASE
        database.getSchema().getOrCreateEdgeType(getEdgeType(level), 1);
      }

      database.begin();

      // BUILD THE EDGES
      long totalVertices = 0L;
      long totalEdges = 0L;
      iter = origin.iterateNodes();
      for (int txCounter = 0; iter.hasNext(); ++txCounter) {
        final HnswVectorIndexRAM.Node node = iter.next();

        final Vertex source = pointersToRIDMapping[node.id].asVertex();
        ++totalVertices;

        final MutableIntList[] connections = node.connections();
        for (int level = 0; level < connections.length; level++) {
          final String edgeTypeLevel = getEdgeType(level);

          final MutableIntList pointers = connections[level];
          for (int i = 0; i < pointers.size(); i++) {
            final int pointer = pointers.get(i);

            final RID destination = pointersToRIDMapping[pointer];

            if (destination == null)
              LogManager.instance().log(this, Level.WARNING, "Destination vertex %d is null", pointer);
            else {
              source.newEdge(edgeTypeLevel, destination, false);
              ++totalEdges;
            }
          }
        }

        if (txCounter % buildIndexBatchSize == 0) {
          database.commit();
          database.begin();
        }

        if (edgeCallback != null)
          edgeCallback.onDocumentIndexed(source, totalEdges);
      }

      database.commit();

      save();

      return totalVertices;
    }

    // TODO: NOT SUPPORTED WITHOUT RAM INDEX
    return 0L;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof HnswVectorIndex))
      return false;
    return componentName.equals(((HnswVectorIndex) obj).componentName) && underlyingIndex.equals(obj);
  }

  public List<IndexInternal> getSubIndexes() {
    return underlyingIndex.getSubIndexes();
  }

  @Override
  public int hashCode() {
    return Objects.hash(componentName, underlyingIndex.hashCode());
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
    return underlyingIndex.getKeyTypes();
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

  public IndexCursor range(final boolean ascending, final Object[] beginKeys, final boolean beginKeysInclusive, final Object[] endKeys,
      boolean endKeysInclusive) {
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

      final Vertex vertex = loadVertexFromRID(cursor.next());
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

      final Vertex vertex = loadVertexFromRID(itemRID);
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
    return vertexType;
  }

  @Override
  public List<String> getPropertyNames() {
    return List.of(idPropertyName, vectorPropertyName);
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

      return loadVertexFromRID(cursor.next());
    } finally {
      globalLock.unlock();
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
            getVectorFromVertex(loadVertexFromRID(secondPair.nodeId)),//
            getVectorFromVertex(loadVertexFromRID(currentPair.nodeId)));

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
}
