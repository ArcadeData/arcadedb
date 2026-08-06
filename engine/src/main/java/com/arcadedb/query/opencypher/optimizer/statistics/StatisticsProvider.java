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
package com.arcadedb.query.opencypher.optimizer.statistics;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Collects and provides runtime statistics for query optimization.
 * Statistics are collected on-demand and stored in-memory for the current query.
 *
 * Most estimators use ArcadeDB's cached Bucket.count() (O(1)); {@link #getMeanEdgesPerConnectedPair}
 * is the exception - it samples up to {@value #MULTIPLICITY_SAMPLE_LIMIT} edge records per call.
 */
public class StatisticsProvider {
  // Bounded sample size for multiplicity estimation, so a busy edge type is never fully scanned during planning.
  private static final int    MULTIPLICITY_SAMPLE_LIMIT             = 2000;
  private static final double DEFAULT_MEAN_EDGES_PER_CONNECTED_PAIR = 1.0;
  // Prefix sampling can overestimate badly if the sampled prefix happens to land entirely inside one
  // heavily-parallel pair's edges; cap the blast radius the same way calculateAverageDegree does.
  private static final double MAX_MEAN_EDGES_PER_CONNECTED_PAIR     = 1000.0;

  private final DatabaseInternal database;
  private final Map<String, TypeStatistics> typeStatsCache;
  private final Map<String, List<IndexStatistics>> indexStatsCache;
  private final Map<String, Double> averageDegreeCache;
  private final Map<String, Double> meanEdgesPerConnectedPairCache;

  public StatisticsProvider(final DatabaseInternal database) {
    this.database = database;
    this.typeStatsCache = new HashMap<>();
    this.indexStatsCache = new HashMap<>();
    this.averageDegreeCache = new HashMap<>();
    this.meanEdgesPerConnectedPairCache = new HashMap<>();
  }

  /**
   * Collects statistics for all types referenced in the query.
   * This is called once per query to populate the cache.
   *
   * @param typeNames list of type names referenced in the query
   */
  public void collectStatistics(final Collection<String> typeNames) {
    final Schema schema = database.getSchema();

    for (final String typeName : typeNames) {
      if (typeStatsCache.containsKey(typeName)) {
        continue; // Already collected
      }

      // Skip types that do not exist in the schema (issue #4090).
      // schema.getType() throws SchemaException for unknown names, so existsType() must be checked first.
      if (!schema.existsType(typeName))
        continue;

      final DocumentType type = schema.getType(typeName);

      // Collect type cardinality using cached O(1) count
      final long recordCount = database.countType(typeName, false);
      final boolean isVertexType = type instanceof VertexType;

      final TypeStatistics typeStats = new TypeStatistics(typeName, recordCount, isVertexType);
      typeStatsCache.put(typeName, typeStats);

      // Collect index statistics for this type
      collectIndexStatistics(type);
    }
  }

  /**
   * Collects index metadata for a given type.
   */
  private void collectIndexStatistics(final DocumentType type) {
    final String typeName = type.getName();
    final List<IndexStatistics> indexStatsList = new ArrayList<>();

    final Collection<TypeIndex> indexes = type.getAllIndexes(false);
    for (final TypeIndex index : indexes) {
      final List<String> propertyNames = index.getPropertyNames();
      final boolean isUnique = index.isUnique();
      final String indexName = index.getName();

      final IndexStatistics indexStats = new IndexStatistics(
          typeName,
          propertyNames,
          isUnique,
          indexName
      );
      indexStatsList.add(indexStats);
    }

    indexStatsCache.put(typeName, indexStatsList);
  }

  /**
   * Returns statistics for a specific type.
   *
   * @param typeName the type name
   * @return type statistics, or null if not collected
   */
  public TypeStatistics getTypeStatistics(final String typeName) {
    return typeStatsCache.get(typeName);
  }

  /**
   * Returns all indexes defined on a specific type.
   *
   * @param typeName the type name
   * @return list of index statistics, empty list if none exist
   */
  public List<IndexStatistics> getIndexesForType(final String typeName) {
    return indexStatsCache.getOrDefault(typeName, new ArrayList<>());
  }

  /**
   * Finds an index that can be used for the given property on a type.
   * Returns the most selective index (unique > non-unique).
   *
   * @param typeName the type name
   * @param propertyName the property name
   * @return index statistics, or null if no suitable index exists
   */
  public IndexStatistics findIndexForProperty(final String typeName, final String propertyName) {
    final List<IndexStatistics> indexes = getIndexesForType(typeName);

    IndexStatistics bestIndex = null;
    for (final IndexStatistics index : indexes) {
      if (index.canBeUsedForProperty(propertyName)) {
        // Prefer unique indexes (more selective)
        if (bestIndex == null || (index.isUnique() && !bestIndex.isUnique())) {
          bestIndex = index;
        }
      }
    }

    return bestIndex;
  }

  /**
   * Checks if an index exists for the given property on a type.
   *
   * @param typeName the type name
   * @param propertyName the property name
   * @return true if an index exists
   */
  public boolean hasIndexForProperty(final String typeName, final String propertyName) {
    return findIndexForProperty(typeName, propertyName) != null;
  }

  /**
   * Returns the cardinality (row count) for a specific type.
   *
   * @param typeName the type name
   * @return cardinality, or 0 if statistics not collected
   */
  public long getCardinality(final String typeName) {
    final TypeStatistics stats = getTypeStatistics(typeName);
    return stats != null ? stats.getRecordCount() : 0L;
  }

  /**
   * Calculates the average degree (edges per vertex) for a relationship type.
   * <p>
   * Formula: avgDegree = (2 * edgeCount) / (sourceVertexCount + targetVertexCount)
   * <p>
   * This represents how many edges, on average, each vertex has for this relationship type.
   * The factor of 2 accounts for both outgoing and incoming edges.
   *
   * @param relationshipType  name of the edge type
   * @param sourceVertexLabel label of source vertex type (optional, can be null)
   * @param targetVertexLabel label of target vertex type (optional, can be null)
   * @return estimated average degree, or 10.0 as fallback
   */
  public double getAverageDegree(
      final String relationshipType,
      final String sourceVertexLabel,
      final String targetVertexLabel) {

    // Check cache first
    final String cacheKey = relationshipType + ":" + sourceVertexLabel + ":" + targetVertexLabel;
    if (averageDegreeCache.containsKey(cacheKey)) {
      return averageDegreeCache.get(cacheKey);
    }

    // Calculate average degree
    final double avgDegree = calculateAverageDegree(relationshipType, sourceVertexLabel, targetVertexLabel);
    averageDegreeCache.put(cacheKey, avgDegree);

    return avgDegree;
  }

  /**
   * Calculates average degree for a relationship type.
   */
  private double calculateAverageDegree(
      final String relationshipType,
      final String sourceVertexLabel,
      final String targetVertexLabel) {

    final Schema schema = database.getSchema();

    // Get edge type statistics
    if (!schema.existsType(relationshipType))
      return 10.0; // Fallback: no edge type found
    final DocumentType edgeType = schema.getType(relationshipType);
    if (!(edgeType instanceof EdgeType)) {
      return 10.0; // Fallback: no edge type found
    }

    final long edgeCount = database.countType(relationshipType, false);
    if (edgeCount == 0) {
      return 0.0; // No edges exist
    }

    // Get vertex counts
    long sourceVertexCount = 0;
    long targetVertexCount = 0;

    if (sourceVertexLabel != null) {
      final TypeStatistics sourceStats = getTypeStatistics(sourceVertexLabel);
      if (sourceStats != null) {
        sourceVertexCount = sourceStats.getRecordCount();
      }
    }

    if (targetVertexLabel != null) {
      final TypeStatistics targetStats = getTypeStatistics(targetVertexLabel);
      if (targetStats != null) {
        targetVertexCount = targetStats.getRecordCount();
      }
    }

    // If no vertex labels provided, estimate using all vertices
    if (sourceVertexCount == 0 && targetVertexCount == 0) {
      // Collect all vertex type counts
      long totalVertexCount = 0;
      for (final DocumentType type : schema.getTypes()) {
        if (type instanceof VertexType) {
          totalVertexCount += database.countType(type.getName(), false);
        }
      }

      if (totalVertexCount == 0) {
        return 10.0; // Fallback: no vertices
      }

      // Average degree = edges / vertices (simplified estimate)
      return (double) edgeCount / totalVertexCount;
    }

    // Calculate average degree with specific vertex types
    // Formula: (2 * edgeCount) / (sourceCount + targetCount)
    // Factor of 2 because each edge contributes to degree of both vertices
    final long totalVertexCount = sourceVertexCount + targetVertexCount;
    if (totalVertexCount == 0) {
      return 10.0; // Fallback
    }

    final double avgDegree = (2.0 * edgeCount) / totalVertexCount;

    // Clamp to reasonable range (at least 1, at most 1000)
    return Math.max(1.0, Math.min(avgDegree, 1000.0));
  }

  /**
   * Estimates the mean number of parallel edges joining a connected pair of vertices for an edge type,
   * sampled from a bounded prefix of the type instead of a full scan.
   * <p>
   * A result of 1.0 means the type behaves like a simple graph: at most one edge per (source, target)
   * pair, so a hop bound at both ends filters rather than multiplies. A result above 1.0 means the type
   * is a multigraph - connected pairs are typically joined by more than one edge - which a bound-target
   * hop's cardinality estimate must scale by rather than ignore.
   *
   * @param edgeType the edge type name
   * @return estimated mean edges per connected pair, at least 1.0; 1.0 as fallback when the type is
   *         unknown, is not an edge type, or has no edges
   */
  public double getMeanEdgesPerConnectedPair(final String edgeType) {
    if (meanEdgesPerConnectedPairCache.containsKey(edgeType))
      return meanEdgesPerConnectedPairCache.get(edgeType);

    final double mean = calculateMeanEdgesPerConnectedPair(edgeType);
    meanEdgesPerConnectedPairCache.put(edgeType, mean);
    return mean;
  }

  /**
   * Calculates mean edges per connected pair by sampling edges of the type and counting how many
   * distinct (out, in) pairs they resolve to.
   * <p>
   * The sample is the first {@value #MULTIPLICITY_SAMPLE_LIMIT} edges in storage order, not a
   * uniform or reservoir sample - edges for one pair are often created (and therefore stored)
   * together, so a prefix can over- or under-represent multiplicity depending on where the cut
   * falls relative to that clustering. Acceptable for a cost-model heuristic; not a statistically
   * unbiased estimator.
   */
  private double calculateMeanEdgesPerConnectedPair(final String edgeType) {
    final Schema schema = database.getSchema();
    if (!schema.existsType(edgeType))
      return DEFAULT_MEAN_EDGES_PER_CONNECTED_PAIR;

    final DocumentType type = schema.getType(edgeType);
    if (!(type instanceof EdgeType))
      return DEFAULT_MEAN_EDGES_PER_CONNECTED_PAIR;

    final Set<Map.Entry<RID, RID>> distinctPairs = new HashSet<>();
    long sampledEdges = 0;

    for (final Iterator<Edge> it = database.<Edge>iterateType(edgeType, false); it.hasNext(); ) {
      final Edge edge = it.next();
      distinctPairs.add(Map.entry(edge.getOut(), edge.getIn()));
      if (++sampledEdges >= MULTIPLICITY_SAMPLE_LIMIT)
        break;
    }

    if (distinctPairs.isEmpty())
      return DEFAULT_MEAN_EDGES_PER_CONNECTED_PAIR;

    // mean is always >= 1.0 here: every sampled edge contributes to exactly one pair, so sampledEdges
    // can never be smaller than distinctPairs.size(). Only the upper clamp is needed.
    final double mean = (double) sampledEdges / distinctPairs.size();
    return Math.min(MAX_MEAN_EDGES_PER_CONNECTED_PAIR, mean);
  }

  /**
   * Clears all cached statistics.
   * Useful for testing or when schema changes.
   */
  public void clear() {
    typeStatsCache.clear();
    indexStatsCache.clear();
    averageDegreeCache.clear();
    meanEdgesPerConnectedPairCache.clear();
  }

  @Override
  public String toString() {
    return "StatisticsProvider{" +
        "types=" + typeStatsCache.size() +
        ", indexes=" + indexStatsCache.values().stream().mapToInt(List::size).sum() +
        '}';
  }
}
