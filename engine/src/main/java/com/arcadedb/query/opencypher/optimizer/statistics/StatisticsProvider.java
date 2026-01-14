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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects and provides runtime statistics for query optimization.
 * Statistics are collected on-demand and stored in-memory for the current query.
 *
 * Uses ArcadeDB's cached Bucket.count() (O(1)) for cardinality estimation.
 */
public class StatisticsProvider {
  private final DatabaseInternal database;
  private final Map<String, TypeStatistics> typeStatsCache;
  private final Map<String, List<IndexStatistics>> indexStatsCache;
  private final Map<String, Double> averageDegreeCache;

  public StatisticsProvider(final DatabaseInternal database) {
    this.database = database;
    this.typeStatsCache = new HashMap<>();
    this.indexStatsCache = new HashMap<>();
    this.averageDegreeCache = new HashMap<>();
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

      final DocumentType type = schema.getType(typeName);
      if (type == null) {
        continue; // Type doesn't exist
      }

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
    final DocumentType edgeType = schema.getType(relationshipType);
    if (edgeType == null || !(edgeType instanceof EdgeType)) {
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
   * Clears all cached statistics.
   * Useful for testing or when schema changes.
   */
  public void clear() {
    typeStatsCache.clear();
    indexStatsCache.clear();
    averageDegreeCache.clear();
  }

  @Override
  public String toString() {
    return "StatisticsProvider{" +
        "types=" + typeStatsCache.size() +
        ", indexes=" + indexStatsCache.values().stream().mapToInt(List::size).sum() +
        '}';
  }
}
