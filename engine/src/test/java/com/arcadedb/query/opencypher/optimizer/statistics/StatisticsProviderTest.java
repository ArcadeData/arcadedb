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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for StatisticsProvider class.
 */
class StatisticsProviderTest {
  private static final String DB_PATH = "./target/teststatistics";
  private Database database;
  private StatisticsProvider statisticsProvider;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    statisticsProvider = new StatisticsProvider((DatabaseInternal) database);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void collectStatisticsForVertexType() {
    // Create type and insert records
    database.getSchema().getOrCreateVertexType("Person");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newVertex("Person").set("id", i).save();
      }
    });

    // Collect statistics
    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    // Verify statistics
    final TypeStatistics stats = statisticsProvider.getTypeStatistics("Person");
    assertThat(stats).isNotNull();
    assertThat(stats.getTypeName()).isEqualTo("Person");
    assertThat(stats.getRecordCount()).isEqualTo(100);
    assertThat(stats.isVertexType()).isTrue();
  }

  @Test
  void collectStatisticsForEdgeType() {
    // Create types
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");

    database.transaction(() -> {
      final var v1 = database.newVertex("Person").save();
      final var v2 = database.newVertex("Person").save();
      v1.newEdge("KNOWS", v2, true, (Object[]) null);
    });

    // Collect statistics
    statisticsProvider.collectStatistics(Arrays.asList("KNOWS"));

    // Verify statistics
    final TypeStatistics stats = statisticsProvider.getTypeStatistics("KNOWS");
    assertThat(stats).isNotNull();
    assertThat(stats.getTypeName()).isEqualTo("KNOWS");
    assertThat(stats.getRecordCount()).isEqualTo(1);
    assertThat(stats.isVertexType()).isFalse();
  }

  @Test
  void collectIndexStatistics() {
    // Create type with index
    final var personType = database.getSchema().getOrCreateVertexType("Person");
    personType.createProperty("id", Integer.class);
    personType.createProperty("name", String.class);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    // Collect statistics
    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    // Verify index statistics
    final List<IndexStatistics> indexes = statisticsProvider.getIndexesForType("Person");
    assertThat(indexes).hasSize(2);

    // Find id index
    final IndexStatistics idIndex = indexes.stream()
        .filter(idx -> idx.getPropertyNames().contains("id"))
        .findFirst()
        .orElse(null);
    assertThat(idIndex).isNotNull();
    assertThat(idIndex.isUnique()).isTrue();
    assertThat(idIndex.getPropertyNames()).containsExactly("id");

    // Find name index
    final IndexStatistics nameIndex = indexes.stream()
        .filter(idx -> idx.getPropertyNames().contains("name"))
        .findFirst()
        .orElse(null);
    assertThat(nameIndex).isNotNull();
    assertThat(nameIndex.isUnique()).isFalse();
    assertThat(nameIndex.getPropertyNames()).containsExactly("name");
  }

  @Test
  void findIndexForProperty() {
    // Create type with indexes
    final var personType = database.getSchema().getOrCreateVertexType("Person");
    personType.createProperty("id", Integer.class);
    personType.createProperty("name", String.class);
    personType.createProperty("age", Integer.class);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "age");

    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    // Find index for "id" property
    final IndexStatistics idIndex = statisticsProvider.findIndexForProperty("Person", "id");
    assertThat(idIndex).isNotNull();
    assertThat(idIndex.isUnique()).isTrue();
    assertThat(idIndex.canBeUsedForProperty("id")).isTrue();

    // Find index for "name" property (composite index)
    final IndexStatistics nameIndex = statisticsProvider.findIndexForProperty("Person", "name");
    assertThat(nameIndex).isNotNull();
    assertThat(nameIndex.canBeUsedForProperty("name")).isTrue();

    // Cannot find index for "age" (second property in composite index)
    final IndexStatistics ageIndex = statisticsProvider.findIndexForProperty("Person", "age");
    assertThat(ageIndex).isNull();
  }

  @Test
  void hasIndexForProperty() {
    // Create type with index
    final var personType = database.getSchema().getOrCreateVertexType("Person");
    personType.createProperty("email", String.class);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "email");

    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    assertThat(statisticsProvider.hasIndexForProperty("Person", "email")).isTrue();
    assertThat(statisticsProvider.hasIndexForProperty("Person", "nonexistent")).isFalse();
  }

  @Test
  void getCardinality() {
    database.getSchema().getOrCreateVertexType("Person");
    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        database.newVertex("Person").save();
      }
    });

    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    assertThat(statisticsProvider.getCardinality("Person")).isEqualTo(50);
    assertThat(statisticsProvider.getCardinality("Nonexistent")).isEqualTo(0);
  }

  @Test
  void clear() {
    database.getSchema().getOrCreateVertexType("Person");
    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    assertThat(statisticsProvider.getTypeStatistics("Person")).isNotNull();

    statisticsProvider.clear();

    assertThat(statisticsProvider.getTypeStatistics("Person")).isNull();
  }

  @Test
  void getMeanEdgesPerConnectedPairOnSimpleGraph() {
    // Every pair below is joined by exactly one edge - a simple graph for this type
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var v1 = database.newVertex("Person").save();
        final var v2 = database.newVertex("Person").save();
        v1.newEdge("KNOWS", v2, true, (Object[]) null);
      }
    });

    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(1.0);
  }

  @Test
  void getMeanEdgesPerConnectedPairOnMultigraph() {
    // One pair joined by 5 parallel edges, another joined by 1: mean = 6 edges / 2 pairs = 3.0
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      final var a = database.newVertex("Person").save();
      final var b = database.newVertex("Person").save();
      for (int i = 0; i < 5; i++)
        a.newEdge("KNOWS", b, true, (Object[]) null);

      final var c = database.newVertex("Person").save();
      final var d = database.newVertex("Person").save();
      c.newEdge("KNOWS", d, true, (Object[]) null);
    });

    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(3.0);
  }

  @Test
  void getMeanEdgesPerConnectedPairFallsBackToOneWhenUnknown() {
    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("Nonexistent")).isEqualTo(1.0);

    database.getSchema().getOrCreateEdgeType("KNOWS"); // no edges inserted
    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(1.0);

    database.getSchema().getOrCreateVertexType("Person"); // not an edge type
    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("Person")).isEqualTo(1.0);
  }

  @Test
  void getMeanEdgesPerConnectedPairClampsAPathologicallyClusteredSample() {
    // Prefix sampling reads storage order, so if every sampled edge belongs to a single pair the raw
    // ratio would be sampledEdges / 1, unboundedly large. The estimate must not inflate the planner's
    // cardinality by orders of magnitude on that pathological case - it is capped at 1000.
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      final var a = database.newVertex("Person").save();
      final var b = database.newVertex("Person").save();
      for (int i = 0; i < 1200; i++)
        a.newEdge("KNOWS", b, true, (Object[]) null);
    });

    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(1000.0);
  }

  @Test
  void meanEdgesPerConnectedPairIsSharedAcrossStatisticsProviderInstancesOnTheSameDatabase() {
    // Simulates two distinct queries planning over the same edge type: each CypherOptimizer creates
    // its own StatisticsProvider, so without a database-scoped cache each would re-sample from scratch.
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      final var a = database.newVertex("Person").save();
      final var b = database.newVertex("Person").save();
      for (int i = 0; i < 5; i++)
        a.newEdge("KNOWS", b, true, (Object[]) null);
    });

    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(5.0);

    // A second provider (a second query's planning) must reuse the cached value from the shared,
    // database-scoped cache rather than resampling - observable via the cache having an entry already.
    final StatisticsProvider secondProvider = new StatisticsProvider((DatabaseInternal) database);
    assertThat(((DatabaseInternal) database).getGraphStatisticsCache().size()).isGreaterThan(0);
    assertThat(secondProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(5.0);
  }

  @Test
  void meanEdgesPerConnectedPairResamplesAfterEdgesAreAddedToTheType() {
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      final var a = database.newVertex("Person").save();
      final var b = database.newVertex("Person").save();
      for (int i = 0; i < 5; i++)
        a.newEdge("KNOWS", b, true, (Object[]) null);
    });

    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(5.0);

    // More edges land on a new pair after the first query planned - the shared cache entry is now stale
    // (edge count changed) and a fresh StatisticsProvider must resample rather than serve 5.0 forever.
    database.transaction(() -> {
      final var c = database.newVertex("Person").save();
      final var d = database.newVertex("Person").save();
      c.newEdge("KNOWS", d, true, (Object[]) null);
    });

    final StatisticsProvider secondProvider = new StatisticsProvider((DatabaseInternal) database);
    // 6 edges / 2 pairs = 3.0, not the stale 5.0.
    assertThat(secondProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(3.0);
  }

  @Test
  void averageDegreeIsSharedAcrossStatisticsProviderInstancesOnTheSameDatabase() {
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var a = database.newVertex("Person").save();
        final var b = database.newVertex("Person").save();
        a.newEdge("KNOWS", b, true, (Object[]) null);
      }
    });
    statisticsProvider.collectStatistics(Arrays.asList("Person", "KNOWS"));

    final double degree = statisticsProvider.getAverageDegree("KNOWS", "Person", "Person");

    final StatisticsProvider secondProvider = new StatisticsProvider((DatabaseInternal) database);
    secondProvider.collectStatistics(Arrays.asList("Person", "KNOWS"));
    assertThat(((DatabaseInternal) database).getGraphStatisticsCache().size()).isGreaterThan(0);
    assertThat(secondProvider.getAverageDegree("KNOWS", "Person", "Person")).isEqualTo(degree);
  }

  @Test
  void averageDegreeResamplesAfterAVertexOnlyMutationEvenThoughTheEdgeCountIsUnchanged() {
    // avgDegree = 2*edgeCount / (sourceCount+targetCount): a vertex-only mutation (e.g. bulk-loading more
    // vertices without touching the edge type) changes the true answer without changing the edge count,
    // so the cache stamp must include vertex counts, not just the edge count.
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      final var vertices = new MutableVertex[5];
      for (int i = 0; i < 5; i++)
        vertices[i] = database.newVertex("Person").save();
      for (int i = 0; i < 20; i++)
        vertices[i % 5].newEdge("KNOWS", vertices[(i + 1) % 5], true, (Object[]) null);
    });

    // 2*20 edges / (5+5) vertices = 4.0.
    assertThat(statisticsProvider.getAverageDegree("KNOWS", "Person", "Person")).isEqualTo(4.0);

    // 5 more Person vertices, no new KNOWS edges: edge count is unchanged, vertex count is not.
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newVertex("Person").save();
    });

    final StatisticsProvider afterGrowth = new StatisticsProvider((DatabaseInternal) database);
    afterGrowth.collectStatistics(Arrays.asList("Person", "KNOWS"));
    // 2*20 edges / (10+10) vertices = 2.0, not the stale 4.0 an edge-count-only stamp would still serve.
    assertThat(afterGrowth.getAverageDegree("KNOWS", "Person", "Person")).isEqualTo(2.0);
  }

  @Test
  void meanEdgesPerConnectedPairPicksUpANewlyBuiltGAVEvenThoughTheEdgeCountDidNotChange() {
    // Same divergence trick as CypherExpandIntoMultiplicityCardinalityTest: the first 2000 edges in
    // creation order (what a sample sees) yield a different mean than the type's true population.
    // pair1+pair2 fill the 2000-edge sample window exactly (sampled mean = 2000/2 = 1000); pair3's
    // single edge sits just outside it (exact mean = 2001/3 = 667). Building a GAV afterward does not
    // change the edge count, so only the build-triggered cache invalidation - not the count stamp -
    // can make a later query see the exact CSR answer instead of the stale sampled one.
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").save();
      final MutableVertex b = database.newVertex("Person").save();
      for (int i = 0; i < 1000; i++)
        a.newEdge("KNOWS", b, true, (Object[]) null);

      final MutableVertex c = database.newVertex("Person").save();
      final MutableVertex d = database.newVertex("Person").save();
      for (int i = 0; i < 1000; i++)
        c.newEdge("KNOWS", d, true, (Object[]) null);

      final MutableVertex e = database.newVertex("Person").save();
      final MutableVertex f = database.newVertex("Person").save();
      e.newEdge("KNOWS", f, true, (Object[]) null);
    });

    // No GAV yet - caches the sampled value (1000) under the current edge count (2001).
    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(1000.0);

    // Must go through the builder: only it calls registerAsTraversalProvider(), which is what makes
    // this view discoverable via GraphTraversalProviderRegistry.findProvider() in the first place.
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS")
        .build();
    try {
      final StatisticsProvider afterGav = new StatisticsProvider((DatabaseInternal) database);
      // Exact CSR answer (667), not the stale sampled one (1000) the count-stamp alone would still serve.
      assertThat(afterGav.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(667.0);
    } finally {
      gav.drop();
    }
  }

  @Test
  void getMeanEdgesPerConnectedPairAndAverageDegreeDoNotNpeWhenTheSharedCacheIsUnavailable() throws Exception {
    // Simulates a DatabaseInternal implementation whose getGraphStatisticsCache() returns null (e.g. a
    // test double that only implements what its own tests need) - the shared-cache lookup must be
    // guarded, not assumed present, the same way the constructor already guards a null database.
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").save();
      final MutableVertex b = database.newVertex("Person").save();
      a.newEdge("KNOWS", b, true, (Object[]) null);
    });

    final Field cacheField = StatisticsProvider.class.getDeclaredField("graphStatisticsCache");
    cacheField.setAccessible(true);
    cacheField.set(statisticsProvider, null);

    assertThat(statisticsProvider.getMeanEdgesPerConnectedPair("KNOWS")).isEqualTo(1.0);
    assertThat(statisticsProvider.getAverageDegree("KNOWS", "Person", "Person")).isGreaterThan(0.0);
  }

  @Test
  void preferUniqueIndexOverNonUnique() {
    // Create type with unique and non-unique indexes on different properties
    final var personType = database.getSchema().getOrCreateVertexType("Person");
    personType.createProperty("id", Integer.class);
    personType.createProperty("email", String.class);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "email"); // Non-unique
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");     // Unique

    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    // Should return unique index for id
    final IndexStatistics idIndex = statisticsProvider.findIndexForProperty("Person", "id");
    assertThat(idIndex).isNotNull();
    assertThat(idIndex.isUnique()).isTrue();

    // Should return non-unique index for email
    final IndexStatistics emailIndex = statisticsProvider.findIndexForProperty("Person", "email");
    assertThat(emailIndex).isNotNull();
    assertThat(emailIndex.isUnique()).isFalse();
  }
}
