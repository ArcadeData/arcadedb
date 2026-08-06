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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link GraphStatisticsCache}, the database-scoped cache backing
 * {@link StatisticsProvider#getMeanEdgesPerConnectedPair} and {@link StatisticsProvider#getAverageDegree}
 * across queries (issue #5834).
 */
class GraphStatisticsCacheTest {

  @Test
  void meanEdgesPerConnectedPairMissesWhenNeverPut() {
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    assertThat(cache.getMeanEdgesPerConnectedPair("KNOWS", 10)).isNull();
  }

  @Test
  void meanEdgesPerConnectedPairHitsWhenGenerationMatches() {
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    cache.putMeanEdgesPerConnectedPair("KNOWS", 3.0, 10L);

    assertThat(cache.getMeanEdgesPerConnectedPair("KNOWS", 10L)).isEqualTo(3.0);
  }

  @Test
  void meanEdgesPerConnectedPairMissesWhenEdgeCountChanged() {
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    cache.putMeanEdgesPerConnectedPair("KNOWS", 3.0, 10L);

    // Edge count moved from 10 to 15 since the entry was cached (inserts happened) - stale, must miss.
    assertThat(cache.getMeanEdgesPerConnectedPair("KNOWS", 15L)).isNull();

    // Same for a shrink (deletes happened).
    assertThat(cache.getMeanEdgesPerConnectedPair("KNOWS", 5L)).isNull();
  }

  @Test
  void meanEdgesPerConnectedPairIsKeyedPerEdgeType() {
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    cache.putMeanEdgesPerConnectedPair("KNOWS", 3.0, 10L);
    cache.putMeanEdgesPerConnectedPair("FOLLOWS", 1.0, 10L);

    assertThat(cache.getMeanEdgesPerConnectedPair("KNOWS", 10L)).isEqualTo(3.0);
    assertThat(cache.getMeanEdgesPerConnectedPair("FOLLOWS", 10L)).isEqualTo(1.0);
  }

  @Test
  void averageDegreeMissesWhenNeverPut() {
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    assertThat(cache.getAverageDegree("KNOWS:Person:Person", 10L, 5L, 5L)).isNull();
  }

  @Test
  void averageDegreeHitsWhenGenerationMatchesAndMissesOnEdgeCountChange() {
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    cache.putAverageDegree("KNOWS:Person:Person", 2.5, 10L, 5L, 5L);

    assertThat(cache.getAverageDegree("KNOWS:Person:Person", 10L, 5L, 5L)).isEqualTo(2.5);
    assertThat(cache.getAverageDegree("KNOWS:Person:Person", 11L, 5L, 5L)).isNull();
  }

  @Test
  void averageDegreeMissesWhenOnlyASourceOrTargetVertexCountChanges() {
    // avgDegree = 2*edgeCount / (sourceCount + targetCount): the edge count alone does not determine it,
    // so a vertex-only mutation (e.g. bulk-loading vertices before wiring edges) must also invalidate.
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    cache.putAverageDegree("KNOWS:Person:Person", 2.5, 10L, 5L, 5L);

    assertThat(cache.getAverageDegree("KNOWS:Person:Person", 10L, 6L, 5L)).isNull();
    assertThat(cache.getAverageDegree("KNOWS:Person:Person", 10L, 5L, 6L)).isNull();
  }

  @Test
  void putOverwritesAPreviousEntryForTheSameKey() {
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    cache.putMeanEdgesPerConnectedPair("KNOWS", 3.0, 10L);
    cache.putMeanEdgesPerConnectedPair("KNOWS", 4.0, 20L);

    assertThat(cache.getMeanEdgesPerConnectedPair("KNOWS", 10L)).isNull();
    assertThat(cache.getMeanEdgesPerConnectedPair("KNOWS", 20L)).isEqualTo(4.0);
  }

  @Test
  void clearRemovesAllEntries() {
    final GraphStatisticsCache cache = new GraphStatisticsCache();
    cache.putMeanEdgesPerConnectedPair("KNOWS", 3.0, 10L);
    cache.putAverageDegree("KNOWS:Person:Person", 2.5, 10L, 5L, 5L);
    assertThat(cache.size()).isEqualTo(2);

    cache.clear();

    assertThat(cache.size()).isEqualTo(0);
    assertThat(cache.getMeanEdgesPerConnectedPair("KNOWS", 10L)).isNull();
    assertThat(cache.getAverageDegree("KNOWS:Person:Person", 10L, 5L, 5L)).isNull();
  }
}
