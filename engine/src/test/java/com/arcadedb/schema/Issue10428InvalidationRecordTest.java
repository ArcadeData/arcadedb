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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10428: the range a delete recorded on a continuous aggregate is cleared by the refresh that recomputed it, and only by
 * that one. A delete recorded after the refresh read the range must survive the clearing even when it lies inside the range and
 * widens nothing: it may have landed after the refresh read the source, so the next refresh has to recompute it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10428InvalidationRecordTest extends TestHelper {

  private ContinuousAggregateImpl newAggregate() {
    return new ContinuousAggregateImpl(database, "A", "SELECT 1", "A", "S", 3_600_000L, "hr", "ts");
  }

  @Test
  void aRangeIsMergedIntoTheHullOfTheDeletes() {
    final ContinuousAggregateImpl aggregate = newAggregate();
    assertThat(aggregate.getInvalidation()).isNull();

    aggregate.invalidate(100, 200);
    aggregate.invalidate(150, 400);
    aggregate.invalidate(50, 60);

    assertThat(aggregate.getInvalidation()).isEqualTo(new ContinuousAggregateImpl.Invalidation(50, 400));
  }

  @Test
  void theRefreshThatRecomputedARangeClearsIt() {
    final ContinuousAggregateImpl aggregate = newAggregate();
    aggregate.invalidate(100, 200);

    aggregate.clearInvalidation(aggregate.getInvalidation());

    assertThat(aggregate.getInvalidation()).isNull();
  }

  @Test
  void aDeleteInsideTheRangeRecordedAfterTheRefreshReadItSurvivesTheClearing() {
    final ContinuousAggregateImpl aggregate = newAggregate();
    aggregate.invalidate(100, 400);
    final ContinuousAggregateImpl.Invalidation readByTheRefresh = aggregate.getInvalidation();

    // a delete that lands while the refresh recomputes, inside the range: the hull does not change
    aggregate.invalidate(150, 250);
    assertThat(aggregate.getInvalidation()).isEqualTo(readByTheRefresh);

    aggregate.clearInvalidation(readByTheRefresh);

    assertThat(aggregate.getInvalidation()).as("the delete is recomputed by the next refresh").isNotNull();
  }

  @Test
  void aRefreshAskedForWhileAnotherRunsWithNothingToRecomputeIsSkippedAsItAlwaysWas() {
    final ContinuousAggregateImpl aggregate = newAggregate();
    assertThat(aggregate.tryBeginRefresh()).isTrue();
    try {
      // an append: the refresh in progress or the next append picks its rows up, so the INSERT path asks for nothing more
      ContinuousAggregateRefresher.incrementalRefresh(database, aggregate);

      assertThat(aggregate.isRefreshRequested()).isFalse();
    } finally {
      aggregate.endRefresh();
    }
  }

  @Test
  void aRefreshAskedForWhileAnotherRunsWithARangeToRecomputeIsRemembered() {
    final ContinuousAggregateImpl aggregate = newAggregate();
    aggregate.invalidate(100, 200);
    assertThat(aggregate.tryBeginRefresh()).isTrue();
    try {
      // a delete: the refresh in progress may have read the aggregate before it, so it has to run again when it ends
      ContinuousAggregateRefresher.incrementalRefresh(database, aggregate);

      assertThat(aggregate.isRefreshRequested()).isTrue();
    } finally {
      aggregate.endRefresh();
    }
  }

  @Test
  void onlyARangeThatWidensTheRecordedOneNeedsTheSchemaToBeWritten() {
    final ContinuousAggregateImpl aggregate = newAggregate();

    assertThat(aggregate.invalidate(100, 200)).as("nothing recorded before").isTrue();
    assertThat(aggregate.invalidate(120, 180)).as("inside").isFalse();
    assertThat(aggregate.invalidate(100, 200)).as("the same").isFalse();
    assertThat(aggregate.invalidate(100, 250)).as("later end").isTrue();
    assertThat(aggregate.invalidate(50, 250)).as("earlier start").isTrue();
    assertThat(aggregate.getInvalidation()).isEqualTo(new ContinuousAggregateImpl.Invalidation(50, 250));

    aggregate.clearInvalidation(aggregate.getInvalidation());
    assertThat(aggregate.invalidate(120, 180)).as("nothing recorded any more").isTrue();
  }

  @Test
  void everyRecordedRangeIsANewInstanceEvenWhenItEqualsTheOneThere() {
    final ContinuousAggregateImpl aggregate = newAggregate();
    aggregate.invalidate(100, 200);
    final ContinuousAggregateImpl.Invalidation first = aggregate.getInvalidation();

    aggregate.invalidate(100, 200);
    final ContinuousAggregateImpl.Invalidation second = aggregate.getInvalidation();

    // the clearing of the range a refresh read compares references: it must not take an equal record for the one it read
    assertThat(second).isEqualTo(first).isNotSameAs(first);
  }

  @Test
  void aRangeWhoseClearingCouldNotBePersistedComesBackWithWhatWasRecordedSince() {
    final ContinuousAggregateImpl aggregate = newAggregate();
    aggregate.invalidate(100, 200);
    final ContinuousAggregateImpl.Invalidation readByTheRefresh = aggregate.getInvalidation();
    aggregate.clearInvalidation(readByTheRefresh);
    aggregate.invalidate(500, 600);

    aggregate.restoreInvalidation(readByTheRefresh);

    assertThat(aggregate.getInvalidation()).isEqualTo(new ContinuousAggregateImpl.Invalidation(100, 600));
  }

  @Test
  void theRangeSurvivesTheDefinitionBeingWrittenAndRead() {
    final ContinuousAggregateImpl aggregate = newAggregate();
    aggregate.invalidate(Long.MIN_VALUE, 400);

    final JSONObject json = aggregate.toJSON();
    assertThat(json.getLong("invalidatedFromTs")).isEqualTo(Long.MIN_VALUE);
    assertThat(json.getLong("invalidatedToTs")).isEqualTo(400L);

    assertThat(ContinuousAggregateImpl.fromJSON(database, json).getInvalidation())
        .isEqualTo(new ContinuousAggregateImpl.Invalidation(Long.MIN_VALUE, 400));

    aggregate.clearInvalidation(aggregate.getInvalidation());
    final JSONObject cleared = aggregate.toJSON();
    assertThat(cleared.has("invalidatedFromTs")).isFalse();
    assertThat(ContinuousAggregateImpl.fromJSON(database, cleared).getInvalidation()).isNull();
  }
}
