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
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.exception.SchemaException;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7119: {@code LocalDocumentType.bucketSelectionStrategy} is the fifth copy-on-write member of the class, next
 * to the four bucket lists issues #6678/#7033 made {@code volatile}. It is reassigned by
 * {@code setBucketSelectionStrategy} and read lock-free by {@code getBucketIdByRecord}/{@code getBucketIndexByKeys}
 * on the record-write path and by the planner's partition pruning, so it needs the same happens-before edge. It was
 * also published before it was bound: the field was assigned first and {@code setType(this)} called afterwards, so a
 * concurrent insert could reach a strategy whose bucket count was still 0 or whose type was still null.
 * <p>
 * The first test pins the modifier, like the sibling {@link Issue6678PolymorphicBucketCacheVisibilityTest} does for
 * the four lists. The second uses a strategy that records what the type was publishing at the moment it was bound:
 * with the fix it is still the previous strategy, never the half-bound one. The last two check the rollback contract
 * the old try/catch provided survives the reordering, once per exit of the block it wrapped: a strategy that throws
 * while binding, and a real {@link PartitionedBucketSelectionStrategy} the suitability check refuses after binding
 * succeeded, are both never published.
 */
class Issue7119BucketSelectionStrategyPublicationTest extends TestHelper {

  @Test
  void bucketSelectionStrategyMustBeVolatileForLockFreeReaders() throws Exception {
    final Field field = LocalDocumentType.class.getDeclaredField("bucketSelectionStrategy");
    assertThat(Modifier.isVolatile(field.getModifiers()))
        .as("bucketSelectionStrategy is copy-on-write reassigned by setBucketSelectionStrategy and read lock-free by "
            + "the record-write path and the planner - it must be volatile like its four sibling bucket lists (issue #7119)")
        .isTrue();
  }

  @Test
  void strategyIsBoundBeforeItIsPublished() {
    database.transaction(() -> database.getSchema().createDocumentType("Product", 2));

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Product");
    final BucketSelectionStrategy previous = type.getBucketSelectionStrategy();
    final RecordingStrategy recording = new RecordingStrategy();

    database.transaction(() -> type.setBucketSelectionStrategy(recording));

    assertThat(recording.publishedWhenBound)
        .as("setType(this) must run before the field is assigned, so the type still publishes the previous strategy")
        .isSameAs(previous);
    assertThat(recording.bucketsWhenBound).isEqualTo(2);
    assertThat(type.getBucketSelectionStrategy()).isSameAs(recording);

    // LEAVE THE TYPE ON A BUILT-IN STRATEGY SO THE PERSISTED SCHEMA DOES NOT NAME A TEST CLASS
    database.transaction(() -> type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy()));
  }

  @Test
  void strategyRefusedWhileBindingIsNeverPublished() {
    database.transaction(() -> database.getSchema().createDocumentType("Product", 2));

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Product");
    final BucketSelectionStrategy previous = type.getBucketSelectionStrategy();

    assertThatThrownBy(() -> database.transaction(() -> type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy() {
      @Override
      public void setType(final LocalDocumentType type) {
        throw new IllegalStateException("refused");
      }
    }))).isInstanceOf(IllegalStateException.class).hasMessage("refused");

    assertThat(type.getBucketSelectionStrategy()).isSameAs(previous);
  }

  @Test
  void strategyRefusedBySuitabilityCheckIsNeverPublished() {
    database.transaction(() -> database.getSchema().createDocumentType("Product", 2));

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Product");
    final BucketSelectionStrategy previous = type.getBucketSelectionStrategy();

    // NO UNIQUE AUTOMATIC INDEX ON `id`, SO checkSuitability() REPORTS A BLOCKER AND THE ASSIGNMENT IS REFUSED AFTER
    // setType(this) HAS ALREADY BOUND THE STRATEGY - THE OTHER EXIT OF THE BLOCK THAT USED TO NEED THE ROLLBACK
    assertThatThrownBy(() -> database.transaction(
        () -> type.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(List.of("id")))))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("cannot find a unique automatic index");

    assertThat(type.getBucketSelectionStrategy()).isSameAs(previous);
  }

  @Test
  void strategyBucketCountMustBeVolatileForTheInPlaceRebind() throws Exception {
    final Field field = ThreadBucketSelectionStrategy.class.getDeclaredField("total");
    assertThat(Modifier.isVolatile(field.getModifiers()))
        .as("addBucketInternal()/removeBucket() rebind the strategy that is ALREADY published, by calling setType() on "
            + "it and making no volatile write afterwards, so the bucket count it caches must carry its own "
            + "happens-before edge to the lock-free readers of getBucketIdByRecord() (issue #7119)")
        .isTrue();
  }

  @Test
  void inPlaceRebindTracksABucketCountThatGrows() {
    database.transaction(() -> database.getSchema().createDocumentType("Product", 2));

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Product");
    final ThreadBucketSelectionStrategy strategy = new ThreadBucketSelectionStrategy();
    database.transaction(() -> type.setBucketSelectionStrategy(strategy));
    assertThat(readTotal(strategy)).isEqualTo(2);

    database.transaction(
        () -> type.addBucket(database.getSchema().createBucket("Product_extra")));

    assertThat(readTotal(strategy))
        .as("the strategy published on the type is rebound in place when the bucket list grows")
        .isEqualTo(3);
    // AND THE PLACEMENT IT HANDS OUT IS STILL AN INDEX INTO THE CURRENT LIST
    assertThat(strategy.getBucketIdByRecord(null, false)).isBetween(0, 2);

    // LEAVE THE TYPE ON A BUILT-IN STRATEGY SO THE PERSISTED SCHEMA DOES NOT NAME A TEST-LOCAL SETUP
    database.transaction(() -> type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy()));
  }

  private static int readTotal(final ThreadBucketSelectionStrategy strategy) {
    try {
      final Field total = ThreadBucketSelectionStrategy.class.getDeclaredField("total");
      total.setAccessible(true);
      return total.getInt(strategy);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Public with a public no-arg constructor so that, should the schema be reloaded while it is assigned, the strategy
   * can be re-instantiated by name the way {@code setBucketSelectionStrategy(String, Object...)} does.
   */
  public static class RecordingStrategy extends RoundRobinBucketSelectionStrategy {
    BucketSelectionStrategy publishedWhenBound;
    int                     bucketsWhenBound;

    @Override
    public void setType(final LocalDocumentType type) {
      publishedWhenBound = type.getBucketSelectionStrategy();
      bucketsWhenBound = type.getBuckets(false).size();
      super.setType(type);
    }

    @Override
    public String getName() {
      return getClass().getName();
    }
  }
}
