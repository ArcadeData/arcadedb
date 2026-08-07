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
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the ArcadeGraph pool: exhaustion, reuse after release, and the counter semantics.
 */
class ArcadeGraphFactoryPoolTest {

  private static final String DB_PATH = "./target/test-graphfactory-pool";

  private ArcadeGraphFactory factory;

  @BeforeEach
  void setup() {
    try (final DatabaseFactory databaseFactory = new DatabaseFactory(DB_PATH)) {
      if (!databaseFactory.exists())
        databaseFactory.create().close();
    }
  }

  @AfterEach
  void teardown() {
    if (factory != null)
      factory.close();
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    graph.drop();
  }

  @Test
  void theDefaultMaximumIs32() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    assertThat(factory.getMaxInstances()).isEqualTo(32);
  }

  @Test
  void exhaustingThePoolIsRejected() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    factory.setMaxInstances(2);
    final List<ArcadeGraph> held = new ArrayList<>();
    held.add(factory.get());
    held.add(factory.get());
    assertThatThrownBy(() -> factory.get()).isInstanceOf(IllegalArgumentException.class);
    for (final ArcadeGraph g : held)
      g.close();
  }

  @Test
  void aReleasedInstanceIsReusedRatherThanRecreated() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    final ArcadeGraph first = factory.get();
    assertThat(factory.getTotalInstancesCreated()).isEqualTo(1);
    first.close();
    final ArcadeGraph second = factory.get();
    assertThat(factory.getTotalInstancesCreated())
        .as("a released instance must come back from the pool, not be created afresh")
        .isEqualTo(1);
    assertThat(second).isSameAs(first);
    second.close();
  }

  @Test
  void releasingThenReacquiringDoesNotTripTheLimit() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    factory.setMaxInstances(1);
    for (int i = 0; i < 5; i++) {
      final ArcadeGraph g = factory.get();
      g.close();
    }
    assertThat(factory.getTotalInstancesCreated()).isEqualTo(1);
  }

  @Test
  void closingTheFactoryDisposesPooledInstances() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    final ArcadeGraph g = factory.get();
    final BasicDatabase database = g.getDatabase();
    g.close();
    assertThat(database.isOpen())
        .as("the underlying database must still be open while the instance merely sits released in the pool")
        .isTrue();
    factory.close();
    assertThat(database.isOpen())
        .as("factory.close() must dispose pooled instances, closing the underlying database")
        .isFalse();
    factory = null;
  }
}
