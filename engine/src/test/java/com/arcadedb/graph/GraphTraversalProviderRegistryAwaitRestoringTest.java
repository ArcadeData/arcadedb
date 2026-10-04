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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link GraphTraversalProviderRegistry#awaitRestoring}: waits for the providers that cover a request and are still
 * restoring, under one shared deadline, and lets the caller abort the wait.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphTraversalProviderRegistryAwaitRestoringTest {
  private static final String DB_PATH = "./target/databases/test-registry-await-restoring";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen()) {
      GraphTraversalProviderRegistry.clearAll(database);
      database.close();
    }
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  @Test
  void returnsFalseWithoutWaitingWhenNothingIsRestoring() {
    final StubProvider idle = register(new StubProvider("idle", true, true));

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final boolean waited = GraphTraversalProviderRegistry.awaitRestoring(database, null, 600_000L, () -> {
    });

    stopwatch.assertGaveUpWithin(5_000L, "a call with nothing restoring versus waiting out the 10 minute budget");
    assertThat(waited).isFalse();
    assertThat(idle.restoring.get()).isFalse();
  }

  @Test
  void waitsUntilTheRestoreEndsAndReportsThatItWaited() throws Exception {
    final StubProvider restoring = register(new StubProvider("restoring", true, true));
    restoring.restoring.set(true);
    final Thread finisher = new Thread(() -> {
      try {
        Thread.sleep(150L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      restoring.restoring.set(false);
    });
    finisher.start();

    final boolean waited = GraphTraversalProviderRegistry.awaitRestoring(database, null, 600_000L, () -> {
    });
    finisher.join();

    assertThat(waited).as("a covering provider was restoring when the call started, so asking again is worthwhile")
        .isTrue();
    assertThat(restoring.restoring.get()).isFalse();
  }

  @Test
  void givesUpAtTheBudgetWhenTheRestoreNeverEnds() {
    final StubProvider stuck = register(new StubProvider("stuck", true, true));
    stuck.restoring.set(true);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final boolean waited = GraphTraversalProviderRegistry.awaitRestoring(database, null, 300L, () -> {
    });

    stopwatch.assertGaveUpWithin(5_000L, "a 300 ms budget versus waiting for a restore that never ends");
    assertThat(stopwatch.elapsedMs()).as("it did wait for its budget").isGreaterThanOrEqualTo(250L);
    assertThat(waited).isTrue();
    assertThat(stuck.restoring.get()).as("the restore itself is not cancelled by giving up").isTrue();
  }

  @Test
  void oneBudgetIsSharedByEveryCoveringProvider() {
    register(new StubProvider("a", true, true)).restoring.set(true);
    register(new StubProvider("b", true, true)).restoring.set(true);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    GraphTraversalProviderRegistry.awaitRestoring(database, null, 400L, () -> {
    });

    stopwatch.assertStayedUnder(750L, "one deadline shared by both providers, not one 400 ms wait per provider");
  }

  @Test
  void theAbortCheckStopsTheWait() {
    register(new StubProvider("stuck", true, true)).restoring.set(true);
    final AtomicInteger checks = new AtomicInteger();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> GraphTraversalProviderRegistry.awaitRestoring(database, null, 600_000L, () -> {
      if (checks.incrementAndGet() > 3)
        throw new IllegalStateException("command timed out");
    })).isInstanceOf(IllegalStateException.class).hasMessageContaining("command timed out");

    stopwatch.assertGaveUpWithin(10_000L, "an aborted wait versus the 10 minute budget it was given");
    assertThat(checks.get()).isGreaterThan(3);
  }

  @Test
  void aProviderThatDoesNotCoverTheEdgeTypeIsNotWaitedFor() {
    final StubProvider other = register(new StubProvider("other", false, true));
    other.restoring.set(true);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final boolean waited = GraphTraversalProviderRegistry.awaitRestoring(database, new String[] { "EDGE" }, 600_000L, () -> {
    });

    stopwatch.assertGaveUpWithin(5_000L, "a restoring view that cannot serve the request versus waiting for it");
    assertThat(waited).isFalse();
  }

  @Test
  void aProviderThatDoesNotCoverEveryVertexTypeIsNotWaitedFor() {
    final StubProvider subset = register(new StubProvider("subset", true, false));
    subset.restoring.set(true);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final boolean waited = GraphTraversalProviderRegistry.awaitRestoring(database, null, 600_000L, () -> {
    });

    stopwatch.assertGaveUpWithin(5_000L, "a view over a vertex subset is rejected by the algorithms, so waiting is pointless");
    assertThat(waited).isFalse();
  }

  private StubProvider register(final StubProvider provider) {
    GraphTraversalProviderRegistry.register(database, provider);
    return provider;
  }

  /** A provider whose restore state the test drives by hand. */
  private static final class StubProvider implements GraphTraversalProvider {
    final AtomicBoolean restoring = new AtomicBoolean(false);
    private final String  name;
    private final boolean coversEdges;
    private final boolean coversVertices;

    StubProvider(final String name, final boolean coversEdges, final boolean coversVertices) {
      this.name = name;
      this.coversEdges = coversEdges;
      this.coversVertices = coversVertices;
    }

    @Override
    public boolean isRestoring() {
      return restoring.get();
    }

    @Override
    public boolean isReady() {
      return !restoring.get();
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean coversVertexType(final String typeName) {
      return coversVertices;
    }

    @Override
    public boolean coversEdgeType(final String edgeTypeName) {
      return coversEdges;
    }

    @Override
    public int getNodeCount() {
      return 0;
    }

    @Override
    public int getNodeId(final RID rid) {
      return -1;
    }

    @Override
    public RID getRID(final int nodeId) {
      return null;
    }

    @Override
    public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return new int[0];
    }

    @Override
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return 0;
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
        final String... edgeTypes) {
      return false;
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return null;
    }
  }
}
