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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AbstractAlgoProcedure#findProvider}: the lookup that follows a wait for a restoring view must not depend on
 * what the wait itself reported.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AbstractAlgoProcedureFindProviderTest {
  private static final String DB_PATH = "./target/databases/test-algo-find-provider";

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

  /**
   * The first lookup is what dispatches a view's deferred restore and answers "not ready" for that call. The restore
   * runs on another thread, so it can finish before the wait samples it; the wait then has nothing to wait for, yet
   * the view is ready. Without a second lookup that call took the record-by-record path, the symptom of #9220.
   */
  @Test
  void aRestoreThatEndsBeforeTheWaitSamplesItIsStillUsed() {
    final FinishesAfterTheFirstAsk provider = new FinishesAfterTheFirstAsk();
    GraphTraversalProviderRegistry.register(database, provider);

    // An edge type request, so that only the registry lookup (one isReady() call per pass) is consulted
    final GraphTraversalProvider found = new Probe().find(database, new String[] { "EDGE" });

    assertThat(found).as("the view was ready by the second lookup").isSameAs(provider);
    assertThat(provider.readyCalls.get()).as("the lookup was repeated after the wait").isEqualTo(2);
  }

  /**
   * The whole-graph fallback accepts a ready view on the same terms as the exact-match lookup: it must cover every vertex
   * type, not only every edge type, or the algorithm would run on a part of the graph and answer as if it were the whole.
   */
  @Test
  void aWholeGraphLookupRefusesAViewOverSomeVertexTypesOnly() {
    GraphTraversalProviderRegistry.register(database, new FinishesAfterTheFirstAsk() {
      @Override
      public boolean coversVertexType(final String typeName) {
        return "Person".equals(typeName);
      }

      @Override
      public boolean isReady() {
        return true;
      }
    });

    assertThat(new Probe().find(database, null)).as("a view over one vertex type").isNull();
    assertThat(new Probe().find(database, new String[0])).as("a view over one vertex type, empty request").isNull();
  }

  @Test
  void aWholeGraphLookupAcceptsAReadyViewOverEveryVertexType() {
    final FinishesAfterTheFirstAsk provider = new FinishesAfterTheFirstAsk() {
      @Override
      public boolean isReady() {
        return true;
      }
    };
    GraphTraversalProviderRegistry.register(database, provider);

    assertThat(new Probe().find(database, null)).isSameAs(provider);
  }

  /**
   * A view serves the committed graph only, so the whole-graph fallback must refuse it while the calling transaction holds
   * changes, as the registry lookup in front of it does: the algorithm would otherwise miss the transaction's own writes.
   */
  @Test
  void aWholeGraphLookupRefusesAViewWhileTheTransactionHoldsChanges() {
    final FinishesAfterTheFirstAsk provider = new FinishesAfterTheFirstAsk() {
      @Override
      public boolean isReady() {
        return true;
      }
    };
    GraphTraversalProviderRegistry.register(database, provider);
    database.getSchema().createVertexType("Person");

    database.begin();
    try {
      database.newVertex("Person").save();
      assertThat(new Probe().find(database, null)).as("uncommitted vertex").isNull();
    } finally {
      database.rollback();
    }
    assertThat(new Probe().find(database, null)).as("nothing pending").isSameAs(provider);
  }

  /** Exposes the protected lookup. */
  private static final class Probe extends AbstractAlgoProcedure {
    GraphTraversalProvider find(final Database db, final String[] relTypes) {
      return findProvider(db, relTypes, null);
    }

    @Override
    public String getName() {
      return "test.probe";
    }

    @Override
    public int getMinArgs() {
      return 0;
    }

    @Override
    public int getMaxArgs() {
      return 0;
    }

    @Override
    public String getDescription() {
      return "test probe";
    }

    @Override
    public List<String> getYieldFields() {
      return List.of();
    }

    @Override
    public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
      return Stream.empty();
    }
  }

  /** A provider whose restore has already ended by the time the second question arrives: not ready once, then ready. */
  private static class FinishesAfterTheFirstAsk implements GraphTraversalProvider {
    final AtomicInteger readyCalls = new AtomicInteger();

    @Override
    public boolean isReady() {
      return readyCalls.incrementAndGet() > 1;
    }

    @Override
    public String getName() {
      return "finishes-after-the-first-ask";
    }

    @Override
    public boolean coversVertexType(final String typeName) {
      return true;
    }

    @Override
    public boolean coversEdgeType(final String edgeTypeName) {
      return true;
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
