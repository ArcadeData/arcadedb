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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #10136 and #10268: the storage shape (LIGHTWEIGHT or record-backed) and the direction (bidirectional or not) of an edge
 * are properties of its type, so every public way to create an edge has to honour both. Each entry point is one constant of
 * {@link EntryPoint}: a new one added there is held to the same contract, and an entry point that decides either from its own
 * arguments fails here.
 */
class EdgeCreationEntryPointsContractTest extends TestHelper {

  private static final String LIGHTWEIGHT    = "Light";
  private static final String UNIDIRECTIONAL = "OneWay";

  enum EntryPoint {
    VERTEX_NEW_EDGE {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        return inTransaction(database, () -> from.asVertex().newEdge(type, to));
      }
    },
    MUTABLE_VERTEX_NEW_EDGE_BIDIRECTIONAL_FLAG {
      @Override
      @SuppressWarnings("deprecation")
      Edge create(final Database database, final RID from, final RID to, final String type) {
        return inTransaction(database, () -> from.asVertex().modify().newEdge(type, to, true, new Object[0]));
      }
    },
    GRAPH_ENGINE_NEW_EDGE {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        return inTransaction(database, () -> engine(database).newEdge((VertexInternal) from.asVertex(), type, to));
      }
    },
    GRAPH_ENGINE_NEW_EDGE_REPORTING_VETO {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        return inTransaction(database, () -> engine(database).newEdgeReportingVeto(from.asVertex(), type, to));
      }
    },
    GRAPH_ENGINE_NEW_EDGE_INITIALIZED_REPORTING_VETO {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        return inTransaction(database, () -> engine(database).newEdgeInitializedReportingVeto(from.asVertex(), type, to, null));
      }
    },
    GRAPH_ENGINE_NEW_EDGES {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        return inTransaction(database, () -> engine(database).newEdges((VertexInternal) from.asVertex(),
            List.of(new GraphEngine.CreateEdgeOperation(type, to, new Object[0])), true).getFirst());
      }
    },
    ASYNC_NEW_EDGE {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        final AtomicReference<Edge> created = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        database.async().onError((ErrorCallback) failure::set);
        try {
          // THE FOURTH ARGUMENT IS "light", NOT A DIRECTION: THIS OVERLOAD TAKES THE DIRECTION FROM THE TYPE
          database.async().newEdge(from.asVertex(), type, to, false, (edge, newSource, newDestination) -> created.set(edge));
          database.async().waitCompletion();
        } finally {
          database.async().onError((ErrorCallback) null);
        }
        // AN ASYNC FAILURE DOES NOT REACH THE CALLER: SURFACE IT HERE, OR THE TEST WOULD ONLY SEE A MISSING EDGE
        assertThat(failure.get()).isNull();
        assertThat(created.get()).isNotNull();
        return created.get();
      }
    },
    GRAPH_BATCH {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        try (final GraphBatch batch = database.batch().build()) {
          batch.newEdge(from, type, to);
        }
        return null;
      }
    },
    SQL_CREATE_EDGE {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        database.transaction(() -> {
          try (final ResultSet result = database.command("sql", "CREATE EDGE " + type + " FROM ? TO ?", from, to)) {
            assertThat(result.stream().count()).isEqualTo(1L);
          }
        });
        return null;
      }
    },
    CYPHER_CREATE {
      @Override
      Edge create(final Database database, final RID from, final RID to, final String type) {
        database.transaction(() -> {
          try (final ResultSet result = database.command("opencypher",
              "MATCH (a), (b) WHERE id(a) = $from AND id(b) = $to CREATE (a)-[r:" + type + "]->(b) RETURN r", "from",
              from.toString(), "to", to.toString())) {
            assertThat(result.stream().count()).isEqualTo(1L);
          }
        });
        return null;
      }
    };

    /** Creates one edge of {@code type} from {@code from} to {@code to}, and returns it when the entry point hands it back. */
    abstract Edge create(Database database, RID from, RID to, String type);

    private static GraphEngine engine(final Database database) {
      return ((DatabaseInternal) database).getGraphEngine();
    }

    private static Edge inTransaction(final Database database, final Supplier<Edge> creation) {
      final AtomicReference<Edge> created = new AtomicReference<>();
      database.transaction(() -> created.set(creation.get()));
      return created.get();
    }
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("V").create();
      database.getSchema().buildEdgeType().withName(LIGHTWEIGHT).withLightweight(true).create();
      database.getSchema().buildEdgeType().withName(UNIDIRECTIONAL).withBidirectional(false).create();
    });
  }

  @ParameterizedTest
  @EnumSource(EntryPoint.class)
  void aLightweightTypeGetsALightEdgeAndNoRecord(final EntryPoint entryPoint) {
    final RID[] v = newVertices();

    final Edge created = entryPoint.create(database, v[0], v[1], LIGHTWEIGHT);

    if (created != null)
      assertThat(created).isInstanceOf(LightEdge.class);
    assertThat(database.countType(LIGHTWEIGHT, true)).isZero();
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, LIGHTWEIGHT)).isEqualTo(1L);
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, LIGHTWEIGHT)).isEqualTo(1L);
    assertThat(v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, LIGHTWEIGHT).iterator().next()).isInstanceOf(LightEdge.class);
  }

  @ParameterizedTest
  @EnumSource(EntryPoint.class)
  void aUnidirectionalTypeGetsNoIncomingSide(final EntryPoint entryPoint) {
    final RID[] v = newVertices();

    final Edge created = entryPoint.create(database, v[0], v[1], UNIDIRECTIONAL);

    if (created != null)
      assertThat(created).isNotInstanceOf(LightEdge.class);
    assertThat(database.countType(UNIDIRECTIONAL, true)).isEqualTo(1L);
    assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, UNIDIRECTIONAL)).isEqualTo(1L);
    assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, UNIDIRECTIONAL)).isZero();
  }

  private RID[] newVertices() {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("V").save().getIdentity();
      rids[1] = database.newVertex("V").save().getIdentity();
    });
    return rids;
  }
}
