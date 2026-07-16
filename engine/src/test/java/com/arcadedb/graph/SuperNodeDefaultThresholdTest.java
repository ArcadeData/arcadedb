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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Super-node promotion at the SHIPPED DEFAULT threshold (GRAPH_SUPERNODE_THRESHOLD=4096).
 * <p>
 * The other super-node tests all lower the threshold to 64/128, which promotes through the geometric
 * chunk-size estimate in {@link EdgeLinkedList#tryPromoteToSuperNode}. At the default the estimate saturates
 * at ~2048 edges (the 8192-byte chunk cap over ~8 bytes per entry), so promotion can only ever happen through
 * the BOUNDED CHAIN-WALK branch - the branch every production deployment uses and no test covered.
 * <p>
 * Both tests below assert edge-list reads over a vertex promoted that way, using the aggregation reported in
 * issue #565 ({@code both().size()}).
 */
@Tag("slow")
class SuperNodeDefaultThresholdTest extends TestHelper {
  private int savedThreshold;
  private int savedStripes;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
  }

  @Test
  void bothSizeOnPromotedHubAtDefaultThreshold() {
    // Promotion via the bounded-walk path: the geometric estimate saturates below 4096.
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(4096);

    database.transaction(() -> {
      database.getSchema().createVertexType("Account", 4).createProperty("number", Type.INTEGER);
      database.getSchema().createEdgeType("TRANSFERS", 8);
    });

    final RID[] hubHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account");
      hub.set("number", 0);
      hub.save();
      hubHolder[0] = hub.getIdentity();
    });
    final RID hubRID = hubHolder[0];

    // Enough edges to cross the 4096 threshold through the chunk-size cap + walk path.
    final int total = 12_000;
    for (int batch = 0; batch < total / 200; batch++) {
      final int base = batch * 200;
      database.transaction(() -> {
        for (int i = 0; i < 200; i++) {
          final MutableVertex src = database.newVertex("Account");
          src.set("number", base + i + 1);
          src.save();
          src.newEdge("TRANSFERS", hubRID);
        }
      });
    }

    // PRECONDITION: the hub must actually be promoted, otherwise this test proves nothing.
    database.transaction(() -> {
      final RID inHead = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
      assertThat(database.lookupByRID(inHead, true))
          .as("hub must be promoted to the striped layout at the default threshold")
          .isInstanceOf(StripeDirectory.class);
    });

    // THE CLIENT'S QUERY, verbatim.
    database.transaction(() -> {
      try (final ResultSet rs = database.query("SQL",
          "SELECT number, both().size() FROM Account ORDER BY both().size() DESC LIMIT 5")) {
        assertThat(rs.hasNext()).isTrue();
        final Result top = rs.next();
        assertThat(top.<Integer>getProperty("number")).isEqualTo(0);
        assertThat(top.<Number>getProperty("both().size()").intValue()).isEqualTo(total);
      }
    });
  }

  /**
   * A database promoted before a restart rewrites its directory (stripe chunk-full -> updateSlot) from a
   * buffer LOADED from disk rather than built in-process. The existing reopen test adds only 10 edges, too few
   * to fill any stripe chunk, so it never re-serialises a disk-loaded directory. This does.
   */
  @Test
  void promotedHubRewrittenAfterReopenStaysReadable() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(4096);

    database.transaction(() -> {
      database.getSchema().createVertexType("Account", 4).createProperty("number", Type.INTEGER);
      database.getSchema().createEdgeType("TRANSFERS", 8);
    });

    final RID[] hubHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account");
      hub.set("number", 0);
      hub.save();
      hubHolder[0] = hub.getIdentity();
    });

    addEdges(hubHolder[0], 0, 12_000);
    reopenDatabase();

    final RID hub = new RID(hubHolder[0].getBucketId(), hubHolder[0].getPosition());
    database.transaction(() -> assertThat(database.lookupByRID(((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk(), true))
        .isInstanceOf(StripeDirectory.class));

    // Heavy appends AFTER the reopen: every stripe fills repeatedly, so the directory is re-serialised from a
    // disk-loaded buffer many times.
    addEdges(hub, 12_000, 8_000);
    reopenDatabase();

    final RID hub2 = new RID(hub.getBucketId(), hub.getPosition());
    database.transaction(() -> {
      try (final ResultSet rs = database.query("SQL",
          "SELECT number, both().size() FROM Account ORDER BY both().size() DESC LIMIT 5")) {
        final Result top = rs.next();
        assertThat(top.<Integer>getProperty("number")).isEqualTo(0);
        assertThat(top.<Number>getProperty("both().size()").intValue()).isEqualTo(20_000);
      }
      assertThat(hub2.asVertex().countEdges(Vertex.DIRECTION.IN, "TRANSFERS")).isEqualTo(20_000);
    });
  }

  private void addEdges(final RID hubRID, final int startNumber, final int count) {
    for (int batch = 0; batch < count / 200; batch++) {
      final int base = startNumber + batch * 200;
      database.transaction(() -> {
        for (int i = 0; i < 200; i++) {
          final MutableVertex src = database.newVertex("Account");
          src.set("number", base + i + 1);
          src.save();
          src.newEdge("TRANSFERS", hubRID);
        }
      });
    }
  }
}
