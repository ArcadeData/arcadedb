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
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * From a customer report: a read-only query
 * <pre>SELECT number, both().size() FROM Account ORDER BY both().size() DESC LIMIT 5</pre>
 * on a database holding a hot Account vertex promoted to the striped super-node layout returned
 * {@code java.nio.BufferUnderflowException}. This test exercises the EXACT reported query against a HEALTHY
 * promoted super-node to establish whether the read path itself is defective. A green run means the healthy
 * super-node traversal + {@code both().size()} aggregation is correct, pointing the reported exception at
 * pre-existing on-disk corruption rather than a new read bug.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SuperNodeBothSizeQueryTest extends TestHelper {
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

  /**
   * Reproduces the reported query on a healthy promoted super-node. The hot Account collects {@code hotDegree}
   * inbound edges - well over the promotion threshold - so its IN edge list is a {@link StripeDirectory} with
   * generation-0 (pre-promotion) chain plus striped chains. {@code both().size()} must aggregate every edge
   * across all of them without throwing.
   */
  @Test
  void bothSizeOnPromotedSuperNodeIsCorrect() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);

    final int hotDegree = 20_000;
    final int coldAccounts = 4;

    database.transaction(() -> {
      database.getSchema().createVertexType("Account", 4).createProperty("number", Type.INTEGER);
      database.getSchema().createEdgeType("TX", 8);
    });

    // THE HOT ACCOUNT (number=0): every other Account points an edge at it, so its IN list promotes.
    final RID[] hubHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account");
      hub.set("number", 0);
      hub.save();
      hubHolder[0] = hub.getIdentity();
    });

    // A few COLD accounts (low degree, classic layout) so ORDER BY ... DESC LIMIT 5 has more than one row.
    for (int c = 1; c <= coldAccounts; c++) {
      final int number = c;
      database.transaction(() -> {
        final MutableVertex cold = database.newVertex("Account");
        cold.set("number", number);
        cold.save();
        cold.newEdge("TX", hubHolder[0]);
      });
    }

    // DRIVE THE HOT ACCOUNT PAST PROMOTION: hotDegree inbound edges, batched.
    final int batch = 1_000;
    for (int b = 0; b < hotDegree / batch; b++)
      database.transaction(() -> {
        for (int i = 0; i < batch; i++) {
          final MutableVertex src = database.newVertex("Account");
          src.set("number", 100_000 + i);
          src.save();
          src.newEdge("TX", hubHolder[0]);
        }
      });

    // THE HOT ACCOUNT MUST HAVE BEEN PROMOTED TO THE STRIPED SUPER-NODE LAYOUT.
    database.transaction(() -> {
      final RID inHead = ((VertexInternal) hubHolder[0].asVertex(true)).getInEdgesHeadChunk();
      final Record head = database.lookupByRID(inHead, true);
      assertThat(head).isInstanceOf(StripeDirectory.class);
    });

    final int hubIncoming = hotDegree + coldAccounts; // every cold + every hot source points at the hub

    // THE EXACT REPORTED QUERY - must not throw BufferUnderflowException and must rank the hub first.
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT number, both().size() AS degree FROM Account ORDER BY both().size() DESC LIMIT 5")) {
        assertThat(rs.hasNext()).isTrue();
        final Result top = rs.next();
        assertThat(((Number) top.getProperty("number")).intValue()).isEqualTo(0);
        assertThat(((Number) top.getProperty("degree")).longValue()).isEqualTo(hubIncoming);
      }
    });

    // AND the direct both().size() on the hub agrees with countEdges across the promotion boundary.
    database.transaction(() -> {
      final Vertex hub = hubHolder[0].asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.BOTH, "TX")).isEqualTo(hubIncoming);
    });
  }
}
