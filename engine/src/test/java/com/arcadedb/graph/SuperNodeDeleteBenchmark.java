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
import com.arcadedb.log.LogManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cost of deleting a high-degree vertex, on both edge-list layouts.
 * <p>
 * Written for #5760, whose subject is work the delete was doing and did not need: disconnecting every edge from the
 * vertex being deleted, over lists dropped wholesale a moment later. The striped case is the one that measured the
 * change - {@code StripedEdgeList.removeEdge} resolves one chain per generation for every single removal, and
 * generation 0 is the whole pre-promotion chain - while the classic single chain barely moved, because there the
 * emptied head chunk keeps the per-edge probe short. Numbers on an Apple M-series laptop, 100k edges into one hub:
 * <pre>
 *   striped (promoted super-node): 2374 ms -&gt; 446 ms
 *   classic (promotion disabled):   350 ms -&gt; 308 ms
 * </pre>
 * The assertions are only sanity checks, so a wedged run fails rather than passes silently; the output is the point.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class SuperNodeDeleteBenchmark extends TestHelper {

  private static final int EDGES = 100_000;

  @Test
  void stripedLayoutDelete() {
    benchmarkHubDelete(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger(), "striped");
  }

  /** The same degree on the classic single-chain layout, with super-node promotion disabled. */
  @Test
  void classicLayoutDelete() {
    benchmarkHubDelete(0, "classic");
  }

  private void benchmarkHubDelete(final int threshold, final String label) {
    final Object savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValue();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(threshold);
    try {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 8);
        database.getSchema().createEdgeType("LINK", 8);
      });

      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub");
        hub.save();
        holder[0] = hub.getIdentity();
      });
      final RID hubRID = holder[0];

      database.transaction(() -> {
        for (int i = 0; i < EDGES; i++) {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          src.newEdge("LINK", hubRID);
        }
      });

      database.transaction(
          () -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(EDGES));

      final long begin = System.currentTimeMillis();
      database.transaction(() -> hubRID.asVertex().delete());
      final long elapsed = System.currentTimeMillis() - begin;

      LogManager.instance()
          .log(this, Level.SEVERE, "#5760 %s-layout delete benchmark: %d edges in %d ms", label, EDGES, elapsed);

      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).isFalse();
        assertThat(database.countType("LINK", false)).isEqualTo(0L);
      });
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }
  }
}
