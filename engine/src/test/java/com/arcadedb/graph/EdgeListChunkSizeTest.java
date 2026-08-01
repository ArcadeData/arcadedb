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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code arcadedb.graph.edgeListInitialChunkSize}, the size of the first chunk of a vertex's edge list.
 * <p>
 * The best value tracks the degree distribution and there is no single winner: measured over lightweight and regular
 * edges at degree 2, 10 and 100, a first chunk of 128 B is 10-20% better than the default on both time and space at
 * degree 10, but 70% worse on space at degree 2. Hence a knob with the previous constant as its default, rather than
 * a changed default.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeListChunkSizeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("V").create();
      database.getSchema().buildEdgeType().withName("Follows").withLightweight(true).create();
    });
  }

  @Test
  void theConfiguredSizeIsUsedForTheFirstChunk() {
    final int saved = GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.getValueAsInteger();
    try {
      GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(256);
      assertThat(LocalDatabase.getNewEdgeListSize(0)).isEqualTo(256);

      // subsequent chunks keep doubling, capped
      assertThat(LocalDatabase.getNewEdgeListSize(256)).isEqualTo(512);
      assertThat(LocalDatabase.getNewEdgeListSize(LocalDatabase.MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE))
          .isEqualTo(LocalDatabase.MAX_RECOMMENDED_EDGE_LIST_CHUNK_SIZE);
    } finally {
      GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(saved);
    }
  }

  /**
   * The chunk buffer is not auto-resizable, so a value too small to hold the header plus one entry used to fail at
   * the first append with "Cannot resize the buffer" - far from the setting that caused it.
   */
  @Test
  void aTooSmallConfiguredSizeIsFloored() {
    final int saved = GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.getValueAsInteger();
    try {
      GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(1);
      assertThat(LocalDatabase.getNewEdgeListSize(0)).isEqualTo(LocalDatabase.MIN_EDGE_LIST_CHUNK_SIZE);

      // and the graph still works at the floor
      final RID[] rids = new RID[6];
      database.transaction(() -> {
        for (int i = 0; i < rids.length; i++)
          rids[i] = database.newVertex("V").set("id", i).save().getIdentity();
        final MutableVertex source = database.lookupByRID(rids[0], true).asVertex().modify();
        for (int i = 1; i < rids.length; i++)
          source.newEdge("Follows", rids[i]);
      });

      database.transaction(() -> assertThat(database.lookupByRID(rids[0], true).asVertex()
          .countEdges(Vertex.DIRECTION.OUT)).isEqualTo(rids.length - 1));
    } finally {
      GlobalConfiguration.GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE.setValue(saved);
    }
  }
}
