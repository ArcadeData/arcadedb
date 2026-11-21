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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link GraphBatch#newEdge} takes its endpoints on trust, and a batch handed one that is not an existing vertex leaves the whole
 * batch unreachable (issue #12895). {@link GraphBatch#requireVertex} is what a caller that takes RIDs from outside asks first.
 */
class GraphBatchRequireVertexTest extends TestHelper {

  @Test
  void requireVertexAcceptsAnExistingVertexAndRefusesTheRest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("RV");
      database.getSchema().createEdgeType("RE");
      database.getSchema().createDocumentType("RD");
    });
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("RV").set("n", 0).save().getIdentity();
      rids[1] = database.newDocument("RD").set("n", 1).save().getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      assertThatCode(() -> batch.requireVertex(rids[0])).doesNotThrowAnyException();

      assertThatThrownBy(() -> batch.requireVertex(rids[1])).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(rids[1].toString()).hasMessageContaining("is not a vertex").hasMessageContaining("RD");
      assertThatThrownBy(() -> batch.requireVertex(new RID(99, 0))).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("#99:0").hasMessageContaining("does not exist");
      assertThatThrownBy(() -> batch.requireVertex(new RID(rids[0].getBucketId(), 99_999))).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("does not exist");
      assertThatThrownBy(() -> batch.requireVertex(null)).isInstanceOf(IllegalArgumentException.class);
    }
  }

  /**
   * What the check is for: a caller refuses the bad endpoint before buffering the edge, and the valid edge buffered before it is
   * a whole edge at close(). Handed to newEdge instead, an endpoint that does not exist fails the batch at close() with the edge
   * records written and no vertex leading to them, the valid edge included.
   */
  @Test
  void aRefusedEndpointLeavesTheEdgeBeforeItWhole() {
    database.transaction(() -> {
      database.getSchema().createVertexType("PV");
      database.getSchema().createEdgeType("PE");
    });
    final RID[] v = new RID[2];
    database.transaction(() -> {
      v[0] = database.newVertex("PV").set("n", 0).save().getIdentity();
      v[1] = database.newVertex("PV").set("n", 1).save().getIdentity();
    });

    // checked: the bad endpoint is refused before it is buffered, and the valid edge before it is a whole edge at close()
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(v[0], "PE", v[1]);
      assertThatThrownBy(() -> batch.requireVertex(new RID(99, 0))).isInstanceOf(IllegalArgumentException.class);
    }
    assertThat(database.countType("PE", false)).isEqualTo(1);
    assertThat(database.query("sql", "select out('PE').size() as s from PV where n = 0").next().<Integer>getProperty("s")).isEqualTo(1);
  }
}
