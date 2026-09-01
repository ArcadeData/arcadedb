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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.WorkGuard;

import org.junit.jupiter.api.Test;

import static com.arcadedb.query.opencypher.executor.steps.SparseNodeIdProvider.HIGH_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Regression coverage for issue #6967 in {@link PartitionedTriangleOp}. */
class PartitionedTriangleOpSparseNodeIdsTest {

  @Test
  void countsTriangleContainingLiveNodeAboveLiveNodeCount() {
    final SparseNodeIdProvider provider = new SparseNodeIdProvider().withoutViews()
        .withEdges("IN_CITY", Vertex.DIRECTION.OUT, 2, 4)
        .withEdges("IN_CITY", Vertex.DIRECTION.OUT, 3, 4)
        .withEdges("IN_CITY", Vertex.DIRECTION.OUT, HIGH_ID, 4)
        .withEdges("KNOWS", Vertex.DIRECTION.BOTH, 2, 3, HIGH_ID)
        .withEdges("KNOWS", Vertex.DIRECTION.BOTH, 3, 2, HIGH_ID)
        .withEdges("KNOWS", Vertex.DIRECTION.BOTH, HIGH_ID, 2, 3);
    final PartitionedTriangleOp op = new PartitionedTriangleOp(new String[] { "IN_CITY" },
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT }, "KNOWS");

    final long count = op.execute(provider, null, WorkGuard.forCommandDeadline(null));

    assertThat(count).isEqualTo(6L);
  }
}
