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

/** Regression coverage for issue #6967 in {@link AntiJoinChainOp}. */
class AntiJoinChainOpSparseNodeIdsTest {

  @Test
  void evaluatesLiveAnchorAboveLiveNodeCount() {
    final SparseNodeIdProvider provider = new SparseNodeIdProvider().withoutViews()
        .withEdges("CHAIN", Vertex.DIRECTION.OUT, 0, 2)
        .withEdges("CHAIN", Vertex.DIRECTION.OUT, HIGH_ID, 3);
    final AntiJoinChainOp op = new AntiJoinChainOp(new String[] { null, null },
        new String[] { "CHAIN" }, new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT },
        0, 1, "BLOCKS", Vertex.DIRECTION.OUT, -1, -1);

    final long count = op.execute(provider, null, WorkGuard.forCommandDeadline(null));

    assertThat(count).isEqualTo(2L);
  }
}
