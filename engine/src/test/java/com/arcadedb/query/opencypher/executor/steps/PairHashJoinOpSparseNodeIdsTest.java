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

import com.arcadedb.database.Database;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static com.arcadedb.query.opencypher.executor.steps.SparseNodeIdProvider.HIGH_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/** Regression coverage for issue #6967 in {@link PairHashJoinOp}. */
class PairHashJoinOpSparseNodeIdsTest {

  @Test
  void buildsPairFromLiveNodeAboveLiveNodeCount() {
    final SparseNodeIdProvider provider = new SparseNodeIdProvider().withoutViews()
        .withEdges("ARM_1", Vertex.DIRECTION.OUT, 0, 2)
        .withEdges("ARM_2", Vertex.DIRECTION.OUT, 0, 3)
        .withEdges("ARM_1", Vertex.DIRECTION.OUT, HIGH_ID, 2)
        .withEdges("ARM_2", Vertex.DIRECTION.OUT, HIGH_ID, 3)
        .withEdges("PROBE", Vertex.DIRECTION.OUT, 2, 3);
    final PairHashJoinOp op = new PairHashJoinOp("Build",
        new String[] { "ARM_1" }, new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT }, null,
        new String[] { "ARM_2" }, new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT }, null,
        "PROBE", Vertex.DIRECTION.OUT);

    final long count = op.execute(provider, databaseWithBuildBucket(), WorkGuard.forCommandDeadline(null));

    assertThat(count).isEqualTo(2L);
  }

  private static Database databaseWithBuildBucket() {
    final Database db = Mockito.mock(Database.class);
    final Schema schema = Mockito.mock(Schema.class);
    final DocumentType buildType = Mockito.mock(DocumentType.class);
    final Bucket buildBucket = Mockito.mock(Bucket.class);
    when(db.getSchema()).thenReturn(schema);
    when(schema.existsType("Build")).thenReturn(true);
    when(schema.getType("Build")).thenReturn(buildType);
    when(buildType.getBuckets(true)).thenReturn(List.of(buildBucket));
    when(buildBucket.getFileId()).thenReturn(7);
    return db;
  }
}
