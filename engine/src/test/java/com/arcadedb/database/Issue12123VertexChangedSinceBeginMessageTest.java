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
package com.arcadedb.database;

import com.arcadedb.engine.TransactionManager.VertexChange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12123: a follower commit refused because a replicated apply wrote the page of a vertex it links to said the vertex "was
 * deleted by a concurrent transaction" although nothing deleted it. Each cause of the refusal has its own wording; the exception and
 * its retry are unchanged (the commit paths are covered by {@code ReplicatedApplyEdgeListHeadMergeTest} and
 * {@code Issue11592EdgeListHeadMergeReusedRidTest}).
 */
class Issue12123VertexChangedSinceBeginMessageTest {
  private static final RID RID = new RID(1, 0);

  @Test
  void aDeletedVertexIsReportedAsDeleted() {
    assertThat(TransactionContext.vertexChangedSinceBeginMessage(RID, VertexChange.DELETED))
        .isEqualTo("Record #1:0 was deleted by a concurrent transaction. Please retry the operation");
  }

  @Test
  void aReplicatedPageWriteIsNotReportedAsADelete() {
    assertThat(TransactionContext.vertexChangedSinceBeginMessage(RID, VertexChange.PAGE_REPLICATED))
        .isEqualTo("The page of record #1:0 was written by a replicated transaction after this transaction began. Please retry the operation")
        .doesNotContain("deleted");
  }

  @Test
  void aDroppedBucketIsReportedAsDropped() {
    assertThat(TransactionContext.vertexChangedSinceBeginMessage(RID, VertexChange.BUCKET_DROPPED))
        .isEqualTo("The bucket of record #1:0 was dropped by a concurrent transaction. Please retry the operation");
  }

  @Test
  void anUnchangedVertexHasNoRefusal() {
    assertThatThrownBy(() -> TransactionContext.vertexChangedSinceBeginMessage(RID, VertexChange.NONE))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
