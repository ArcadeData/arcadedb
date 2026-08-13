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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6128 (3): {@code autoFix} counted repairs that did not happen.
 * <p>
 * The vertex and edge arms called {@code autoFix.incrementAndGet()} BEFORE attempting the delete, so a record they
 * then failed to remove was still reported as fixed. That is not a cosmetic miscount: {@code autoFix} is the number
 * an operator reads to decide whether a run did anything, and one that includes failed deletes cannot answer that.
 * <p>
 * The case reached here is the ordinary one rather than a contrived failure. {@code checkEdges} flags BOTH ends of a
 * dangling edge - the edge record itself and the RID it points at - and that second RID is flagged precisely because
 * the record is NOT there, so its delete raises {@code RecordNotFoundException} every time. A database with one
 * dangling edge therefore reported two repairs while performing one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseAutoFixAccountingTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The dangling edge is injected on purpose; the post-test check would (correctly) flag it.
    return false;
  }

  @Test
  void autoFixCountsOnlyTheRecordsItActuallyRemoved() {
    final RID[] edge = new RID[1];
    final RID[] target = new RID[1];

    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

    database.transaction(() -> {
      final MutableVertex from = database.newVertex(VERTEX_TYPE).set("name", "from").save();
      final MutableVertex to = database.newVertex(VERTEX_TYPE).set("name", "to").save();
      target[0] = to.getIdentity();
      edge[0] = from.newEdge(EDGE_TYPE, to).getIdentity();
    });

    // Remove the target vertex underneath the edge, leaving the edge pointing at a RID that is not there. Both the
    // edge and the missing target end up in corruptedRecords; only the edge can actually be deleted.
    database.transaction(
        () -> database.getSchema().getBucketById(target[0].getBucketId()).deleteRecord(target[0]));

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkEdges(EDGE_TYPE, true, 0);

    // PRECONDITION: the run really did flag both RIDs, otherwise the miscount could not arise and this test would
    // be asserting nothing.
    assertThat((Collection<RID>) stats.get("corruptedRecords"))
        .as("both the dangling edge and the RID it points at must be flagged: %s", stats)
        .contains(edge[0], target[0]);

    assertThat((Collection<RID>) stats.get("deletedRecordsAfterFix"))
        .as("only the edge exists to be removed: %s", stats)
        .containsExactly(edge[0]);

    assertThat((Long) stats.get("autoFix"))
        .as("autoFix must count the repair it performed, not the one it attempted: %s", stats)
        .isEqualTo(1L);
  }
}
