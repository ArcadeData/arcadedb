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
 * The case reached here is the ordinary one rather than a contrived failure: an adjacency list still naming an EDGE
 * record that is gone. {@code checkIncomingEdges}/{@code checkOutgoingEdges} flag that RID corrupted - it is what
 * the damaged list points at - and it is flagged precisely because the record is NOT there, so its delete raises
 * {@code RecordNotFoundException} every time.
 * <p>
 * #5777 moved this test off its original fixture. It used to reach the same shape through {@code checkEdges}, which
 * flagged BOTH ends of a dangling edge - the edge record and the ABSENT vertex it pointed at. That second flag is
 * gone: an endpoint that is merely not there is reported, never called corrupt, because flagging it had
 * {@code FIX} drop and rebuild every index on the vanished vertex's bucket for nothing. The accounting rule this
 * test pins is unchanged, so it is pinned through the arm that still produces the shape.
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

    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

    database.transaction(() -> {
      final MutableVertex from = database.newVertex(VERTEX_TYPE).set("name", "from").save();
      final MutableVertex to = database.newVertex(VERTEX_TYPE).set("name", "to").save();
      edge[0] = from.newEdge(EDGE_TYPE, to).getIdentity();
    });

    // Remove the EDGE record through its bucket, leaving both vertices' adjacency lists naming a RID that is not
    // there. The walk flags that RID corrupted, and the repair loop can never delete it.
    database.transaction(
        () -> database.getSchema().getBucketById(edge[0].getBucketId()).deleteRecord(edge[0]));

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkVertices(VERTEX_TYPE, true, 0);

    // PRECONDITION: the run really did flag a RID it cannot delete, otherwise the miscount could not arise and this
    // test would be asserting nothing.
    assertThat((Collection<RID>) stats.get("corruptedRecords"))
        .as("the RID the damaged list points at must be flagged: %s", stats)
        .contains(edge[0]);

    assertThat((Collection<RID>) stats.get("deletedRecordsAfterFix"))
        .as("there is nothing left to remove: %s", stats)
        .isEmpty();

    // The per-kind counter is where a failed delete would show up. autoFix itself is NOT zero here - it also folds
    // in the dangling list entries this run pruned, which are repairs that did happen (#6128) - so asserting on it
    // alone could not tell the two apart.
    assertThat((Long) stats.get("removedRecords"))
        .as("a delete that raised must not be counted as a repair: %s", stats)
        .isEqualTo(0L);
    assertThat((Long) stats.get("prunedDanglingEntries"))
        .as("precondition: the repairs autoFix DOES count here are the pruned entries: %s", stats)
        .isGreaterThan(0L);
    assertThat((Long) stats.get("autoFix"))
        .as("autoFix must count the repairs it performed, not the one it attempted: %s", stats)
        .isEqualTo((Long) stats.get("prunedDanglingEntries"));
  }
}
