/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6128 (1), (5): a batched {@code CHECK DATABASE FIX} under a deliberately tight replicated-entry cap.
 * <p>
 * The repair of one type used to be a SINGLE transaction, which on a replicated database is a SINGLE log entry.
 * {@code RaftGroupCommitter.submitAndWait} rejects anything above
 * {@code min(arcadedb.ha.appendBufferSize, arcadedb.ha.grpcMessageSizeMax)} with a
 * {@code ReplicatedEntryTooLargeException} - a {@code TransactionException}, not a {@code NeedRetryException}, so
 * nothing retries it - and a repair that had run for hours was rolled back whole. A transaction entry has no
 * splitter, unlike a schema entry since #4743.
 * <p>
 * WHAT THIS TEST DOES AND DOES NOT PROVE, stated because the distinction is easy to lose: it does NOT reproduce an
 * over-cap rejection. It cannot at a sane fixture size - the WAL carries per-page CHANGED RANGES rather than whole
 * pages, so reconnecting 1500 edges produces well under 128KB, and it was measured passing with batching disabled
 * at both a 1MB and a 128KB cap. Brute-forcing past the cap would need tens of thousands of damaged edges and
 * minutes of cluster setup for a bound that is better expressed directly.
 * <p>
 * What it does prove is the property that matters and that the unit test cannot: a repair split across many
 * transactions still converges all three nodes, and it does so with the cap pinned two orders of magnitude below
 * its default - so any future change that lets one repair transaction grow large again fails here rather than in
 * a customer's cluster. The batching mechanism itself is pinned by {@code CheckDatabaseRepairBatchTest}, which
 * counts the commits.
 * <p>
 * Sibling of {@code RaftCheckDatabaseFix3NodesIT}, which covers the ordinary-sized repair and the leader
 * forwarding.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftCheckDatabaseLargeRepairIT extends BaseRaftHATest {
  private static final String VERTEX_TYPE = "BigRepairNode";
  private static final String EDGE_TYPE   = "BigRepairLink";
  /** Enough damaged vertices that the whole-type repair cannot fit in the lowered append buffer. */
  private static final int    HUBS        = 60;
  private static final int    DEGREE      = 25;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Two orders of magnitude below the 32MB default, with the write buffer kept above it (the server refuses to
    // start otherwise - it must stay >= appendBufferSize + 8 bytes). Every transaction this test performs, setup
    // included, has to fit; that is the point, since it is what turns "the repair stays small" into an assertion.
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "128KB");
    config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, "256KB");
  }

  @Test
  void aBatchedRepairReplicatesUnderATightEntryCap() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);

    final DatabaseInternal leaderDb = wrapped(leaderIndex);
    // Small enough that each batch is comfortably inside the 1MB cap, and small enough that the whole repair
    // needs many of them.
    leaderDb.getConfiguration().setValue(GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES, 8);

    final List<RID> hubs = createDamagedGraph(leaderDb);

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(inDegreeOn(i, hubs.get(0)))
          .as("the damage must have replicated to server %d before the repair", i).isNotEqualTo((long) DEGREE);

    final Result fix = runCheck(leaderIndex, "CHECK DATABASE TYPE " + VERTEX_TYPE + " FIX");
    // The repair for this damage shape is a chain REBUILD, which is reported in the warnings rather than in
    // autoFix (autoFix counts records removed and dangling entries pruned, and a rebuild is neither). Asserted so
    // a run that silently repaired nothing cannot reach the per-node assertions below and pass them trivially by
    // leaving every hub equally broken everywhere.
    assertThat(fix.<Collection<String>>getProperty("warnings"))
        .as("the repair must actually have rebuilt the edge lists: %s", fix.toJSON())
        .anyMatch(w -> w.startsWith("reconnected ") && w.endsWith(" incoming edges"));

    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      for (final RID hub : hubs)
        assertThat(awaitInDegreeOn(server, hub, DEGREE))
            .as("server %d must see hub %s fully repaired: a repair too big for one entry has to reach the "
                + "followers in several, not fail whole", server, hub)
            .isEqualTo(DEGREE);
    }

    assertClusterConsistency();
  }

  /**
   * Issue #6128 (5): a clean result on a replicated database must say which copy it describes. The check is
   * forwarded to the leader and reads only the leader's pages, so "clean" is a statement about one node.
   */
  @Test
  void theResultSaysTheCheckSawOnlyOneNode() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);

    final Result check = runCheck(leaderIndex, "CHECK DATABASE");

    assertThat(check.<String>getProperty("checkedNodeScope"))
        .as("the scope of the answer must be reported, not left to the documentation: %s", check.toJSON())
        .isEqualTo("this node only (replicated database)");

    assertThat(check.<Collection<String>>getProperty("warnings"))
        .as("and it must be visible among the warnings an operator actually reads: %s", check.toJSON())
        .anyMatch(w -> w.contains("inspected only the node that ran it"));
  }

  // ---------------------------------------------------------------------------------------------------------------

  private DatabaseInternal wrapped(final int serverIndex) {
    return ((DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName())).getWrappedDatabaseInstance();
  }

  /** {@link #HUBS} vertices with a full in-edge list each, then every one of those lists broken at its head. */
  private List<RID> createDamagedGraph(final DatabaseInternal db) {
    db.command("sql", "CREATE VERTEX TYPE " + VERTEX_TYPE);
    db.command("sql", "CREATE EDGE TYPE " + EDGE_TYPE);

    // A payload per hub so the hub records span pages rather than packing into one.
    final String payload = "x".repeat(1_500);

    final List<RID> hubs = new ArrayList<>(HUBS);
    for (int h = 0; h < HUBS; h++) {
      final int hub = h;
      final RID[] holder = new RID[1];
      db.transaction(() -> {
        holder[0] = db.newVertex(VERTEX_TYPE).set("name", "hub" + hub).set("payload", payload).save().getIdentity();
        for (int i = 0; i < DEGREE; i++)
          db.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, holder[0].asVertex(true));
      });
      hubs.add(holder[0]);
    }

    // Corrupt each head chunk IN PLACE. Deleting it instead would free the slot, and the repair would then build
    // its new chunks into those freed slots, letting one hub's dangling head alias another hub's live chunk - an
    // artefact no real corruption produces, since damaged bytes do not free a slot.
    for (final RID hub : hubs) {
      final RID[] head = new RID[1];
      db.transaction(() -> head[0] = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk());
      assertThat(head[0]).as("every hub must have an in-edge list to break").isNotNull();
      corruptRecordTypeByte(db, head[0]);
    }
    return hubs;
  }

  /**
   * Overwrites the record-type byte so the record still occupies its slot but cannot be materialised. Through the
   * wrapper, so all three nodes hold the same damage.
   */
  private void corruptRecordTypeByte(final DatabaseInternal db, final RID rid) {
    final int fileId = rid.getBucketId();
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(fileId);
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, pageId), pageSize, false);
        final int slotOffset = Binary.SHORT_SERIALIZED_SIZE + (positionInPage * Binary.INT_SERIALIZED_SIZE);
        final int recordOffset = (int) page.readUnsignedInt(slotOffset);
        assertThat(recordOffset).as("the record must still occupy its slot").isGreaterThan(0);
        final long[] recordSize = page.readNumberAndSize(recordOffset);
        page.writeByte((int) (recordOffset + recordSize[1]), (byte) 99);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private Result runCheck(final int serverIndex, final String statement) {
    try (final ResultSet rs = wrapped(serverIndex).command("sql", statement)) {
      assertThat(rs.hasNext()).as("'%s' must return a result", statement).isTrue();
      return rs.next();
    }
  }

  /** The hub's in-degree on one server, or -1 when its list cannot be walked at all. */
  private long inDegreeOn(final int serverIndex, final RID hubRid) {
    return withResyncRetry(serverIndex, db -> {
      final AtomicLong degree = new AtomicLong(-1);
      db.transaction(() -> {
        try {
          degree.set(db.lookupByRID(hubRid, true).asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE));
        } catch (final DatabaseIsClosedException e) {
          throw e;
        } catch (final Exception e) {
          degree.set(-1);
        }
      });
      return degree.get();
    });
  }

  private long awaitInDegreeOn(final int serverIndex, final RID hubRid, final long expected) throws InterruptedException {
    return awaitValue(expected, () -> inDegreeOn(serverIndex, hubRid));
  }
}
