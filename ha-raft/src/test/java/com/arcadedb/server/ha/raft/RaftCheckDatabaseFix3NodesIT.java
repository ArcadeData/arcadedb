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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.database.Binary;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * First cluster coverage of {@code CHECK DATABASE ... FIX}. Before this class the statement had none: a grep of
 * {@code ha-raft/src/test} for "CHECK DATABASE" returned nothing, and the only file named for a checker
 * ({@code ClusterDatabaseChecker}) is a {@code main()} that diffs three database directories offline.
 * <p>
 * The gap matters because the checker's repair path is not an ordinary write. Three things about it are specific
 * to a replicated database and none of them are exercised by the engine's own {@code CheckDatabase*Test}s:
 * <ul>
 *   <li>{@code CheckDatabaseStatement} is not idempotent, so on a follower {@code RaftReplicatedDatabase.command}
 *   forwards the whole statement to the leader over HTTP - with or without {@code FIX}. The check therefore only
 *   ever inspects the LEADER's pages, and every repair has to reach the other nodes as replicated WAL rather than
 *   by each node repairing itself;</li>
 *   <li>{@code GraphDatabaseChecker.checkVertices} accumulates an entire type's repair in ONE transaction, which
 *   under Raft becomes ONE log entry - {@code RaftTransactionBroker.replicateTransaction} submits it whole, with
 *   no equivalent of the schema-entry splitter added for issue #4743;</li>
 *   <li>the index rebuild at the end of {@code DatabaseChecker.check()} drops and recreates the index, so it runs
 *   inside {@code recordFileChanges} and its build WAL rides a {@code SCHEMA_ENTRY} instead of replicating batch by
 *   batch.</li>
 * </ul>
 * The corruption is induced THROUGH the Raft wrapper on the leader, so all three nodes carry it identically before
 * the repair starts. That is the shape an operator actually meets: the damage was replicated when it happened, and
 * {@code CHECK DATABASE FIX} is being run afterwards to clean it up.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftCheckDatabaseFix3NodesIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "CheckNode";
  private static final String EDGE_TYPE   = "CheckLink";
  /**
   * 500 in-edges on one hub. Big enough that the edge list spans several chunks (the first chunk is
   * {@code GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE} = 64 bytes and each further one doubles up to 8192), so the head
   * chunk has a previous to delete; well under {@code GRAPH_SUPERNODE_THRESHOLD} = 4096, so the hub keeps the
   * classic layout and {@code getInEdgesHeadChunk} means what it says. Same figure as the engine-side
   * {@code GraphDatabaseCheckerChainRebuildTest}, which pins the single-node behaviour this one extends.
   */
  private static final int    DEGREE      = 500;

  private static final String INDEX_TYPE_NAME = "CheckIdxDoc";
  private static final String INDEX_NAME      = "checkIdxDocName";
  private static final int    INDEX_RECORDS   = 50;

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The headline case: a mid-chain edge segment is lost on every node, and {@code CHECK DATABASE FIX} run through
   * the cluster has to rebuild the adjacency from the surviving edge records AND land that rebuild on the two
   * followers. A regression that commits the repair on the inner {@code LocalDatabase} (the #5492 shape) leaves the
   * leader clean and the followers broken, which is exactly what the per-node assertions below separate.
   */
  @Test
  void fixRebuildsBrokenEdgeChainOnEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);

    final DatabaseInternal leaderDb = wrapped(leaderIndex);
    final RID hubRid = createHub(leaderDb);

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitInDegreeOn(i, hubRid, DEGREE))
          .as("baseline: server %d must see the hub's full adjacency before anything is broken", i)
          .isEqualTo(DEGREE);

    breakMidChainChunk(leaderDb, hubRid);

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(inDegreeOn(i, hubRid))
          .as("the corruption itself must have replicated to server %d, otherwise the repair below proves nothing", i)
          .isNotEqualTo(DEGREE);

    final Result fix = runCheck(leaderIndex, "CHECK DATABASE FIX");
    assertThat(fix.<String>getProperty("operation")).isEqualTo("check database");

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitInDegreeOn(i, hubRid, DEGREE))
          .as("server %d must see the rebuilt adjacency: the repair replicates, it is not re-derived per node", i)
          .isEqualTo(DEGREE);

    // A second check, on the leader, must now find nothing. Run WITHOUT fix so a still-dirty database cannot be
    // repaired into silence by the very statement that is supposed to report on it.
    final Result verify = runCheck(leaderIndex, "CHECK DATABASE");
    assertThat(verify.<Long>getProperty("totalCorruptedRecords"))
        .as("the database must be clean after the repair").isZero();

    assertClusterConsistency();
  }

  /**
   * The same repair, issued on a FOLLOWER. {@code CheckDatabaseStatement} does not override
   * {@code Statement.isIdempotent()}, so it is classified non-idempotent and
   * {@code RaftReplicatedDatabase.command} forwards it to the leader rather than running it locally - which is the
   * only correct outcome, since a follower repairing its own pages outside the Raft log is precisely the
   * divergence the forwarding exists to prevent.
   * <p>
   * Asserted by effect rather than by instrumentation: the follower that ISSUED the statement is not the node the
   * repair was computed on, so all three converging is the observable consequence of the forward having happened.
   */
  @Test
  void fixIssuedOnFollowerIsForwardedToTheLeaderAndRepairsTheCluster() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final DatabaseInternal leaderDb = wrapped(leaderIndex);
    final RID hubRid = createHub(leaderDb);

    waitForAllServers();
    assertThat(awaitInDegreeOn(followerIndex, hubRid, DEGREE)).isEqualTo(DEGREE);

    breakMidChainChunk(leaderDb, hubRid);
    waitForAllServers();
    assertThat(inDegreeOn(followerIndex, hubRid)).isNotEqualTo(DEGREE);

    runCheck(followerIndex, "CHECK DATABASE FIX");

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitInDegreeOn(i, hubRid, DEGREE))
          .as("server %d must be repaired by a FIX issued on the follower", i)
          .isEqualTo(DEGREE);

    assertClusterConsistency();
  }

  /**
   * The index-rebuild arm. A corrupted record puts its bucket into {@code affectedBuckets}, and the tail of
   * {@code DatabaseChecker.check()} then drops and recreates every index on that bucket. Under Raft the recreate
   * runs inside {@code recordFileChanges}, so the build's batch commits are buffered into {@code schemaWalBuffer}
   * and shipped as one {@code SCHEMA_ENTRY} (split across entries by {@code splitSchemaEntry} when it does not
   * fit) instead of replicating as they happen.
   * <p>
   * Two distinct failure modes are separated here, because they need different fixes. The index still EXISTING
   * cluster-wide covers the drop-committed-then-rebuild-failed hole: {@code DatabaseChecker}'s rebuild loop
   * catches only {@code NeedRetryException}, so a replication failure raised from inside {@code create()} after
   * that attempt's {@code dropIndex()} already committed its own schema entry propagates straight out of
   * {@code check()} and leaves the index gone on every node. The entry COUNT matching on every node covers the
   * separate question of whether the rebuilt pages actually reached the followers.
   * <p>
   * The type is a VERTEX type, matching the engine-side {@code CheckDatabaseFixPreservesIndexMetadataTest}. Writing
   * this test with a DOCUMENT type is what first surfaced the defect that {@code DatabaseChecker.checkDocuments}
   * flagged a corrupted document without DELETING it, so the rebuild's own bucket scan met the same unreadable
   * record and destroyed the index. That is fixed, and its regression test is
   * {@code CheckDatabaseFixDocumentTypeTest} - single-node, where it belongs: nothing about it needed a cluster.
   */
  @Test
  void fixRebuildsIndexAndKeepsItOnEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);

    final DatabaseInternal leaderDb = wrapped(leaderIndex);

    leaderDb.command("sql", "CREATE VERTEX TYPE " + INDEX_TYPE_NAME);
    leaderDb.command("sql", "CREATE PROPERTY " + INDEX_TYPE_NAME + ".name STRING");

    final AtomicReference<RID> victim = new AtomicReference<>();
    leaderDb.transaction(() -> {
      for (int i = 0; i < INDEX_RECORDS; i++) {
        final Result inserted = leaderDb.command("sql",
            "INSERT INTO " + INDEX_TYPE_NAME + " SET name = 'name" + i + "'").next();
        if (i == 0)
          victim.set(inserted.toElement().getIdentity());
      }
    });
    leaderDb.command("sql", "CREATE INDEX " + INDEX_NAME + " ON " + INDEX_TYPE_NAME + " (name) NOTUNIQUE");

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitIndexEntriesOn(i, INDEX_RECORDS))
          .as("baseline: server %d must hold the whole index before the corruption", i)
          .isEqualTo(INDEX_RECORDS);

    // Corrupt the record's type byte through the wrapper, so every node holds the same unreadable record. This is
    // what makes the checker flag it, delete it under FIX, and rebuild the bucket's indexes.
    corruptRecordTypeByte(leaderDb, victim.get());
    waitForAllServers();

    final Result fix = runCheck(leaderIndex, "CHECK DATABASE TYPE " + INDEX_TYPE_NAME + " FIX");
    assertThat(fix.<Long>getProperty("autoFix"))
        .as("the corrupted record must have been detected and removed").isGreaterThan(0L);

    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      final boolean indexSurvived = withResyncRetry(server, db -> db.getSchema().existsIndex(INDEX_NAME));
      assertThat(indexSurvived)
          .as("server %d must still have index '%s': a rebuild that fails after the drop leaves it missing "
              + "cluster-wide, not merely unrebuilt", server, INDEX_NAME)
          .isTrue();

      assertThat(awaitIndexEntriesOn(server, INDEX_RECORDS - 1L))
          .as("server %d must hold the REBUILT index: its pages ride a SCHEMA_ENTRY, they are not rebuilt locally",
              server)
          .isEqualTo(INDEX_RECORDS - 1L);
    }

    assertClusterConsistency();
  }

  // ---------------------------------------------------------------------------------------------------------------

  /**
   * The Raft wrapper for a server's database. {@code getWrappedDatabaseInstance()} returns the wrapper under HA and
   * the instance itself off it, so resolving it is always correct - and getting it wrong is the #5492 defect, where
   * a commit on the inner instance publishes pages locally and replicates nothing.
   */
  private DatabaseInternal wrapped(final int serverIndex) {
    return ((DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName())).getWrappedDatabaseInstance();
  }

  /** Creates the hub and its {@link #DEGREE} in-edges through the cluster, and returns the hub's RID. */
  private RID createHub(final DatabaseInternal db) {
    db.command("sql", "CREATE VERTEX TYPE " + VERTEX_TYPE);
    db.command("sql", "CREATE EDGE TYPE " + EDGE_TYPE);

    final AtomicReference<RID> hub = new AtomicReference<>();
    db.transaction(() -> hub.set(db.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity()));

    final int batch = 100;
    for (int from = 0; from < DEGREE; from += batch) {
      final int start = from;
      final int end = Math.min(from + batch, DEGREE);
      db.transaction(() -> {
        for (int i = start; i < end; i++)
          db.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, hub.get().asVertex(true));
      });
    }
    return hub.get();
  }

  /**
   * Deletes the chunk BEFORE the head of the hub's in-edge list, through the Raft wrapper so the deletion
   * replicates. The head chunk is the newest, so its previous is genuinely mid-chain: the walk reaches it and
   * fails there, rather than the vertex simply appearing to have no edges.
   */
  private void breakMidChainChunk(final DatabaseInternal db, final RID hubRid) {
    final AtomicReference<RID> midChunk = new AtomicReference<>();
    db.transaction(() -> {
      final RID head = ((VertexInternal) hubRid.asVertex(true)).getInEdgesHeadChunk();
      assertThat(head).as("the hub must have an in-edge list").isNotNull();
      final EdgeSegment headChunk = (EdgeSegment) db.lookupByRID(head, true);
      midChunk.set(headChunk.getPreviousRID());
      assertThat(midChunk.get()).as("the hub's degree must span more than one chunk").isNotNull();
    });

    db.transaction(() -> db.getSchema().getBucketById(midChunk.get().getBucketId()).deleteRecord(midChunk.get()));
  }

  /**
   * Overwrites the record's type byte with a value no type uses, so loading it throws. Reimplemented here rather
   * than reused from the engine's {@code TestHelper}: that copy is {@code protected static} on a class this module
   * does not extend, and {@code ha-raft} does not depend on the engine test-jar.
   * <p>
   * Runs through the wrapper, so the damaged page replicates and all three nodes hold the same unreadable record.
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

  /** Runs a CHECK DATABASE statement against one server and returns its single result row. */
  private Result runCheck(final int serverIndex, final String statement) {
    try (final ResultSet rs = wrapped(serverIndex).command("sql", statement)) {
      assertThat(rs.hasNext()).as("'%s' must return a result", statement).isTrue();
      return rs.next();
    }
  }

  /**
   * The hub's in-degree as seen by one server, or -1 when the edge list cannot be walked at all. Both outcomes are
   * "not {@link #DEGREE}", which is what the corruption assertions test: whether a broken mid-chain surfaces as a
   * throw or as a short count is an engine detail this test has no business pinning.
   */
  private long inDegreeOn(final int serverIndex, final RID hubRid) {
    return withResyncRetry(serverIndex, db -> {
      final AtomicLong degree = new AtomicLong(-1);
      db.transaction(() -> {
        try {
          degree.set(db.lookupByRID(hubRid, true).asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE));
        } catch (final DatabaseIsClosedException e) {
          // A snapshot-reinstall resync, not a finding: withResyncRetry retries it with a fresh handle.
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

  private long indexEntriesOn(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    return db.getSchema().existsIndex(INDEX_NAME) ? db.getSchema().getIndexByName(INDEX_NAME).countEntries() : -1L;
  }

  private long awaitIndexEntriesOn(final int serverIndex, final long expected) throws InterruptedException {
    return awaitValue(expected, () -> indexEntriesOn(serverIndex));
  }
}
