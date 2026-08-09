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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5779: {@code GraphEngine.moveEdge} re-points an edge by deleting the old edge record and creating a replacement,
 * and it reaches the removal through the public {@code GraphEngine.deleteEdge(Edge)}. That method performs the
 * PHYSICAL removal only - it is normally entered from {@code LocalDatabase.deleteRecordNoLock}, which has already
 * cleaned the record's index entries and its EXTERNAL property values by the time the edge arrives. {@code moveEdge}
 * is the one caller that does not come that way, so the old record's index entries survived it.
 * <p>
 * It used to look harmless because the bucket normally hands the just-freed slot straight back to the replacement,
 * leaving the stale entry pointing at a new record carrying the same key - indistinguishable from a correct entry.
 * That is an allocation coincidence, not an invariant. The fixtures below break it the way production would: the
 * edge type has SEVERAL buckets and the default round-robin selection puts the replacement in a DIFFERENT bucket
 * from the record just deleted, so the stale entry names a RID that no longer resolves.
 * <p>
 * The assertions go through {@code Index.countEntries()} and a raw {@code get()} on the index rather than through a
 * query: {@code SELECT} skips entries whose RID does not resolve, which is exactly what hides this leak.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5779MoveEdgeIndexCleanupTest extends TestHelper {

  private static final int BUCKETS = 4;

  /**
   * The leak itself, on a NOTUNIQUE index: after the move the index must hold exactly one entry for the edge's key,
   * and that entry must name the surviving record.
   */
  @Test
  void movingAnEdgeCleansTheOldRecordsIndexEntries() {
    createSchema(false);

    final RID[] rids = createTriangleWithOneEdge();
    final RID edgeRID = rids[0], farRID = rids[2];

    final Index index = database.getSchema().getIndexByName("Link[code]");
    assertThat(index.countEntries()).as("one edge, one index entry").isEqualTo(1L);

    final RID movedRID = moveEdgeIn(edgeRID, farRID);

    assertThat(movedRID).as("the replacement must land in a different bucket, otherwise the fixture proves nothing")
        .isNotEqualTo(edgeRID);

    assertThat(index.countEntries())
        .as("the entry of the deleted record must be gone: one edge, one entry").isEqualTo(1L);

    final List<RID> found = lookup(index, "E1");
    assertThat(found).as("the single surviving entry must name the replacement record").containsExactly(movedRID);

    database.transaction(() -> {
      assertThat(database.existsRecord(edgeRID)).as("the old record is deleted").isFalse();
      assertThat(database.countType("Link", false)).as("a move is not a duplication").isEqualTo(1L);
    });

    assertIntegrityClean();
  }

  /**
   * The sharper consequence of the same leak. The stale entry named a RID in a bucket whose slot is free, so an
   * UNRELATED edge created afterwards can be handed that exact slot - and then a lookup of the OLD key silently
   * returns the unrelated record. This is the failure mode that is worse than a plain dangling entry, because
   * nothing about the result looks wrong.
   */
  @Test
  void aFreedSlotReusedByAnUnrelatedEdgeIsNotReturnedForTheOldKey() {
    createSchema(false);

    final RID[] rids = createTriangleWithOneEdge();
    final RID edgeRID = rids[0], nearRID = rids[1], farRID = rids[2];

    moveEdgeIn(edgeRID, farRID);

    // Fill every bucket of the type so the freed slot of the deleted record is certainly handed out again.
    database.transaction(() -> {
      final Vertex near = nearRID.asVertex();
      final Vertex far = farRID.asVertex();
      for (int i = 0; i < BUCKETS * 3; i++)
        near.newEdge("Link", far, "code", "OTHER" + i).save();
    });

    final Index index = database.getSchema().getIndexByName("Link[code]");
    final List<RID> found = lookup(index, "E1");

    assertThat(found).as("the moved edge's key must resolve to exactly one record").hasSize(1);
    database.transaction(() -> assertThat(found.getFirst().asEdge().getString("code"))
        .as("a stale entry pointing at a reused slot returns a record carrying somebody else's key").isEqualTo("E1"));

    assertIntegrityClean();
  }

  /**
   * A UNIQUE index turns the leak from a silent wrong answer into a hard failure: the stale entry keeps the old key
   * taken, so a later legitimate insert of that key is rejected as a duplicate even though the record that owned it
   * is gone.
   */
  @Test
  void aUniqueIndexAcceptsTheKeyOfAMovedEdgeAfterItIsFreed() {
    createSchema(true);

    final RID[] rids = createTriangleWithOneEdge();
    final RID edgeRID = rids[0], nearRID = rids[1], farRID = rids[2];

    moveEdgeIn(edgeRID, farRID);

    final Index index = database.getSchema().getIndexByName("Link[code]");
    assertThat(index.countEntries()).as("a unique index must not hold two entries for one key").isEqualTo(1L);

    // Deleting the moved edge frees the key for good; re-inserting it must be accepted.
    database.transaction(() -> lookup(index, "E1").getFirst().asEdge().delete());
    database.transaction(() -> nearRID.asVertex().newEdge("Link", farRID.asVertex(), "code", "E1").save());

    assertThat(index.countEntries()).isEqualTo(1L);
    assertIntegrityClean();
  }

  /**
   * The same bypass, on the bucket record counter. {@code deleteRecordNoLock} folds a {@code -1} for the record it
   * removes and the create path folds a {@code +1}, so a move is net zero. Reaching the removal through
   * {@code deleteEdge} skipped the {@code -1} only, and the counter is what {@code count(*)} reads - and it is
   * persisted to {@code statistics.json}, so the drift survives a reopen. Asserted against a full scan
   * ({@code count()}), which is ground truth.
   */
  @Test
  void movingAnEdgeLeavesTheBucketRecordCounterAtTheNumberOfEdgesThatExist() {
    createSchema(false);

    final RID[] rids = createTriangleWithOneEdge();
    final RID edgeRID = rids[0], farRID = rids[2];

    moveEdgeIn(edgeRID, farRID);

    assertThat(scalar("select count() as n from Link")).as("ground truth: one edge survives the move").isEqualTo(1L);
    assertThat(scalar("select count(*) as n from Link"))
        .as("count(*) reads the cached bucket counter: a move must not inflate it").isEqualTo(1L);
    assertThat(database.countType("Link", false)).isEqualTo(1L);
  }

  /**
   * And on EXTERNAL property values. {@code moveEdge} writes FRESH external records for the replacement, so the
   * ones owned by the record it deletes have to go with it or they are orphaned in the paired bucket -
   * {@code deleteRecordNoLock} cascade-deletes them, {@code deleteEdge} does not. {@code CHECK DATABASE} counts
   * exactly this leak.
   */
  @Test
  void movingAnEdgeCascadeDeletesTheOldRecordsExternalValues() {
    final VertexType node = database.getSchema().createVertexType("Node", BUCKETS);
    node.createProperty("name", Type.STRING);
    database.getSchema().createEdgeType("Link", BUCKETS).createProperty("blob", Type.STRING).setExternal(true);

    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex near = database.newVertex("Node").set("name", "near").save();
      final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
      final MutableVertex far = database.newVertex("Node").set("name", "far").save();
      rids[0] = near.newEdge("Link", far0, "blob", "x".repeat(4096)).save().getIdentity();
      rids[1] = near.getIdentity();
      rids[2] = far.getIdentity();
    });

    final RID movedRID = moveEdgeIn(rids[0], rids[2]);

    database.transaction(() -> assertThat(movedRID.asEdge().getString("blob"))
        .as("the replacement must still carry the external value").isEqualTo("x".repeat(4096)));

    assertOrphanedExternalRecords(0L);
    assertIntegrityClean();
  }

  // ---------------------------------------------------------------------------------------------------------------

  private void createSchema(final boolean unique) {
    final VertexType node = database.getSchema().createVertexType("Node", BUCKETS);
    node.createProperty("name", Type.STRING);
    database.getSchema().createEdgeType("Link", BUCKETS).createProperty("code", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, unique, "Link", "code");
  }

  /**
   * Three vertices and a single {@code near -> far0} edge carrying the indexed key {@code E1}. Returns
   * {@code [edge, near, far]}.
   */
  private RID[] createTriangleWithOneEdge() {
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex near = database.newVertex("Node").set("name", "near").save();
      final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
      final MutableVertex far = database.newVertex("Node").set("name", "far").save();
      rids[0] = near.newEdge("Link", far0, "code", "E1").save().getIdentity();
      rids[1] = near.getIdentity();
      rids[2] = far.getIdentity();
    });
    return rids;
  }

  /**
   * Re-points the IN endpoint of {@code edgeRID} at {@code newInRID} through the embedded API and returns the RID of
   * the replacement record.
   */
  private RID moveEdgeIn(final RID edgeRID, final RID newInRID) {
    final RID[] moved = new RID[1];
    database.transaction(() -> {
      final MutableEdge edge = edgeRID.asEdge().modify();
      // set("@in") IS the move: GraphEngine.moveEdge saves the replacement itself and re-points this instance at it,
      // so there is nothing left to save afterwards.
      edge.set("@in", newInRID);
      moved[0] = edge.getIdentity();
    });
    return moved[0];
  }

  private List<RID> lookup(final Index index, final String key) {
    final List<RID> result = new ArrayList<>();
    database.transaction(() -> {
      final IndexCursor cursor = index.get(new Object[] { key });
      while (cursor.hasNext()) {
        final var next = cursor.next();
        if (next != null)
          result.add(next.getIdentity());
      }
    });
    return result;
  }

  private long scalar(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private void assertOrphanedExternalRecords(final long expected) {
    try (final ResultSet rs = database.command("sql", "check database")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(longProperty(row, "orphanedExternalRecords"))
            .as("check database orphanedExternalRecords: " + row.toJSON()).isEqualTo(expected);
      }
    }
  }

  private void assertIntegrityClean() {
    try (final ResultSet rs = database.command("sql", "check database")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(longProperty(row, "autoFix")).as("check database autoFix: " + row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalErrors")).as("check database totalErrors: " + row.toJSON()).isEqualTo(0L);
      }
    }
  }

  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
  }
}
