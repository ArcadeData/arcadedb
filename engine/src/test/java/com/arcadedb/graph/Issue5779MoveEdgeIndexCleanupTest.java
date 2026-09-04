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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  private static final int    BUCKETS         = 4;
  /** Long enough to be found verbatim in the page, and distinctive enough not to collide with anything else. */
  private static final String CORRUPT_MARKER  = "CORRUPT5779MARKERVALUE";

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
    database.transaction(() -> assertThat(found.get(0).asEdge().getString("code"))
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
    database.transaction(() -> lookup(index, "E1").get(0).asEdge().delete());
    database.transaction(() -> nearRID.asVertex().newEdge("Link", farRID.asVertex(), "code", "E1").save());

    assertThat(index.countEntries()).isEqualTo(1L);
    assertIntegrityClean();
  }

  /**
   * The OUT side takes the same route with {@code Vertex.DIRECTION.OUT}, and nothing about the cleanup is
   * direction-specific - which is exactly why it is asserted rather than assumed.
   */
  @Test
  void movingTheOutEndpointCleansTheOldRecordsIndexEntriesToo() {
    createSchema(false);

    final RID[] rids = createTriangleWithOneEdge();
    final RID edgeRID = rids[0], farRID = rids[2];

    final RID[] moved = new RID[1];
    database.transaction(() -> {
      final MutableEdge edge = edgeRID.asEdge().modify();
      edge.set("@out", farRID);
      moved[0] = edge.getIdentity();
    });

    final Index index = database.getSchema().getIndexByName("Link[code]");
    assertThat(moved[0]).isNotEqualTo(edgeRID);
    assertThat(index.countEntries()).isEqualTo(1L);
    assertThat(lookup(index, "E1")).containsExactly(moved[0]);
    database.transaction(() -> assertThat(moved[0].asEdge().getOut()).isEqualTo(farRID));

    assertIntegrityClean();
  }

  /**
   * A move that ALSO changes an indexed property in the same session. {@code DocumentIndexer.deleteDocument} builds
   * the key it removes from the LIVE property values of the record handed to it, and after
   * {@code edge.set("code", ...)} those are the new ones - the index still holds the old key. The cleanup therefore
   * has to run against the state the index was built from, not against the caller's in-flight edits, or it removes
   * a key that was never there and leaves the real entry dangling: the very leak this fix exists to close, reached
   * through a different door.
   */
  @Test
  void movingAnEdgeThatAlsoChangedItsIndexedPropertyCleansTheKeyTheIndexActuallyHolds() {
    createSchema(false);

    final RID[] rids = createTriangleWithOneEdge();
    final RID edgeRID = rids[0], farRID = rids[2];

    final RID[] moved = new RID[1];
    database.transaction(() -> {
      final MutableEdge edge = edgeRID.asEdge().modify();
      edge.set("code", "E2");
      edge.set("@in", farRID);
      moved[0] = edge.getIdentity();
    });

    final Index index = database.getSchema().getIndexByName("Link[code]");
    assertThat(lookup(index, "E1")).as("the key the record used to carry must be gone").isEmpty();
    assertThat(lookup(index, "E2")).as("the key it carries now must name the surviving record")
        .containsExactly(moved[0]);
    assertThat(index.countEntries()).as("one edge, one entry").isEqualTo(1L);

    assertIntegrityClean();
  }

  /**
   * The same move, but with the indexed property already SAVED before it. The record's buffer stays frozen until
   * commit (serialization is deferred), so it still describes {@code E1} while the index has already been moved to
   * {@code E2} by that save - the committed buffer is the wrong pre-image here, and the transaction's indexed
   * snapshot (#4935) is the right one.
   */
  @Test
  void movingAnEdgeAfterAnIndexedPropertyWasAlreadySavedInTheSameTransaction() {
    createSchema(false);

    final RID[] rids = createTriangleWithOneEdge();
    final RID edgeRID = rids[0], farRID = rids[2];

    final RID[] moved = new RID[1];
    database.transaction(() -> {
      final MutableEdge edge = edgeRID.asEdge().modify();
      edge.set("code", "E2");
      edge.save();
      edge.set("@in", farRID);
      moved[0] = edge.getIdentity();
    });

    final Index index = database.getSchema().getIndexByName("Link[code]");
    assertThat(lookup(index, "E1")).as("the committed key was already replaced by the save").isEmpty();
    assertThat(lookup(index, "E2")).as("the key the index actually held must name the surviving record")
        .containsExactly(moved[0]);
    assertThat(index.countEntries()).as("one edge, one entry").isEqualTo(1L);

    assertIntegrityClean();
  }

  /**
   * A move of an edge CREATED in the same transaction, with its indexed property changed in between - the sequence
   * that looks like it should reach {@code indexedImageOf}'s buffer-less fallback and does not: {@code newEdge()}
   * saves the edge, the save serializes, so it arrives with a committed buffer like any other record and the
   * ordinary pre-image path handles it. Worth pinning precisely because the intuition points the other way.
   */
  @Test
  void movingAnEdgeCreatedInTheSameTransactionUsesItsCommittedPreImage() {
    createSchema(false);

    final RID[] moved = new RID[1];
    database.transaction(() -> {
      final MutableVertex near = database.newVertex("Node").set("name", "near").save();
      final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
      final MutableVertex far = database.newVertex("Node").set("name", "far").save();
      final MutableEdge edge = near.newEdge("Link", far0, "code", "E1");
      edge.set("code", "E2");
      edge.set("@in", far.getIdentity());
      moved[0] = edge.getIdentity();
    });

    final Index index = database.getSchema().getIndexByName("Link[code]");
    assertThat(index.countEntries()).as("one edge, one entry - no entry left over from the create").isEqualTo(1L);
    assertThat(lookup(index, "E2")).containsExactly(moved[0]);
    assertThat(lookup(index, "E1")).isEmpty();
    assertThat(scalar("select count() as n from Link")).isEqualTo(1L);
    assertThat(database.countType("Link", false)).isEqualTo(1L);

    assertIntegrityClean();
  }

  /**
   * The BOUNDARY of the guarantee, measured rather than assumed, because it is the one case where
   * {@code cleanUpBeforePhysicalDelete} cannot deliver: an indexed property whose value cannot be deserialized.
   * <p>
   * This fixture uses the #4420 shape - a corrupt length prefix on an inline STRING. The reader TOLERATES it by
   * reporting the value as ABSENT rather than throwing (see {@code Issue4420TolerantDeleteTest}), so nothing here
   * can learn the key the index actually holds, and the entry for the deleted record survives. That is not a
   * regression this fix introduces and not one it can close: {@code LocalDatabase.deleteRecordNoLock} degrades
   * identically on the same record, by design and with a logged warning. CHECK DATABASE does not detect a dangling
   * LSM entry either ({@code totalErrors} 0, {@code corruptedIndexes} empty), so this test is what records the
   * boundary instead.
   * <p>
   * What the move must still get right, and does: the record is removed, the counter stays truthful, and no key is
   * fabricated for the replacement.
   */
  @Test
  void movingAnEdgeWithAnUnreadablePropertyIsBestEffortLikeTheDeletePath() {
    createSchema(false);

    final RID[] rids = createTriangleWithOneEdge(CORRUPT_MARKER);
    final RID edgeRID = rids[0], farRID = rids[2];

    corruptPropertyLength(edgeRID);
    // Reopen so the record is re-read from the corrupted page instead of the in-memory record cache. Every RID
    // captured before this point still names the closed instance, so rebind them to the reopened one.
    reopenDatabase();
    final RID staleEdgeRID = RID.create(database, edgeRID.getBucketId(), edgeRID.getPosition());
    final RID newInRID = RID.create(database, farRID.getBucketId(), farRID.getPosition());

    final Index index = database.getSchema().getIndexByName("Link[code]");
    final RID movedRID = moveEdgeIn(staleEdgeRID, newInRID);

    database.transaction(
        () -> assertThat(database.existsRecord(staleEdgeRID)).as("the old record is still removed").isFalse());
    assertThat(scalar("select count() as n from Link")).as("a move is still not a duplication").isEqualTo(1L);
    assertThat(database.countType("Link", false))
        .as("and the counter stays truthful even on the degraded path").isEqualTo(1L);
    assertThat(movedRID).isNotEqualTo(staleEdgeRID);

    database.transaction(() -> assertThat((Object) movedRID.asEdge().get("code"))
        .as("the unreadable value cannot be carried over, and must not be invented either").isNull());

    // The boundary itself: the entry keyed by the unreadable value outlives the record. Asserted so that a future
    // change able to recover the key (or a decision to fail the move instead) shows up here rather than silently.
    assertThat(lookup(index, CORRUPT_MARKER))
        .as("unreadable key: the stale entry survives, exactly as it does on the ordinary delete path")
        .containsExactly(staleEdgeRID);
  }

  /**
   * {@code cleanUpBeforePhysicalDelete} returns early for a LIGHTWEIGHT edge, because {@code deleteEdge} performs
   * no physical removal for one: there is no index entry, no external value and no counted record, and folding a
   * {@code -1} would drift the cached bucket count the other way and persist that drift to
   * {@code statistics.json}. That branch is currently UNREACHABLE, and this test is what says so out loud - both
   * doors into {@code moveEdge} are shut for a lightweight edge, and if either is ever opened this fails and
   * points at the guard that is then load-bearing rather than defensive.
   */
  @Test
  void aLightweightEdgeCannotReachMoveEdgeAtAll() {
    database.getSchema().createVertexType("Node", BUCKETS).createProperty("name", Type.STRING);
    database.getSchema().buildEdgeType().withName("Link").withTotalBuckets(BUCKETS).withLightweight(true).create();

    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex near = database.newVertex("Node").set("name", "near").save();
      final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
      near.newEdge("Link", far0);
      rids[0] = near.getIdentity();
      rids[1] = far0.getIdentity();
    });

    assertThat(database.countType("Link", false))
        .as("a lightweight edge allocates no record, so there is nothing for the cleanup to account for")
        .isEqualTo(0L);

    // Door 1: a persisted lightweight edge reached through its endpoint's list cannot be made mutable at all.
    database.transaction(() -> assertThatThrownBy(
        () -> rids[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "Link").iterator().next().asEdge().modify())
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("cannot be modified"));

    // Door 2: the MutableLightEdge newEdge() hands back inside the creating session refuses set() outright, so
    // the "@in"/"@out" override in MutableEdge - the only caller of moveEdge - is never entered.
    database.transaction(() -> assertThatThrownBy(
        () -> rids[0].asVertex().modify().newEdge("Link", rids[1].asVertex()).set("@in", rids[0]))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("LIGHTWEIGHT"));

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
    return createTriangleWithOneEdge("E1");
  }

  private RID[] createTriangleWithOneEdge(final String code) {
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex near = database.newVertex("Node").set("name", "near").save();
      final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
      final MutableVertex far = database.newVertex("Node").set("name", "far").save();
      rids[0] = near.newEdge("Link", far0, "code", code).save().getIdentity();
      rids[1] = near.getIdentity();
      rids[2] = far.getIdentity();
    });
    return rids;
  }

  /**
   * The #4420 corruption shape, applied to the edge's indexed STRING property: the length-prefix varint of the
   * inline value is overwritten with one that decodes above {@link Integer#MAX_VALUE}. The record's declared size
   * in the page record table is left alone, so the record still loads - only deserializing that property fails.
   * Lifted from {@code Issue4420TolerantDeleteTest}, which pins the same shape on the delete path.
   */
  private void corruptPropertyLength(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = rid.getBucketId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final byte[] marker = CORRUPT_MARKER.getBytes(StandardCharsets.UTF_8);

    final Binary varintBuffer = new Binary();
    // 4_294_967_245L == (2^32 - 51); cast to int it becomes -51, the exact value reported in issue #4420.
    varintBuffer.putUnsignedNumber(4_294_967_245L);
    final byte[] corruptVarint = varintBuffer.toByteArray();

    db.transaction(() -> {
      try {
        // Page 0: the fixture puts a single edge in this bucket, so its slot is on the first page.
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
        final byte[] content = new byte[page.getContentSize()];
        page.readByteArray(0, content);

        final int markerStart = indexOf(content, marker);
        assertThat(markerStart).as("marker value must be present inline in the page").isGreaterThan(0);

        // The byte immediately before the inline value is its length varint; overwrite it (and the bytes it now
        // spans) with the multi-byte corrupt length. The marker is long enough that this stays inside the record.
        for (int i = 0; i < corruptVarint.length; i++)
          page.writeByte(markerStart - 1 + i, corruptVarint[i]);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static int indexOf(final byte[] haystack, final byte[] needle) {
    outer:
    for (int i = 0; i <= haystack.length - needle.length; i++) {
      for (int j = 0; j < needle.length; j++)
        if (haystack[i + j] != needle[j])
          continue outer;
      return i;
    }
    return -1;
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
