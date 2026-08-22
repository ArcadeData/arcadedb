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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.VertexNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * #6586: the two paths #6572 did not reach, where a caller meets a vertex whose RECORD is gone.
 * <p>
 * #6572 split "the vertex is not there" out of {@code GraphEngine.getEdgeHeadChunkForWrite}'s retryable conflict
 * using the RID the {@code RecordNotFoundException} names, and gave it a non-retryable
 * {@link VertexNotFoundException} carrying a repair that applies. That split fires only when the head-pointer read
 * has to go back to the bucket, so two adjacent paths still answered the same fact in their own, worse way:
 * <ul>
 *   <li>a MATERIALISED handle - a RID whose content was loaded earlier in the transaction and used after the
 *   record went away - answers its head pointers out of the buffer it already holds, so the whole removal walk ran
 *   on stale heads and only the re-read at the end of the delete noticed. It then reported a
 *   {@link ConcurrentModificationException} asserting the vertex "was deleted by a concurrent transaction", which
 *   was retryable (a full budget spent on a foregone failure) and, single-threaded, simply untrue;</li>
 *   <li>the APPEND path ({@code getOrCreateEdgeList}) read the head RID OUTSIDE its {@code try}, so the lazy load
 *   escaped as a bare {@code Record #x:y not found}: the right verdict, and no diagnosis at all - not that the
 *   record is a VERTEX, not that it is an ENDPOINT of an edge being created, not which side of the list, and no
 *   repair to run.</li>
 * </ul>
 * Both sides of the same list must now answer alike, and the delete must decide it before it walks anything.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6586MissingVertexDiagnosisTest extends TestHelper {
  private RID srcRID;
  private RID targetRID;

  /** Most of these tests deliberately leave a reference into a deleted record, so the end-of-test check would fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  // ---------------------------------------------------------------------------------------------------------
  // ITEM 1 - the removal side reached through a materialised handle
  // ---------------------------------------------------------------------------------------------------------

  /**
   * The reported shape: single-threaded, nothing else touching the database, and the failure nonetheless named a
   * concurrent transaction as the cause. It must name what actually happened, and must not advertise a retry that
   * cannot work.
   */
  @Test
  void aDeleteThroughAMaterialisedHandleIsNotReportedAsAConcurrentDelete() {
    createGraph();

    final Throwable thrown = catchThrowable(
        () -> database.transaction(this::deleteTargetThroughAMaterialisedHandle, false, 1));

    assertThat(thrown).isInstanceOf(VertexNotFoundException.class).isInstanceOf(RecordNotFoundException.class);
    assertThat(thrown).as("a record that is gone must not be advertised as retryable: %s", thrown)
        .isNotInstanceOf(NeedRetryException.class);
    assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);

    assertThat(thrown.getMessage()).as("%s", thrown.getMessage())
        .contains(targetRID.toString())
        .contains("does not exist")
        .contains("cannot be deleted")
        .doesNotContain("concurrent");

    // Every other failure this delete can produce says how to recover; this one used to say nothing.
    assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains("CHECK DATABASE FIX");
    assertThat(scopedRepairCommandIn(thrown.getMessage()))
        .as("no runnable RECORD-scoped command may appear, its target is the record that is gone: %s",
            thrown.getMessage())
        .isNull();
  }

  /**
   * The cost the old diagnosis hid: the whole edge walk ran, disconnecting every edge from its FAR endpoint, only
   * to reach a failure that was decidable up front.
   * <p>
   * Proven with a TRIPWIRE rather than by inspection, and the tripwire is armed first so the assertion cannot pass
   * vacuously: src's edge-list head chunk is destroyed, which makes the walk itself fail with a conflict naming
   * that chunk. The first half of this test shows the walk really does run and really does hit it; the second half
   * shows that with the vertex record gone the delete never gets there.
   */
  @Test
  void theDeleteIsRefusedBeforeTheWalkDisconnectsAnything() {
    createGraph();

    final RID headChunk = outHeadChunk(srcRID);
    deleteRecordOf(headChunk);

    final Throwable walked = catchThrowable(() -> database.transaction(() -> targetRID.asVertex().delete(), false, 1));
    assertThat(walked).as("tripwire: with the vertex present the walk must run and fail on src's unreadable list")
        .isInstanceOf(ConcurrentModificationException.class);
    assertThat(walked.getMessage()).as("%s", walked.getMessage()).contains(headChunk.toString());

    final Throwable refused = catchThrowable(
        () -> database.transaction(this::deleteTargetThroughAMaterialisedHandle, false, 1));

    assertThat(refused).as("the missing vertex must be decided before the walk: %s", refused)
        .isInstanceOf(VertexNotFoundException.class);
    assertThat(refused.getMessage()).as("the walk never ran, so no chunk of it may appear: %s", refused.getMessage())
        .doesNotContain(headChunk.toString());
  }

  /** The half that hurt a batch job: the type is what the retry machinery reads. Counted, not inferred. */
  @Test
  void theDeleteDoesNotSpendTheRetryBudget() {
    createGraph();

    final AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      deleteTargetThroughAMaterialisedHandle();
    }, false, 3)).isInstanceOf(VertexNotFoundException.class);

    assertThat(attempts.get()).as("a failure that can never succeed must not be retried").isEqualTo(1);
  }

  /**
   * {@code force} is the documented escape hatch from every other refusal this delete can raise - a list whose
   * chunks cannot be read, a body that will not decode - and it is precisely the flag a later reader would assume
   * should suppress this one too. It must not: a record that is not there cannot be deleted by insisting, and
   * succeeding quietly would hide the reference that outlived it. Pinned so the intent survives that assumption.
   */
  @Test
  void forceDoesNotSuppressAMissingVertexRecord() {
    createDanglingReference();

    final GraphEngine graphEngine = ((DatabaseInternal) database).getGraphEngine();

    final Throwable thrown = catchThrowable(() -> database.transaction(() -> graphEngine.deleteVertex(
        (VertexInternal) database.lookupByRID(targetRID, false).asVertex(), true), false, 1));

    assertThat(thrown).as("force must not turn a record that is gone into a deleted one: %s", thrown)
        .isInstanceOf(VertexNotFoundException.class);
    assertThat(thrown).isNotInstanceOf(NeedRetryException.class);
    assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);

    // And the tripwire for that assertion: the SAME call on a vertex that does exist goes through, so the failure
    // above is about the missing record rather than about force being unusable from here.
    database.transaction(
        () -> graphEngine.deleteVertex((VertexInternal) database.lookupByRID(srcRID, true).asVertex(), true));
    database.transaction(() -> assertThat(database.existsRecord(srcRID)).isFalse());
  }

  /**
   * The other lever that turns a refusal into a forced delete, and the reason it cannot reach this one:
   * {@code LocalDatabase.deleteRecordNoLock} re-runs {@code deleteVertex} with {@code force} when a
   * {@link ConcurrentModificationException} escapes AND the chain is confirmed broken - and since #6572 a missing
   * vertex is no longer a {@code ConcurrentModificationException} at all, so that arm cannot see it. Pinned with
   * the opt-in actually ON, because "it does not apply" is exactly the kind of claim that quietly stops holding.
   */
  @Test
  void theBrokenChainOptInDoesNotForceThroughAMissingVertexRecord() {
    createDanglingReference();

    final Object previous = database.getConfiguration().getValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN);
    database.getConfiguration().setValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN, true);
    try {
      final Throwable thrown = catchThrowable(() -> database.transaction(() -> {
        for (final Vertex v : srcRID.asVertex().getVertices(Vertex.DIRECTION.OUT, "E1"))
          v.delete();
      }, false, 1));

      assertThat(thrown).as("the broken-chain opt-in must not absorb a vertex that is not there: %s", thrown)
          .isInstanceOf(VertexNotFoundException.class);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN, previous);
    }
  }

  /** The same answer whichever handle the caller holds: a LAZY one (#6572's path) and a materialised one agree. */
  @Test
  void bothHandleShapesAnswerTheMissingVertexWithOneCatchableType() {
    createDanglingReference();

    final Throwable throughAStaleEntry = catchThrowable(() -> database.transaction(() -> {
      for (final Vertex v : srcRID.asVertex().getVertices(Vertex.DIRECTION.OUT, "E1"))
        v.delete();
    }, false, 1));

    createGraph();
    final Throwable throughAMaterialisedHandle = catchThrowable(
        () -> database.transaction(this::deleteTargetThroughAMaterialisedHandle, false, 1));

    assertThat(throughAStaleEntry).isInstanceOf(VertexNotFoundException.class);
    assertThat(throughAMaterialisedHandle).isInstanceOf(VertexNotFoundException.class);
  }

  // ---------------------------------------------------------------------------------------------------------
  // ITEM 2 - the append side
  // ---------------------------------------------------------------------------------------------------------

  /**
   * Creating an edge whose TARGET record is gone used to raise {@code RecordNotFoundException: Record #x:y not
   * found} - a verdict with no diagnosis. It must say the same things the removal side says: that the missing
   * record is a vertex, which side of the list was being written, and what to run.
   */
  @Test
  void theAppendPathNamesTheMissingEndpointVertexTheWayTheRemovalSideDoes() {
    createDanglingReference();

    final Throwable thrown = catchThrowable(() -> database.transaction(this::linkEverythingSrcPointsAtToItself, false, 1));

    assertThat(thrown).isInstanceOf(VertexNotFoundException.class).isInstanceOf(RecordNotFoundException.class);
    assertThat(thrown).as("unchanged verdict: a record that is gone was never retryable here either: %s", thrown)
        .isNotInstanceOf(NeedRetryException.class);
    assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);

    assertThat(thrown.getMessage()).as("%s", thrown.getMessage())
        .contains(targetRID.toString())
        .contains("does not exist")
        // The endpoint that is gone is the TARGET, so the side named must be the one being written on it.
        .contains(Vertex.DIRECTION.IN + " edge list")
        .contains("CHECK DATABASE FIX");
    assertThat(scopedRepairCommandIn(thrown.getMessage())).as("%s", thrown.getMessage()).isNull();

    assertThat(thrown.getCause()).as("the diagnosis must WRAP the not-found, not replace it")
        .isInstanceOf(RecordNotFoundException.class);
  }

  /** The other side of the append: the SOURCE vertex is the one that is gone, so OUT is the side named. */
  @Test
  void theAppendPathNamesTheMissingSourceVertexToo() {
    createGraph();
    deleteRecordOf(srcRID);

    // A LAZY handle, the shape a stale RID from a previous traversal step has: resolving it eagerly would fail in
    // the caller's own lookup and never reach the append at all.
    final Throwable thrown = catchThrowable(() -> database.transaction(
        () -> database.lookupByRID(srcRID, false).asVertex().newEdge("E1", targetRID.asVertex()), false, 1));

    assertThat(thrown).isInstanceOf(VertexNotFoundException.class);
    assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(srcRID);
    assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains(Vertex.DIRECTION.OUT + " edge list");
  }

  /**
   * The guard against the fix creeping over the case it must not touch: a vertex that EXISTS whose head chunk
   * cannot be read is still the publication window #5670 answers with a retryable conflict on the append path,
   * exactly as {@code getEdgeHeadChunkForWrite} keeps it on the removal path.
   */
  @Test
  void theAppendPathKeepsItsRetryableConflictForAnUnreadableHeadChunk() {
    createGraph();

    final RID headChunk = outHeadChunk(srcRID);
    deleteRecordOf(headChunk);

    final Throwable thrown = catchThrowable(() -> database.transaction(
        () -> srcRID.asVertex().newEdge("E1", targetRID.asVertex()), false, 1));

    assertThat(thrown).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
    assertThat(thrown).isNotInstanceOf(VertexNotFoundException.class);
    assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains(headChunk.toString());
  }

  /** The advice is only worth printing if it repairs the database, so this runs it. */
  @Test
  void theRepairTheAppendPathAdvertisesIsTheOneThatWorks() {
    createDanglingReference();

    assertThatThrownBy(() -> database.transaction(this::linkEverythingSrcPointsAtToItself, false, 1))
        .isInstanceOf(VertexNotFoundException.class);

    try (final ResultSet rs = database.command("sql", "check database fix")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(longProperty(row, "autoFix")).as("the fix must repair something: %s", row.toJSON()).isGreaterThan(0L);
    }

    // The stale entry is gone, so the sweep that could not run at all now completes - over nothing.
    database.transaction(this::linkEverythingSrcPointsAtToItself);
    database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(0L));

    assertIntegrityClean();
  }

  // ---------------------------------------------------------------------------------------------------------
  // The probe must not change what a healthy delete does
  // ---------------------------------------------------------------------------------------------------------

  /**
   * The slot probe reads the BUCKET, not the transaction's record caches, so it has to see a record this very
   * transaction created a statement earlier - whose slot is allocated eagerly at save() but whose only in-memory
   * form is the transaction's own copy. If that assumption were wrong, create-then-delete would stop working.
   */
  @Test
  void aVertexCreatedAndDeletedInTheSameTransactionStillDeletes() {
    createGraph();

    final RID[] created = new RID[1];
    database.transaction(() -> {
      final MutableVertex extra = database.newVertex("Target");
      extra.save();
      created[0] = extra.getIdentity();
      srcRID.asVertex().newEdge("E1", extra);
      extra.delete();
    });

    database.transaction(() -> {
      assertThat(database.existsRecord(created[0])).isFalse();
      assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1L);
    });
    assertIntegrityClean();
  }

  /** An ordinary delete of a healthy connected vertex is untouched, edges and all. */
  @Test
  void anOrdinaryVertexDeleteIsUnaffected() {
    createGraph();

    database.transaction(() -> targetRID.asVertex().delete());

    database.transaction(() -> {
      assertThat(database.existsRecord(targetRID)).isFalse();
      assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(0L);
    });
    assertIntegrityClean();
  }

  /** Deleting the same vertex twice in one transaction is the plainest form of "a RID held past the delete". */
  @Test
  void deletingTheSameVertexTwiceInOneTransactionSaysItIsGone() {
    createGraph();

    final Throwable thrown = catchThrowable(() -> database.transaction(() -> {
      final Vertex target = targetRID.asVertex();
      target.delete();
      target.delete();
    }, false, 1));

    assertThat(thrown).isInstanceOf(VertexNotFoundException.class);
    assertThat(thrown).isNotInstanceOf(NeedRetryException.class);
    assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);
  }

  // ---------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------

  /** {@code src --E1--> target}, plain vertex types, single-threaded, nothing else writing the database. */
  private void createGraph() {
    if (!database.getSchema().existsType("Src"))
      database.transaction(() -> {
        database.getSchema().createVertexType("Src", 1);
        database.getSchema().createVertexType("Target", 1);
        database.getSchema().createEdgeType("E1", 1);
      });

    database.transaction(() -> {
      final MutableVertex src = database.newVertex("Src");
      src.save();
      final MutableVertex target = database.newVertex("Target");
      target.save();
      srcRID = src.getIdentity();
      targetRID = target.getIdentity();
      src.newEdge("E1", target);
    });
  }

  /**
   * The same graph with target's RECORD dropped straight out of its bucket and the adjacency entry on src left in
   * place: the state a pre-#5670 best-effort edge delete left behind, reproduced without any concurrency.
   */
  private void createDanglingReference() {
    createGraph();
    deleteRecordOf(targetRID);

    database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1L));
  }

  /**
   * A RID held past its record: the buffer is loaded first, so every later read of the vertex answers from it and
   * touches no bucket at all - which is why #6572's discriminator, which lives in a read that DOES go back to the
   * bucket, never runs on this shape.
   */
  private void deleteTargetThroughAMaterialisedHandle() {
    final Vertex target = database.lookupByRID(targetRID, true).asVertex();
    assertThat(target.getPropertyNames()).as("the buffer must be materialised for this fixture to mean anything")
        .isNotNull();

    database.getSchema().getBucketById(targetRID.getBucketId()).deleteRecord(targetRID);

    target.delete();
  }

  /** Walk what src points at and create an edge back: reaching the missing target through the stale entry. */
  private void linkEverythingSrcPointsAtToItself() {
    for (final Vertex v : srcRID.asVertex().getVertices(Vertex.DIRECTION.OUT, "E1"))
      srcRID.asVertex().newEdge("E1", v);
  }

  private RID outHeadChunk(final RID vertexRID) {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getOutEdgesHeadChunk());
    assertThat(holder[0]).as("the vertex must have an outgoing edge list").isNotNull();
    return holder[0];
  }

  private void deleteRecordOf(final RID rid) {
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
    database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
  }

  /** The {@code CHECK DATABASE RECORD <rid> FIX} command a message tells the operator to run, or null. */
  private static String scopedRepairCommandIn(final String message) {
    final int start = message.indexOf("`CHECK DATABASE RECORD ");
    if (start < 0)
      return null;
    final int end = message.indexOf('`', start + 1);
    return end < 0 ? null : message.substring(start + 1, end);
  }

  /** Asserts on the fields {@code check database} actually reports, so a typo cannot make this vacuously pass. */
  private void assertIntegrityClean() {
    try (final ResultSet rs = database.command("sql", "check database")) {
      assertThat(rs.hasNext()).isTrue();
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(longProperty(row, "autoFix")).as("autoFix: %s", row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "invalidLinks")).as("invalidLinks: %s", row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalWarnings")).as("totalWarnings: %s", row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalCorruptedRecords")).as("totalCorruptedRecords: %s", row.toJSON())
            .isEqualTo(0L);
      }
    }
  }

  /** Reads a numeric check-database property, failing loudly when the field does not exist (a vacuous assertion). */
  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    assertThat(value).as("check database must report '%s': %s", name, row.toJSON()).isNotNull();
    return ((Number) value).longValue();
  }
}
