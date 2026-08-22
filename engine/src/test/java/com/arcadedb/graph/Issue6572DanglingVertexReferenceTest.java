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
 * #6572: meeting a DANGLING VERTEX REFERENCE must be reported as what it is.
 * <p>
 * An adjacency entry whose target record was removed without the entry being disconnected - the documented legacy
 * of a pre-#5670 best-effort edge delete - hands a caller a vertex handle to a record that is not there. Deleting
 * through it reached {@code GraphEngine.getEdgeHeadChunkForWrite}, whose lazy load of the vertex raised
 * {@code RecordNotFoundException}, and that was converted wholesale into the retryable conflict the method exists
 * to produce for an unreadable CHUNK. Two things were then wrong at once, and both hurt a batch job:
 * <ul>
 *   <li>the type said "retry me" for a record that is gone, so every attempt failed identically, the retry budget
 *   was spent on a foregone conclusion and the whole transaction rolled back - one stale reference killing a
 *   nightly sweep, permanently;</li>
 *   <li>the advice said {@code CHECK DATABASE RECORD <rid> FIX}, which rebuilds the edge list OF that record from
 *   the surviving edges - and that record is precisely the one that no longer exists.</li>
 * </ul>
 * The evidence separating the two cases was already on the exception: the RID it names. When it is the vertex's
 * own, this is not a publication window, and these tests pin that it is answered with a non-retryable
 * {@link VertexNotFoundException} naming a repair that actually applies - while the CHUNK case keeps the conflict
 * and the scoped advice #5764 gave it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6572DanglingVertexReferenceTest extends TestHelper {
  private RID srcRID;
  private RID targetRID;
  private RID edgeRID;

  /** These tests deliberately leave a reference into a deleted record, so the end-of-test check would always fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * The reported shape, single-threaded and with nothing else touching the database: the vertex named by the
   * failure and the record reported missing are the SAME RID, which is the one thing a concurrent commit in flight
   * can never produce.
   */
  @Test
  void aDeleteReachedThroughAStaleReferenceFailsAsNotFoundRatherThanAsAConflict() {
    createDanglingReference();

    final Throwable thrown = catchThrowable(() -> database.transaction(this::deleteEverythingSrcPointsAt, false, 3));

    assertThat(thrown).isInstanceOf(VertexNotFoundException.class).isInstanceOf(RecordNotFoundException.class);
    assertThat(thrown).as("a record that is gone must not be advertised as retryable: %s", thrown)
        .isNotInstanceOf(NeedRetryException.class);
    assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);

    assertThat(thrown.getMessage()).as("%s", thrown.getMessage())
        .contains(targetRID.toString())
        .contains("does not exist")
        .doesNotContain("concurrent commit in flight");

    // The advice must name a repair that can be run, and must not hand the operator a copy-pasteable command
    // aimed at the record that is gone - which is precisely what the conflict this replaces used to do.
    assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains("CHECK DATABASE FIX");
    assertThat(scopedRepairCommandIn(thrown.getMessage()))
        .as("no runnable RECORD-scoped command may appear: %s", thrown.getMessage()).isNull();
    assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).doesNotContain("CHECK DATABASE RECORD " + targetRID);

    // The original not-found is kept, so the trace still says which record and which read found it missing.
    assertThat(thrown.getCause()).as("the diagnosis must WRAP the not-found, not replace it")
        .isInstanceOf(RecordNotFoundException.class);
  }

  /**
   * The half of the report that hurt most: the exception type is what the retry machinery reads, so reporting a
   * gone record as a conflict re-ran the whole transaction to fail identically each time. Counted rather than
   * inferred - the block must run exactly ONCE out of the three attempts offered.
   */
  @Test
  void theFailureDoesNotSpendTheRetryBudget() {
    createDanglingReference();

    final AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      deleteEverythingSrcPointsAt();
    }, false, 3)).isInstanceOf(VertexNotFoundException.class);

    assertThat(attempts.get()).as("a failure that can never succeed must not be retried").isEqualTo(1);
  }

  /**
   * The advice is only worth printing if it repairs the database, so this runs it. {@code CHECK DATABASE FIX} at
   * DATABASE scope drops the entry pointing at the missing record; the delete that failed then goes through.
   */
  @Test
  void theRepairItAdvertisesIsTheOneThatWorks() {
    createDanglingReference();

    assertThatThrownBy(() -> database.transaction(this::deleteEverythingSrcPointsAt, false, 1))
        .isInstanceOf(VertexNotFoundException.class);

    try (final ResultSet rs = database.command("sql", "check database fix")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(longProperty(row, "autoFix")).as("the fix must repair something: %s", row.toJSON()).isGreaterThan(0L);
    }

    // The stale entry is gone, so the sweep that could not run at all now completes.
    database.transaction(this::deleteEverythingSrcPointsAt);
    database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(0L));

    assertIntegrityClean();
  }

  /**
   * The other half of the split, guarded so the fix cannot creep over it: a vertex that EXISTS whose head chunk
   * cannot be read is still the publication window #5670 answers with a retryable conflict, and still carries the
   * scoped repair #5764 gave it - the list of a record that is there really can be rebuilt.
   */
  @Test
  void anUnreadableChunkKeepsItsRetryableConflictAndItsScopedAdvice() {
    createDanglingReference();

    final RID headChunk = outHeadChunk(srcRID);
    deleteRecord(headChunk);

    final Throwable thrown = catchThrowable(() -> database.transaction(() -> srcRID.asVertex().delete(), false, 1));

    assertThat(thrown).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
    assertThat(thrown).isNotInstanceOf(VertexNotFoundException.class);
    assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains(headChunk.toString());
    assertThat(scopedRepairCommandIn(thrown.getMessage())).as("%s", thrown.getMessage())
        .isEqualTo("CHECK DATABASE RECORD " + srcRID + " FIX");
  }

  /**
   * Unchanged, and the policy the new tolerance in {@code disconnectEndpoint} extends: an EDGE whose endpoint
   * vertex is gone has nothing to disconnect on that side, so its delete succeeds instead of failing. Deleting the
   * edge is in fact the hand repair for this corruption, and it leaves the graph clean.
   */
  @Test
  void deletingAnEdgeWhoseEndpointVertexIsGoneStaysTolerated() {
    createDanglingReference();

    database.transaction(() -> edgeRID.asEdge().delete());

    database.transaction(() -> {
      assertThat(database.existsRecord(edgeRID)).isFalse();
      assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(0L);
    });
    assertIntegrityClean();
  }

  /**
   * {@code src --E1--> target}, with target's RECORD dropped straight out of its bucket and the adjacency entry on
   * src left in place: the state a pre-#5670 delete left behind, reproduced without any concurrency.
   */
  private void createDanglingReference() {
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
      edgeRID = src.newEdge("E1", target).getIdentity();
    });

    deleteRecord(targetRID);

    // The reference really is still there: src still reports one outgoing edge into the record that is gone.
    database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1L));
  }

  /** The batch job's inner loop: walk what src points at and delete it - reaching the target through the entry. */
  private void deleteEverythingSrcPointsAt() {
    for (final Vertex v : srcRID.asVertex().getVertices(Vertex.DIRECTION.OUT, "E1"))
      v.delete();
  }

  private RID outHeadChunk(final RID vertexRID) {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getOutEdgesHeadChunk());
    assertThat(holder[0]).as("the vertex must have an outgoing edge list").isNotNull();
    return holder[0];
  }

  private void deleteRecord(final RID rid) {
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
        assertThat(longProperty(row, "totalCorruptedRecords")).as("totalCorruptedRecords: %s", row.toJSON()).isEqualTo(0L);
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
