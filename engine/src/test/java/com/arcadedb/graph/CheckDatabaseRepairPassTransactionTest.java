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
import com.arcadedb.utility.ProgressCallback;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6342: what each repair pass of {@link GraphDatabaseChecker} leaves on the thread when its body throws
 * ANYWHERE except in a batch commit.
 * <p>
 * {@code CheckDatabaseRepairBatchFailureTest} pins the one failure that was already safe: a batch commit that throws
 * disposes its own context on the way out ({@code LocalDatabase.commit()} pops it in a {@code finally} whether or not
 * the write succeeded), so that path really does leave no transaction behind. Every OTHER way the body can throw used
 * to leave one, because each pass committed as the LAST statement of its {@code try} and the {@code finally} only
 * filled in the returned counters.
 * <p>
 * What that costs depends on who is underneath, and both cases are pinned below. Under an OUTER transaction - which
 * through the HTTP handler is every production {@code CHECK DATABASE} - the pass's own transaction is nested on top of
 * it, so the caller's next {@code rollback()}, cleaning up after the very exception that caused this, pops the
 * abandoned nested one and leaves the caller's: the opposite of what the caller intended. Embedded, with nothing
 * underneath, the next user of the thread inherits an in-flight transaction from a repair that already failed.
 * <p>
 * The failure is injected through the progress callback, which every pass invokes once it has opened its transaction
 * and before it has finished: it is a plain body throw, it needs no corrupted database to provoke, and it reaches all
 * three passes the same way - which is the point, since the defect was in the shape they share rather than in any one
 * of them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseRepairPassTransactionTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";

  /** Fails the pass the first time it reports progress, which is after it has opened its transaction. */
  private static final ProgressCallback FAIL_ON_FIRST_REPORT = (stepName, stepIndex, totalSteps, done, total) -> {
    throw new IllegalStateException("simulated failure inside the repair pass");
  };

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
      final Vertex one = database.newVertex(VERTEX_TYPE).set("name", "one").save();
      final Vertex two = database.newVertex(VERTEX_TYPE).set("name", "two").save();
      one.newEdge(EDGE_TYPE, two);
    });
  }

  @Test
  void aVertexPassThatThrowsLeavesTheCallersTransactionAndNoOtherBehind() {
    assertTheCallersTransactionSurvives(
        checker -> checker.checkVertices(VERTEX_TYPE, true, 0));
  }

  @Test
  void anEdgePassThatThrowsLeavesTheCallersTransactionAndNoOtherBehind() {
    assertTheCallersTransactionSurvives(
        checker -> checker.checkEdges(EDGE_TYPE, true, 0));
  }

  @Test
  void anOrphanedSegmentReclaimThatThrowsLeavesTheCallersTransactionAndNoOtherBehind() {
    assertTheCallersTransactionSurvives(
        checker -> checker.reclaimOrphanedEdgeSegments(0, 10));
  }

  @Test
  void aVertexPassThatThrowsWithNoCallerTransactionLeavesTheThreadClean() {
    assertTheThreadIsLeftClean(checker -> checker.checkVertices(VERTEX_TYPE, true, 0));
  }

  @Test
  void anEdgePassThatThrowsWithNoCallerTransactionLeavesTheThreadClean() {
    assertTheThreadIsLeftClean(checker -> checker.checkEdges(EDGE_TYPE, true, 0));
  }

  @Test
  void anOrphanedSegmentReclaimThatThrowsWithNoCallerTransactionLeavesTheThreadClean() {
    assertTheThreadIsLeftClean(checker -> checker.reclaimOrphanedEdgeSegments(0, 10));
  }

  /**
   * The HTTP shape: the pass runs nested inside a transaction it did not open, and the caller cleans up after the
   * failure exactly as {@code DatabaseAbstractHandler} does.
   * <p>
   * The assertion is deliberately behavioural rather than a peek at the transaction stack: what a leaked nested
   * transaction actually costs is that the caller's own {@code rollback()} lands on it instead, so the caller is left
   * still holding a transaction it believes it has just rolled back - and the work in it, which the failure means to
   * discard, is still pending.
   */
  private void assertTheCallersTransactionSurvives(final Function<GraphDatabaseChecker, Object> pass) {
    database.begin();

    // Work the caller has done and which its rollback must discard: if the pass's transaction is the one that goes
    // back instead, this vertex survives the rollback below.
    database.newVertex(VERTEX_TYPE).set("name", "workOfTheCaller").save();

    assertThatThrownBy(() -> pass.apply(failingChecker()))
        .as("a pass that cannot run must reach the caller")
        .isInstanceOf(IllegalStateException.class);

    assertThat(database.isTransactionActive())
        .as("the caller's own transaction must still be open after the pass failed inside it")
        .isTrue();

    database.rollback();

    assertThat(database.isTransactionActive())
        .as("the caller's rollback must clean up the CALLER's transaction, not one the failed pass abandoned on top "
            + "of it")
        .isFalse();

    database.transaction(() -> assertThat(database.countType(VERTEX_TYPE, false))
        .as("the caller's rolled-back work must be gone, so its rollback reached the transaction it was made in")
        .isEqualTo(2));
  }

  /** The embedded shape: nothing underneath, so the thread must be left with no transaction at all. */
  private void assertTheThreadIsLeftClean(final Function<GraphDatabaseChecker, Object> pass) {
    assertThat(database.isTransactionActive()).as("the fixture must start with a clean thread").isFalse();

    assertThatThrownBy(() -> pass.apply(failingChecker()))
        .as("a pass that cannot run must reach the caller")
        .isInstanceOf(IllegalStateException.class);

    assertThat(database.isTransactionActive())
        .as("the next user of this thread must not inherit an in-flight transaction from a failed repair")
        .isFalse();

    // And the database is still usable through it.
    database.transaction(() -> assertThat(database.countType(VERTEX_TYPE, false)).isEqualTo(2));
  }

  private GraphDatabaseChecker failingChecker() {
    return new GraphDatabaseChecker((DatabaseInternal) database)
        .setProgress(FAIL_ON_FIRST_REPORT, "failing pass", 1, 1);
  }
}
