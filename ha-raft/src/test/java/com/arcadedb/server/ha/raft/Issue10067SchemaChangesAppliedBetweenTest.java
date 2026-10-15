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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10067: a replica that asks the leader to create a type for a statement of a client transaction accounts for the schema
 * changes that landed while it waited. {@link ArcadeStateMachine#schemaChangesAppliedBetween} says how many were applied in a
 * stretch of the log: exactly one means that type was the only change, so the transaction was not overtaken by anybody else's DDL.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10067SchemaChangesAppliedBetweenTest {

  private final ArcadeStateMachine stateMachine = new ArcadeStateMachine();

  @Test
  void nothingRecordedCountsNothing() {
    assertThat(stateMachine.lastSchemaChangeIndexOf("db")).isEqualTo(-1L);
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 0L, 1_000L)).isZero();
    assertThat(stateMachine.schemaChangesAppliedBetween(null, 0L, 1_000L)).isZero();
  }

  @Test
  void aStretchIsOpenAtItsStartAndClosedAtItsEnd() {
    stateMachine.recordSchemaChangeApplied("db", 10L);
    stateMachine.recordSchemaChangeApplied("db", 20L);
    stateMachine.recordSchemaChangeApplied("db", 30L);

    assertThat(stateMachine.lastSchemaChangeIndexOf("db")).isEqualTo(30L);
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 5L, 25L)).isEqualTo(2);
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 10L, 30L)).as("(10, 30]: not the change AT 10, the one at 30").isEqualTo(2);
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 20L, 30L)).isEqualTo(1);
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 30L, 100L)).isZero();
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 11L, 19L)).isZero();
  }

  @Test
  void aChangeIsCountedOnceAndOnlyOnItsOwnDatabase() {
    stateMachine.recordSchemaChangeApplied("db", 10L);
    stateMachine.recordSchemaChangeApplied("db", 10L);
    stateMachine.recordSchemaChangeApplied("other", 15L);

    assertThat(stateMachine.schemaChangesAppliedBetween("db", 0L, 100L)).isEqualTo(1);
    assertThat(stateMachine.schemaChangesAppliedBetween("other", 0L, 100L)).isEqualTo(1);
    assertThat(stateMachine.lastSchemaChangeIndexOf("db")).isEqualTo(10L);
  }

  /**
   * A restart, an in-place Ratis restart or a snapshot install replaces what the state machine remembered with nothing: no schema
   * change before that point is in its history, and an absence of one there is not an absence of change. A stretch that starts before
   * the point is not vouched for, whatever the history holds; one that starts at or after it is.
   */
  @Test
  void aStretchBeforeWhatThisInstanceWasSeededAtIsNotVouchedFor() {
    stateMachine.schemaHistoryIncompleteUpTo(100L);
    stateMachine.recordSchemaChangeApplied("db", 120L);

    assertThat(stateMachine.schemaHistoryCovers(99L)).isFalse();
    assertThat(stateMachine.schemaHistoryCovers(100L)).as("changes after 100 are all in memory").isTrue();
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 50L, 130L)).isEqualTo(Integer.MAX_VALUE);
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 100L, 130L)).isEqualTo(1);

    stateMachine.schemaHistoryIncompleteUpTo(40L);
    assertThat(stateMachine.schemaHistoryCovers(99L)).as("the floor only ever rises").isFalse();

    stateMachine.schemaHistoryIncompleteUpTo(200L);
    assertThat(stateMachine.schemaHistoryCovers(150L)).as("an install jumped past it").isFalse();
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 150L, 250L)).isEqualTo(Integer.MAX_VALUE);
  }

  /** The history is bounded: a stretch that starts before what it still holds cannot be vouched for, and says so. */
  @Test
  void aStretchTheRetainedHistoryDoesNotReachBackToIsNotVouchedFor() {
    for (long index = 1; index <= 300; index++)
      stateMachine.recordSchemaChangeApplied("db", index);

    assertThat(stateMachine.schemaChangesAppliedBetween("db", 0L, 300L)).as("the oldest changes were dropped")
        .isEqualTo(Integer.MAX_VALUE);
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 290L, 300L)).as("a recent stretch is still exact").isEqualTo(10);
    assertThat(stateMachine.lastSchemaChangeIndexOf("db")).isEqualTo(300L);
  }
}
