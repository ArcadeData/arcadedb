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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gates of the incremental schema apply on {@link ArcadeStateMachine}: the {@code HA_SCHEMA_INCREMENTAL_APPLY} setting and the schema changes applied between two points of the log.
 */
class SchemaIncrementalApplyGateTest {

  // ---- #6988 ----
  // {@code arcadedb.ha.schemaIncrementalApply} is the escape hatch that sends a follower's schema apply
  // back through the full {@code LocalSchema.load()}. Its whole value to an operator is that it takes effect on the
  // NEXT applied entry rather than on the next restart, which means the value must be read per entry and never cached.
  // <p>
  // {@code HaSchemaDeltaReplicationFullRebuildIT} proves the flag reaches the apply path, but it sets the value once at
  // server start, so it cannot tell a per-entry read from a value latched during startup. This test pins that half.

  @AfterEach
  void restoreDefault() {
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.reset();
  }

  // Regression for #6988: arcadedb.ha.schemaIncrementalApply is the escape hatch that sends a follower's schema apply back through the full LocalSchema.load().
  @Test
  void defaultsToEnabled() {
    assertThat(GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.getDefValue()).isEqualTo(true);
    assertThat(GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.getType()).isEqualTo(Boolean.class);
    assertThat(new ArcadeStateMachine().incrementalSchemaApplyEnabled()).isTrue();
  }

  // Regression for #6988: arcadedb.ha.schemaIncrementalApply is the escape hatch that sends a follower's schema apply back through the full LocalSchema.load().
  @Test
  void theSettingIsReReadOnEveryEntryRatherThanLatchedAtStartup() {
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    assertThat(stateMachine.incrementalSchemaApplyEnabled()).isTrue();

    // The same instance that already answered "enabled" must answer "disabled" the moment the value changes: an
    // operator turning the valve on a running server expects the very next applied entry to take the full rebuild.
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);
    assertThat(stateMachine.incrementalSchemaApplyEnabled()).isFalse();

    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(true);
    assertThat(stateMachine.incrementalSchemaApplyEnabled()).isTrue();
  }

  // ---- #10067 ----
  // a replica that asks the leader to create a type for a statement of a client transaction accounts for the schema
  // changes that landed while it waited. {@link ArcadeStateMachine#schemaChangesAppliedBetween} says how many were applied in a
  // stretch of the log: exactly one means that type was the only change, so the transaction was not overtaken by anybody else's DDL.
  //

  private final ArcadeStateMachine stateMachine = new ArcadeStateMachine();

  // Regression for #10067: a replica that asks the leader to create a type for a statement of a client transaction accounts for the schema changes that landed while it waited.
  @Test
  void nothingRecordedCountsNothing() {
    assertThat(stateMachine.lastSchemaChangeIndexOf("db")).isEqualTo(-1L);
    assertThat(stateMachine.schemaChangesAppliedBetween("db", 0L, 1_000L)).isZero();
    assertThat(stateMachine.schemaChangesAppliedBetween(null, 0L, 1_000L)).isZero();
  }

  // Regression for #10067: a replica that asks the leader to create a type for a statement of a client transaction accounts for the schema changes that landed while it waited.
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

  // Regression for #10067: a replica that asks the leader to create a type for a statement of a client transaction accounts for the schema changes that landed while it waited.
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
  // Regression for #10067: a replica that asks the leader to create a type for a statement of a client transaction accounts for the schema changes that landed while it waited.
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
  // Regression for #10067: a replica that asks the leader to create a type for a statement of a client transaction accounts for the schema changes that landed while it waited.
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
