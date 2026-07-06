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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5018.
 * <p>
 * The phase-2 commit-failure handler in {@link RaftReplicatedDatabase} logs a more specific
 * "page version conflict" message when the failure is the engine's page-version conflict
 * ({@link com.arcadedb.exception.ConcurrentModificationException}). The original bug was that the
 * {@code instanceof} test bound to {@link java.util.ConcurrentModificationException} - an unrelated
 * JDK type the engine never throws - so the specific branch was dead and every page-version conflict
 * was logged with the generic wording, hiding the "this may indicate a locking bug" signal operators
 * rely on.
 * <p>
 * This test pins {@link RaftReplicatedDatabase#phase2CommitFailureMessage(Throwable)} to the engine
 * type so a future edit cannot re-bind the check to the JDK type without failing here. It needs no
 * cluster and creates no database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5018Phase2ConflictMessageTest {

  private static final String PAGE_CONFLICT_MARKER = "page version conflict";

  @Test
  void engineConcurrentModificationExceptionSelectsPageConflictMessage() {
    final String message = RaftReplicatedDatabase.phase2CommitFailureMessage(
        new com.arcadedb.exception.ConcurrentModificationException("page 1 v2 != v1"));

    assertThat(message).contains(PAGE_CONFLICT_MARKER);
    assertThat(message).contains("this may indicate a locking bug");
  }

  @Test
  void jdkConcurrentModificationExceptionDoesNotSelectPageConflictMessage() {
    // The JDK type is what the buggy instanceof used to bind to; the engine never throws it here,
    // so it must fall through to the generic message - proving the check no longer matches it.
    final String message = RaftReplicatedDatabase.phase2CommitFailureMessage(
        new java.util.ConcurrentModificationException("iterator modified"));

    assertThat(message).doesNotContain(PAGE_CONFLICT_MARKER);
    assertThat(message).contains("Phase 2 commit failed AFTER successful Raft replication (db=");
  }

  @Test
  void unrelatedExceptionSelectsGenericMessage() {
    final String message = RaftReplicatedDatabase.phase2CommitFailureMessage(
        new RuntimeException("boom"));

    assertThat(message).doesNotContain(PAGE_CONFLICT_MARKER);
    assertThat(message).contains("Phase 2 commit failed AFTER successful Raft replication (db=");
  }
}
