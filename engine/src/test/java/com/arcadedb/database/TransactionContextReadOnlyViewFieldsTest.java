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
package com.arcadedb.database;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8775: {@link TransactionContext#isReadOnlyView()} is a hand-maintained list of the state a write registers. A
 * field added to the transaction without being classified here could make a dirty transaction look clean, and its scan
 * would then miss its own writes. This test fails on any field it does not know, so whoever adds one has to decide
 * whether {@code isReadOnlyView()} must check it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionContextReadOnlyViewFieldsTest {
  /** Fields that a write populates and that {@code isReadOnlyView()} checks, directly or through {@code hasChanges()}. */
  private static final Set<String> CHECKED = Set.of("modifiedPages", "newPages", "indexChanges", "newRecords", "modifiedRecordsCache",
      "deletedRecordsInTx", "updatedRecords", "bucketRecordDelta", "newPageCounters", "isolationLevel");

  /** Fields that are not a write's state, or that always accompany a dirtied page or a new record. */
  private static final Set<String> IGNORED = Set.of(
      "afterCommitCallbacks",
      "asyncFlush",
      "attachments",
      "beginSequence",
      "begunUnderWriteRefusal",
      "commitCount",
      "commitLockTimeout",
      "configuredUseWAL",
      "configuredWALFlush",
      "database",
      "edgeAppendMerge",
      "edgeAppendPoisonedPages",
      "edgeAppendsBySegment",
      "embedded",
      "explicitLock",
      "explicitLockedFiles",
      "immutablePages",
      "immutableRecordsCache",
      "indexChangesReplayed",
      "indexReplayConclusion",
      "insertReservationBuckets",
      "insertReservationCount",
      "insertReservationPages",
      "insertReservationSlots",
      "lockedFiles",
      "offPageFingerprints",
      "parallelScanOverride",
      "phase2WalAppended",
      "registeredCallbackKeys",
      "remotelyCommitted",
      "replicationBasePosition",
      "requester",
      "rollbackOnlyReason",
      "slotMerge",
      "slotMergeMaxBytes",
      "slotRebaseByPage",
      "slotRebasePoisonedPages",
      "slotRebaseTrackedBytes",
      "staleReadCheck",
      "status",
      "txId",
      "unidirectionalEdgeChanges",
      "updatedRecordsIndexSnapshot",
      "useWAL",
      "useWALOverride",
      "walFlush");

  @Test
  void everyFieldIsClassified() {
    final Set<String> unknown = new TreeSet<>();
    for (final Field field : TransactionContext.class.getDeclaredFields())
      if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic() && !CHECKED.contains(field.getName())
          && !IGNORED.contains(field.getName()))
        unknown.add(field.getName());
    assertThat(unknown).as("fields of TransactionContext not classified for isReadOnlyView(): check each one, then add it to CHECKED or IGNORED")
        .isEmpty();
  }
}
