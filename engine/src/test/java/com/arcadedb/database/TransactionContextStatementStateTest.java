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
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10346: a statement that fails is taken back by putting the state of its transaction back, and a field a later change adds to
 * {@link TransactionContext} and nobody tells the savepoint about is state a failed statement silently leaves behind: a partial undo
 * that looks complete. Every field that holds more than a scalar has to be classified here, so adding one fails this test until
 * whoever adds it has decided how a statement is taken back from it, and says so.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionContextStatementStateTest {
  /** The journal of the savepoint puts the previous value back (see {@code StatementSavepoint}). */
  private static final String JOURNALED  = "journaled";
  /** Only grows: the savepoint takes a mark when the statement begins and cuts back to it. */
  private static final String MARKED     = "marked";
  /** Copied when the statement begins, and put back when it is taken back. */
  private static final String CAPTURED   = "captured";
  /** Rebuilt or emptied when the statement is taken back. */
  private static final String REBUILT    = "rebuilt";
  /** Tracks the writes a page can be rebuilt from at commit: the pages the statement wrote to are poisoned. */
  private static final String POISONED   = "poisoned";
  /** The statement that touches it cannot be taken back (markUnrestorable), or the journal of its owner puts it back. */
  private static final String DELEGATED  = "delegated";
  /** Read-only snapshots, commit-time bookkeeping or services of the context: a statement does not change them. */
  private static final String NOT_STATEMENT_STATE = "not statement state";

  private static final Map<String, String> CLASSIFICATION = new TreeMap<>();

  static {
    // The records and the pages
    CLASSIFICATION.put("modifiedRecordsCache", JOURNALED);
    CLASSIFICATION.put("updatedRecords", JOURNALED);
    CLASSIFICATION.put("updatedRecordsIndexSnapshot", JOURNALED);
    CLASSIFICATION.put("offPageFingerprints", JOURNALED);
    CLASSIFICATION.put("deletedRecordsInTx", JOURNALED);
    CLASSIFICATION.put("modifiedPages", JOURNALED);
    CLASSIFICATION.put("newPages", JOURNALED);
    CLASSIFICATION.put("immutablePages", JOURNALED);
    CLASSIFICATION.put("newPageCounters", JOURNALED);
    CLASSIFICATION.put("bucketRecordDelta", JOURNALED);
    CLASSIFICATION.put("lightEdgeEntryChanges", JOURNALED);
    CLASSIFICATION.put("unidirectionalEdgeTargets", JOURNALED);
    CLASSIFICATION.put("registeredCallbackKeys", JOURNALED);
    CLASSIFICATION.put("indexChanges", JOURNALED);
    CLASSIFICATION.put("scannedBuckets", CAPTURED);
    CLASSIFICATION.put("newRecords", MARKED);
    CLASSIFICATION.put("afterCommitCallbacks", MARKED);
    CLASSIFICATION.put("insertReservationBuckets", MARKED);
    CLASSIFICATION.put("insertReservationPages", MARKED);
    CLASSIFICATION.put("insertReservationSlots", MARKED);
    CLASSIFICATION.put("immutableRecordsCache", REBUILT);
    CLASSIFICATION.put("newRecordRIDs", REBUILT);
    CLASSIFICATION.put("unidirectionalEdgeChanges", REBUILT);
    CLASSIFICATION.put("edgeAppendsBySegment", POISONED);
    CLASSIFICATION.put("edgeAppendPoisonedPages", POISONED);
    CLASSIFICATION.put("slotRebaseByPage", POISONED);
    CLASSIFICATION.put("slotRebasePoisonedPages", POISONED);
    CLASSIFICATION.put("attachments", DELEGATED);
    // Issue #10687: work owed by a change made BESIDE the transaction (a TIMESERIES write commits its own), which a statement taken
    // back does not undo either: the callback has to stay, as it stays through a rollback of the whole transaction
    CLASSIFICATION.put("afterCompletionCallbacks", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("afterCompletionOwner", NOT_STATEMENT_STATE);

    // What a statement reads or the commit writes, and the services of the context
    CLASSIFICATION.put("database", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("embedded", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("indexReplayConclusion", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("snapshotChunkChains", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("snapshotRecordContents", NOT_STATEMENT_STATE);
    // Issue #10712: the committed bucket counters a REPEATABLE_READ transaction first read, kept like a pinned page
    CLASSIFICATION.put("snapshotBucketCounts", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("lockedFiles", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("explicitLockedFiles", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("explicitLock", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("requester", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("statement", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("pooledStatement", NOT_STATEMENT_STATE);
    // Issue #10768: a final callback of the context the page manager calls when the commit starts publishing, holding no state
    CLASSIFICATION.put("markPublicationStarted", NOT_STATEMENT_STATE);
    // Issue #10588. bucketFirstWriteSequences: when the transaction first wrote to a bucket, to know which indexes bound since owe
    // the writes an entry. A statement taken back leaves the note of a write it undid, which can only be EARLY: the commit then asks
    // for a reconciliation that finds nothing to queue, or refuses retryably a transaction that wrote after the index, never queues
    // a wrong entry. indexesReconciled: the indexes a build already queued the transaction's writes for; the build is DDL, which a
    // statement taken back does not undo, and a build that fails drops its index, so a stale name matches no index
    CLASSIFICATION.put("bucketFirstWriteSequences", NOT_STATEMENT_STATE);
    CLASSIFICATION.put("indexesReconciled", NOT_STATEMENT_STATE);
  }

  @Test
  void everyFieldOfTheTransactionContextIsClassified() {
    final TreeSet<String> unclassified = new TreeSet<>();
    final TreeSet<String> existing = new TreeSet<>();

    for (final Field field : TransactionContext.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic() || isScalar(field.getType()))
        continue;
      existing.add(field.getName());
      if (!CLASSIFICATION.containsKey(field.getName()))
        unclassified.add(field.getName() + " (" + field.getType().getSimpleName() + ")");
    }

    assertThat(unclassified).as("fields of TransactionContext that hold state and that a failed statement is not told how to put back "
        + "(add them to StatementSavepoint, or classify them here with the reason)").isEmpty();
    assertThat(CLASSIFICATION.keySet()).as("fields classified here that TransactionContext does not have any more").isSubsetOf(existing);
  }

  private static boolean isScalar(final Class<?> type) {
    return type.isPrimitive() || type.isEnum() || type == String.class || type == Boolean.class || type == Integer.class
        || type == Long.class || Number.class.isAssignableFrom(type);
  }
}
