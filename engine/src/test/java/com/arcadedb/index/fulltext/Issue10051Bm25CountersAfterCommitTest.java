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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10051 (#10018): the BM25 corpus counters of a FULL_TEXT index were bumped at index time, before the transaction
 * committed, and nothing undid them. A rolled-back insert (a {@code ConcurrentModificationException} retry under write
 * contention is the everyday case) left {@code totalDocs} / {@code sumDocLength} inflated, which the next search then
 * noticed through the stale check and repaired with a whole-type scan on the query thread. The counters now move only
 * when the transaction commits.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051Bm25CountersAfterCommitTest extends TestHelper {
  private static final int BUCKETS = 3;
  private static final int DOCS    = 12;

  private void createAndFill() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
  }

  /** The one metadata every bucket sub-index of the logical index shares. */
  private FullTextIndexMetadata metadata() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    final Index first = typeIndex.getIndexesOnBuckets()[0];
    return ((LSMTreeFullTextIndex) first).getFullTextMetadata();
  }

  @Test
  void committedInsertsMoveTheCounters() {
    createAndFill();
    assertThat(metadata().isCommitApplying()).as("nothing is left marked as being applied").isFalse();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  @Test
  void rolledBackInsertsLeaveTheCountersUntouched() {
    createAndFill();

    database.begin();
    for (int i = 0; i < 5; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.rollback();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    assertThat(database.countType("Doc", false)).isEqualTo(DOCS);
  }

  @Test
  void rolledBackDeletesLeaveTheCountersUntouched() {
    createAndFill();

    database.begin();
    database.command("sql", "DELETE FROM Doc LIMIT 4");
    database.rollback();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  @Test
  void theCountersDoNotMoveBeforeTheCommit() {
    createAndFill();

    database.begin();
    database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta epsilon'");
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    database.commit();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 1L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 5);
  }

  @Test
  void aCommittedDeleteMovesTheCountersOnceTheTransactionCommits() {
    createAndFill();

    database.begin();
    database.command("sql", "DELETE FROM Doc LIMIT 2");
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    database.commit();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS - 2L);
    assertThat(metadata().getSumDocLength()).isEqualTo((DOCS - 2L) * 3L);
  }

  /** An update is a remove and an add inside one transaction: only their net effect may reach the counters, and only on commit. */
  @Test
  void anUpdateNetsOutAtCommit() {
    createAndFill();

    database.begin();
    database.command("sql", "UPDATE Doc SET content = 'alpha beta gamma delta epsilon zeta' LIMIT 1");
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    database.commit();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 3);
  }

  /** The stale check compares the type-wide live count with the counter: after a rollback they must agree, so nothing rescans. */
  @Test
  void aRolledBackInsertLeavesNothingForTheStaleCheckToRepair() {
    createAndFill();

    database.begin();
    for (int i = 0; i < 4; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.rollback();

    final FullTextIndexMetadata m = metadata();
    assertThat(m.getTotalDocs()).isEqualTo(database.countType("Doc", false));
    final long mutationsBefore = m.getMutations();

    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();

    assertThat(m.getTotalDocs()).isEqualTo(DOCS);
    assertThat(m.getSumDocLength()).isEqualTo(DOCS * 3L);
    assertThat(m.getMutations()).isEqualTo(mutationsBefore);
  }

  /**
   * A search that has to rebuild cold counters inside a transaction that already holds uncommitted inserts scans them too
   * (a transaction reads its own writes): the commit must not add them a second time.
   */
  @Test
  void aColdCountersScanInsideTheWritingTransactionDoesNotDoubleCount() {
    createAndFill();
    metadata().setCountersValid(false);

    database.begin();
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    database.commit();

    final long live = database.countType("Doc", false);
    assertThat(live).isEqualTo(DOCS + 3L);
    assertThat(metadata().getTotalDocs()).isEqualTo(live);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 3 * 4L);
  }

  /**
   * The pending effect of the writing transaction is negative when it removed committed documents (the scan no longer sees
   * them), and mixed when it also inserted: what is subtracted from the scan must leave exactly the committed data, so the
   * commit lands the counters on the final state.
   */
  @Test
  void aColdCountersScanInsideATransactionThatDeletedAndInsertedCountsTheCommittedDataOnce() {
    createAndFill();
    metadata().setCountersValid(false);

    database.begin();
    database.command("sql", "DELETE FROM Doc LIMIT 2");
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(metadata().getTotalDocs()).as("the scan described the committed data: %d documents", DOCS).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    database.commit();

    assertThat(database.countType("Doc", false)).isEqualTo(DOCS + 1L);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 1L);
    // 2 documents of 3 tokens left, 3 of 4 tokens came in
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L - 2 * 3L + 3 * 4L);
  }

  /** A scan inside a transaction that only deleted: the pending effect is negative and is added back. */
  @Test
  void aColdCountersScanInsideATransactionThatOnlyDeletedCountsTheCommittedDataOnce() {
    createAndFill();
    metadata().setCountersValid(false);

    database.begin();
    database.command("sql", "DELETE FROM Doc LIMIT 4");
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    database.commit();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS - 4L);
    assertThat(metadata().getSumDocLength()).isEqualTo((DOCS - 4L) * 3L);
  }

  /**
   * A transaction whose WAL ships inside a schema entry carries no corpus deltas: it marks the counters of the indexes it wrote
   * as recomputed instead, and the schema entry's counters then reach the followers.
   */
  @Test
  void advancingTheEpochsMarksExactlyTheIndexesTheTransactionWrote() {
    createAndFill();
    assertThat(metadata().getCountersEpoch()).isZero();

    database.begin();
    database.command("sql", "INSERT INTO Doc SET content = 'alpha beta'");
    final FullTextCorpusDeltas deltas = FullTextCorpusDeltas.peek(((DatabaseInternal) database).getTransaction());
    assertThat(deltas).isNotNull();
    deltas.advanceEpochs();
    database.commit();

    assertThat(metadata().getCountersEpoch()).isEqualTo(1L);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 1L);
  }

  /**
   * A scan that overlaps a commit between making its pages visible and applying its delta reads documents the counters do not
   * hold yet, and would count them twice once the delta lands: it must not call its result exact, or the drift check is spent and
   * the error stays until the next explicit recompute. The commit marks itself, and the scan arms the check.
   */
  @Test
  void aScanOverlappingACommitThatIsApplyingItsDeltaIsNotCalledExact() {
    createAndFill();
    reopenDatabase();
    final FullTextIndexMetadata m = metadata();
    m.applyCorpusDelta(5, 20); // a drift, so the first search scans

    // The marker is held for the whole search here, so every scan the search runs (one per bucket index) overlaps it; a real
    // window lasts microseconds and only the first scan can meet it
    m.commitApplyStarted();
    try {
      database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    } finally {
      m.commitApplyFinished();
    }
    final long scansWhileApplying = m.getScans();
    assertThat(scansWhileApplying).isGreaterThanOrEqualTo(1L);
    assertThat(m.isStaleCheckDone()).as("the result was not exact: the check stays armed").isFalse();

    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(m.getScans()).as("with no commit in flight it rescans, even though the counts agree")
        .isGreaterThan(scansWhileApplying);
    assertThat(m.isStaleCheckDone()).isTrue();

    final long settled = m.getScans();
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    assertThat(m.getScans()).as("then it settles").isEqualTo(settled);
    assertThat(m.getTotalDocs()).isEqualTo(DOCS);
  }

  /** The same scan, but the transaction is then rolled back: the scan result must describe the committed data only. */
  @Test
  void aColdCountersScanInsideATransactionThatRollsBackDescribesTheCommittedData() {
    createAndFill();
    metadata().setCountersValid(false);

    database.begin();
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma delta'");
    database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true").close();
    database.rollback();

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
  }

  /**
   * A commit moves the counters but does not dirty the schema (that would rewrite schema.json after every commit), so the close
   * has to persist what the commits since the last schema save did: a restart then finds them exact and scans nothing.
   */
  @Test
  void theCountersCommittedSinceTheLastSchemaSaveArePersistedAtClose() {
    createAndFill();
    assertThat(metadata().isCountersDirty()).as("dirty before close").isTrue();

    reopenDatabase();

    assertThat(metadata().isCountersValid()).isTrue();
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
    assertThat(metadata().getScans()).isZero();
  }

  /**
   * Serializing the counters for something that is not a write of the schema file (an export, a replication payload) must not
   * make the close believe they were saved.
   */
  @Test
  void serializingTheSchemaWithoutWritingItDoesNotLoseTheCountersAtClose() {
    createAndFill();
    assertThat(metadata().isCountersDirty()).isTrue();

    database.getSchema().getEmbedded().toJSON();
    database.getSchema().getEmbedded().toJSON();

    assertThat(metadata().isCountersDirty()).as("an export saved nothing").isTrue();
    reopenDatabase();
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getScans()).isZero();
  }

  /** The generation a write records as saved is the one read BEFORE it serialized: a change made while the file was written stays unsaved. */
  @Test
  void aChangeMadeWhileTheSchemaWasBeingWrittenStaysUnsaved() {
    createAndFill();
    final FullTextIndexMetadata m = metadata();

    final long readBeforeSerializing = m.getCountersGeneration();
    m.applyCorpusDelta(1, 3); // a commit lands while the file is being written
    m.markCountersSaved(readBeforeSerializing);

    assertThat(m.isCountersDirty()).as("the commit made during the write is not in the file").isTrue();

    m.markCountersSaved(m.getCountersGeneration());
    assertThat(m.isCountersDirty()).isFalse();
  }

  @Test
  void aCompletedSchemaWriteMarksTheCountersSavedUnlessTheyMovedMeanwhile() {
    createAndFill();
    assertThat(metadata().isCountersDirty()).isTrue();

    database.getSchema().getEmbedded().saveConfiguration();
    assertThat(metadata().isCountersDirty()).isFalse();

    metadata().applyCorpusDelta(1, 3);
    assertThat(metadata().isCountersDirty()).isTrue();
  }

  /**
   * The retry of a transaction that a ConcurrentModificationException refused runs its body again on the same context: what the
   * refused attempt accumulated must be gone, or the counters move twice.
   */
  @Test
  void aTransactionRetriedAfterAConcurrentModificationCountsOnlyTheAttemptThatCommits() {
    createAndFill();
    final RID rid = database.iterateType("Doc", false).next().getIdentity();
    final AtomicInteger attempts = new AtomicInteger();

    database.transaction(() -> {
      final boolean firstAttempt = attempts.incrementAndGet() == 1;
      database.lookupByRID(rid, true).asDocument().modify().set("content", "alpha beta gamma delta").save();
      database.newDocument("Doc").set("content", "alpha beta").save();
      if (firstAttempt) {
        // another thread commits a change to the same record before this attempt does
        final Thread other = new Thread(() -> database.transaction(
            () -> database.lookupByRID(rid, true).asDocument().modify().set("content", "alpha").save()));
        other.start();
        try {
          other.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, false, 3);

    assertThat(attempts.get()).as("the first attempt was refused").isEqualTo(2);
    // 11 untouched documents of 3 tokens, the one that was rewritten to 4, and the one the committed attempt inserted (2)
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 1L);
    assertThat(metadata().getSumDocLength()).isEqualTo((DOCS - 1) * 3L + 4 + 2);
  }

  /**
   * A commit that the cluster-wide check refuses with a ConcurrentModificationException counts nothing, and the transaction that
   * won counts exactly its own effect: the commit-time index replay never goes through the counting code.
   */
  @Test
  void aCommitRefusedWithAConcurrentModificationNeverCountsItsDocuments() throws Exception {
    createAndFill();
    final long lengthBefore = metadata().getSumDocLength();

    database.begin();
    final MutableDocument doc = database.iterateType("Doc", false).next().asDocument().modify();
    doc.set("content", "alpha beta gamma delta epsilon zeta eta theta").save();
    final RID rid = doc.getIdentity();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread other = new Thread(() -> {
      try {
        database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("content", "alpha").save());
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    other.start();
    other.join(30_000L);
    assertThat(other.isAlive()).as("the concurrent transaction finished").isFalse();
    assertThat(failure.get()).isNull();

    assertThatThrownBy(database::commit).isInstanceOf(ConcurrentModificationException.class);
    if (database.isTransactionActive())
      database.rollback();

    // The winner shortened one document from 3 tokens to 1; the refused transaction (8 tokens) is nowhere in the counters
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(lengthBefore - 2);
  }
}
