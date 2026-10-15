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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.WALFile;
import com.arcadedb.index.fulltext.FullTextCorpusDeltas;
import com.arcadedb.index.fulltext.FullTextCorpusDeltas.CorpusDelta;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10051 (#10018): after {@code commit2ndPhase} failed, the leader brings its pages in line with the committed Raft entry by
 * applying the entry's WAL with {@code applyChanges}, which folds the BM25 corpus deltas of the entry like the record counts. The
 * invariant that keeps the leader from counting a transaction twice is the one the replay relies on: the fold happens only when
 * a page advances, and the local commit folds as the last step of publishing its pages. Both shapes of that failure are
 * driven here against a real database, with the same WAL bytes the entry carries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051ReconcileAppliesDeltasOnceTest {
  private static final String TYPE  = "Doc10051R";
  private static final String INDEX = TYPE + "[content]";
  private static final int    SEED  = 4;

  @TempDir
  Path tempDir;

  private LocalDatabase database;

  @BeforeEach
  void setUp() {
    database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue10051r").toString()).create();
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
      database.command("sql", "CREATE PROPERTY " + TYPE + ".content STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE + " (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < SEED; i++)
        database.command("sql", "INSERT INTO " + TYPE + " SET content = 'alpha beta gamma'");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen()) {
      database.rollbackAllNested();
      database.drop();
    }
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(INDEX));
  }

  /** What phase 1 of a replicated commit hands to the entry: the WAL bytes, the record deltas and the corpus deltas. */
  private record Phase1Entry(TransactionContext tx, TransactionContext.TransactionPhase1 phase1, byte[] walData,
                             Map<Integer, Integer> bucketDeltas, List<CorpusDelta> corpusDeltas) {
  }

  private Phase1Entry preparedTransactionInserting(final int count) {
    database.begin();
    for (int i = 0; i < count; i++)
      database.newDocument(TYPE).set("content", "alpha beta gamma delta").save();
    final TransactionContext tx = database.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    final FullTextCorpusDeltas deltas = FullTextCorpusDeltas.peek(tx);
    assertThat(deltas).isNotNull();
    return new Phase1Entry(tx, phase1, phase1.result.toByteArray(), tx.getBucketRecordDelta(), deltas.snapshot());
  }

  /** The local commit published every page and folded the deltas; the failure came after, and reconciling must fold nothing. */
  @Test
  void reconcilingAfterTheLocalCommitPublishedItsPagesCountsTheTransactionOnce() {
    final Phase1Entry entry = preparedTransactionInserting(3);
    entry.tx().commit2ndPhase(entry.phase1());
    assertThat(metadata().getTotalDocs()).isEqualTo(SEED + 3L);

    final WALFile.WALTransaction walTx = ArcadeStateMachine.deserializeWalTransaction(entry.walData());
    database.getTransactionManager().applyChanges(walTx, entry.bucketDeltas(), entry.corpusDeltas(), true);

    assertThat(metadata().getTotalDocs()).as("the entry's pages were already published").isEqualTo(SEED + 3L);
    assertThat(metadata().getSumDocLength()).isEqualTo(SEED * 3L + 3 * 4L);
  }

  /** The failure came before any page was published: reconciling applies the pages and folds the deltas, once. */
  @Test
  void reconcilingWhenNoPageWasPublishedCountsTheTransactionOnce() {
    final Phase1Entry entry = preparedTransactionInserting(3);
    entry.tx().rollback();
    assertThat(metadata().getTotalDocs()).as("a transaction that published nothing counted nothing").isEqualTo(SEED);

    final WALFile.WALTransaction walTx = ArcadeStateMachine.deserializeWalTransaction(entry.walData());
    database.getTransactionManager().applyChanges(walTx, entry.bucketDeltas(), entry.corpusDeltas(), true);

    assertThat(metadata().getTotalDocs()).isEqualTo(SEED + 3L);
    assertThat(metadata().getSumDocLength()).isEqualTo(SEED * 3L + 3 * 4L);

    // and a second reconcile of the same entry finds every page at its version
    database.getTransactionManager().applyChanges(walTx, entry.bucketDeltas(), entry.corpusDeltas(), true);
    assertThat(metadata().getTotalDocs()).isEqualTo(SEED + 3L);
  }
}
