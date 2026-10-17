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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.fulltext.FullTextCorpusDeltas;
import com.arcadedb.index.fulltext.FullTextCorpusDeltas.CorpusDelta;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10132: a transaction whose net effect on a full-text index is {@code (0, 0)} - one document deleted and another of the
 * same length inserted, or a rewrite that keeps the number of tokens - shipped no corpus delta at all, so a node applying its
 * replicated entry never marked the counters as being applied and never counted a mutation. A corpus scan overlapping that
 * apply could read both documents and still call its result exact.
 * <p>
 * The property pinned here holds for every transaction shape: the node that applies the replicated entry observes the same
 * number of {@code mutations} bumps as the node that committed it. Both {@code commitApplyStarted} (the "being applied" marker)
 * and {@code applyCorpusDelta} bump the counter, so an equal count is also an equal number of marker transitions.
 */
class Issue10132ZeroNetCorpusDeltaReplicationTest extends TestHelper {
  private static final String INDEX = "Doc[content]";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma', tag = 'seed'");
      database.command("sql", "INSERT INTO Doc SET content = 'one two three', tag = 'victim'");
    });
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(INDEX));
  }

  /** The issue's repro: a delete and an insert of the same length net to {@code (0, 0)}, and the entry must still say so. */
  @Test
  void aZeroNetTransactionShipsItsIndexWithAZeroPair() {
    database.begin();
    database.command("sql", "DELETE FROM Doc WHERE tag = 'victim'");
    database.command("sql", "INSERT INTO Doc SET content = 'four five six', tag = 'replacement'");
    final FullTextCorpusDeltas deltas = FullTextCorpusDeltas.peek(((DatabaseInternal) database).getTransaction());
    assertThat(deltas).as("the transaction wrote the index").isNotNull();
    final List<CorpusDelta> shipped = deltas.snapshot();
    database.commit();

    assertThat(shipped).containsExactly(new CorpusDelta(INDEX, 0, 0));
  }

  @Test
  void deleteAndInsertOfTheSameLengthBumpTheFollowerLikeTheLeader() throws Exception {
    assertSameMutationsOnBothSides(() -> {
      database.command("sql", "DELETE FROM Doc WHERE tag = 'victim'");
      database.command("sql", "INSERT INTO Doc SET content = 'four five six', tag = 'replacement'");
    });
  }

  @Test
  void anUpdateThatKeepsItsTokenLengthBumpsTheFollowerLikeTheLeader() throws Exception {
    assertSameMutationsOnBothSides(() -> database.command("sql", "UPDATE Doc SET content = 'seven eight nine' WHERE tag = 'victim'"));
  }

  @Test
  void anInsertBumpsTheFollowerLikeTheLeader() throws Exception {
    assertSameMutationsOnBothSides(() -> database.command("sql", "INSERT INTO Doc SET content = 'ten eleven', tag = 'new'"));
  }

  @Test
  void aDeleteBumpsTheFollowerLikeTheLeader() throws Exception {
    assertSameMutationsOnBothSides(() -> database.command("sql", "DELETE FROM Doc WHERE tag = 'victim'"));
  }

  /** Applying a zero pair moves no total: the counters stay what the local commit left them. */
  @Test
  void applyingAZeroPairLeavesTheTotalsAlone() throws Exception {
    final long docs = metadata().getTotalDocs();
    final long length = metadata().getSumDocLength();

    final boolean applied = ((DatabaseInternal) database).getTransactionManager()
        .applyChanges(entryAdvancingTheBucketPage(), Collections.emptyMap(), List.of(new CorpusDelta(INDEX, 0, 0)), false);

    assertThat(applied).isTrue();
    assertThat(metadata().getTotalDocs()).isEqualTo(docs);
    assertThat(metadata().getSumDocLength()).isEqualTo(length);
    assertThat(metadata().isCommitApplying()).as("the apply left nothing marked as being applied").isFalse();
  }

  /**
   * Runs {@code work} in a transaction committed on this node, then applies the deltas that transaction would have shipped as a
   * replicated entry on the same counters, and checks both sides bumped {@code mutations} by the same, non-zero, amount.
   */
  private void assertSameMutationsOnBothSides(final Runnable work) throws Exception {
    database.begin();
    work.run();
    final FullTextCorpusDeltas deltas = FullTextCorpusDeltas.peek(((DatabaseInternal) database).getTransaction());
    assertThat(deltas).as("the transaction wrote the index").isNotNull();
    final List<CorpusDelta> shipped = deltas.snapshot();
    final long beforeCommit = metadata().getMutations();
    database.commit();
    final long committedBumps = metadata().getMutations() - beforeCommit;

    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();
    final long beforeApply = metadata().getMutations();
    final boolean applied = ((DatabaseInternal) database).getTransactionManager()
        .applyChanges(entry, Collections.emptyMap(), shipped, false);
    final long appliedBumps = metadata().getMutations() - beforeApply;

    assertThat(applied).isTrue();
    assertThat(committedBumps).as("the committing node saw the counters touched").isPositive();
    assertThat(appliedBumps).as("the node applying the replicated entry must learn the same as the node that committed it")
        .isEqualTo(committedBumps);
    assertThat(metadata().isCommitApplying()).isFalse();
  }

  /** A WAL entry that raises page 0 of the type's bucket by one version, whatever this node's copy holds. */
  private WALFile.WALTransaction entryAdvancingTheBucketPage() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType("Doc").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageManager pageManager = db.getPageManager();
    assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

    final ImmutablePage page = pageManager.getImmutablePage(new PageId(db, fileId, 0), file.getPageSize(), false, true);

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = (int) page.getVersion() + 1;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();
    final byte[] content = new byte[11];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = 10132;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };
    return walTx;
  }
}
