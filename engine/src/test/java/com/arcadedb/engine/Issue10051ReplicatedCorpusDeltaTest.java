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
import com.arcadedb.index.fulltext.FullTextCorpusDeltas.CorpusDelta;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10051 (#10018): the BM25 corpus counters of a full-text index are not part of the pages a replicated transaction
 * writes, so an HA follower never moved them (they changed only on a full schema reload or a restart). The transaction now
 * carries what it did to them, and {@link TransactionManager#applyChanges} folds that in the way it folds the bucket record
 * counters: only when the entry advanced a page, so replaying an entry that was already applied counts it once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051ReplicatedCorpusDeltaTest extends TestHelper {
  private static final int DOCS = 6;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma'");
    });
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName("Doc[content]"));
  }

  /** A WAL entry that raises page 0 of the type's bucket by one version, whatever the follower's copy holds. */
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
    walTx.txId = 10051;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };
    return walTx;
  }

  @Test
  void aReplicatedEntryMovesTheCountersByWhatTheTransactionCarried() throws Exception {
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);

    final boolean applied = ((DatabaseInternal) database).getTransactionManager()
        .applyChanges(entry, Collections.emptyMap(), List.of(new CorpusDelta("Doc[content]", 4, 20)), false);

    assertThat(applied).isTrue();
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 4L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 20);
    assertThat(metadata().isCommitApplying()).as("the apply left nothing marked as being applied").isFalse();
  }

  @Test
  void aNegativeDeltaTakesDocumentsOutAndNeverGoesBelowZero() throws Exception {
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();

    ((DatabaseInternal) database).getTransactionManager()
        .applyChanges(entry, Collections.emptyMap(), List.of(new CorpusDelta("Doc[content]", -2, -6)), false);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS - 2L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L - 6);

    final WALFile.WALTransaction next = entryAdvancingTheBucketPage();
    ((DatabaseInternal) database).getTransactionManager()
        .applyChanges(next, Collections.emptyMap(), List.of(new CorpusDelta("Doc[content]", -100, -1000)), false);
    assertThat(metadata().getTotalDocs()).isZero();
    assertThat(metadata().getSumDocLength()).isZero();
  }

  /** The replay of an entry the follower already holds writes its pages again but must not count its documents again. */
  @Test
  void replayingAnEntryAlreadyAppliedDoesNotCountItTwice() throws Exception {
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    final List<CorpusDelta> deltas = List.of(new CorpusDelta("Doc[content]", 4, 20));

    transactionManager.applyChanges(entry, Collections.emptyMap(), deltas, false);
    transactionManager.applyChanges(entry, Collections.emptyMap(), deltas, false);

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 4L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 20);
  }

  /**
   * A follower that restarts replays the Raft log from its snapshot marker, so it meets again entries it had applied before the
   * restart, whose effect the counters it persisted at close already include. The pages are at the entry's version, so nothing
   * advances and the counters are not moved a second time.
   */
  @Test
  void replayingAnEntryAfterARestartDoesNotCountItAgainWhenThePersistedCountersHoldIt() throws Exception {
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();
    final List<CorpusDelta> deltas = List.of(new CorpusDelta("Doc[content]", 4, 20));
    ((DatabaseInternal) database).getTransactionManager().applyChanges(entry, Collections.emptyMap(), deltas, false);
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 4L);

    reopenDatabase();
    assertThat(metadata().getTotalDocs()).as("the close persisted the counters with the entry in them").isEqualTo(DOCS + 4L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 20);

    ((DatabaseInternal) database).getTransactionManager().applyChanges(entry, Collections.emptyMap(), deltas, false);

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 4L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 20);
  }

  @Test
  void anIndexThisNodeNoLongerHasIsIgnored() throws Exception {
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();

    final boolean applied = ((DatabaseInternal) database).getTransactionManager()
        .applyChanges(entry, Collections.emptyMap(), List.of(new CorpusDelta("Dropped[content]", 4, 20)), false);

    assertThat(applied).isTrue();
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
  }

  /** An index dropped between the proposal of the transaction and its apply is nobody's business: the entry still applies. */
  @Test
  void anIndexDroppedBeforeTheApplyIsTolerated() throws Exception {
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();
    database.command("sql", "DROP INDEX `Doc[content]`");
    assertThat(database.getSchema().existsIndex("Doc[content]")).isFalse();

    final boolean applied = ((DatabaseInternal) database).getTransactionManager()
        .applyChanges(entry, Collections.emptyMap(), List.of(new CorpusDelta("Doc[content]", 4, 20)), false);

    assertThat(applied).isTrue();
  }

  /** Whatever the lookup of a delta's index raises, the entry itself still applies and the other deltas are not lost. */
  @Test
  void aDeltaWhoseIndexCannotBeResolvedDoesNotFailTheApplyNorTheOtherDeltas() throws Exception {
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();

    final boolean applied = ((DatabaseInternal) database).getTransactionManager().applyChanges(entry, Collections.emptyMap(),
        List.of(new CorpusDelta(null, 9, 9), new CorpusDelta("Doc[content]", 4, 20)), false);

    assertThat(applied).isTrue();
    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS + 4L);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L + 20);
  }

  @Test
  void anEntryWithoutDeltasLeavesTheCountersAlone() throws Exception {
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();

    ((DatabaseInternal) database).getTransactionManager().applyChanges(entry, Collections.emptyMap(), false);

    assertThat(metadata().getTotalDocs()).isEqualTo(DOCS);
    assertThat(metadata().getSumDocLength()).isEqualTo(DOCS * 3L);
  }
}
