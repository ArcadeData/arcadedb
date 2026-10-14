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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
