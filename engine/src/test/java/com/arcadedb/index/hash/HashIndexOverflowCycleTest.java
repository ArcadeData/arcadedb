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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #4743 (Frigg Tech): a customer's server ran at 200% CPU for two days. The jstack showed two
 * XNIO worker threads pinned RUNNABLE at 100% CPU, both stuck at the identical frame
 * {@code HashIndexBucket.searchBucket -> PageManager.getImmutablePage}, reached through the unique-constraint check
 * ({@code checkUniqueIndexKeys}) of a batch vertex import.
 *
 * <p>Root cause: a corrupted (cyclic) overflow chain in a UNIQUE hash index. The read/scan walkers
 * ({@code searchBucket} and friends) walked the chain with {@code while (currentPage != NO_OVERFLOW_PAGE)} and no
 * cycle detection - unlike the write paths ({@code insertIntoOverflow}/{@code insertRawEntry}), which already guard
 * with a {@code visited} set. When the chain looped back on itself the read loop spun forever, never returning and
 * never throwing, pinning the core.
 *
 * <p>These tests inject a self-referential overflow pointer (page N -> page N) and assert the walk now fails with an
 * actionable {@link IndexException} telling the operator to rebuild the index, instead of hanging. The tight
 * {@link Timeout} turns a regression (the infinite spin returning) into a fast test failure rather than a hung build.
 */
class HashIndexOverflowCycleTest extends TestHelper {
  private static final String TYPE_NAME = "Account";
  private static final int    ACCOUNTS  = 16;

  private String indexName;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType account = database.getSchema().createVertexType(TYPE_NAME);
      account.createProperty("bank", Type.STRING);
      account.createProperty("number", Type.STRING);
      final Index index = database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "bank", "number" })
          .withType(Schema.INDEX_TYPE.HASH)
          .withUnique(true)
          .create();
      indexName = index.getName();

      // A handful of small entries all land in the single initial bucket (globalDepth 0, no split), so the bucket the
      // directory points at is the one we corrupt below.
      for (int n = 0; n < ACCOUNTS; n++)
        database.newVertex(TYPE_NAME).set("bank", "bank_0").set("number", String.format("%06d", n)).save();
    });
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void cyclicOverflowChainFailsLoudlyOnLookupInsteadOfSpinning() {
    injectSelfReferentialOverflowChain();
    reopenDatabase();
    assertThat(database.isOpen()).isTrue();

    // Look up a key that does NOT exist: a unique lookup that finds a match returns from the first page before the
    // overflow pointer is followed, so we must miss on the primary page to make the walk step into the cycle.
    database.transaction(() -> assertThatThrownBy(() -> {
      final IndexCursor cursor = database.getSchema().getIndexByName(indexName).get(new Object[] { "bank_0", "999999" });
      cursor.hasNext();
    }).isInstanceOf(IndexException.class)
        .hasMessageContaining("cycle")
        .hasMessageContaining(TYPE_NAME)   // names the affected index (the underlying hash bucket of the Account type)
        .hasMessageContaining("rebuild")); // tells the operator how to recover
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void cyclicOverflowChainFailsLoudlyOnUniqueCheckDuringInsert() {
    // Mirrors the customer's stack: the spin was reached via checkUniqueIndexKeys while committing a new vertex.
    injectSelfReferentialOverflowChain();
    reopenDatabase();
    assertThat(database.isOpen()).isTrue();

    // The commit wraps the underlying IndexException in a TransactionException, so assert on the root cause.
    assertThatThrownBy(() -> database.transaction(() ->
        database.newVertex(TYPE_NAME).set("bank", "bank_0").set("number", "999999").save()))
        .hasRootCauseInstanceOf(IndexException.class)
        .hasStackTraceContaining("cycle")
        .hasStackTraceContaining("rebuild");
  }

  /**
   * Corrupts the hash index by making the bucket page the directory points at reference itself as its own overflow
   * page (page N -> page N), the minimal cyclic chain. Commits so the change is flushed to disk on the next reopen.
   */
  private void injectSelfReferentialOverflowChain() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = ((IndexInternal) db.getSchema().getIndexByName(indexName)).getFileIds().getFirst();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

    db.transaction(() -> {
      try {
        // Directory page (page 1), entry 0 holds the bucket page number for a fresh, unsplit index.
        final MutablePage dirPage = db.getTransaction().getPageToModify(new PageId(db, fileId, 1), pageSize, false);
        final int bucketPageNum = dirPage.readInt(0);

        final MutablePage bucketPage = db.getTransaction()
            .getPageToModify(new PageId(db, fileId, bucketPageNum), pageSize, false);
        bucketPage.writeInt(HashIndexBucket.BUCKET_OVERFLOW_PAGE, bucketPageNum); // point the bucket at itself
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
