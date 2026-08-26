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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6141: two transactions updating the same PLACEHOLDER-backed record both committed, and the second one
 * silently overwrote the first - no {@code ConcurrentModificationException} was raised anywhere.
 * <p>
 * The reason is that a record update is deferred: {@code TransactionContext.addUpdatedRecord} pins the record's OWN
 * page at {@code save()} time and the write runs at commit, after the file's commit lock is taken. For a
 * placeholder-backed record the pinned page holds the 8-byte POINTER, which an update that the content record can
 * absorb never touches - so nothing on it is version-checked - while the CONTENT page is pinned by nothing and is
 * loaded fresh, at the newest committed version, by both transactions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6141PlaceholderLostUpdateTest extends BucketPageLayoutTestSupport {
  /** 20 KB of content: big enough to spill out of a sealed page, small enough to fit a page of its own. */
  private static final int CONTENT_SIZE = 20 * 1024;

  /**
   * The issue itself: the concurrent update fits the content record, so not a byte of the pointer's page changes and
   * the whole write lands on a page neither transaction pinned. The second commit must be told to retry instead of
   * quietly winning.
   */
  @Test
  void aConcurrentUpdateOfThePlaceholderContentIsRefused() throws Exception {
    final RID placeholder = placeholderBackedRecord("Absorbed");

    final String concurrent = "c".repeat(CONTENT_SIZE);

    database.begin();
    placeholder.asDocument(true).modify().set("v", "l".repeat(CONTENT_SIZE)).save();

    commitInAnotherThread(placeholder, concurrent);

    assertConflictOnCommit();

    database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v"))
        .as("the concurrent update must be the one that survived").isEqualTo(concurrent));
    checkDatabase();
  }

  /**
   * The other half of the same branch: this transaction's value no longer fits the content record, so the slot is
   * rebuilt from scratch (the old content record is deleted and the record re-spills). That path rewrites the
   * pointer's page - but it does so from a content record another transaction has meanwhile replaced, so it drops
   * that write just as silently.
   */
  @Test
  void aConcurrentUpdateIsRefusedEvenWhenTheContentRecordHasToBeRebuilt() throws Exception {
    final RID placeholder = placeholderBackedRecord("Rebuilt");

    final String concurrent = "c".repeat(CONTENT_SIZE);

    database.begin();
    // 200 KB fits no page at all: the content record cannot absorb it and the slot is rebuilt.
    placeholder.asDocument(true).modify().set("v", "l".repeat(200 * 1024)).save();

    commitInAnotherThread(placeholder, concurrent);

    assertConflictOnCommit();

    database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(concurrent));
    checkDatabase();
  }

  /**
   * The counter-check that keeps the fix from being a blanket conflict: a transaction that updates the placeholder
   * record while another one updates a DIFFERENT record must still commit. The content record's fingerprint is about
   * that record alone, so an unrelated write - even one landing on the very page the content lives on - is none of
   * its business.
   */
  @Test
  void anUnrelatedConcurrentUpdateStillCommits() throws Exception {
    final RID placeholder = placeholderBackedRecord("Unrelated");

    final RID[] other = new RID[1];
    database.transaction(() -> other[0] = database.newDocument("Unrelated").set("v", "o").save().getIdentity());

    final String mine = "m".repeat(CONTENT_SIZE);

    database.begin();
    placeholder.asDocument(true).modify().set("v", mine).save();

    commitInAnotherThread(other[0], "other rewritten");

    database.commit();

    database.transaction(() -> {
      assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(mine);
      assertThat(other[0].asDocument(true).getString("v")).isEqualTo("other rewritten");
    });
    checkDatabase();
  }

  /**
   * The fingerprint is not part of the disjoint-slot merge, it only shares the moment the merge's own fingerprint is
   * taken: for a placeholder-backed record it is the ONLY conflict detection there is, so switching the merge off -
   * which restores the unconditional page poisoning that predates it - must not switch the protection off with it.
   */
  @Test
  void theConcurrentUpdateIsRefusedWithTheSlotMergeSwitchedOffToo() throws Exception {
    final RID placeholder = placeholderBackedRecord("MergeOff");

    final String concurrent = "c".repeat(CONTENT_SIZE);

    database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
    try {
      database.begin();
      placeholder.asDocument(true).modify().set("v", "l".repeat(CONTENT_SIZE)).save();

      commitInAnotherThread(placeholder, concurrent);

      assertConflictOnCommit();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
    }

    database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(concurrent));
    checkDatabase();
  }

  /**
   * Builds the one record shape this issue is about: a slot holding a placeholder POINTER, with the record's whole
   * content on another page. Since #6149 that takes a page with a free tail of exactly zero - the only shape left
   * where a spilling record cannot find the 14 bytes a chunk header needs.
   */
  private RID placeholderBackedRecord(final String typeName) {
    final RID[] tiny = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING);
      tiny[0] = database.newDocument(typeName).set("v", "p").save().getIdentity();
    });
    sealFirstPage(typeName);

    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(CONTENT_SIZE)).save());

    final Map<String, Object> layout = bucketStats(typeName);
    assertThat((Long) layout.get("totalPlaceholderRecords"))
        .as("this test is about a placeholder-backed record, and " + typeName + " has none: " + layout).isEqualTo(1L);
    assertThat((Long) layout.get("totalSurrogateRecords")).as("with its content on another page: " + layout).isEqualTo(1L);

    return tiny[0];
  }

  /** Rewrites {@code rid} in a transaction of its own, on another thread, and waits for that commit to land. */
  private void commitInAnotherThread(final RID rid, final String value) throws InterruptedException {
    final Thread concurrent = new Thread(
        () -> database.transaction(() -> rid.asDocument(true).modify().set("v", value).save()));
    concurrent.start();
    concurrent.join();
  }

  private void assertConflictOnCommit() {
    try {
      database.commit();
      throw new AssertionError("the commit must be refused: the record was rewritten by a concurrent transaction");
    } catch (final ConcurrentModificationException expected) {
      // THE CONTRACT: A CONCURRENT WRITE TO THE SAME RECORD IS A RETRYABLE CONFLICT, NOT A SILENT OVERWRITE
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }
}
