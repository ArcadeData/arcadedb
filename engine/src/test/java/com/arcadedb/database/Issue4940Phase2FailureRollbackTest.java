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

import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #4940: a {@code commit2ndPhase} failure that happens BEFORE the transaction is appended to the WAL must
 * restore user-held record state exactly like a phase-1 failure does ({@code rollback()}), not just release
 * resources ({@code reset()}). Otherwise records created in the failed transaction keep an optimistically-assigned
 * RID pointing at a record that was never persisted, and re-saving them in a retry silently updates a missing
 * record instead of re-inserting (the #4562 class of bug, resurfacing through the phase-2 failure path).
 * <p>
 * A failure AFTER the WAL append is intentionally NOT rolled back: the transaction is durable and recovery replays
 * it, so the assigned RIDs remain valid.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4940Phase2FailureRollbackTest extends TestHelper {

  private static final String TYPE = "Issue4940Doc";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE));
  }

  @Test
  void phase2FailureBeforeWalAppendResetsCreatedRecordIdentity() {
    database.begin();
    final MutableDocument doc = database.newDocument(TYPE);
    doc.set("name", "one");
    doc.save();
    assertThat(doc.getIdentity()).as("record gets an optimistic RID at creation").isNotNull();

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

    // Force a phase-2 failure BEFORE the WAL append: invoking the 2nd phase without having run the 1st fails the
    // status precondition, which is one of the pre-durability failure points of commit2ndPhase (same finally block
    // as a WAL-append failure).
    final Throwable error = catchThrowable(
        () -> tx.commit2ndPhase(new TransactionContext.TransactionPhase1(null, List.of())));
    assertThat(error).isInstanceOf(TransactionException.class);
    assertThat(database.isTransactionActive()).isFalse();

    // #4940: nothing durable exists, so the failure must behave like any other failed commit: the identity is
    // provisional again and the same in-memory object can be cleanly re-inserted on retry.
    assertThat(doc.getIdentity()).as("identity must be reset after a pre-durability phase-2 failure").isNull();

    database.begin();
    doc.save();
    database.commit();

    assertThat(doc.getIdentity()).isNotNull();
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
    database.begin();
    assertThat(database.lookupByRID(doc.getIdentity(), true).asDocument().getString("name")).isEqualTo("one");
    database.commit();
  }

  @Test
  void phase2FailureBeforeWalAppendReloadsModifiedRecords() {
    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE);
      doc.set("name", "committed");
      doc.save();
      holder[0] = doc;
    });
    final RID rid = holder[0].getIdentity();

    database.begin();
    final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
    doc.set("name", "uncommitted");
    doc.save();

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final Throwable error = catchThrowable(
        () -> tx.commit2ndPhase(new TransactionContext.TransactionPhase1(null, List.of())));
    assertThat(error).isInstanceOf(TransactionException.class);

    // #4940: the user-held object must be reloaded to its committed content, like rollback() does.
    assertThat(doc.getString("name")).as("modified record must be reloaded to the committed version").isEqualTo("committed");

    database.begin();
    assertThat(database.lookupByRID(rid, true).asDocument().getString("name")).isEqualTo("committed");
    database.commit();
  }

  @Test
  void phase2PublishFailureWithoutWalStillRollsBack() throws Exception {
    // With useWAL=false there is NO WAL append at all: nothing is ever durable in phase 2, so even a failure in the
    // page-publish stage (past the point where a WAL-enabled transaction would have crossed durability) must roll
    // back like any other pre-durability failure. Guards the placement of the walAppended flag: only a REAL append
    // may suppress the rollback.
    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE);
      doc.set("name", "committed");
      doc.save();
      holder[0] = doc;
    });
    final RID rid = holder[0].getIdentity();

    database.begin();
    database.setUseWAL(false);

    final MutableDocument modified = database.lookupByRID(rid, true).asDocument().modify();
    modified.set("name", "uncommitted");
    modified.save();

    final MutableDocument created = database.newDocument(TYPE);
    created.set("name", "new");
    created.save();
    assertThat(created.getIdentity()).isNotNull();

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    assertThat(phase1).isNotNull();
    assertThat(phase1.result).as("useWAL=false produces no WAL buffer").isNull();

    // Sabotage the publish stage: bump the committed version of the record's page behind the transaction's back, so
    // commit2ndPhase fails with a ConcurrentModificationException INSIDE updatePages - after the (skipped) WAL stage.
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    final PageId pageId = new PageId(database, rid.getBucketId(), (int) (rid.getPosition() / bucket.getMaxRecordsInPage()));
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final MutablePage conflicting = pageManager.getMutablePage(pageId, bucket.getPageSize(), false, false);
    pageManager.updatePages(null, Map.of(pageId, conflicting), false);

    final Throwable error = catchThrowable(() -> tx.commit2ndPhase(phase1));
    assertThat(error).isInstanceOf(ConcurrentModificationException.class);
    assertThat(database.isTransactionActive()).isFalse();

    // nothing was durable: the failed transaction must be rolled back, not just reset
    assertThat(created.getIdentity()).as("identity must be reset: no WAL record exists to ever replay this tx").isNull();
    assertThat(modified.getString("name")).as("modified record must be reloaded to the committed version").isEqualTo("committed");
  }
}
