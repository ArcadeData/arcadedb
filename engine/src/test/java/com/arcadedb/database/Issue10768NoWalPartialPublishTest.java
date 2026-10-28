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
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10768: with {@code arcadedb.txWAL=false} no WAL record is appended, so a commit whose page publication failed halfway was
 * concluded with a full {@code rollback()}: the caller was told nothing happened while the pages already written stayed on disk
 * and in the read cache, and the records created in the transaction had their identities reset, so a retry inserted them again.
 * The start of the publication is now a point of no return of its own: past it the database is fenced and the transaction is
 * released without touching the records the caller holds, as a failure past the WAL append is.
 * <p>
 * The page write that fails is chosen in turn, from the first page of the transaction to the last but one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10768NoWalPartialPublishTest extends TestHelper {
  private static final int RECORDS = 3_000;

  @Test
  void aPublicationThatFailsOnAnyPageFencesAndKeepsTheRecordsTheCallerHolds() {
    final int pages = pagesWrittenByOneTransaction();
    assertThat(pages).as("the transaction must span several pages for a failure to land halfway").isGreaterThan(2);

    for (int failingWrite = 1; failingWrite < pages; failingWrite++) {
      final String typeName = "Doc" + failingWrite;
      database.getSchema().createDocumentType(typeName);
      noWalSynchronousFlush();

      final List<MutableDocument> created = new ArrayList<>(RECORDS);
      final AtomicInteger writes = new AtomicInteger();
      final int failAt = failingWrite;
      try {
        assertThatThrownBy(() -> database.transaction(() -> {
          for (int i = 0; i < RECORDS; i++)
            created.add(database.newDocument(typeName).set("i", i).set("payload", "x".repeat(64)).save());
          // ARMED LAST, SO ONLY THE PAGES OF THIS COMMIT COUNT
          PageManager.setPageWriteFaultInjector(pageId -> {
            if (writes.incrementAndGet() == failAt)
              throw new IOException("injected failure of page write " + failAt);
          });
        }, false, 0)).isInstanceOf(TransactionException.class);
      } finally {
        PageManager.setPageWriteFaultInjector(null);
      }

      assertThat(writes.get()).as("the injected failure was reached").isGreaterThanOrEqualTo(failAt);
      // NOT "NOTHING HAPPENED": THE DATABASE REFUSES TO COMMIT ON TOP OF A HALF PUBLISHED TRANSACTION...
      assertThat(isFenced()).as("fenced, failing write %d", failAt).isTrue();
      // ...AND THE RECORDS THE CALLER HOLDS KEEP THE IDENTITIES SOME OF THEM ALREADY HAVE ON DISK, SO A RETRY CANNOT DUPLICATE THEM
      assertThat(created).hasSize(RECORDS).allSatisfy(doc -> assertThat(doc.getIdentity()).isNotNull());

      reopenDatabase();
    }
  }

  @Test
  void aFailureBeforeThePublicationStartsStillRollsBack() {
    database.getSchema().createDocumentType("Doc");
    noWalSynchronousFlush();

    final List<MutableDocument> created = new ArrayList<>();
    assertThatThrownBy(() -> database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        created.add(database.newDocument("Doc").set("i", i).save());
      // REFUSED BY THE 1ST PHASE: NOTHING WAS PUBLISHED, SO THE ROLLBACK IS THE RIGHT CONCLUSION AND NOTHING IS FENCED
      ((DatabaseInternal) database).getTransaction().setRollbackOnly("refused before the publication");
    }, false, 0)).isInstanceOf(TransactionException.class);

    assertThat(isFenced()).isFalse();
    assertThat(created).allSatisfy(doc -> assertThat(doc.getIdentity()).isNull());
    assertThat(database.countType("Doc", false)).isZero();
  }

  /** The pages a commit of {@link #RECORDS} records writes, measured on a type of its own with the same flush settings. */
  private int pagesWrittenByOneTransaction() {
    database.getSchema().createDocumentType("Probe");
    noWalSynchronousFlush();
    final AtomicInteger writes = new AtomicInteger();
    try {
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument("Probe").set("i", i).set("payload", "x".repeat(64)).save();
        PageManager.setPageWriteFaultInjector(pageId -> writes.incrementAndGet());
      });
    } finally {
      PageManager.setPageWriteFaultInjector(null);
    }
    return writes.get();
  }

  private boolean isFenced() {
    return ((DatabaseInternal) database).getEmbedded() instanceof LocalDatabase local && local.isFencedForRecovery();
  }

  private void noWalSynchronousFlush() {
    database.setUseWAL(false);
    database.setAsyncFlush(false);
  }
}
