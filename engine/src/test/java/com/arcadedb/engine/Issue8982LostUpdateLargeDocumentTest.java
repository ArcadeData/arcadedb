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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8982: under READ_COMMITTED, a read-modify-write of a document stored as a multi-page record silently overwrote
 * a change committed between the read and the update. The #6950 guard compared the image the update was computed from
 * with the slot, but a head chunk holds only the start of the record, so it answered "unchanged" without looking.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8982LostUpdateLargeDocumentTest extends TestHelper {

  private RID createDocument(final int size) {
    database.getSchema().getOrCreateDocumentType("Doc");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("n", 0).set("s", "x".repeat(size)).save().getIdentity());
    return rid[0];
  }

  @Test
  void staleReadModifyWriteIsRefusedForPlainAndMultiPageDocuments() throws Exception {
    final ExecutorService threadA = Executors.newSingleThreadExecutor();
    final ExecutorService threadB = Executors.newSingleThreadExecutor();
    try {
      for (final int size : new int[] { 30_000, 70_000, 300_000 }) {
        final RID rid = createDocument(size);
        final Document[] read = new Document[1];
        threadA.submit(() -> {
          database.begin();
          read[0] = database.lookupByRID(rid, true).asDocument();
        }).get();

        threadB.submit(() -> database.transaction(() -> {
          final MutableDocument d = database.lookupByRID(rid, true).asDocument().modify();
          d.set("n", d.getInteger("n") + 1);
          d.save();
        })).get();

        assertThatThrownBy(() -> threadA.submit(() -> {
          try {
            final MutableDocument d = read[0].modify();
            d.set("n", read[0].getInteger("n") + 1);
            d.save();
            database.commit();
          } finally {
            if (database.isTransactionActive())
              database.rollback();
          }
          return null;
        }).get()).as("size " + size).hasRootCauseInstanceOf(ConcurrentModificationException.class);

        assertThat(database.lookupByRID(rid, true).asDocument().getInteger("n")).as("size " + size).isEqualTo(1);
      }
    } finally {
      threadA.shutdown();
      threadB.shutdown();
    }
  }

  @Test
  void concurrentCountersOnMultiPageDocumentLoseNoIncrement() throws Exception {
    final RID rid = createDocument(70_000);
    final int threads = 4;
    final int perThread = 100;
    final AtomicInteger committed = new AtomicInteger();
    final Thread[] workers = new Thread[threads];
    final Throwable[] failure = new Throwable[1];
    for (int t = 0; t < threads; t++) {
      workers[t] = new Thread(() -> {
        try {
          for (int i = 0; i < perThread; i++) {
            database.transaction(() -> {
              final MutableDocument d = database.lookupByRID(rid, true).asDocument().modify();
              d.set("n", d.getInteger("n") + 1);
              d.save();
            }, false, 1000);
            committed.incrementAndGet();
          }
        } catch (final Throwable e) {
          failure[0] = e;
        }
      });
      workers[t].start();
    }
    for (final Thread w : workers)
      w.join();

    assertThat(failure[0]).isNull();
    assertThat(database.lookupByRID(rid, true).asDocument().getInteger("n")).isEqualTo(committed.get()).isEqualTo(threads * perThread);
  }
}
