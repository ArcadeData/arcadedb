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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8312: a full scan builds each record from the page it already holds, instead of handing out a lazy shell that
 * re-reads the record through a second page lookup on its first property access. {@code modify()} no longer reloads
 * a record whose content is still the one on the current page version, so "scan, then modify" reads the record once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8312ScanEagerRecordTest extends TestHelper {
  private static final int RECORDS = 10_000;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc8312");
    database.getSchema().createVertexType("V8312");
    database.getSchema().createEdgeType("E8312");
  }

  /**
   * Reading a property of every scanned record used to cost one more page-cache lookup per record (the lazy load).
   */
  @Test
  void readingPropertiesOfScannedRecordsDoesNotLookThemUpAgain() {
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Doc8312").set("id", i).save();
    });

    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    assertThat(sumIds()).isEqualTo((long) RECORDS * (RECORDS - 1) / 2); // WARM-UP: EVERY PAGE CACHED

    final long hitsBefore = pageManager.getStats().cacheHits;
    assertThat(sumIds()).isEqualTo((long) RECORDS * (RECORDS - 1) / 2);
    final long hits = pageManager.getStats().cacheHits - hitsBefore;

    assertThat(hits).as("page-cache lookups to scan %d records and read a property of each", RECORDS)
        .isLessThan(RECORDS / 10);
  }

  private RID saved(final Supplier<Record> creation) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = creation.get().getIdentity());
    return rid[0];
  }

  private long sumIds() {
    long sum = 0;
    final Iterator<Record> it = database.iterateType("Doc8312", false);
    while (it.hasNext())
      sum += it.next().asDocument().getInteger("id");
    return sum;
  }

  /**
   * Scan, then modify: the record's read events fire once. The content the scan built is still the one on the page, so
   * modify() must not reload it (which would notify the listeners a second time).
   */
  @Test
  void scanThenModifyNotifiesReadListenersOnce() {
    database.transaction(() -> {
      database.newVertex("V8312").set("id", 1).save();
      database.newDocument("Doc8312").set("id", 1).save();
    });

    final AtomicInteger before = new AtomicInteger();
    final AtomicInteger after = new AtomicInteger();
    final BeforeRecordReadListener beforeListener = rid -> {
      before.incrementAndGet();
      return true;
    };
    final AfterRecordReadListener afterListener = record -> {
      after.incrementAndGet();
      return record;
    };
    database.getEvents().registerListener(beforeListener);
    database.getEvents().registerListener(afterListener);
    try {
      for (final String type : new String[] { "V8312", "Doc8312" }) {
        before.set(0);
        after.set(0);
        database.transaction(() -> {
          final MutableDocument modified = database.iterateType(type, false).next().asDocument().modify();
          modified.set("modified", true).save();
        });
        assertThat(before.get()).as("before-read on %s", type).isEqualTo(1);
        assertThat(after.get()).as("after-read on %s", type).isEqualTo(1);
      }
    } finally {
      database.getEvents().unregisterListener(beforeListener);
      database.getEvents().unregisterListener(afterListener);
    }

    database.transaction(() -> {
      assertThat(database.iterateType("V8312", false).next().asDocument().getBoolean("modified")).isTrue();
      assertThat(database.iterateType("Doc8312", false).next().asDocument().getBoolean("modified")).isTrue();
    });
  }

  /**
   * The reload modify() skips is only skippable while the page is unchanged: a commit landing between the scan and the
   * modify() must be seen, or the modify would write back the stale content and silently undo it.
   */
  @Test
  void modifyAfterAConcurrentCommitStillSeesIt() throws Exception {
    final RID rid = saved(() -> database.newVertex("V8312").set("id", 1).save());

    database.begin();
    final Vertex scanned = database.iterateType("V8312", false).next().asVertex();
    assertThat(scanned.getIdentity()).isEqualTo(rid);

    // ANOTHER THREAD COMMITS A CHANGE TO THE SAME RECORD AFTER THE SCAN READ IT
    final Thread other = new Thread(() -> database.transaction(() -> rid.asVertex().modify().set("other", "yes").save()));
    other.start();
    other.join();

    final MutableVertex modified = scanned.modify();
    assertThat(modified.getString("other")).isEqualTo("yes");
    modified.set("mine", "yes").save();
    database.commit();

    final Vertex reloaded = rid.asVertex();
    assertThat(reloaded.getString("other")).isEqualTo("yes");
    assertThat(reloaded.getString("mine")).isEqualTo("yes");
  }

  /**
   * A record modified earlier in the same transaction is answered from the transaction, not from the page.
   */
  @Test
  void scanInsideATransactionReturnsItsOwnChanges() {
    final RID rid = saved(() -> database.newDocument("Doc8312").set("id", 1).save());

    database.transaction(() -> {
      rid.asDocument().modify().set("id", 2).save();
      final Document scanned = database.iterateType("Doc8312", false).next().asDocument();
      assertThat(scanned.getInteger("id")).isEqualTo(2);
    });
  }

  /**
   * An after-read listener that transforms the record (the shape of the encryption hook) applies to every record a scan
   * returns, including one that moved to a placeholder after an update made it grow, and a multi-page one. A record the
   * listener filters away is not returned.
   */
  @Test
  void afterReadAppliesToEveryScannedRecordShape() {
    database.transaction(() -> {
      for (int i = 0; i < 300; i++) {
        final MutableDocument doc = database.newDocument("Doc8312").set("id", i).set("secret", "s" + i);
        doc.save();
      }
    });
    database.transaction(() -> {
      final Iterator<Record> it = database.iterateType("Doc8312", false);
      int i = 0;
      while (it.hasNext()) {
        final Document doc = it.next().asDocument();
        if (i % 10 == 0)
          // GROWS PAST ITS SLOT (PLACEHOLDER), EVERY THIRD ONE PAST A PAGE (MULTI-PAGE)
          doc.modify().set("payload", "x".repeat(i % 30 == 0 ? 200_000 : 5_000)).save();
        ++i;
      }
    });

    final AfterRecordReadListener listener = record -> {
      final Document doc = record.asDocument();
      if (doc.getInteger("id") % 7 == 0)
        return null;
      return doc.modify().set("secret", "decrypted");
    };
    database.getEvents().registerListener(listener);
    try {
      final Set<Integer> ids = new HashSet<>();
      final Iterator<Record> it = database.iterateType("Doc8312", false);
      while (it.hasNext()) {
        final Document doc = it.next().asDocument();
        assertThat(doc.getString("secret")).as("record %s", doc.getInteger("id")).isEqualTo("decrypted");
        assertThat(ids.add(doc.getInteger("id"))).isTrue();
      }
      assertThat(ids).hasSize(300 - 43).noneMatch(id -> id % 7 == 0);
    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  /**
   * A scanned record answers the same database instance as one looked up by RID, so that anything the record does
   * through it (modify, save, graph operations) goes through the same wrapper the lookup would give it.
   */
  @Test
  void scannedRecordsBelongToTheSameDatabaseInstanceAsLookedUpOnes() {
    final RID rid = saved(() -> database.newDocument("Doc8312").set("id", 1).save());
    final Document scanned = database.iterateType("Doc8312", false).next().asDocument();
    assertThat(scanned.getDatabase()).isSameAs(database.lookupByRID(rid, true).getDatabase());
  }
}
