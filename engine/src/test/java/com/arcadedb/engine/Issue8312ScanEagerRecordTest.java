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

import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
class Issue8312ScanEagerRecordTest extends BucketPageLayoutTestSupport {
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
   * The same guarantee for a document and an edge: their content now comes from the scan, not from a lazy load at
   * modify() time, so modify() has to notice the page moved on and reload, as it does for a vertex.
   */
  @Test
  void modifyOfDocumentAndEdgeAfterAConcurrentCommitStillSeesIt() throws Exception {
    final RID docRid = saved(() -> database.newDocument("Doc8312").set("id", 1).save());
    final RID[] edgeRid = new RID[1];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V8312").set("id", 10).save();
      final MutableVertex b = database.newVertex("V8312").set("id", 11).save();
      edgeRid[0] = a.newEdge("E8312", b).getIdentity();
    });

    for (final RID rid : new RID[] { docRid, edgeRid[0] }) {
      final String type = rid.equals(docRid) ? "Doc8312" : "E8312";
      database.begin();
      final Document scanned = database.iterateType(type, false).next().asDocument();
      assertThat(scanned.getIdentity()).isEqualTo(rid);

      final Thread other = new Thread(
          () -> database.transaction(() -> rid.asDocument().modify().set("other", "yes").save()));
      other.start();
      other.join();

      final MutableDocument modified = scanned.modify();
      assertThat(modified.getString("other")).as("%s sees the concurrent commit", type).isEqualTo("yes");
      modified.set("mine", "yes").save();
      database.commit();

      final Document reloaded = rid.asDocument();
      assertThat(reloaded.getString("other")).as(type).isEqualTo("yes");
      assertThat(reloaded.getString("mine")).as(type).isEqualTo("yes");
    }
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
   * returns: one stored in its own slot, one that moved to a placeholder after an update made it grow, and a multi-page
   * one. A record the listener filters away is not returned.
   */
  @Test
  void afterReadAppliesToEveryScannedRecordShape() {
    // THE LAYOUT THAT FORCES A PLACEHOLDER (#6149): A TINY RECORD ON PAGE 0, PAGE 0 SEALED WITH NO FREE TAIL (ITS LAST
    // RECORD SPILLS INTO CHUNKS: THE MULTI-PAGE ONE), THEN THE TINY RECORD GROWN PAST WHAT THE PAGE CAN HOST
    final RID[] tiny = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Shape8312", 1).createProperty("v", Type.STRING);
      tiny[0] = database.newDocument("Shape8312").set("v", "p").save().getIdentity();
    });
    final RID sealing = sealFirstPage("Shape8312");
    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Shape8312").set("v", "small").set("hidden", i % 2 == 0).save();
    });

    // THE FIXTURE MUST REALLY HOLD EVERY SHAPE, OR THIS TEST PROVES NOTHING ABOUT THE ONES IT LACKS
    final Map<String, Object> layout = bucketStats("Shape8312");
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("placeholder records: %s", layout).isPositive();
    assertThat((Long) layout.get("totalMultiPageRecords")).as("multi-page records: %s", layout).isPositive();
    // EVERY RECORD OF THE TYPE, COUNTED BY A SCAN WITH NO LISTENER REGISTERED
    int total = 0;
    for (final Iterator<Record> all = database.iterateType("Shape8312", false); all.hasNext(); all.next())
      ++total;

    final AfterRecordReadListener listener = record -> {
      final Document doc = record.asDocument();
      if (Boolean.TRUE.equals(doc.getBoolean("hidden")))
        return null;
      return doc.modify().set("secret", "decrypted");
    };
    database.getEvents().registerListener(listener);
    try {
      final Set<RID> seen = new HashSet<>();
      final Iterator<Record> it = database.iterateType("Shape8312", false);
      while (it.hasNext()) {
        final Document doc = it.next().asDocument();
        assertThat(doc.getString("secret")).as("record %s", doc.getIdentity()).isEqualTo("decrypted");
        assertThat(doc.getBoolean("hidden")).as("record %s", doc.getIdentity()).isNotEqualTo(Boolean.TRUE);
        assertThat(seen.add(doc.getIdentity())).isTrue();
      }
      assertThat(seen).hasSize(total - 5).contains(tiny[0], sealing);
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

  /**
   * The scan no longer goes through lookupByRID(), which is what counted records into the readRecord statistic: it has
   * to report them itself, one per scanned record.
   */
  @Test
  void scannedRecordsAreCountedInTheReadRecordStatistic() {
    database.transaction(() -> {
      for (int i = 0; i < 3_000; i++)
        database.newDocument("Doc8312").set("id", i).save();
    });

    final long before = ((Number) database.getStats().get("readRecord")).longValue();
    int scanned = 0;
    final Iterator<Record> it = database.iterateType("Doc8312", false);
    while (it.hasNext()) {
      it.next();
      ++scanned;
    }
    assertThat(scanned).isEqualTo(3_000);
    assertThat(((Number) database.getStats().get("readRecord")).longValue() - before).isEqualTo(3_000L);

    // A RECORD A BEFORE-READ EVENT HIDES WAS STILL READ: lookupByRID() COUNTS IT, AND SO DOES THE SCAN
    final BeforeRecordReadListener hideAll = rid -> false;
    database.getEvents().registerListener(hideAll);
    try {
      final long beforeHidden = ((Number) database.getStats().get("readRecord")).longValue();
      assertThat(database.iterateType("Doc8312", false).hasNext()).isFalse();
      assertThat(((Number) database.getStats().get("readRecord")).longValue() - beforeHidden).isEqualTo(3_000L);
    } finally {
      database.getEvents().unregisterListener(hideAll);
    }
  }

  /**
   * A record that moved to a placeholder is notified to a before-read listener once, under its own RID, whether it is
   * scanned or looked up. The loader used to notify it a second time under the internal position of its content.
   */
  @Test
  void placeholderRecordIsNotifiedOnceUnderItsOwnRid() {
    final RID[] tiny = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Ph8312", 1).createProperty("v", Type.STRING);
      tiny[0] = database.newDocument("Ph8312").set("v", "p").save().getIdentity();
    });
    sealFirstPage("Ph8312");
    database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
    assertThat((Long) bucketStats("Ph8312").get("totalPlaceholderRecords")).isPositive();

    final List<RID> notified = new ArrayList<>();
    final BeforeRecordReadListener listener = rid -> {
      notified.add(rid);
      return true;
    };
    database.getEvents().registerListener(listener);
    try {
      final List<RID> scanned = new ArrayList<>();
      final Iterator<Record> it = database.iterateType("Ph8312", false);
      while (it.hasNext())
        scanned.add(it.next().getIdentity());
      // EXACTLY ONE NOTIFICATION PER RECORD RETURNED, UNDER THAT RECORD'S RID: NONE UNDER THE CONTENT'S INTERNAL ONE
      assertThat(scanned).contains(tiny[0]);
      assertThat(notified).as("scan").containsExactlyInAnyOrderElementsOf(scanned);

      notified.clear();
      database.lookupByRID(tiny[0], true);
      assertThat(notified).as("lookup").containsExactly(tiny[0]);
    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }
}
