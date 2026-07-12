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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Correctness tests for the LSM read/merge/compaction paths. Auto compaction is disabled (on the database's own
 * context configuration, which snapshots the global at creation time) so that every compaction here is explicit
 * and verified to have actually run; otherwise a background compaction steals the COMPACTION_SCHEDULED status and
 * silently turns the explicit calls into no-ops, voiding the tests. Regression targets:
 * <ul>
 * <li>#4942: the compactor merged pages oldest-to-newest into an insertion-ordered set, so a re-inserted RID
 * (ADD, REMOVE, ADD of the same key+RID across pages) kept its pre-tombstone position and the reader, which
 * treats the LAST position as the newest entry, resolved the tombstone as the winner: the row vanished from the
 * index after compaction.</li>
 * <li>#4944: the cursor constructor left a cursor parked on a full-key tombstone alive but uncounted in
 * {@code validIterators}, while {@code next()} decrements the counter when any cursor is exhausted: a range scan
 * could stop with valid cursors still holding rows.</li>
 * <li>#4945: {@code get()} on a non-unique index resurrected a deleted RID when a per-RID tombstone and the
 * original ADD lived in different pages, because the {@code deletedRIDs} set was page-local instead of threaded
 * across pages and the compacted sub-index (like {@code removedKeys}).</li>
 * </ul>
 * The remaining tests guard multi-page and multi-series layouts, including multiple compactions, partial series produced
 * within one RAM-bounded compaction, cross-page deletes, and deleted boundary keys.
 */
class LSMTreeCompactionCorrectnessTest extends TestHelper {

  private static final String TYPE_NAME = "Doc";

  @Override
  public void beforeTest() {
    // Disable AUTO compaction: the database snapshots this into its own context configuration, so it must be set
    // on the database instance too (see createType). With the default (10) an async background compaction races
    // the test and steals the COMPACTION_SCHEDULED status, silently turning every explicit compactAll() into a
    // no-op and making all these tests false negatives.
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  private void createType(final boolean unique) {
    createType(unique, 1024);
  }

  private void createType(final boolean unique, final int pageSize) {
    // The database was created before beforeTest() ran, so its context configuration snapshot still holds the
    // default: override it on the instance before the index reads it at creation time.
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("email", String.class);
    // Small pages so a handful of entries already seals immutable pages the compactor will merge.
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(unique).withPageSize(pageSize).create();
  }

  private void filler(final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newDocument(TYPE_NAME).set("email", String.format("f%06d", i)).save();
    });
  }

  private void compactAll() {
    for (final Index index : database.getSchema().getIndexes()) {
      if (index instanceof TypeIndex) {
        try {
          // Both must succeed: a false return means the compaction silently did not run and the test is void.
          assertThat(((IndexInternal) index).scheduleCompaction()).as("compaction scheduled").isTrue();
          assertThat(((IndexInternal) index).compact()).as("compaction executed").isTrue();
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private List<RID> lookup(final String value) {
    final List<RID> rids = new ArrayList<>();
    final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = ?", value);
    while (rs.hasNext())
      rids.add(rs.next().getIdentity().get());
    return rids;
  }

  @Test
  void caseInsensitiveFlagsSurviveCompactionFileSwap() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("email", String.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withCollations(List.of("CI"))
        .withUnique(false)
        .withPageSize(1_024)
        .create();

    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument(TYPE_NAME).set("email", i % 2 == 0 ? "MixedCase" : "mixedcase").save();
    });
    assertThat(lookup("MIXEDCASE")).hasSize(1_000);

    compactAll();

    assertThat(lookup("MIXEDCASE")).hasSize(1_000);
  }

  // ---- #4942: compaction must not lose a re-inserted key/RID (ADD, REMOVE, ADD of the same key+RID) ----

  @Test
  void reinsertAfterDeleteSurvivesCompactionUnique() {
    createType(true);
    reinsertScenario();
  }

  @Test
  void reinsertAfterDeleteSurvivesCompactionNonUnique() {
    createType(false);
    reinsertScenario();
  }

  private void reinsertScenario() {
    // Use a key value that sorts in the middle of the filler ("f...") so its ADD/REMOVE/ADD entries land in
    // interior immutable pages that the compactor merges.
    final String key = "f500000-K";

    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> holder[0] = database.newDocument(TYPE_NAME).set("email", key).save());
    final RID rid = holder[0].getIdentity();

    // Enough filler before/after each op to seal the entries into immutable pages (compactor skips the current
    // mutable page and needs >= 2 pages). For key `key` the index sees ADD(rid), REMOVE(rid), ADD(rid).
    filler(0, 400);
    database.transaction(() -> holder[0].modify().set("email", "temp-away").save()); // REMOVE key->rid
    filler(400, 400);
    database.transaction(() -> holder[0].modify().set("email", key).save());          // ADD key->rid again
    // Heavy filler + a second compaction so the key's three entries end up merged in the compacted layer and the
    // final ADD is not left masking the bug in the current mutable page.
    filler(800, 2000);
    compactAll();
    filler(2800, 2000);
    compactAll();

    assertThat(lookup(key)).as("re-inserted key must survive compaction").containsExactly(rid);
    assertThat(lookup("temp-away")).as("intermediate value must be gone").isEmpty();
  }

  // ---- descending iteration over compacted series must remain globally ordered and must not emit phantom RIDs ----

  @Test
  void descendingScanAfterTwoCompactionsHasNoPhantoms() {
    createType(false);

    for (int i = 0; i < 200; i++) {
      final int v = i;
      database.transaction(() -> database.newDocument(TYPE_NAME).set("email", String.format("k%04d", v)).save());
    }
    compactAll(); // series 1

    for (int i = 200; i < 400; i++) {
      final int v = i;
      database.transaction(() -> database.newDocument(TYPE_NAME).set("email", String.format("k%04d", v)).save());
    }
    compactAll(); // series 2

    final ResultSet rs = database.query("sql", "SELECT email, @rid AS rid FROM " + TYPE_NAME + " ORDER BY email DESC");
    final List<String> seen = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final String email = r.getProperty("email");
      final RID rid = (RID) r.getProperty("rid");
      // A phantom root-page entry surfaces as bucketId 0 (RID #0:x); real records live in positive buckets.
      assertThat(rid.getBucketId()).as("no phantom #0:x RID for key " + email).isNotEqualTo(0);
      assertThat(email).as("no null/phantom key in DESC scan").isNotNull();
      seen.add(email);
    }
    // Exactly the 400 distinct keys, no duplicates, descending.
    assertThat(seen).hasSize(400);
    assertThat(seen).doesNotHaveDuplicates();
    final List<String> sortedDesc = new ArrayList<>(seen);
    sortedDesc.sort((a, b) -> b.compareTo(a));
    assertThat(seen).isEqualTo(sortedDesc);
  }

  @Test
  void descendingScanAcrossPartialCompactionSeriesIsGloballyOrdered() throws Exception {
    final int totalKeys = 20_000;
    final int pageSize = 256 * 1024;

    // Force a single compaction round to split the mutable pages into several independently sorted series.
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);
    createType(false, pageSize);

    database.transaction(() -> {
      for (int i = 0; i < totalKeys; i++)
        database.newDocument(TYPE_NAME).set("email", "K" + "x".repeat(64) + String.format("%08d", i)).save();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("email").getFirst();
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    assertThat(bucketIndex.getMutableIndex().getTotalPages()).as("mutable index must exceed the 1 MiB compaction budget")
        .isGreaterThan(4);

    compactAll();

    assertThat(bucketIndex.getMutableIndex().getSubIndex().newIterators(true, null, null))
        .as("compaction must produce more than one series for this regression")
        .hasSizeGreaterThan(1);

    final IndexCursor cursor = typeIndex.iterator(false);
    String previous = null;
    int count = 0;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        final String current = (String) cursor.getKeys()[0];
        if (previous != null)
          assertThat(current).as("descending keys must remain globally ordered across compacted series")
              .isLessThan(previous);
        previous = current;
        count++;
      }
    } finally {
      cursor.close();
    }

    assertThat(count).isEqualTo(totalKeys);
  }

  // ---- #4945: get() on a non-unique index must not resurrect a deleted RID across pages ----

  @Test
  void nonUniqueGetDoesNotResurrectDeletedAcrossPages() {
    createType(false);

    // Insert many records under the same key so the key's entries span multiple pages, then delete them all.
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        rids.add(database.newDocument(TYPE_NAME).set("email", "SHARED").save().getIdentity());
    });
    assertThat(lookup("SHARED")).hasSize(500);

    database.transaction(() -> {
      for (final RID rid : rids)
        database.deleteRecord(database.lookupByRID(rid, true).asDocument());
    });

    assertThat(lookup("SHARED")).as("all deleted entries must be gone, none resurrected").isEmpty();
  }

  // ---- #4944: a range scan must not be truncated when an older page's only in-range key is a full-key tombstone.
  // The cursor constructor left a cursor parked on a full-key tombstone alive but NOT counted in validIterators,
  // while next() decrements validIterators when ANY cursor is exhausted: the counter hit zero with valid cursors
  // still holding rows, silently truncating the scan. Full-key tombstones come from Index.remove(keys) without a
  // RID (public API, also used internally by the geospatial index). ----

  @Test
  void rangeScanNotTruncatedByFullKeyTombstoneInOlderPage() {
    createType(false);

    // ADD k0000: lands in the current mutable page, sealed by the following filler.
    database.transaction(() -> database.newDocument(TYPE_NAME).set("email", "k0000").save());
    filler(0, 300);

    // Full-key tombstone for k0000: lands in a newer page where it is the highest in-range key, then seal it.
    database.transaction(() -> {
      for (final Index idx : database.getSchema().getIndexes())
        if (idx instanceof TypeIndex ti)
          ti.remove(new Object[] { "k0000" });
    });
    filler(300, 300);

    // Newest page holds the surviving keys of the range.
    database.transaction(() -> {
      for (int i = 1; i <= 9; i++)
        database.newDocument(TYPE_NAME).set("email", "k000" + i).save();
    });

    database.transaction(() -> {
      for (final Index idx : database.getSchema().getIndexes())
        if (idx instanceof TypeIndex ti) {
          final IndexCursor cursor = ti.range(true, new Object[] { "k0000" }, true, new Object[] { "k0009" }, true);
          final List<RID> seen = new ArrayList<>();
          while (cursor.hasNext()) {
            final RID rid = (RID) cursor.next();
            if (rid != null)
              seen.add(rid);
          }
          assertThat(seen).as("rows after a tombstoned boundary key must not be lost").hasSize(9);
        }
    });

    // Same range through SQL: the 9 surviving rows must all be visible.
    final ResultSet rs = database.query("sql",
        "SELECT email FROM " + TYPE_NAME + " WHERE email >= 'k0000' AND email <= 'k0009' ORDER BY email ASC");
    final List<String> seenEmails = new ArrayList<>();
    while (rs.hasNext())
      seenEmails.add(rs.next().getProperty("email"));
    assertThat(seenEmails).containsExactly("k0001", "k0002", "k0003", "k0004", "k0005", "k0006", "k0007", "k0008",
        "k0009");
  }

  // ---- range/full scan must not lose rows or resurrect deletes when a boundary key is a tombstone ----

  @Test
  void rangeScanWithDeletedBoundaryKeyLosesNoRows() {
    createType(false);

    final List<RID> keptRids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 300; i++) {
        final RID rid = database.newDocument(TYPE_NAME).set("email", String.format("k%04d", i)).save().getIdentity();
        if (i > 0)
          keptRids.add(rid);
      }
    });

    // Delete the smallest key so the first in-range entry of the newest layer is a tombstone.
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE email = 'k0000'");
      while (rs.hasNext())
        database.deleteRecord(rs.next().getRecord().get());
    });

    compactAll();

    // Full ascending scan must still return all 299 surviving rows.
    final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE_NAME + " ORDER BY email ASC");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).as("no rows lost when the boundary key is a tombstone").isEqualTo(299);
    assertThat(lookup("k0000")).as("deleted boundary key must not resurrect").isEmpty();
  }
}
