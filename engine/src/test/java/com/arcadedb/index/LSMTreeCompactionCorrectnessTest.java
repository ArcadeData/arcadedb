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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Correctness tests for the LSM read/merge/compaction paths. The primary regression target is issue #4945:
 * {@code get()} on a non-unique index resurrected a deleted RID when a per-RID tombstone and the original ADD
 * lived in different pages, because the {@code deletedRIDs} set was local to each page lookup instead of being
 * threaded across pages and the compacted sub-index (like {@code removedKeys}). The remaining tests are general
 * correctness guards over compaction and multi-page/2-series layouts (descending scans, cross-page deletes,
 * deleted boundary keys).
 */
class LSMTreeCompactionCorrectnessTest extends TestHelper {

  private static final String TYPE_NAME = "Doc";

  @Override
  public void beforeTest() {
    // Force real compaction with tiny inputs.
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  private void createType(final boolean unique) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("email", String.class);
    // Small pages so a handful of entries already seals immutable pages the compactor will merge.
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(unique).withPageSize(1024).create();
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
          ((IndexInternal) index).scheduleCompaction();
          ((IndexInternal) index).compact();
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

  // ---- #4943: descending iteration over a 2+ series compacted index must not emit phantom RIDs ----

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

  // ---- #4944: range/full scan must not lose rows or resurrect deletes when a boundary key is a tombstone ----

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
