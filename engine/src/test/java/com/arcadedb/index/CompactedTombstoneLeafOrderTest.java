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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A key whose values overflow one compacted leaf page is written oldest-chunk-first onto ascending pages, each carrying its own
 * root entry. The reader must walk those leaves NEWEST first, because {@code lookupInPageAndAddInResultset} resolves deletions
 * with a forward-poisoning {@code deletedRIDs} set: a tombstone only suppresses RIDs seen after it. Walking oldest-first let a
 * tombstone in an early chunk kill the live re-add sitting in a later chunk, so a value removed and then added back under the
 * same key vanished from the index while a plain scan still returned the record. The defect is in the shared compacted read
 * path; a heavily duplicated full-text token is the easiest way to overflow a leaf.
 * <p>
 * The remove and the re-add must land in SEPARATE transactions: within one transaction a REMOVE followed by an ADD on the same
 * (key, rid) collapses to a single ADD in the transaction overlay, so no tombstone is ever written.
 */
class CompactedTombstoneLeafOrderTest extends TestHelper {

  private static final int DOCS = 3_000;

  /** Small pages so the shared token's posting list provably overflows a leaf and spans many compacted pages. */
  private static final int PAGE_SIZE = 1024;

  /**
   * Ensures the compacted sub-index is in play. This page count always crosses {@code INDEX_COMPACTION_MIN_PAGES_SCHEDULE}, so a
   * background compaction is already scheduled or running: wait for it, then compact once more so the merge is complete no
   * matter who ran it.
   */
  private void compact(final String indexName) {
    database.async().waitCompletion();
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
      try {
        for (int attempt = 0; attempt < 200 && !bucketIndex.scheduleCompaction(); ++attempt)
          Thread.sleep(25); // a background compaction owns the status; let it finish
        bucketIndex.compact();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
  }

  @Test
  void fullTextFindsTokenRemovedThenAddedBack() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create();
      database.getSchema().getType("Doc").createProperty("words", String.class);
      database.getSchema().buildTypeIndex("Doc", new String[] { "words" })
          .withType(Schema.INDEX_TYPE.FULL_TEXT).withFullTextType().withPageSize(PAGE_SIZE).create();
    });

    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        database.newDocument("Doc").set("words", "data tok" + i).save();
    });

    final List<RID> touched = new ArrayList<>();
    database.transaction(() -> {
      final Iterator<com.arcadedb.database.Record> it = database.iterateType("Doc", false);
      int n = 0;
      while (it.hasNext()) {
        final RID rid = it.next().getIdentity();
        if (n++ % 3 == 0)
          touched.add(rid);
      }
    });

    // Drop the shared token: writes a tombstone for ('data', rid).
    database.transaction(() -> {
      for (final RID rid : touched) {
        final MutableDocument doc = rid.asDocument().modify();
        doc.set("words", doc.getString("words").replace("data ", ""));
        doc.save();
      }
    });

    // Add it back: a live re-add for the same ('data', rid), landing in a later chunk than its tombstone.
    database.transaction(() -> {
      for (final RID rid : touched) {
        final MutableDocument doc = rid.asDocument().modify();
        doc.set("words", "data " + doc.getString("words"));
        doc.save();
      }
    });

    compact("Doc[words]");

    final Set<RID> expected = new HashSet<>();
    database.transaction(() -> {
      final Iterator<com.arcadedb.database.Record> it = database.iterateType("Doc", false);
      while (it.hasNext()) {
        final Document doc = (Document) it.next();
        if (doc.getString("words").contains("data"))
          expected.add(doc.getIdentity());
      }
    });
    assertThat(expected).hasSize(DOCS);

    final Set<RID> found = new HashSet<>();
    database.transaction(() -> {
      final IndexCursor cursor = ((TypeIndex) database.getSchema().getIndexByName("Doc[words]")).getIndexesOnBuckets()[0]
          .get(new Object[] { "data" });
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
    });

    assertThat(found).isEqualTo(expected);
  }
}
