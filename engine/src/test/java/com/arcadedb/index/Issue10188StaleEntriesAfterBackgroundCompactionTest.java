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
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10188: a NOTUNIQUE LSM-tree index ended up with extra entries when documents were updated while the engine compacted
 * the index in the background. {@code TypeIndex.iterator()}, {@code countEntries()} and {@code SELECT k FROM T ORDER BY k} (which
 * reads the index in key order) returned more rows than the type has - most of the extra ones with a key the record no longer
 * holds - while the point lookups ({@code WHERE k = ?}, {@code TypeIndex.get(k)}) stayed right.
 * <p>
 * One thread, only updates, a compaction every two pages of the mutable index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue10188StaleEntriesAfterBackgroundCompactionTest extends TestHelper {
  private static final int KEYS      = 50;
  private static final int DOCUMENTS = 50_000;
  private static final int PAGE_SIZE = 16_384;

  private final Object[] saved = new Object[2];

  @BeforeEach
  void configure() {
    saved[0] = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValue();
    saved[1] = GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.getValue();
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(2);
    GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.setValue(PAGE_SIZE);
  }

  @AfterEach
  void restore() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(saved[0]);
    GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.setValue(saved[1]);
  }

  @Test
  void theIndexHoldsOneEntryPerRecordAfterUpdatesDuringBackgroundCompaction() throws InterruptedException {
    database.getSchema().createDocumentType("T").createProperty("k", Type.INTEGER);
    database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");

    final Random random = new Random(1);
    final RID[] rids = new RID[DOCUMENTS];
    database.transaction(() -> {
      for (int i = 0; i < DOCUMENTS; i++)
        rids[i] = database.newDocument("T").set("k", random.nextInt(KEYS)).save().getIdentity();
    });

    final long end = System.nanoTime() + 8_000_000_000L;
    while (System.nanoTime() < end) {
      database.begin();
      final RID rid = rids[random.nextInt(rids.length)];
      database.lookupByRID(rid, true).asDocument().modify().set("k", random.nextInt(KEYS)).save();
      database.commit();
    }
    // The compaction scheduled by the last commits runs in the background
    Thread.sleep(3_000);

    assertTheIndexMatchesTheRecords();
  }

  private void assertTheIndexMatchesTheRecords() {
    final Map<Long, Integer> keyOfRecord = new HashMap<>();
    final Iterator<Record> records = database.iterateType("T", true);
    while (records.hasNext()) {
      final Document document = records.next().asDocument();
      keyOfRecord.put(document.getIdentity().getPosition(), ((Number) document.get("k")).intValue());
    }

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("T[k]");
    long entries = 0, stale = 0, duplicates = 0, orphans = 0;
    final Set<String> seen = new HashSet<>();
    for (final IndexCursor cursor = index.iterator(true); cursor.hasNext(); ) {
      final Identifiable rid = cursor.next();
      final Object[] keys = cursor.getKeys();
      ++entries;
      final Integer key = keyOfRecord.get(rid.getIdentity().getPosition());
      if (key == null)
        ++orphans;
      else if (key.intValue() != ((Number) keys[0]).intValue())
        ++stale;
      else if (!seen.add(rid.getIdentity() + ":" + key))
        ++duplicates;
    }

    long ordered = 0;
    try (final ResultSet resultSet = database.query("sql", "SELECT k FROM T ORDER BY k")) {
      while (resultSet.hasNext()) {
        resultSet.next();
        ++ordered;
      }
    }

    long byKey = 0, found = 0;
    for (int k = 0; k < KEYS; k++) {
      try (final ResultSet resultSet = database.query("sql", "SELECT k FROM T WHERE k = ?", k)) {
        while (resultSet.hasNext()) {
          resultSet.next();
          ++byKey;
        }
      }
      for (final IndexCursor cursor = index.get(new Object[] { k }); cursor.hasNext(); ) {
        cursor.next();
        ++found;
      }
    }

    final String state = "records " + keyOfRecord.size() + ", index iterated " + entries + " (stale key " + stale + ", duplicate "
        + duplicates + ", no record " + orphans + "), countEntries() " + index.countEntries() + ", ORDER BY rows " + ordered
        + ", WHERE k = ? rows " + byKey + ", TypeIndex.get rows " + found;
    assertThat(entries).as(state).isEqualTo(keyOfRecord.size());
    assertThat(index.countEntries()).as(state).isEqualTo(keyOfRecord.size());
    assertThat(ordered).as(state).isEqualTo(keyOfRecord.size());
    assertThat(byKey).as(state).isEqualTo(keyOfRecord.size());
    assertThat(found).as(state).isEqualTo(keyOfRecord.size());
  }
}
