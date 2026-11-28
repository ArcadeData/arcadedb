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
package com.arcadedb.index.lsm;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #13243: the lookup of a key in a type with many buckets probes the index of every bucket, and all of them but
 * one miss. A probe that found nothing built three collections and a cursor of its own, and the lookup of one key allocated about
 * 8 KB at 16 buckets, most of it in probes that answered nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexMissedProbeTest extends TestHelper {
  private static final int BUCKETS = 16;
  private static final int COMPACTED_KEYS = 40_000;

  @Test
  void aProbeThatFindsNothingAnswersWithTheSharedEmptyCursor() throws IOException {
    final LSMTreeIndexMutable[] probes = createTypeAndIndex();

    final IndexCursor first = probes[0].get(new Object[] { "no-such-key-1" }, -1);
    final IndexCursor second = probes[1].get(new Object[] { "no-such-key-2" }, -1);

    assertThat(first.hasNext()).isFalse();
    assertThat(first.estimateSize()).isZero();
    assertThat((Object) second).as("no cursor is built for a probe that found nothing").isSameAs(first);
  }

  /**
   * The structural property the allocation figures of the issue stand for, asserted without a byte count (which depends on the JIT
   * and the GC): a lookup of a key no page holds builds no result collection, so the compacted lookup answers with the shared empty set
   * and the mutable one with the shared empty cursor, while a lookup that finds the key answers with its own entries. The
   * compacted index is searched directly, after a real compaction, for a hit and for a miss.
   */
  @Test
  void aCompactedLookupBuildsACollectionOnlyForAKeyItHolds() throws Exception {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(1L);
    try {
      database.getSchema().buildDocumentType().withName("Wide").withTotalBuckets(1).create().createProperty("k", Type.STRING);
      database.getSchema().buildTypeIndex("Wide", new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(256 * 1024).create();
      database.transaction(() -> {
        for (int i = 0; i < COMPACTED_KEYS; i++)
          database.newDocument("Wide").set("k", wideKey(i)).save();
      });

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Wide[k]");
      assertThat(((IndexInternal) typeIndex).scheduleCompaction()).isTrue();
      assertThat(((IndexInternal) typeIndex).compact()).isTrue();

      final LSMTreeIndexMutable mutable = ((LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0]).getMutableIndex();
      final LSMTreeIndexCompacted compacted = mutable.getSubIndex();
      assertThat(compacted).as("the compaction produced a sub-index").isNotNull();

      final Set<IndexCursorEntry> miss = compacted.get(new Object[] { "no-such-key" }, -1);
      assertThat(miss).isEmpty();
      assertThat((Object) miss).as("a miss builds no collection").isSameAs(Collections.emptySet());

      final Set<IndexCursorEntry> hit = compacted.get(new Object[] { wideKey(123) }, -1);
      assertThat(hit).hasSize(1);
      assertThat((Object) hit).isNotSameAs(Collections.emptySet());
      assertThat(database.lookupByRID(hit.iterator().next().record.getIdentity(), true).asDocument().getString("k"))
          .isEqualTo(wideKey(123));

      // and through the type index, which asks the mutable pages and the compacted series
      try (final IndexCursor cursor = typeIndex.get(new Object[] { wideKey(COMPACTED_KEYS - 1) })) {
        assertThat(cursor.hasNext()).isTrue();
        cursor.next();
        assertThat(cursor.hasNext()).isFalse();
      }
      try (final IndexCursor cursor = typeIndex.get(new Object[] { "no-such-key" })) {
        assertThat(cursor.hasNext()).isFalse();
      }
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.reset();
    }
  }

  private static String wideKey(final int i) {
    return "K" + "x".repeat(64) + String.format("%08d", i);
  }

  @Test
  void aProbeThatFindsTheKeyAnswersWithItsEntry() throws IOException {
    final LSMTreeIndexMutable[] probes = createTypeAndIndex();

    int found = 0;
    for (final LSMTreeIndexMutable probe : probes)
      try (final IndexCursor cursor = probe.get(new Object[] { "key-7" }, -1)) {
        while (cursor.hasNext()) {
          final Identifiable record = cursor.next();
          assertThat(database.lookupByRID(record.getIdentity(), true).asDocument().getString("k")).isEqualTo("key-7");
          found++;
        }
      }
    assertThat(found).as("exactly one of the %d bucket indexes holds the key", BUCKETS).isEqualTo(1);
  }

  @Test
  void aKeyThatWasRemovedIsStillNotFound() throws IOException {
    final LSMTreeIndexMutable[] probes = createTypeAndIndex();

    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE k = 'key-7'"));

    for (final LSMTreeIndexMutable probe : probes)
      assertThat(probe.get(new Object[] { "key-7" }, -1).hasNext()).isFalse();

    // the key written again lands in one index, found by the one lookup that asks them all
    database.transaction(() -> database.newDocument("Doc").set("k", "key-7").save());
    try (final IndexCursor cursor = ((TypeIndex) database.getSchema().getIndexByName("Doc[k]")).get(new Object[] { "key-7" })) {
      assertThat(cursor.hasNext()).isTrue();
      cursor.next();
      assertThat(cursor.hasNext()).isFalse();
    }
  }

  private LSMTreeIndexMutable[] createTypeAndIndex() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(BUCKETS).create().createProperty("k", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k");
    });
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Doc").set("k", "key-" + i).save();
    });

    final IndexInternal[] subIndexes = ((TypeIndex) database.getSchema().getIndexByName("Doc[k]")).getIndexesOnBuckets();
    assertThat(subIndexes).hasSize(BUCKETS);
    final LSMTreeIndexMutable[] probes = new LSMTreeIndexMutable[BUCKETS];
    for (int i = 0; i < BUCKETS; i++)
      probes[i] = ((LSMTreeIndex) subIndexes[i]).getMutableIndex();
    return probes;
  }
}
