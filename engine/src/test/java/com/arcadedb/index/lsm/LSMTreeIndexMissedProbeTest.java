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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.sun.management.ThreadMXBean;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;

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
  private static final long LIMIT   = 320L;

  @Test
  void aProbeThatFindsNothingAnswersWithTheSharedEmptyCursor() throws IOException {
    final LSMTreeIndexMutable[] probes = createTypeAndIndex();

    final IndexCursor first = probes[0].get(new Object[] { "no-such-key-1" }, -1);
    final IndexCursor second = probes[1].get(new Object[] { "no-such-key-2" }, -1);

    assertThat(first.hasNext()).isFalse();
    assertThat(first.estimateSize()).isZero();
    assertThat((Object) second).as("no cursor is built for a probe that found nothing").isSameAs(first);
  }

  @Test
  void aProbeThatFindsNothingAllocatesNothingOfItsOwn() throws IOException {
    final LSMTreeIndexMutable[] probes = createTypeAndIndex();
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();
    final Object[] keys = new Object[] { "no-such-key" };
    final int lookups = 2_000;

    for (int i = 0; i < lookups; i++)
      probes[0].get(keys, -1).close();

    final long before = threads.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < lookups; i++)
      probes[0].get(keys, -1).close();
    final long perProbe = (threads.getThreadAllocatedBytes(threadId) - before) / lookups;

    // A probe that read no page took 464 bytes: three collections, a cursor and the sink; the rest is the key conversion and the lookup
    assertThat(perProbe).as("bytes allocated per probe that found nothing").isLessThan(LIMIT);
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
