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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12205 (the same report as #12206): {@code COMPACT INDEX} sent in a loop while other threads insert and delete records
 * must leave the index with exactly one entry per live record, and a UNIQUE index must still refuse a second record with a key
 * already taken. Reproduces the reporter's shape on the embedded engine: a type with 3 buckets, a UNIQUE index on a long STRING
 * key, 4 writers inserting batches of 50 and deleting 10 of their own records, one thread compacting the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue12205CompactIndexConcurrentWritersTest extends TestHelper {
  private static final String TYPE_NAME   = "R";
  private static final int    WRITERS     = 4;
  private static final long   RUN_MILLIS  = 6_000;
  private static final int    INSERT_SIZE = 50;
  private static final int    DELETE_SIZE = 10;

  @Test
  void compactIndexWhileRecordsAreInsertedAndDeletedKeepsOneEntryPerLiveRecord() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
      type.createProperty("k", Integer.class);
      type.createProperty("s", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .create();
      // small pages: many pages, many sealed and open-page rounds in a few seconds
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "s" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(8_192).create();
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicInteger nextKey = new AtomicInteger();
    final AtomicInteger compactions = new AtomicInteger();
    final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    final List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < WRITERS; ++w) {
      final int writerId = w;
      threads.add(new Thread(() -> {
        final Random random = new Random(writerId);
        final List<Integer> mine = new ArrayList<>();
        try {
          while (!stop.get()) {
            if (mine.size() < INSERT_SIZE || random.nextBoolean()) {
              final int base = nextKey.getAndAdd(INSERT_SIZE);
              database.transaction(() -> {
                for (int k = base; k < base + INSERT_SIZE; ++k)
                  database.newDocument(TYPE_NAME).set("k", k).set("s", keyText(k, random)).save();
              }, false, 10);
              for (int k = base; k < base + INSERT_SIZE; ++k)
                mine.add(k);
            } else {
              final List<Integer> victims = new ArrayList<>(DELETE_SIZE);
              for (int i = 0; i < DELETE_SIZE; ++i)
                victims.add(mine.remove(random.nextInt(mine.size())));
              database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE k IN ?", victims).close(),
                  false, 10);
            }
          }
        } catch (final Throwable t) {
          errors.add(t);
        }
      }, "writer-" + w));
    }
    threads.add(new Thread(() -> {
      try {
        while (!stop.get()) {
          Thread.sleep(20);
          database.command("sql", "COMPACT INDEX `R[s]`").close();
          compactions.incrementAndGet();
        }
      } catch (final Throwable t) {
        errors.add(t);
      }
    }, "compactor"));

    for (final Thread t : threads)
      t.start();
    Thread.sleep(RUN_MILLIS);
    stop.set(true);
    for (final Thread t : threads)
      t.join();

    assertThat(errors).as("writers and compactor ran without errors").isEmpty();
    assertThat(compactions.get()).as("the compactor ran").isGreaterThan(10);

    final Map<String, RID> records = new HashMap<>();
    final Set<String> recordEntries = new HashSet<>();
    final Set<String> indexEntries = new HashSet<>();
    database.transaction(() -> {
      for (final Iterator<Record> it = database.iterateType(TYPE_NAME, false); it.hasNext(); ) {
        final Document doc = (Document) it.next();
        records.put(doc.getString("s"), doc.getIdentity());
        recordEntries.add(doc.getString("s") + "|" + doc.getIdentity());
      }
      final IndexCursor cursor = typeIndex().iterator(true);
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        assertThat(indexEntries.add(cursor.getKeys()[0] + "|" + rid)).as("the index returns %s once", rid).isTrue();
      }
    });

    assertThat(records).isNotEmpty();
    final Set<String> missing = new HashSet<>(recordEntries);
    missing.removeAll(indexEntries);
    final Set<String> stale = new HashSet<>(indexEntries);
    stale.removeAll(recordEntries);
    assertThat(missing).as("live records with no index entry").isEmpty();
    assertThat(stale).as("index entries of deleted records or stale keys").isEmpty();

    // the UNIQUE constraint holds for every key taken
    final String takenKey = records.keySet().iterator().next();
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument dup = database.newDocument(TYPE_NAME).set("k", -1).set("s", takenKey);
      dup.save();
    })).isInstanceOf(DuplicatedKeyException.class);
  }

  private TypeIndex typeIndex() {
    return database.getSchema().getType(TYPE_NAME).getIndexesByProperties("s").getFirst();
  }

  private static String keyText(final int k, final Random random) {
    return "s" + k + "-" + "x".repeat(50 + random.nextInt(350));
  }
}
