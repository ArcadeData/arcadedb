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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11526: a transaction that creates a record and then updates an EXTERNAL property of it must still be refused when
 * another transaction commits the same UNIQUE key.
 * <p>
 * The update is deferred to commit time, where it writes the value into the paired {@code <bucket>_ext} bucket, a file that was
 * not in the lock set computed up front. The commit then found that file as a late joiner (#4937) and released every lock it held
 * to take a wider set AFTER the unique-key check had passed: in that window a concurrent transaction committed the same key into
 * another bucket's sub-index, and nothing checked the key again.
 */
class Issue11526UniqueKeyExternalPropertyUpdateRaceTest extends TestHelper {
  private static final int    KEYS   = 40;
  private static final String UPDATE = "update T set ext = 'x' where name = :name";

  @Test
  void createThenExternalUpdateOfTheSameKeyKeepsTheKeyUniqueWithManyBuckets() throws Exception {
    createType(false, 8, true);
    assertKeysAreUnique(race(UPDATE));
  }

  @Test
  void createThenExternalUpdateOfTheSameKeyKeepsTheKeyUniqueWithTwoBuckets() throws Exception {
    createType(false, 2, true);
    assertKeysAreUnique(race(UPDATE));
  }

  @Test
  void createThenExternalUpdateOfTheSameKeyKeepsTheKeyUniqueOnAVertexType() throws Exception {
    createType(true, 8, true);
    assertKeysAreUnique(race(UPDATE));
  }

  /** Control: the same race with an inline property was refused before the fix too. */
  @Test
  void theSameRaceWithAnInlinePropertyUpdateIsRefused() throws Exception {
    createType(false, 8, false);
    assertKeysAreUnique(race(UPDATE));
  }

  /** The race is not vacuous: the deferred update really writes the value into the paired external bucket. */
  @Test
  void theExternalUpdateIsCommittedIntoThePairedBucket() {
    createType(false, 8, true);
    database.transaction(() -> {
      database.newDocument("T").set("name", "single").save();
      database.command("sql", UPDATE, Map.of("name", "single"));
    });

    try (final var result = database.query("sql", "select ext from T where name = 'single'")) {
      assertThat(result.next().<String>getProperty("ext")).isEqualTo("x");
    }
  }

  private void createType(final boolean vertex, final int buckets, final boolean external) {
    final DocumentType type = vertex ?
        database.getSchema().createVertexType("T", buckets) :
        database.getSchema().createDocumentType("T", buckets);
    type.createProperty("name", Type.STRING);
    type.createProperty("ext", Type.STRING).setExternal(external);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
  }

  /** Two threads per key create it, run the update and commit together; returns the successful commits per key. */
  private Map<String, Integer> race(final String updateSql) throws Exception {
    final Map<String, Integer> committed = new HashMap<>();
    final boolean vertex = database.getSchema().getType("T") instanceof VertexType;
    for (int k = 0; k < KEYS; k++) {
      final String name = "key-" + k;
      final CyclicBarrier beforeCommit = new CyclicBarrier(2);
      final AtomicInteger commits = new AtomicInteger();
      final AtomicInteger unexpected = new AtomicInteger();
      final Thread[] threads = new Thread[2];
      for (int t = 0; t < 2; t++) {
        threads[t] = new Thread(() -> {
          database.begin();
          try {
            if (vertex)
              database.newVertex("T").set("name", name).save();
            else
              database.newDocument("T").set("name", name).save();
            database.command("sql", updateSql, Map.of("name", name));
            beforeCommit.await();
            database.commit();
            commits.incrementAndGet();
          } catch (final DuplicatedKeyException | NeedRetryException e) {
            // REFUSED: THE OTHER TRANSACTION WON
            if (database.isTransactionActive())
              database.rollback();
          } catch (final Exception e) {
            unexpected.incrementAndGet();
            if (database.isTransactionActive())
              database.rollback();
          }
        });
        threads[t].start();
      }
      for (final Thread thread : threads)
        thread.join();
      assertThat(unexpected.get()).as("unexpected failures racing %s", name).isZero();
      committed.put(name, commits.get());
    }
    return committed;
  }

  private void assertKeysAreUnique(final Map<String, Integer> committed) {
    // A SCAN OF THE TYPE, NOT AN INDEX LOOKUP: THE DUPLICATE IS A RECORD THE INDEX DOES NOT HOLD
    final Map<String, Integer> recordsPerKey = new HashMap<>();
    final Iterator<Record> records = database.iterateType("T", false);
    while (records.hasNext())
      recordsPerKey.merge(records.next().asDocument().getString("name"), 1, Integer::sum);

    for (final Map.Entry<String, Integer> entry : committed.entrySet()) {
      assertThat(entry.getValue()).as("commits that succeeded for the key %s", entry.getKey()).isEqualTo(1);
      assertThat(recordsPerKey.getOrDefault(entry.getKey(), 0)).as("records holding the key %s", entry.getKey()).isEqualTo(1);
    }
  }
}
