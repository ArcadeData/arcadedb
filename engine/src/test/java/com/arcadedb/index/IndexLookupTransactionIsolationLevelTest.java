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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13159: the index lookups that release the pages a REPEATABLE_READ transaction pinned, when an entry they read has no
 * record in those pages, decided whether the transaction is REPEATABLE_READ from the database's default level, not from the level
 * the running transaction began with. A transaction begun with {@code begin(REPEATABLE_READ)} on a database left at its
 * READ_COMMITTED default (what {@code POST /begin} with an isolation level does) pins pages, but the lookups took it for
 * READ_COMMITTED, released nothing and dropped the entry of a record committed after the transaction pinned its bucket page.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class IndexLookupTransactionIsolationLevelTest extends TestHelper {
  private static final int RECORDS  = 10;
  private static final int NEW_KEY  = 1_000;
  private static final int READ_KEY = 5;
  // more entries of one key than GetValueFromIndexEntryStep holds, so the lookup streams them (pointLookupRow)
  private static final int GROUP    = 300;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      // one bucket, so the record committed later lands on the bucket page the transaction pinned
      final DocumentType type = database.getSchema().buildDocumentType().withName("Item").withTotalBuckets(1).create();
      type.createProperty("k", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Item").set("k", i).save();

      final DocumentType group = database.getSchema().buildDocumentType().withName("Member").withTotalBuckets(1).create();
      group.createProperty("g", Type.INTEGER);
      group.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "g");
      for (int i = 0; i < GROUP; i++)
        database.newDocument("Member").set("g", 1).save();
    });
  }

  private TypeIndex index() {
    return (TypeIndex) database.getSchema().getIndexByName("Item[k]");
  }

  /**
   * Regression test for issue #13159: {@link ConsistentKeyLookup} under {@code begin(REPEATABLE_READ)} on a READ_COMMITTED default
   * database finds the record committed on a bucket page the transaction pinned before, instead of dropping its entry as dangling.
   */
  @Test
  void consistentKeyLookupReleasesThePinsOfATransactionBegunRepeatableRead() throws Exception {
    assertThat(database.getTransactionIsolationLevel()).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinTheBucketPage();
      insertFromAnotherThread();

      final List<IndexCursorEntry> entries = ConsistentKeyLookup.lookup((DatabaseInternal) database, index(),
          List.<Object[]>of(new Object[] { NEW_KEY }));
      assertThat(entries).hasSize(1);
      assertThat(entries.getFirst().record.asDocument().getInteger("k")).isEqualTo(NEW_KEY);
    } finally {
      database.rollback();
    }
  }

  /**
   * Regression test for issue #13159: the SQL lookup of {@code GetValueFromIndexEntryStep} under {@code begin(REPEATABLE_READ)} on a
   * READ_COMMITTED default database answers the record committed on a bucket page the transaction pinned before.
   */
  @Test
  void sqlIndexLookupReleasesThePinsOfATransactionBegunRepeatableRead() throws Exception {
    assertThat(database.getTransactionIsolationLevel()).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinTheBucketPage();
      insertFromAnotherThread();

      assertThat(keysOf("SELECT FROM Item WHERE k = " + NEW_KEY)).containsExactly(NEW_KEY);
    } finally {
      database.rollback();
    }
  }

  /**
   * Regression test for issue #13159: a key with more entries than the SQL lookup holds is streamed, and the entry whose record is
   * not on the bucket page this {@code begin(REPEATABLE_READ)} transaction pinned is read again after releasing it, not skipped.
   */
  @Test
  void sqlStreamedIndexLookupReleasesThePinsOfATransactionBegunRepeatableRead() throws Exception {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final long[] pinned = { 0 };
      database.scanType("Member", false, record -> {
        ++pinned[0];
        return true;
      });
      assertThat(pinned[0]).isEqualTo(GROUP);

      runInAnotherThread(() -> database.newDocument("Member").set("g", 1).save());

      int rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM Member WHERE g = 1")) {
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
      }
      assertThat(rows).isEqualTo(GROUP + 1);
    } finally {
      database.rollback();
    }
  }

  /** Same as above, with an IN listing several keys, so the lookup of several entries goes through the same path. */
  @Test
  void sqlIndexInLookupReleasesThePinsOfATransactionBegunRepeatableRead() throws Exception {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinTheBucketPage();
      insertFromAnotherThread();

      assertThat(keysOf("SELECT FROM Item WHERE k IN [" + READ_KEY + ", " + NEW_KEY + "]")).containsExactlyInAnyOrder(READ_KEY,
          NEW_KEY);
    } finally {
      database.rollback();
    }
  }

  /**
   * Control: a transaction begun at the READ_COMMITTED default pins nothing, so the record committed meanwhile is found as well. Shows
   * the tests above fail on the pinning alone.
   */
  @Test
  void readCommittedTransactionFindsTheRecordCommittedMeanwhile() throws Exception {
    database.begin();
    try {
      pinTheBucketPage();
      insertFromAnotherThread();

      assertThat(keysOf("SELECT FROM Item WHERE k = " + NEW_KEY)).containsExactly(NEW_KEY);
      final List<IndexCursorEntry> entries = ConsistentKeyLookup.lookup((DatabaseInternal) database, index(),
          List.<Object[]>of(new Object[] { NEW_KEY }));
      assertThat(entries).hasSize(1);
    } finally {
      database.rollback();
    }
  }

  /** Reads a record of the only bucket, which a REPEATABLE_READ transaction pins with its page; the index is not read. */
  private void pinTheBucketPage() {
    final List<Integer> keys = new ArrayList<>();
    database.scanType("Item", false, record -> {
      keys.add(record.asDocument().getInteger("k"));
      return true;
    });
    assertThat(keys).contains(READ_KEY).doesNotContain(NEW_KEY);
  }

  private void insertFromAnotherThread() throws InterruptedException {
    runInAnotherThread(() -> database.newDocument("Item").set("k", NEW_KEY).save());
  }

  /** Commits {@code write} in its own transaction, on another thread, and waits for it. */
  private void runInAnotherThread(final Runnable write) throws InterruptedException {
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.transaction(write::run);
      } catch (final Throwable t) {
        error.set(t);
      }
    });
    writer.start();
    writer.join(30_000);
    assertThat(writer.isAlive()).isFalse();
    assertThat(error.get()).isNull();
  }

  private List<Integer> keysOf(final String sql) {
    final List<Integer> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        keys.add(row.getProperty("k"));
      }
    }
    return keys;
  }
}
