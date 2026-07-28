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

import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5499: a transaction that writes to a type carrying a unique index used to take an exclusive lock on
 * every sub-index file of EVERY index on that type, not only on the unique one.
 * <p>
 * The all-buckets fan-out is required for the unique index itself: it is partitioned by the record's bucket,
 * so {@code checkUniqueIndexKeys} reads the whole polymorphic TypeIndex and that read has to be atomic against
 * a concurrent inserter. Extending it to the NOTUNIQUE siblings bought nothing - they enforce no cross-bucket
 * invariant and are never read by that check - and multiplied the per-commit lock set by the number of indexes
 * on the type, serialising unrelated writers.
 * <p>
 * These tests pin both halves: the lock set is narrowed, and the uniqueness guarantee that motivated the wide
 * lock still holds, including when the unique index is inherited from a super-type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionIndexLockSetTest {
  private static final int BUCKETS = 32;

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/transaction-index-lock-set").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * The schema from the report: one UNIQUE index plus five NOTUNIQUE ones, 32 buckets each. Inserting a single
   * record must lock the 32 sub-index files of the unique index, not 6 x 32.
   */
  @Test
  void notUniqueSiblingIndexesAreNotLocked() {
    final DocumentType type = database.getSchema().createDocumentType("TRANSFER", BUCKETS);
    type.createProperty("transactionId", Type.STRING);
    type.createProperty("date", Type.STRING);
    type.createProperty("isLaundering", Type.BOOLEAN);
    type.createProperty("timestamp", Type.DATETIME);
    type.createProperty("amountReceived", Type.DECIMAL);
    type.createProperty("receivingCurrency", Type.STRING);
    type.createProperty("toBank", Type.STRING);

    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "transactionId", "date");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "isLaundering", "date");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "timestamp");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "amountReceived");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "receivingCurrency");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "toBank");

    final IntHashSet indexFiles = indexFilesLockedForOneInsert();

    // Every sub-index file of the UNIQUE index must be locked: the uniqueness check spans all of them.
    for (final IndexInternal sub : uniqueIndexOf(type).getIndexesOnBuckets())
      assertThat(indexFiles.contains(sub.getFileId()))
          .as("sub-index file %d of the unique index must be locked", sub.getFileId()).isTrue();

    // BUCKETS files for the unique index (the whole fan-out, needed by the uniqueness check) plus exactly one
    // file per NOTUNIQUE index: the record lands in a single bucket, so each sibling writes to a single
    // sub-index, and only that one has to be locked. Before #5499 this was 6 x BUCKETS.
    final int notUniqueIndexes = 5;
    assertThat(indexFiles.size())
        .as("unique index fan-out (%d) + one written sub-index per NOTUNIQUE index (%d); the wide lock was %d",
            BUCKETS, notUniqueIndexes, 6 * BUCKETS)
        .isEqualTo(BUCKETS + notUniqueIndexes);
  }

  /**
   * A type whose only index is the unique one is unaffected: the lock set was already minimal there.
   */
  @Test
  void aTypeWithOnlyTheUniqueIndexIsUnaffected() {
    final DocumentType type = database.getSchema().createDocumentType("TRANSFER", BUCKETS);
    type.createProperty("transactionId", Type.STRING);
    type.createProperty("date", Type.STRING);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "transactionId", "date");

    assertThat(indexFilesLockedForOneInsert().size()).isEqualTo(BUCKETS);
  }

  /**
   * A unique index inherited from a super-type still gets its whole bucket set locked when the record is
   * written into a sub-type: {@code getAllIndexes(true)} walks up the hierarchy and the inherited TypeIndex
   * reports {@code isUnique()}, so the {@code isUnique()} filter must not drop it.
   */
  @Test
  void uniqueIndexInheritedFromASuperTypeIsStillFullyLocked() {
    final DocumentType parent = database.getSchema().createDocumentType("Parent", BUCKETS);
    parent.createProperty("code", Type.STRING);
    parent.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "code");

    final DocumentType child = database.getSchema().createDocumentType("Child", BUCKETS);
    child.addSuperType(parent);
    child.createProperty("tag", Type.STRING);
    child.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "tag");

    final IntHashSet indexFiles = new IntHashSet(64);
    database.begin();
    try {
      final MutableDocument doc = database.newDocument("Child");
      doc.set("code", "K1");
      doc.set("tag", "T1");
      doc.save();
      ((DatabaseInternal) database).getTransaction().getIndexChanges().addFilesToLock(indexFiles);
    } finally {
      database.rollback();
    }

    for (final IndexInternal sub : uniqueIndexOf(parent).getIndexesOnBuckets())
      assertThat(indexFiles.contains(sub.getFileId()))
          .as("inherited unique sub-index file %d must be locked", sub.getFileId()).isTrue();
  }

  /**
   * The guarantee the wide lock existed to protect. Many threads race to insert the SAME key into a type with
   * 32 buckets; exactly one must win and the rest must see a duplicate. A narrowed lock set that broke the
   * cross-bucket serialisation would let two of them through.
   */
  @Test
  void concurrentInsertsOfTheSameKeyStillYieldExactlyOneWinner() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("TRANSFER", BUCKETS);
    type.createProperty("transactionId", Type.STRING);
    type.createProperty("payload", Type.STRING);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "transactionId");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "payload");

    final int threads = 8;
    final int keys = 40;
    final AtomicInteger inserted = new AtomicInteger();
    final AtomicInteger duplicates = new AtomicInteger();
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);

    try {
      for (int t = 0; t < threads; t++) {
        pool.submit(() -> {
          start.await();
          for (int k = 0; k < keys; k++) {
            final String key = "K" + k;
            try {
              database.transaction(() -> {
                final MutableDocument doc = database.newDocument("TRANSFER");
                doc.set("transactionId", key);
                doc.set("payload", "p");
                doc.save();
              });
              inserted.incrementAndGet();
            } catch (final DuplicatedKeyException e) {
              duplicates.incrementAndGet();
            }
          }
          return null;
        });
      }
      start.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();
    } finally {
      if (!pool.isTerminated())
        pool.shutdownNow();
    }

    assertThat(inserted.get()).as("exactly one insert per key may succeed").isEqualTo(keys);
    assertThat(duplicates.get()).isEqualTo(threads * keys - keys);
    assertThat(database.countType("TRANSFER", false)).isEqualTo(keys);

    // And the NOTUNIQUE sibling index, whose files are no longer locked wholesale, is still consistent.
    final ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();
    database.transaction(() -> database.iterateType("TRANSFER", false)
        .forEachRemaining(r -> seen.put(r.asDocument().getString("transactionId"), true)));
    assertThat(seen).hasSize(keys);
  }

  /**
   * Runs one insert against the current schema and returns the index files its commit would lock, with the
   * data buckets filtered out.
   */
  private IntHashSet indexFilesLockedForOneInsert() {
    final IntHashSet files = new IntHashSet(256);
    database.begin();
    try {
      final MutableDocument doc = database.newDocument("TRANSFER");
      doc.set("transactionId", "S1");
      doc.set("date", "2026-06-18");
      doc.set("isLaundering", false);
      doc.set("timestamp", System.currentTimeMillis());
      doc.set("amountReceived", 1);
      doc.set("receivingCurrency", "UGX");
      doc.set("toBank", "SBU");
      doc.set("payload", "p");
      doc.set("tag", "T1");
      doc.save();
      ((DatabaseInternal) database).getTransaction().getIndexChanges().addFilesToLock(files);
    } finally {
      database.rollback();
    }
    return onlyIndexFiles(files);
  }

  /** Drops the data-bucket file ids, keeping the index component files. */
  private IntHashSet onlyIndexFiles(final IntHashSet files) {
    final IntHashSet indexFileIds = new IntHashSet(256);
    for (final com.arcadedb.index.Index idx : database.getSchema().getIndexes())
      if (idx instanceof final TypeIndex ti)
        for (final IndexInternal sub : ti.getIndexesOnBuckets())
          for (final Integer component : sub.getFileIds())
            indexFileIds.add(component);

    final IntHashSet result = new IntHashSet(256);
    files.forEach(f -> {
      if (indexFileIds.contains(f))
        result.add(f);
    });
    return result;
  }

  private TypeIndex uniqueIndexOf(final DocumentType type) {
    for (final TypeIndex idx : type.getAllIndexes(false))
      if (idx.isUnique())
        return idx;
    throw new IllegalStateException("no unique index on " + type.getName());
  }
}
