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
package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.WALFile;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8129: building an {@code LSM_VECTOR} index switched the WAL off for bulk loading on the
 * thread's own {@link TransactionContext}, which is reused across begin()/commit() cycles, and did not put it back.
 * Every later transaction on that thread then committed without writing the WAL at all, so {@code txWalFlush=2} cost
 * nothing because there was nothing left to flush.
 * <p>
 * The durability a session asked for belongs to the session: a vector build or graph persist must leave it exactly
 * as it found it, whether that is the configured default or a value the caller set on purpose.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8129VectorBuildLeavesWalOnTest {
  private static final String DB_PATH    = "./target/databases/Issue8129VectorBuildLeavesWalOnTest";
  private static final int    SEED_ROWS  = 200;
  private static final int    DIMENSIONS = 16;

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    factory = new DatabaseFactory(DB_PATH);
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void transactionsAfterAVectorIndexBuildStillWriteTheWal() {
    database.setWALFlush(WALFile.FlushType.YES_FULL);
    seedAndIndex();

    assertSessionDurability(true, WALFile.FlushType.YES_FULL, "after CREATE INDEX ... LSM_VECTOR");
    assertCommitWritesTheWal();

    // A search persists the graph in a transaction of its own: that path must leave the session alone as well.
    searchEveryBucket(new Random(1));
    assertSessionDurability(true, WALFile.FlushType.YES_FULL, "after a search that persisted the vector graph");
    assertCommitWritesTheWal();
  }

  @Test
  void anExplicitRebuildLeavesTheWalOn() {
    seedAndIndex();

    database.command("sql", "REBUILD INDEX `P[embedding]`");
    searchEveryBucket(new Random(2));

    assertSessionDurability(true, currentWalFlush(), "after REBUILD INDEX");
    assertCommitWritesTheWal();
  }

  @Test
  void aSessionThatDisabledTheWalOnPurposeKeepsItDisabled() {
    database.setUseWAL(false);
    database.setWALFlush(WALFile.FlushType.NO);
    seedAndIndex();
    searchEveryBucket(new Random(3));

    // The build used to restore the CONFIGURED value rather than the one it found, re-enabling the WAL on a session
    // that had switched it off for its own bulk load.
    assertSessionDurability(false, WALFile.FlushType.NO, "after a vector build on a WAL-less session");
  }

  @Test
  void theTransactionOverrideDoesNotOutliveItsTransaction() {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.setUseWALForThisTransaction(false);
    assertThat(tx.isUseWAL()).isFalse();
    database.commit();

    assertSessionDurability(true, currentWalFlush(), "after a commit whose transaction skipped the WAL");

    database.begin();
    ((DatabaseInternal) database).getTransaction().setUseWALForThisTransaction(false);
    database.rollback();

    assertSessionDurability(true, currentWalFlush(), "after a rollback whose transaction skipped the WAL");
  }

  @Test
  void aBuildSharingTheCallersTransactionLeavesItsOverrideAsItFoundIt() {
    seedAndIndex();
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("P[embedding]");

    // A build that joins a transaction somebody else opened: its finally is the only thing standing between the
    // build's WAL-less override and the caller's own commit, and it must hand back what the caller had set.
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.setUseWALForThisTransaction(true);
    typeIndex.build(IndexBuilder.BUILD_BATCH_SIZE, true, null);

    assertThat(((DatabaseInternal) database).getTransaction()).isSameAs(tx);
    assertThat(tx.getUseWALForThisTransaction()).as("the caller's override after a build that shared its transaction")
        .isTrue();
    database.commit();

    assertSessionDurability(true, currentWalFlush(), "after a build that shared the caller's transaction");
    assertCommitWritesTheWal();
  }

  private void seedAndIndex() {
    seedRows();
    createVectorIndex();
  }

  private void seedRows() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE PROPERTY P.pid INTEGER");
    database.command("sql", "CREATE PROPERTY P.embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON P (pid) UNIQUE");

    final Random rnd = new Random(8129);
    database.transaction(() -> {
      for (int i = 0; i < SEED_ROWS; i++)
        database.newVertex("P").set("pid", i).set("views", 0).set("embedding", randomVector(rnd)).save();
    });
  }

  private void createVectorIndex() {
    database.command("sql", "CREATE INDEX ON P (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"COSINE\" }");
  }

  private void assertSessionDurability(final boolean useWAL, final WALFile.FlushType walFlush, final String when) {
    database.begin();
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      assertThat(tx.isUseWAL()).as("useWAL of a new transaction " + when).isEqualTo(useWAL);
      assertThat(tx.getWALFlush()).as("walFlush of a new transaction " + when).isEqualTo(walFlush);
    } finally {
      database.rollback();
    }
  }

  private void assertCommitWritesTheWal() {
    final long before = walBytesWritten();
    database.transaction(() -> database.command("sql", "UPDATE P SET views = views + 1 WHERE pid < 6"));
    assertThat(walBytesWritten()).as("a committed update must be written to the WAL").isGreaterThan(before);
  }

  private WALFile.FlushType currentWalFlush() {
    database.begin();
    try {
      return ((DatabaseInternal) database).getTransaction().getWALFlush();
    } finally {
      database.rollback();
    }
  }

  private long walBytesWritten() {
    return (Long) ((DatabaseInternal) database).getTransactionManager().getStats().get("bytesWritten");
  }

  private void searchEveryBucket(final Random rnd) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("P[embedding]");
    for (final var bucketIndex : typeIndex.getIndexesOnBuckets())
      ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(randomVector(rnd), 5, 64);
  }

  private static float[] randomVector(final Random rnd) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = rnd.nextFloat();
    return v;
  }
}
