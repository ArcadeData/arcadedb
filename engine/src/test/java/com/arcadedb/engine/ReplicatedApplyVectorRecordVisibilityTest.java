/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexMutable;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13369: a node that applies a replicated entry writes its pages one by one, and a vector index learns of its page as that page is
 * applied. A vector search of the node in between found the vector of a record whose page the entry had not written yet (an insert), or
 * of a record the entry had already removed (a delete), and a row made without reading its record (issue #10973) came back with every
 * property null. The record exists and reads normally once the entry is applied (or is gone). The search of an index with such an apply in
 * flight reads the records, so a vector whose record cannot be read is no row of the answer.
 * <p>
 * The leader and the follower are two databases. The leader's WAL transactions are the entries; their pages are put in the order that opens
 * the window, as the order of the pages of an entry is that of a hash map and either occurs.
 */
class ReplicatedApplyVectorRecordVisibilityTest {
  private static final String LEADER_PATH   = "target/test-databases/ReplicatedApplyVectorRecordVisibilityTest-leader";
  private static final String FOLLOWER_PATH = "target/test-databases/ReplicatedApplyVectorRecordVisibilityTest-follower";
  private static final String WAL_COPIES_PATH = "target/test-databases/ReplicatedApplyVectorRecordVisibilityTest-wal";
  private static final int    DIMENSIONS    = 8;
  // Past the 1000 vectors under which a search rebuilds a graph that a write made stale, inside the search
  private static final int    BASE          = 1100;
  private static final int    BATCH         = 5;
  private static final String ROWS          =
      "SELECT @rid AS r, external_id AS eid FROM (SELECT expand(vector.neighbors('Chunk[embedding]', ?, ?)))";

  private final Random                      random        = new Random(13369);
  private final Map<String, float[]>        vectors       = new HashMap<>();
  private final List<RID>                   baseRids      = new ArrayList<>();
  private final Map<String, Long>           walPositions  = new HashMap<>();
  private       DatabaseFactory             leaderFactory;
  private       DatabaseFactory             followerFactory;
  private       Database                    leader;
  private       Database                    follower;
  private       LSMVectorIndex              followerIndex;

  @BeforeEach
  void setUp() throws Exception {
    FileUtils.deleteRecursively(new File(LEADER_PATH));
    FileUtils.deleteRecursively(new File(FOLLOWER_PATH));
    leaderFactory = new DatabaseFactory(LEADER_PATH);
    leader = leaderFactory.create();
    leader.transaction(() -> {
      leader.getSchema().buildVertexType().withName("Chunk").withTotalBuckets(1).create().createProperty("embedding", Type.ARRAY_OF_FLOATS);
      leader.getSchema().getType("Chunk").createProperty("external_id", Type.STRING);
      // INT8 with the vectors in the graph: the follower scores the vector of an entry from the index page, so it needs no record to answer it
      leader.command("sql", "CREATE INDEX ON Chunk (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"INT8\", \"storeVectorsInGraph\": true }");
    });
    leader.transaction(() -> {
      for (int i = 0; i < BASE; i++)
        baseRids.add(newChunk("base-" + i));
    });

    // the follower is the leader as it is now, then only the entries the leader writes from here on reach it
    final DatabaseInternal leaderInternal = (DatabaseInternal) leader;
    assertThat(leaderInternal.getPageManager().waitAllPagesOfDatabaseAreFlushed(leaderInternal)).isTrue();
    new File(FOLLOWER_PATH).mkdirs();
    for (final File file : new File(LEADER_PATH).listFiles())
      if (file.isFile() && !file.getName().endsWith(".wal") && !file.getName().equals("database.lck"))
        Files.copy(file.toPath(), new File(FOLLOWER_PATH, file.getName()).toPath(), StandardCopyOption.REPLACE_EXISTING);
    for (final File wal : new File(LEADER_PATH).listFiles((dir, name) -> name.endsWith(".wal")))
      walPositions.put(wal.getName(), wal.length());

    followerFactory = new DatabaseFactory(FOLLOWER_PATH);
    follower = followerFactory.open();
    followerIndex = (LSMVectorIndex) follower.getSchema().getType("Chunk").getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];
    followerIndex.buildVectorGraphNow();
  }

  @AfterEach
  void tearDown() {
    TransactionManager.applyChangesHookForTesting = null;
    if (follower != null && follower.isOpen())
      follower.close();
    if (leader != null && leader.isOpen())
      leader.close();
    if (followerFactory != null)
      followerFactory.close();
    if (leaderFactory != null)
      leaderFactory.close();
    FileUtils.deleteRecursively(new File(LEADER_PATH));
    FileUtils.deleteRecursively(new File(FOLLOWER_PATH));
  }

  /** The entry of an insert: the vector page is applied first, the page of the record after it. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aSearchWhileAnInsertEntryIsAppliedAnswersNoRowWithoutProperties() throws Exception {
    final WALFile.WALTransaction entry = insertEntry();
    putPagesInOrder(entry, true);
    final float[] query = vectors.get("new-0");
    final AtomicReference<List<Result>> during = new AtomicReference<>();
    TransactionManager.applyChangesHookForTesting = () -> during.set(rows(query));

    ((DatabaseInternal) follower).getTransactionManager().applyChanges(entry, Collections.emptyMap(), false);

    assertThat(during.get()).as("the search ran between the first page of the entry and the others").isNotNull().isNotEmpty();
    assertThat(during.get()).as("every row of the answer has its properties").allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
    final List<Result> after = rows(query);
    assertThat(after).hasSize(BASE + BATCH);
    assertThat(after.getFirst().<String>getProperty("eid")).isEqualTo("new-0");
    assertThat(after).allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
  }

  /** The entry of a delete: the page of the records is applied first, the vector page, which removes the vectors, after it. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aSearchWhileADeleteEntryIsAppliedAnswersNoRowWithoutProperties() throws Exception {
    final WALFile.WALTransaction entry = deleteEntry();
    putPagesInOrder(entry, false);
    final float[] query = vectors.get("base-0");
    final AtomicReference<List<Result>> during = new AtomicReference<>();
    TransactionManager.applyChangesHookForTesting = () -> during.set(rows(query));

    ((DatabaseInternal) follower).getTransactionManager().applyChanges(entry, Collections.emptyMap(), false);

    assertThat(during.get()).as("the search ran between the first page of the entry and the others").isNotNull().isNotEmpty();
    assertThat(during.get()).as("every row of the answer has its properties").allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
    final List<Result> after = rows(query);
    assertThat(after).hasSize(BASE - BATCH);
    assertThat(after).allSatisfy(r -> assertThat(r.<String>getProperty("eid")).isNotNull());
  }

  /** The mark is up from the first page to the last and down once the entry is applied, whether it applies or fails. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void theApplyIsInFlightUntilTheEntryIsPublishedWhetherItAppliesOrFails() throws Exception {
    assertThat(followerIndex.hasReplicatedApplyInFlight()).isFalse();

    final WALFile.WALTransaction failing = insertEntry();
    putPagesInOrder(failing, true);
    // a page of the entry is more than one version ahead of the follower's: the apply stops there, after the vector page
    failing.pages[failing.pages.length - 1].currentPageVersion += 5;
    final AtomicReference<Boolean> inFlightDuring = new AtomicReference<>();
    TransactionManager.applyChangesHookForTesting = () -> inFlightDuring.set(followerIndex.hasReplicatedApplyInFlight());
    assertThatThrownBy(() -> ((DatabaseInternal) follower).getTransactionManager().applyChanges(failing, Collections.emptyMap(), false))
        .isInstanceOf(WALVersionGapException.class);
    assertThat(inFlightDuring.get()).as("in flight while its pages are applied").isTrue();
    assertThat(followerIndex.hasReplicatedApplyInFlight()).as("a failed apply is no longer in flight").isFalse();
  }

  private WALFile.WALTransaction insertEntry() throws Exception {
    leader.transaction(() -> {
      for (int i = 0; i < BATCH; i++)
        newChunk("new-" + i);
    });
    return capture();
  }

  private WALFile.WALTransaction deleteEntry() throws Exception {
    leader.transaction(() -> {
      for (int i = 0; i < BATCH; i++)
        leader.lookupByRID(baseRids.get(i), true).asVertex().delete();
    });
    return capture();
  }

  private RID newChunk(final String externalId) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < vector.length; i++)
      vector[i] = (float) random.nextGaussian();
    vectors.put(externalId, vector);
    return leader.newVertex("Chunk").set("external_id", externalId).set("embedding", vector).save().getIdentity();
  }

  /** The one WAL transaction the leader wrote since the last call: the bytes a follower is sent. */
  private WALFile.WALTransaction capture() throws IOException {
    // the leader's WAL files are open for writing: a copy is read, so the reader never shares the leader's channel
    final File copies = new File(WAL_COPIES_PATH);
    FileUtils.deleteRecursively(copies);
    copies.mkdirs();
    try {
      final List<WALFile.WALTransaction> written = new ArrayList<>();
      for (final File wal : new File(LEADER_PATH).listFiles((dir, name) -> name.endsWith(".wal"))) {
        final File copy = new File(copies, wal.getName());
        Files.copy(wal.toPath(), copy.toPath(), StandardCopyOption.REPLACE_EXISTING);
        final WALFile walFile = new WALFile(copy.getAbsolutePath());
        try {
          long position = walPositions.getOrDefault(wal.getName(), 0L);
          WALFile.WALTransaction transaction = walFile.getTransaction(position);
          while (transaction != null) {
            written.add(transaction);
            position = transaction.endPositionInLog;
            transaction = walFile.getTransaction(position);
          }
          walPositions.put(wal.getName(), position);
        } finally {
          walFile.close();
        }
      }
      assertThat(written).as("the commit of the leader wrote one transaction to its WAL").hasSize(1);
      return written.getFirst();
    } finally {
      FileUtils.deleteRecursively(copies);
    }
  }

  /** The pages of the vector index first or last, each group in the order the leader wrote it. */
  private void putPagesInOrder(final WALFile.WALTransaction entry, final boolean vectorPagesFirst) {
    final List<WALFile.WALPage> vectorPages = new ArrayList<>();
    final List<WALFile.WALPage> otherPages = new ArrayList<>();
    for (final WALFile.WALPage page : entry.pages)
      (((DatabaseInternal) leader).getSchema().getFileById(page.fileId) instanceof LSMVectorIndexMutable ? vectorPages : otherPages).add(page);
    assertThat(vectorPages).as("the entry carries a page of the vector index").isNotEmpty();
    assertThat(otherPages).as("the entry carries a page of the records").isNotEmpty();
    final List<WALFile.WALPage> ordered = new ArrayList<>(vectorPagesFirst ? vectorPages : otherPages);
    ordered.addAll(vectorPagesFirst ? otherPages : vectorPages);
    for (int i = 0; i < ordered.size(); i++)
      entry.pages[i] = ordered.get(i);
  }

  private List<Result> rows(final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = follower.query("sql", ROWS, query, BASE + BATCH + 10)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }
}
