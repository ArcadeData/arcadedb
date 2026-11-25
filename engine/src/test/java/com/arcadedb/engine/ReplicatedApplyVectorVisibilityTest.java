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
package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13323 on the replicated apply path: a follower applies an entry page by page, and decoded the page of a dense vector index
 * into the index as soon as that page was applied. A search between that page and the page holding the record answered the new
 * vector, and its row, made without reading the record (issue #10973), had every property null. The index pages of an entry are
 * now decoded once the whole entry is applied, as the postings of a sparse index are (issue #11257).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class ReplicatedApplyVectorVisibilityTest {
  private static final String LEADER_PATH   = "target/test-databases/ReplicatedApplyVectorVisibilityTest-leader";
  private static final String FOLLOWER_PATH = "target/test-databases/ReplicatedApplyVectorVisibilityTest-follower";
  private static final int    DIMENSIONS    = 8;
  // Past the 1000 vectors under which a search rebuilds a stale graph inside the search
  private static final int    BASE          = 1100;
  private static final String ROWS          =
      "SELECT @rid AS r, external_id AS eid FROM (SELECT expand(vector.neighbors('Chunk[embedding]', ?, ?)))";

  private final Random random = new Random(13323);

  @BeforeEach
  void setUp() {
    deleteDatabases();
  }

  @AfterEach
  void tearDown() {
    TransactionManager.pageAppliedHookForTesting = null;
    deleteDatabases();
  }

  @Test
  void aFollowerAnswersNoVectorOfAnEntryBeforeItsRecordIsApplied() throws Exception {
    try (final DatabaseFactory leaderFactory = new DatabaseFactory(LEADER_PATH)) {
      final Database seed = leaderFactory.create();
      // Quantized, with the vectors in the graph, as the reporter's index: a search scores the entry from the quantized codes of the
      // index page, where an index that keeps no vector in the graph reads the vector back from the record, and skips the entry while
      // the record is not there
      seed.transaction(() -> {
        seed.getSchema().buildVertexType().withName("Chunk").withTotalBuckets(1).create()
            .createProperty("embedding", Type.ARRAY_OF_FLOATS);
        seed.getSchema().getType("Chunk").createProperty("external_id", Type.STRING);
        seed.command("sql", "CREATE INDEX ON Chunk (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"INT8\", \"storeVectorsInGraph\": true }");
      });
      seed.transaction(() -> {
        for (int i = 0; i < BASE; i++)
          seed.newVertex("Chunk").set("external_id", "base-" + i).set("embedding", vector()).save();
      });
      indexOf(seed).buildVectorGraphNow();
      seed.close();
    }
    // The follower starts from the same files as the leader
    FileUtils.copyDirectory(new File(LEADER_PATH), new File(FOLLOWER_PATH));

    try (final DatabaseFactory leaderFactory = new DatabaseFactory(LEADER_PATH);
        final DatabaseFactory followerFactory = new DatabaseFactory(FOLLOWER_PATH)) {
      final Database leader = leaderFactory.open();
      final Database follower = followerFactory.open();
      try {
        assertThat(rows(follower, vector())).hasSize(BASE);

        // The leader commits one record, and ships the WAL of its pages
        final float[] vector = vector();
        leader.begin();
        final RID inserted = leader.newVertex("Chunk").set("external_id", "replicated").set("embedding", vector).save()
            .getIdentity();
        final TransactionContext transaction = ((DatabaseInternal) leader).getTransaction();
        final TransactionContext.TransactionPhase1 phase1 = transaction.commit1stPhase(true);
        final WALFile.WALTransaction entry = parse(phase1.result);
        transaction.commit2ndPhase(phase1);

        // The page of the record last, as a record on a page the commit created is: new pages follow the pages it modified
        final int bucketFileId = follower.getSchema().getBucketById(inserted.getBucketId()).getFileId();
        final List<WALFile.WALPage> pages = new ArrayList<>(List.of(entry.pages));
        pages.sort((a, b) -> Boolean.compare(a.fileId == bucketFileId, b.fileId == bucketFileId));
        assertThat(pages.getLast().fileId).isEqualTo(bucketFileId);
        assertThat(pages.getFirst().fileId).as("the entry writes the vector index as well").isNotEqualTo(bucketFileId);
        entry.pages = pages.toArray(new WALFile.WALPage[0]);

        // A search after every page of the entry the follower applies
        final List<String> missing = new ArrayList<>();
        final int[] searches = new int[1];
        TransactionManager.pageAppliedHookForTesting = () -> {
          ++searches[0];
          for (final Result row : rows(follower, vector))
            if (row.getProperty("eid") == null)
              missing.add(row.getProperty("r") + " after page " + searches[0]);
        };
        assertThat(((DatabaseInternal) follower).getTransactionManager().applyChanges(entry, Collections.emptyMap(), false)).isTrue();
        TransactionManager.pageAppliedHookForTesting = null;

        assertThat(searches[0]).as("a search ran between the pages of the entry").isGreaterThan(1);
        assertThat(missing).as("no row without properties while the entry is applied").isEmpty();

        // Applied whole, the follower answers the record, with its properties
        final List<Result> after = rows(follower, vector);
        assertThat(after).hasSize(BASE + 1);
        assertThat(after.getFirst().<RID>getProperty("r")).isEqualTo(inserted);
        assertThat(after.getFirst().<String>getProperty("eid")).isEqualTo("replicated");
      } finally {
        leader.drop();
        follower.drop();
      }
    }
  }

  /** The layout {@link WALFile#writeTransactionToBuffer} writes, which a follower reads back from the replicated entry. */
  private static WALFile.WALTransaction parse(final Binary buffer) {
    final ByteBuffer bytes = ByteBuffer.wrap(buffer.toByteArray());
    final WALFile.WALTransaction transaction = new WALFile.WALTransaction();
    transaction.txId = bytes.getLong();
    transaction.timestamp = bytes.getLong();
    final int segments = bytes.getInt();
    bytes.getInt(); // the size of the segments
    transaction.pages = new WALFile.WALPage[segments];
    for (int i = 0; i < segments; i++) {
      final WALFile.WALPage page = new WALFile.WALPage();
      page.fileId = bytes.getInt();
      page.pageNumber = bytes.getInt();
      page.changesFrom = bytes.getInt();
      page.changesTo = bytes.getInt();
      page.currentPageVersion = bytes.getInt();
      page.currentPageSize = bytes.getInt();
      final byte[] content = new byte[page.changesTo - page.changesFrom + 1];
      bytes.get(content);
      page.currentContent = new Binary(content);
      transaction.pages[i] = page;
    }
    return transaction;
  }

  private static List<Result> rows(final Database db, final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = db.query("sql", ROWS, query, BASE + 10)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  private static LSMVectorIndex indexOf(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Chunk").getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];
  }

  private float[] vector() {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < vector.length; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }

  private static void deleteDatabases() {
    FileUtils.deleteRecursively(new File(LEADER_PATH));
    FileUtils.deleteRecursively(new File(FOLLOWER_PATH));
  }
}
