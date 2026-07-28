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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5470: the WAL buffer of a transaction - which on a replicated database <i>is</i> the
 * Raft entry shipped to the followers - must be a function of the data the transaction wrote, not of the number of
 * pages it happened to touch.
 * <p>
 * A typed index is split per bucket, so a bulk load of N vertices touches one 256 KB LSM page per bucket per indexed
 * property, and every one of those pages is written at both ends (the sorted pointer array grows up from the header,
 * the key/value content down from the tail). While the WAL shipped the hull of the modified bytes, each of those
 * pages contributed its whole 256 KB: a 2,500-vertex transaction of ~120-byte records produced a 4.7 MB entry, and
 * lowering the batch size did not help because the per-page cost is fixed. In the field the entries reached 34 MB
 * and hit the maximum replicated Raft entry size regardless of how small the batches were made.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5470WalWriteAmplificationTest extends TestHelper {

  private static final int BUCKETS    = 8;
  private static final int WARMUP     = 5_000;
  private static final int MEASURED   = 1_000;
  private static final int RECORD_LEN = 150;

  @Test
  void aTransactionDoesNotShipTheWholeOfEveryIndexPageItTouched() {
    database.transaction(() -> {
      final VertexType type = database.getSchema().buildVertexType().withName("Address").withTotalBuckets(BUCKETS)
          .create();
      type.createProperty("entityId", Type.STRING);
      type.createProperty("fid", Type.STRING);
      type.createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Address", "entityId");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Address", "fid");
    });

    final DatabaseInternal db = (DatabaseInternal) database;

    // Fill the mutable index pages first: on an empty page there is little to over-ship, the amplification appears
    // as soon as the pages hold data.
    createVertices(db, WARMUP);

    final byte[] wal = createVertices(db, MEASURED);

    final Map<Integer, Integer> bytesPerFile = new HashMap<>();
    final Set<Long> indexPages = new HashSet<>();
    int indexBytes = 0;

    final ByteBuffer buffer = ByteBuffer.wrap(wal);
    buffer.getLong(); // txId
    buffer.getLong(); // timestamp
    final int segments = buffer.getInt();
    buffer.getInt();  // segment size

    for (int i = 0; i < segments; i++) {
      final int fileId = buffer.getInt();
      final int pageNumber = buffer.getInt();
      final int from = buffer.getInt();
      final int to = buffer.getInt();
      buffer.getInt(); // version
      buffer.getInt(); // content size
      final int deltaSize = to - from + 1;
      buffer.position(buffer.position() + deltaSize);

      bytesPerFile.merge(fileId, deltaSize, Integer::sum);
      if (isIndexFile(db, fileId)) {
        indexPages.add(((long) fileId << 32) | pageNumber);
        indexBytes += deltaSize;
      }
    }

    assertThat(indexPages).as("both indexes of every bucket must take part in the load")
        .hasSizeGreaterThanOrEqualTo(2 * BUCKETS);

    // The bound that matters: the transaction must not pay a full index page per page it touched. Before the fix
    // indexBytes was ~indexPages * 256 KB; the delta of MEASURED keys spread over those pages is orders of
    // magnitude smaller, so a quarter of the old cost is a threshold with plenty of headroom in both directions.
    assertThat(indexBytes).as("index pages shipped: %d", indexPages.size())
        .isLessThan(indexPages.size() * LSMTreeIndexAbstract.DEF_PAGE_SIZE / 4);

    // And the whole transaction stays within a small multiple of the records it actually wrote.
    assertThat(wal.length).isLessThan(20 * MEASURED * RECORD_LEN);

    assertThat(database.countType("Address", false)).isEqualTo(WARMUP + MEASURED);
  }

  /**
   * Creates {@code count} vertices in one transaction and returns the WAL buffer the transaction would replicate.
   */
  private byte[] createVertices(final DatabaseInternal db, final int count) {
    db.begin();
    db.getTransaction().setUseWAL(true);

    for (int i = 0; i < count; i++) {
      final String id = UUID.randomUUID().toString().replace("-", "");
      db.newVertex("Address")
          .set("entityId", id)
          .set("fid", "dk/address/" + id)
          .set("name", "simple address")
          .save();
    }

    final TransactionContext tx = db.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    final byte[] wal = phase1.result.toByteArray();
    tx.commit2ndPhase(phase1);
    return wal;
  }

  private static boolean isIndexFile(final DatabaseInternal db, final int fileId) {
    final ComponentFile file = db.getFileManager().getFile(fileId);
    return file instanceof PaginatedComponentFile && file.getFileName().contains("idx");
  }
}
