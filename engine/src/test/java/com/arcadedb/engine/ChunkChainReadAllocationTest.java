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

import com.arcadedb.TestHelper;
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #13244: reading a record that outgrew its page, a head chunk
 * and a short tail on another page, copied each chunk twice, once when the walk kept it and once into the record, and sized the walk's
 * bookkeeping arrays for eight chunks. A bucket whose records were all grown by one small property (the "add a property to every
 * record" migration) read each of them at more than four times its size.
 * <p>
 * Measured by the bytes the reading thread allocates, which are exact per thread, and never by time.
 */
class ChunkChainReadAllocationTest extends TestHelper {
  private static final int RECORDS = 400;
  private static final int READS   = 2;

  @Test
  void aTwoChunkRecordIsReadWithOneCopyOfItsChunks() {
    database.transaction(() -> database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create()
        .createProperty("text", Type.STRING));

    // records that fill their pages to the brim, so that growing one of them cannot be done in place
    final String text = "x".repeat(700);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Doc").set("text", text).set("idx", i).set("g", "graph-1").save();
    });

    final List<RID> rids = new ArrayList<>(RECORDS);
    database.transaction(() -> {
      final Iterator<Document> it = database.iterateType("Doc", false);
      while (it.hasNext()) {
        final MutableDocument grown = it.next().modify();
        grown.set("embedding_model", "embed-v1").set("updated_at", "2026-01-02T00:00:00+00:00").save();
        rids.add(grown.getIdentity());
      }
    });

    try (final ResultSet check = database.command("sql", "CHECK DATABASE")) {
      final Result result = check.next();
      assertThat(result.<Long>getProperty("totalMultiPageRecords")).as("records that spilled into a chunk chain")
          .isGreaterThan(RECORDS / 2);
    }

    final int stored = storedSize(rids.getFirst());
    final long perRead = allocatedPerRead(rids);
    // One copy of the chunks into the record, the record, the Document and its property map: 2,651 bytes for 771 stored. The
    // walk used to copy every chunk twice and to size its trace for eight chunks: 3,774 bytes.
    assertThat(perRead).as("bytes allocated per read of a record of %d stored bytes", stored).isLessThan(stored * 3L + 1_024);
  }

  private int storedSize(final RID rid) {
    return ((BaseRecord) database.lookupByRID(rid, true)).getBuffer().size();
  }

  private long allocatedPerRead(final List<RID> rids) {
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();

    for (final RID rid : rids)
      ((Document) database.lookupByRID(rid, true)).get("embedding_model");

    final long before = threads.getThreadAllocatedBytes(threadId);
    for (int pass = 0; pass < READS; pass++)
      for (final RID rid : rids)
        ((Document) database.lookupByRID(rid, true)).get("embedding_model");
    return (threads.getThreadAllocatedBytes(threadId) - before) / ((long) READS * rids.size());
  }
}
