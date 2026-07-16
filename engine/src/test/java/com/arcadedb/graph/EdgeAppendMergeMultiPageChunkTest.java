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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #565: the commutative edge-append merge (GRAPH_EDGE_APPEND_MERGE) corrupted edge
 * chunks STORED AS MULTI-PAGE RECORDS (a chunk allocated near the end of a page continues on the next page).
 * <p>
 * The rebase re-derived only the conflicted FIRST page of the chunk; the record's continuation slice was
 * committed from the transaction's stale drain-time copy, whose page passed the MVCC version check because it
 * was created only at drain time. Concurrently committed bytes on the continuation page were silently
 * reverted: zeroed tails on young chunks, shifted pairs (aliased edges/vertices) on filled ones, and lost
 * edges - exactly the client-reported corruption shape behind #565.
 * <p>
 * A 16 KB bucket page makes a cap-size (8 KB) chunk straddle a page boundary roughly every other chunk, so a
 * short concurrent run reliably exercises the multi-page + rebase combination the 64 KB default only hits at
 * scale. The fix makes multi-page/indirected chunk records non-rebasable (full retry instead).
 */
class EdgeAppendMergeMultiPageChunkTest extends TestHelper {
  private static final int ADDERS     = 8;
  private static final int PER_ADDER  = 1_500;

  private int     savedThreshold;
  private boolean savedMerge;
  private int     savedPageSize;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedMerge = GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean();
    savedPageSize = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(savedMerge);
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(savedPageSize);
  }

  @Test
  void concurrentAppendsOnPageStraddlingChunksLoseNothing() throws Exception {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(true);
    // The buckets (including the lazily-created *_in_edges chunk bucket) are created below, so the small page
    // applies to them: a cap-size 8 KB chunk then straddles a 16 KB page boundary roughly every other chunk.
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(16_384);

    database.transaction(() -> {
      database.getSchema().createVertexType("Account", 8).createProperty("number", Type.INTEGER);
      database.getSchema().createEdgeType("TRANSFERS", 8);
    });

    final RID[] hubHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account");
      hub.set("number", 0);
      hub.save();
      hubHolder[0] = hub.getIdentity();
    });
    final RID hubRID = hubHolder[0];

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final Set<String> expectedPairs = java.util.Collections.synchronizedSet(new HashSet<>(ADDERS * PER_ADDER));
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < ADDERS; t++) {
      final int id = t;
      threads.add(new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < PER_ADDER; i++) {
            final int n = id * PER_ADDER + i;
            final String[] pairHolder = new String[1];
            database.transaction(() -> {
              final MutableVertex src = database.newVertex("Account");
              src.set("number", n + 1);
              src.save();
              final MutableEdge e = src.newEdge("TRANSFERS", hubRID);
              pairHolder[0] = e.getIdentity() + "|" + src.getIdentity();
            }, true, 100);
            expectedPairs.add(pairHolder[0]);
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "adder-" + t));
    }
    threads.forEach(Thread::start);
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty()) {
      errors.getFirst().printStackTrace();
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());
    }

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges;
    assertThat(merges).as("edge-append rebase must actually have fired").isGreaterThan(0);

    // WALK THE HUB'S RAW PAIR STREAM: every appended pair present exactly once, nothing alien, no zeroed bytes.
    final List<String> diskPairs = new ArrayList<>(expectedPairs.size());
    database.transaction(() -> {
      EdgeSegment seg = (EdgeSegment) database.lookupByRID(((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk(), true);
      while (seg != null) {
        final MutableEdgeSegment m = (MutableEdgeSegment) seg;
        final AtomicInteger pos = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);
        final int used = m.getUsed();
        while (pos.get() < used) {
          final RID e = m.getRID(pos);
          final RID v = m.getRID(pos);
          assertThat(e.getBucketId() == 0 && e.getPosition() == 0 && v.getBucketId() == 0 && v.getPosition() == 0)
              .as("zeroed (unwritten) bytes inside the used range of chunk " + m.getIdentity()).isFalse();
          diskPairs.add(e + "|" + v);
        }
        seg = seg.getPrevious();
      }
    });

    final Set<String> disk = new HashSet<>(diskPairs);
    assertThat(disk).as("no pair duplicated by a rebase").hasSize(diskPairs.size());
    assertThat(disk).as("every committed pair present, none alien").isEqualTo(expectedPairs);

    database.transaction(() ->
        assertThat(hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "TRANSFERS")).isEqualTo(ADDERS * PER_ADDER));
  }
}
