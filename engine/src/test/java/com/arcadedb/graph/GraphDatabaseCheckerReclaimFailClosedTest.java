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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.RecordNotFoundException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Data-loss guard (PR #5379 review): the orphaned-edge-segment reclaim must FAIL CLOSED. If a vertex cannot be
 * walked during the reachability pass (phase 1), its live segments never make it into the reachable set, so
 * deleting the "unreachable" segments in phase 2 would destroy them. The reclaim must skip the whole deletion
 * phase whenever the walk is incomplete.
 * <p>
 * Here the hub vertex RECORD is corrupted so it cannot be loaded, then {@code reclaimOrphanedEdgeSegments} is
 * invoked directly: with the guard the hub's live IN chain survives and nothing is reclaimed; without it the hub
 * chain (unmarked because the hub could not be walked) would be deleted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphDatabaseCheckerReclaimFailClosedTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Test
  void reclaimSkippedWhenAVertexCannotBeWalked() {
    final RID hub = buildHubWithMultiChunkInChain(500);
    final List<RID> hubInChunks = collectInChunks(hub);

    // Corrupt the hub vertex record so it can no longer be loaded during the reachability walk (scan-load path).
    shrinkRecordContent(hub);
    reopenDatabase();

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .reclaimOrphanedEdgeSegments(0, Integer.MAX_VALUE);

    assertReclaimSkipped(stats);

    // The hub's live IN chunks all survive.
    database.transaction(() -> {
      for (final RID chunk : hubInChunks) {
        try {
          assertThat(((DatabaseInternal) database).lookupByRID(chunk, true))
              .as("live hub chunk " + chunk + " must survive the fail-closed reclaim").isNotNull();
        } catch (final RecordNotFoundException e) {
          throw new AssertionError("live hub chunk " + chunk + " was deleted by the reclaim", e);
        }
      }
    });
  }

  /**
   * PR #5379 review point 1: the fail-closed guarantee must also cover a read error that happens MID-WALK on a
   * live chain (not just an unloadable vertex). Here a middle chunk of the hub's IN chain is corrupted so its
   * lookup throws during {@code markChain}, leaving the downstream (older) chunks unmarked. Without the guard
   * phase 2 would delete those live downstream chunks; with it the whole reclaim is skipped and they survive.
   */
  @Test
  void reclaimSkippedWhenAChainReadFailsMidWalk() {
    final RID hub = buildHubWithMultiChunkInChain(500);
    final List<RID> hubInChunks = collectInChunks(hub);
    assertThat(hubInChunks.size()).as("need at least 3 chunks: head, the corrupted middle, and a downstream tail")
        .isGreaterThanOrEqualTo(3);

    final RID middleChunk = hubInChunks.get(1);       // walk starts at the readable head then throws HERE...
    final RID downstreamChunk = hubInChunks.get(2);   // ...leaving this LIVE chunk beyond it unmarked

    // Remove a MIDDLE chunk (not the head, not the vertex) so markChain marks the head, then the next lookup
    // throws RecordNotFoundException part-way through - a live chain whose walk fails mid-way.
    database.transaction(() -> database.getSchema().getBucketById(middleChunk.getBucketId()).deleteRecord(middleChunk));

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .reclaimOrphanedEdgeSegments(0, Integer.MAX_VALUE);

    assertReclaimSkipped(stats);

    // The downstream live chunk beyond the read failure survives (the data-loss the guard prevents).
    database.transaction(() -> {
      try {
        assertThat(((DatabaseInternal) database).lookupByRID(downstreamChunk, true))
            .as("live downstream chunk " + downstreamChunk + " must survive the fail-closed reclaim").isNotNull();
      } catch (final RecordNotFoundException e) {
        throw new AssertionError("live downstream chunk " + downstreamChunk + " was deleted by the reclaim", e);
      }
    });
  }

  private RID buildHubWithMultiChunkInChain(final int degree) {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
    });

    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());

    final int batch = 250;
    for (int from = 0; from < degree; from += batch) {
      final int start = from;
      final int end = Math.min(from + batch, degree);
      database.transaction(() -> {
        for (int i = start; i < end; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, hub[0]);
      });
    }
    return hub[0];
  }

  /** The hub's IN chain chunk RIDs, head first. */
  private List<RID> collectInChunks(final RID hub) {
    final List<RID> chunks = new ArrayList<>();
    database.transaction(() -> {
      RID current = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
      while (current != null) {
        chunks.add(current);
        current = ((EdgeSegment) ((DatabaseInternal) database).lookupByRID(current, true)).getPreviousRID();
      }
    });
    assertThat(chunks.size()).as("the hub IN chain must span more than one chunk").isGreaterThan(1);
    return chunks;
  }

  private void assertReclaimSkipped(final Map<String, Object> stats) {
    // NOTHING was reclaimed: the incomplete walk disabled the deletion phase.
    assertThat(((Number) stats.get("orphanedEdgeSegmentsReclaimed")).longValue())
        .as("an incomplete reachability walk must skip the deletion phase").isEqualTo(0L);
    assertThat(((Number) stats.get("orphanedEdgeSegments")).longValue()).isEqualTo(0L);
    // The skip is reported.
    assertThat(((Collection<String>) stats.get("warnings")).stream()
        .anyMatch(w -> w.contains("did not complete"))).as("the skipped reclaim must be reported").isTrue();
  }

  /**
   * Shrinks the record-size varint of the record at {@code rid} (page 0, slot 0) to a valid but tiny value (1
   * content byte). Unlike a corrupt/oversized marker - which the bucket layer auto-deletes on the next scan - a
   * small-but-valid size survives the reopen repair (it only checks size bounds), yet the truncated view is too
   * short for {@code asVertex(true)} to read the fixed edge-head-chunk prefix, so the vertex fails to load during
   * the reachability walk. The record is chosen tiny enough for its size to fit in a single-byte varint.
   */
  private void shrinkRecordContent(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = rid.getBucketId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
        final int recordTableOffset = Binary.SHORT_SERIALIZED_SIZE;
        final int recordOffset = (int) page.readUnsignedInt(recordTableOffset);
        final long[] recordSize = page.readNumberAndSize(recordOffset);
        assertThat(recordSize[1]).as("the tiny hub record must use a single-byte size varint").isEqualTo(1L);
        page.writeByte(recordOffset, (byte) 2); // zigzag(1) == 2  ->  content size 1 byte (too short for a vertex)
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
