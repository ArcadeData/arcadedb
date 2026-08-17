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

import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6319 - the continuation chunks an ordinary UPDATE leaves behind when a record shrinks back exactly onto a chunk
 * boundary.
 * <p>
 * {@code updateMultiPageRecord} rewrites the chain a record already has and hands whatever it no longer needs to
 * {@code freeChunkChain}. It decided two things from two different comparisons: the chain was CUT whenever the new
 * content ended at or before the chunk being written, and the tail was FREED only when it ended strictly before it.
 * Content ending exactly on a boundary therefore fell in between - cut, and freed by nothing. The chunks past the cut
 * kept their slots and their pointers to one another, reachable from no record, until an admin ran
 * {@code CHECK DATABASE FIX}.
 * <p>
 * The boundary is not a coincidence to be hit by luck: a chain grown one field at a time has a boundary exactly where
 * that field's bytes were appended, so a record that loses the field lands on one by construction. That is what made
 * {@code CRUDTest.multiUpdatesOverlap} leak 243821 chunks out of 1545495 - every record that stayed a chain across its
 * shrink round leaked its whole tail.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6319ChunkChainTailLeakTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "Chunked";
  /** Several pages' worth, so the record is a chain of several chunks rather than a head plus one. */
  private static final int    BASE  = 200_000;
  /** What one more field costs: enough to be appended as a chunk of its own at the end of the chain. */
  private static final int    FIELD = 100;

  /**
   * The defect in its smallest form: grow a chained record by one chunk, then take that chunk's worth of content away
   * again. The chain has to come back to exactly what it was, with nothing left over.
   */
  @Test
  void aRecordShrinkingBackOntoAChunkBoundaryFreesTheTailItDrops() {
    final RID rid = spilledRecord();

    final long chunksBefore = statistic("totalChunks");
    assertThat(chunksBefore).as("the fixture must really be a chain of several chunks").isGreaterThan(2L);

    // GROW: the content no longer fits the chain, so one more chunk is appended after the last one.
    write(rid, BASE + FIELD);
    assertThat(statistic("totalChunks")).as("growing by a field's worth must append exactly one chunk")
        .isEqualTo(chunksBefore + 1);

    // SHRINK BACK: the content now ends exactly where the chain ended before, on the boundary between the last chunk
    // it still needs and the one it does not.
    write(rid, BASE);

    assertThat(statistic("orphanedChunks")).as("the chunk the record dropped must be freed, not merely unlinked: "
        + bucketStats(TYPE)).isZero();
    assertThat(statistic("totalChunks")).as("so the chain is back to the size it had: " + bucketStats(TYPE))
        .isEqualTo(chunksBefore);
    assertThat(readPayload(rid)).as("and the record still reads back whole").isEqualTo(payload(BASE));

    checkDatabase();
  }

  /**
   * The same defect as the workload meets it, and the reason it is worth fixing at the source rather than repairing:
   * every round adds a field and drops the one before it, so every round ends on a boundary and leaks the chunk that
   * field's bytes were written into. Left alone the backlog grows for the life of the bucket.
   */
  @Test
  void repeatedGrowAndShrinkRoundsLeakNothing() {
    final RID rid = spilledRecord();
    final long chunksBefore = statistic("totalChunks");

    for (int round = 1; round <= 8; ++round) {
      write(rid, BASE + FIELD);
      write(rid, BASE);

      assertThat(statistic("orphanedChunks")).as("round " + round + " leaked: " + bucketStats(TYPE)).isZero();
      assertThat(statistic("totalChunks")).as("round " + round + " grew the chain: " + bucketStats(TYPE))
          .isEqualTo(chunksBefore);
    }

    assertThat(readPayload(rid)).isEqualTo(payload(BASE));
    checkDatabase();
  }

  /**
   * The boundary is only the case that always leaks; a shrink that stops SHORT of one has always freed its tail, and
   * must go on doing so now that both answers come from the same condition.
   */
  @Test
  void aRecordShrinkingPastAChunkBoundaryStillFreesItsTail() {
    final RID rid = spilledRecord();
    final long chunksBefore = statistic("totalChunks");

    write(rid, BASE / 2);

    assertThat(statistic("orphanedChunks")).as("nothing may be left behind: " + bucketStats(TYPE)).isZero();
    assertThat(statistic("totalChunks")).as("and the chain must really have shrunk: " + bucketStats(TYPE))
        .isLessThan(chunksBefore);
    assertThat(readPayload(rid)).isEqualTo(payload(BASE / 2));

    checkDatabase();
  }

  /** A single record grown until it has spilled into a chunk chain of {@link #BASE} bytes of payload. */
  private RID spilledRecord() {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("payload", Type.STRING);
      rid[0] = database.newDocument(TYPE).set("payload", payload(BASE)).save().getIdentity();
    });

    final Map<String, Object> layout = bucketStats(TYPE);
    assertThat((Long) layout.get("totalMultiPageRecords")).as("the fixture record must spill into chunks: " + layout)
        .isEqualTo(1L);
    return rid[0];
  }

  private void write(final RID rid, final int size) {
    database.transaction(() -> rid.asDocument(true).modify().set("payload", payload(size)).save());
  }

  private String readPayload(final RID rid) {
    final String[] payload = new String[1];
    database.transaction(() -> payload[0] = rid.asDocument(true).getString("payload"));
    return payload[0];
  }

  /** Content of a given length, distinct per length so a stale read cannot pass for a fresh one. */
  private static String payload(final int size) {
    final String marker = "s" + size + "-";
    return marker + "p".repeat(size - marker.length());
  }

  private long statistic(final String name) {
    return ((Number) bucketStats(TYPE).get(name)).longValue();
  }
}
