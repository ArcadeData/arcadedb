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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-user regression for #5120 (and the companion #5119): an LSM index lookup silently disagrees with a full scan
 * after compaction, {@code REBUILD INDEX} repairs it, but the next compaction breaks it again, and after a restart
 * the on-disk compacted index is incomplete.
 * <p>
 * Root cause (fixed by #4946, commit b67973388): a compaction round that fails mid-merge (root-page overflow, I/O
 * error, thread interrupt, OOM - any {@code Throwable}) flushes leaf pages eagerly with no WAL and used to leave
 * them counted in the in-RAM page count. The NEXT successful round then published those orphan pages through
 * {@code setCompactedTotalPages()}, producing a malformed series that the positional read logic walks wrong - so
 * index lookups re-expose tombstoned rows and/or miss live rows, exactly the "index != scan" symptom the issue
 * reports. Heavy-duplicate keys make it easier to observe because they inflate the compacted leaf-page count. The
 * atomic-rollback fix rolls the page count back on failure so the orphan region is overwritten by the next round.
 * <p>
 * This drives the user-visible contract (the index must agree with the scan) through the failure-then-success
 * sequence; it fails on 26.7.1 and passes on the fixed code. It complements {@link LSMTreeCompactionAtomicityTest},
 * which asserts the internal page-count and temp-file invariants of the same fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5120CompactionDuplicatesTest extends TestHelper {

  private static final String TYPE_NAME  = "Tok";
  private static final int    SHORT_KEY  = 8;
  private static final int    LONG_KEY   = 60;

  @Override
  public void beforeTest() {
    // Explicit compaction only, so the failure and the following success are deterministic (a background round
    // would otherwise steal the COMPACTION_SCHEDULED status).
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  private LSMTreeIndex createType() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("word", String.class);
    // Small pages so a round with many long-keyed leaf pages deterministically overflows the single root page -
    // the real mid-merge failure the issue hit (equally reachable via I/O error / interrupt / OOM on any page size).
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "word" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(1024).create();
    return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
  }

  private static String key(final int len, final int i) {
    return "k".repeat(len) + "-" + String.format("%08d", i);
  }

  private void insert(final int from, final int to, final int len) {
    database.transaction(() -> {
      for (int i = from; i < to; i++)
        database.newDocument(TYPE_NAME).set("word", key(len, i)).save();
    });
  }

  /** Background compaction swallows exceptions; mimic that so a failed round is not fatal to the caller. */
  private void tryCompact(final LSMTreeIndex index) {
    try {
      if (index.scheduleCompaction())
        index.compact();
    } catch (final Throwable ignore) {
      // as the async compactor does: log and move on, leaving (pre-#4946) orphan pages behind.
    }
  }

  private long scanCount() {
    final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME);
    return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
  }

  private long lookupTotal(final int from, final int to, final int len) {
    long total = 0;
    for (int i = from; i < to; i++) {
      final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word = ?", key(len, i));
      if (rs.hasNext())
        total += ((Number) rs.next().getProperty("c")).longValue();
    }
    return total;
  }

  @Test
  void indexAgreesWithScanAfterFailedThenSuccessfulCompaction() {
    final LSMTreeIndex index = createType();

    // Round 1: small - succeeds and links the compacted sub-index with a well-formed series.
    insert(0, 200, SHORT_KEY);
    tryCompact(index);
    assertThat(index.getMutableIndex().getSubIndex()).as("first round links the sub-index").isNotNull();

    // Round 2: oversized long keys - fails mid-merge with a root-page overflow AFTER flushing orphan leaf pages
    // into the sub-index file and bumping the in-RAM page count.
    insert(1_000_000, 1_003_000, LONG_KEY);
    tryCompact(index);

    // Delete the oversized rows (full scan, no index) so the next compaction is small enough to SUCCEED.
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME);
      while (rs.hasNext()) {
        final var doc = rs.next().getRecord().get().asDocument();
        if (doc.getString("word").length() > 30)
          doc.delete();
      }
    });

    // Round 3: small - succeeds. Pre-#4946 its setCompactedTotalPages() publishes the round-2 orphan pages as live
    // series content, so the positional walk mis-reads and the deleted long keys resurrect through the index.
    insert(200, 400, SHORT_KEY);
    tryCompact(index);

    final long scan = scanCount();
    assertThat(scan).as("only the 400 short-keyed rows remain in storage").isEqualTo(400);

    // The index must agree with the scan: every surviving short key is reachable, and no deleted long key resurrects.
    assertThat(lookupTotal(0, 400, SHORT_KEY)).as("surviving rows must all be reachable through the index").isEqualTo(400);
    assertThat(lookupTotal(1_000_000, 1_003_000, LONG_KEY))
        .as("deleted rows must not resurrect through a malformed compacted series (#5120)").isEqualTo(0);
  }
}
