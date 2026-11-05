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
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for LSM index compaction (counters, names, null strategy, duplicates, stale entries), one nested
 * class per issue. The general compaction stress tests live in {@link LSMTreeIndexCompactionTest}.
 */
class IndexCompactionRegressionTest extends TestHelper {
  /**
   * Regression test for issue #4960: {@code LSMTreeIndexMutable.onAfterLoad()} hardcoded
   * {@code currentMutablePages = 1} regardless of how many uncompacted pages the reloaded file actually
   * contains. After a restart, an index that had accumulated N pages (but had not yet crossed the
   * auto-compaction threshold) restarted its counter at 1, deferring the scheduled compaction by up to a
   * full threshold's worth of new pages every restart.
   */
  @Nested
  @DisplayName("#4960 reloaded index restores the mutable pages counter")
  class Issue4960ReloadRestoresMutablePageCount {
    private static final String TYPE_NAME = "ReloadItem";
    private static final int    PAGE_SIZE = 4096;

    @Test
    void reloadedIndexRestoresMutablePageCountFromFile() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("id", Integer.class);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();
      });

      final LSMTreeIndexMutable before = bucketMutableIndex();
      final int bucketId = database.getSchema().getType(TYPE_NAME).getFirstBucketId();

      // Enough entries to span several 4K pages, but below the auto-compaction threshold (default 10)
      // so no compaction interferes with the counter.
      database.transaction(() -> {
        for (int i = 0; i < 1200; ++i)
          before.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
      });

      final int pagesBeforeReopen = before.getTotalPages();
      assertThat(pagesBeforeReopen).as("the index must span multiple pages for the test to be meaningful")
          .isGreaterThan(1);

      reopenDatabase();

      final LSMTreeIndexMutable after = bucketMutableIndex();
      assertThat(after.getTotalPages()).isEqualTo(pagesBeforeReopen);
      assertThat(after.getCurrentMutablePages())
          .as("after a reload the mutable-pages counter must track the file's page count (approximate: "
              + "getTotalPages() includes the header/root page, which is fine for a pacing-only signal), "
              + "otherwise auto-compaction is deferred by a full threshold every restart")
          .isEqualTo(after.getTotalPages());
    }

    private LSMTreeIndexMutable bucketMutableIndex() {
      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("id").get(0);
      for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
        if (bucketIndex instanceof LSMTreeIndex lsmIndex)
          return lsmIndex.getMutableIndex();
      throw new IllegalStateException("No LSM bucket index found for type " + TYPE_NAME);
    }
  }

  /**
   * Regression test for issue #5067 (item 3): {@code LSMTreeIndexMutable.createNewPage()} increments
   * {@code currentMutablePages} eagerly inside the transaction, so a rollback discards the pages but not
   * the increments. The counter drifted above the real page count for the rest of the run (it only
   * self-corrected on reload after #5063's onAfterLoad fix), inflating the auto-compaction scheduling
   * check in {@code onAfterCommit}. The fix clamps the counter to the real page count at commit time.
   */
  @Nested
  @DisplayName("#5067 rolled back page creations do not drift the mutable pages counter")
  class Issue5067MutablePagesCounterDrift {

    @BeforeEach
    void createIndex() {
      final DocumentType type = database.getSchema().createDocumentType("Issue5067Doc");
      type.createProperty("id", Type.STRING);
      database.getSchema().buildTypeIndex("Issue5067Doc", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false).withPageSize(4_096).create();
    }

    @Test
    void rolledBackPageCreationsDoNotDriftTheMutablePagesCounter() {
      final TypeIndex typeIndex = database.getSchema().getType("Issue5067Doc").getAllIndexes(false).iterator().next();
      final LSMTreeIndex lsmIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
      final LSMTreeIndexMutable mutable = lsmIndex.getMutableIndex();
      final int bucketFileId = database.getSchema().getType("Issue5067Doc").getBuckets(false).get(0).getFileId();

      // Create index pages inside a transaction that rolls back: put directly on the mutable index (the
      // regular record path defers index writes to commit) so createNewPage runs, then discard everything.
      database.begin();
      try {
        for (int i = 0; i < 2_000; i++)
          mutable.put(new Object[] { "key-" + i }, new RID[] { new RID(bucketFileId, i) });
      } finally {
        database.rollback();
      }

      // Precondition documenting the drift: the rolled-back page creations left the counter above the
      // real (persistent) page count.
      assertThat(mutable.getCurrentMutablePages())
          .as("rolled-back page creations must have drifted the counter for this test to be meaningful")
          .isGreaterThan(mutable.getTotalPages());

      // A committed write to the index triggers onAfterCommit, which must re-align the counter with the
      // real page count instead of feeding the drifted value to the compaction scheduling check.
      database.transaction(() -> database.newDocument("Issue5067Doc").set("id", "committed").save());

      assertThat(mutable.getCurrentMutablePages())
          .as("after a commit the counter must not exceed the real page count")
          .isLessThanOrEqualTo(mutable.getTotalPages());
    }
  }

  /**
   * Second-round regression for #5120: a strict-prefix lookup (5 of 9 components) on a wide composite NOTUNIQUE index
   * returns FEWER rows than the equivalent full scan once the index has been compacted, while the full-key lookup and
   * the same index with compaction disabled are correct.
   * <p>
   * Root cause is NOT the compaction atomicity bug fixed by #4946, but the signed/unsigned key-comparison desync fixed
   * by #5321 (commit 02d5800ea): {@code BinaryComparator.compareBytes(byte[], Binary)} - used by the LSM binary-search
   * seek - compared bytes as signed, while the compactor writes the compacted pages in the engine-wide UNSIGNED byte
   * order. For ASCII-only values the two orders agree (which is why a synthetic reproducer with plain values never
   * fails); as soon as a key component holds a multi-byte UTF-8 character (umlauts, accents: lead/continuation bytes
   * &gt;= 0x80, negative as a Java byte) the seek walks the compacted pages in the wrong order: it either stops short of
   * the matching range (rows missing from the lookup while the scan still sees them - the reported undercount) or lands
   * on an unrelated range and emits foreign rows. A rebuild repairs the in-RAM mutable pages, but the next compaction -
   * or a restart, which reads from the compacted pages - brings the divergence back, exactly the durability symptom
   * reported here.
   * <p>
   * Both methods pass on the fixed code and fail without it; on the signed comparator the compacted phase shows both
   * {@code index=0 scan=1} and wildly inflated counts on the very same dataset.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5120 prefix lookup on a compacted index with non-ASCII values")
  class Issue5120CompactedPrefixNonAscii {

    private static final String TYPE_NAME  = "Repro5120";
    private static final int    COLUMNS    = 9;
    private static final int    PREFIX_LEN = 5;
    private static final int    ROWS       = 20_000;

    // Low-cardinality values, as in the report. The non-ASCII ones are what makes the compacted seek diverge.
    // Each accented value has an ASCII sibling that shares its prefix and diverges at the very byte that carries the
    // multi-byte character, so the signed and the unsigned byte orders disagree on their relative position.
    private static final String[] VALUES = { "Müller", "Muster", "Straße", "Strasse", "Grün", "Gruppe", "Köln", "Kosten",
        "Weiß", "Weiss", "Zürich", "Zusatz" };

    @BeforeEach
    void configure() {
      // Compaction is driven explicitly so the before/after state is deterministic.
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    }

    private LSMTreeIndex createSchema() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      final String[] properties = new String[COLUMNS];
      for (int c = 0; c < COLUMNS; c++) {
        properties[c] = "c" + c;
        type.createProperty(properties[c], String.class);
      }

      database.getSchema().buildTypeIndex(TYPE_NAME, properties).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
          .withPageSize(4096).create();

      return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
          .getIndexesOnBuckets()[0];
    }

    private static String value(final int row, final int column) {
      return VALUES[(row / (1 + column * 3) + column) % VALUES.length];
    }

    private void populate() {
      database.transaction(() -> {
        for (int r = 0; r < ROWS; r++) {
          final var doc = database.newDocument(TYPE_NAME);
          for (int c = 0; c < COLUMNS; c++)
            doc.set("c" + c, value(r, c));
          doc.save();
        }
      });
    }

    private String prefixWhere() {
      final StringBuilder where = new StringBuilder();
      for (int c = 0; c < PREFIX_LEN; c++) {
        if (c > 0)
          where.append(" AND ");
        where.append("c").append(c).append(" = ?");
      }
      return where.toString();
    }

    private Object[] prefixParams(final int row) {
      final Object[] params = new Object[PREFIX_LEN];
      for (int c = 0; c < PREFIX_LEN; c++)
        params[c] = value(row, c);
      return params;
    }

    /** Prefix lookup through the index. */
    private long indexCount(final int row) {
      final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE " + prefixWhere(),
          prefixParams(row));
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
    }

    /** Same predicate evaluated by scanning every record, bypassing the index. */
    private long scanCount(final int row) {
      final Object[] params = prefixParams(row);
      long count = 0;
      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME);
      while (rs.hasNext()) {
        final var doc = rs.next().getRecord().get().asDocument();
        boolean matches = true;
        for (int c = 0; c < PREFIX_LEN && matches; c++)
          matches = params[c].equals(doc.getString("c" + c));
        if (matches)
          count++;
      }
      return count;
    }

    private void assertIndexAgreesWithScan(final String phase) {
      final List<String> mismatches = new ArrayList<>();
      for (int row = 0; row < 40; row++) {
        final long scan = scanCount(row);
        final long index = indexCount(row);
        if (index != scan)
          mismatches.add("row " + row + ": index=" + index + " scan=" + scan);
      }
      assertThat(mismatches).as(phase + ": every prefix lookup must return exactly what the scan returns").isEmpty();
    }

    /** Everything served from the mutable pages, the configuration the reporter used as "compaction disabled". */
    @Test
    void prefixLookupAgreesWithScanOnMutablePages() {
      createSchema();
      populate();

      assertIndexAgreesWithScan("mutable");
    }

    /** Same data read through the compacted sub-index, where the reporter sees the undercount. */
    @Test
    void prefixLookupAgreesWithScanAfterCompaction() throws Exception {
      final LSMTreeIndex index = createSchema();
      populate();

      if (index.scheduleCompaction())
        index.compact();
      assertThat(index.getMutableIndex().getSubIndex()).as("compaction produced a sub-index").isNotNull();

      assertIndexAgreesWithScan("compacted");
    }
  }

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
  @Nested
  @DisplayName("#5120 index agrees with the scan after a failed then successful compaction")
  class Issue5120CompactionDuplicates {

    private static final String TYPE_NAME  = "Tok";
    private static final int    SHORT_KEY  = 8;
    private static final int    LONG_KEY   = 60;

    @BeforeEach
    void configure() {
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

  /**
   * Regression test for issue #9213. A compaction swaps a new mutable file named {@code <prefix>_<nanoTime>} into an
   * {@link LSMTreeIndex} but keeps the index's logical name, while every re-read of the schema (a restart, the full
   * {@code load()} an HA follower takes for a compaction entry, an incremental refresh) built the bucket index from the
   * file and so named it after the new file. The live instance and any reloaded one then disagreed on the name, which
   * {@code TypeIndex.equals} compares, so {@code DatabaseComparator} reported the type as configured differently.
   */
  @Nested
  @DisplayName("#9213 compaction keeps the bucket index name")
  class Issue9213CompactionKeepsBucketIndexName {
    private static final String TYPE_NAME  = "Issue9213Item";
    private static final int    PAGE_SIZE  = 8192;
    private static final int    RECORDS    = 700;
    private static final int    GEO_POINTS = 1_000;

    @Test
    void bucketIndexKeepsItsNameAcrossCompactionAndRestart() throws Exception {
      final List<String> keys = createAndFill(false);
      final String nameBefore = bucketIndex().getName();

      compact();
      assertThat(bucketIndex().getMostRecentFileName()).as("the compaction must have swapped in a new mutable file")
          .isNotEqualTo(nameBefore);
      assertThat(bucketIndex().getName()).isEqualTo(nameBefore);

      reopenDatabase();

      assertThat(bucketIndex().getName()).as("a restart must keep the name the live index answered to")
          .isEqualTo(nameBefore);
      assertThat(database.getSchema().existsIndex(nameBefore)).isTrue();
      assertIndexServes(keys);

      // A second compaction after the restart, and a second restart, keep the same name.
      insertMore(keys);
      compact();
      reopenDatabase();
      assertThat(bucketIndex().getName()).isEqualTo(nameBefore);
      assertIndexServes(keys);
    }

    @Test
    void uniqueBucketIndexKeepsItsNameAcrossCompactionAndRestart() throws Exception {
      final List<String> keys = createAndFill(true);
      final String nameBefore = bucketIndex().getName();

      compact();
      reopenDatabase();

      assertThat(bucketIndex().getName()).isEqualTo(nameBefore);
      assertIndexServes(keys);
    }

    /**
     * The path an HA follower takes for a compaction schema entry (the entry retires a file): a full schema
     * {@code load()} on a live database, compared against the instance the leader keeps.
     */
    @Test
    void fullSchemaLoadKeepsTheBucketIndexName() throws Exception {
      final List<String> keys = createAndFill(false);
      final String nameBefore = bucketIndex().getName();
      compact();

      ((LocalSchema) database.getSchema().getEmbedded()).load(ComponentFile.MODE.READ_WRITE, true);

      final LSMTreeIndex reloaded = bucketIndex();
      assertThat(reloaded.getName()).isEqualTo(nameBefore);
      assertThat(database.getSchema().getIndexByName(nameBefore)).isSameAs(reloaded);
      assertThat(database.getSchema().existsIndex(reloaded.getMostRecentFileName()))
          .as("the index must not stay registered under its file name as well").isFalse();
      assertIndexServes(keys);
    }

    @Test
    void incrementalSchemaLoadKeepsTheBucketIndexAttached() throws Exception {
      final List<String> keys = createAndFill(false);
      final String nameBefore = bucketIndex().getName();
      compact();

      // Persist the post-compaction schema (the logical name included) and re-read it incrementally.
      database.getSchema().getEmbedded().saveConfiguration();
      ((LocalSchema) database.getSchema().getEmbedded()).loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of());

      assertThat(bucketIndex().getName()).isEqualTo(nameBefore);
      assertThat(database.getSchema().getType(TYPE_NAME).getIndexesByProperties("key")).hasSize(1);
      assertIndexServes(keys);
    }

    /**
     * A full-text index is an {@link LSMTreeIndex} behind a wrapper the load re-creates from the schema entry: the wrapper
     * answers to the inner index's name, so it must come back registered under the logical name too.
     */
    @Test
    void fullTextIndexKeepsItsNameAcrossCompactionAndRestart() throws Exception {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("text", String.class);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "text" })
            .withType(Schema.INDEX_TYPE.FULL_TEXT).withPageSize(PAGE_SIZE).create();
      });
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE_NAME).set("text", "word" + i + " other" + (i * 7) + " shared").save();
      });

      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("text").get(0);
      final IndexInternal bucketIndex = typeIndex.getIndexesOnBuckets()[0];
      final String nameBefore = bucketIndex.getName();
      final LSMTreeIndex lsm = (LSMTreeIndex) ((LSMTreeIndexMutable) bucketIndex.getComponent()).getMainIndex();
      database.async().waitCompletion();
      lsm.scheduleCompaction();
      assertThat(lsm.compact()).isTrue();

      reopenDatabase();

      final TypeIndex reloaded = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("text").get(0);
      assertThat(reloaded.getIndexesOnBuckets()[0].getName()).isEqualTo(nameBefore);
      assertThat(database.getSchema().getIndexByName(nameBefore).getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);
      assertThat(reloaded.get(new Object[] { "word42" }).hasNext()).isTrue();
    }

    /** Same as the full-text case, for the geospatial wrapper. */
    @Test
    void geospatialIndexKeepsItsNameAcrossCompactionAndRestart() throws Exception {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("location", String.class);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "location" })
            .withType(Schema.INDEX_TYPE.GEOSPATIAL).withPageSize(PAGE_SIZE).create();
      });
      database.transaction(() -> {
        for (int i = 0; i < GEO_POINTS; i++)
          database.newDocument(TYPE_NAME).set("location", "POINT (" + (i % 170) + "." + i + " " + (i % 80) + ".5)").save();
      });

      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("location").get(0);
      final IndexInternal bucketIndex = typeIndex.getIndexesOnBuckets()[0];
      final String nameBefore = bucketIndex.getName();
      final LSMTreeIndex lsm = (LSMTreeIndex) ((LSMTreeIndexMutable) bucketIndex.getComponent()).getMainIndex();
      database.async().waitCompletion();
      assertThat(lsm.getMutableIndex().getTotalPages()).as("the compaction needs at least 2 mutable pages to run")
          .isGreaterThanOrEqualTo(2);
      lsm.scheduleCompaction();
      assertThat(lsm.compact()).isTrue();
      assertThat(lsm.getMostRecentFileName()).isNotEqualTo(nameBefore);

      reopenDatabase();

      final TypeIndex reloaded = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("location").get(0);
      assertThat(reloaded.getIndexesOnBuckets()[0].getName()).isEqualTo(nameBefore);
      assertThat(database.getSchema().getIndexByName(nameBefore).getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
    }

    /** An index that never compacted still answers to its file's name, and its schema entry stays exactly as before. */
    @Test
    void aNeverCompactedIndexWritesNoLogicalName() {
      createAndFill(false);
      assertThat(bucketIndex().getName()).isEqualTo(bucketIndex().getMostRecentFileName());
      assertThat(schemaIndexEntry().has("logicalName")).isFalse();
    }

    @Test
    void aCompactedIndexPersistsItsLogicalName() throws Exception {
      createAndFill(false);
      final String nameBefore = bucketIndex().getName();
      compact();
      assertThat(schemaIndexEntry().getString("logicalName")).isEqualTo(nameBefore);
    }

    private JSONObject schemaIndexEntry() {
      final JSONObject indexes = database.getSchema().getEmbedded().toJSON().getJSONObject("types")
          .getJSONObject(TYPE_NAME).getJSONObject("indexes");
      return indexes.getJSONObject(bucketIndex().getMostRecentFileName());
    }

    private List<String> createAndFill(final boolean unique) {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("key", String.class);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "key" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(unique).withPageSize(PAGE_SIZE).create();
      });

      final List<String> keys = new ArrayList<>(RECORDS * 2);
      insertMore(keys);
      return keys;
    }

    private void insertMore(final List<String> keys) {
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++) {
          final String key = UUID.randomUUID().toString();
          keys.add(key);
          database.newDocument(TYPE_NAME).set("key", key).save();
        }
      });
    }

    private void compact() throws Exception {
      final LSMTreeIndex index = bucketIndex();
      assertThat(index.getMutableIndex().getTotalPages()).as("the compaction needs at least 2 mutable pages to run")
          .isGreaterThanOrEqualTo(2);
      // Kept below the auto-compaction threshold, so nothing else has scheduled it; drained anyway for safety.
      database.async().waitCompletion();
      index.scheduleCompaction();
      assertThat(index.compact()).as("the compaction must actually run").isTrue();
    }

    private void assertIndexServes(final List<String> keys) {
      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("key").get(0);
      assertThat(typeIndex.countEntries()).isEqualTo(keys.size());
      for (int i = 0; i < keys.size(); i += 97)
        assertThat(typeIndex.get(new Object[] { keys.get(i) }).hasNext()).as("key %s", keys.get(i)).isTrue();
    }

    private LSMTreeIndex bucketIndex() {
      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("key").get(0);
      return (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    }
  }

  /**
   * Issue #9434: a MANUAL LSM index (no type) lost the name its creator gave it after a compaction + restart, because the
   * compaction named the new file after the old one's prefix up to the last underscore and the schema had no entry to
   * restore the name from; a name with no underscore made the compaction throw.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#9434 manual index keeps its name across compaction and restart")
  class Issue9434ManualIndexCompaction {
    private static final int PAGE_SIZE = 8192;
    private static final int RECORDS   = 700;

    @Test
    void nameWithUnderscoreSurvivesCompactionAndRestart() throws Exception {
      assertNameSurvives("Manual_Idx");
    }

    @Test
    void nameWithoutUnderscoreCompactsAndSurvivesRestart() throws Exception {
      assertNameSurvives("ManualIdx");
    }

    @Test
    void nameEndingWithDigitsSurvivesCompactionAndRestart() throws Exception {
      assertNameSurvives("Manual_123");
    }

    @Test
    void secondCompactionAfterRestartKeepsTheName() throws Exception {
      final List<String> keys = fill("Manual_Idx");
      compact("Manual_Idx");
      reopenDatabase();
      insert("Manual_Idx", keys);
      compact("Manual_Idx");
      reopenDatabase();

      assertThat(database.getSchema().existsIndex("Manual_Idx")).isTrue();
      assertServes("Manual_Idx", keys);
    }

    private void assertNameSurvives(final String name) throws Exception {
      final List<String> keys = fill(name);
      final LSMTreeIndex before = (LSMTreeIndex) database.getSchema().getIndexByName(name);
      compact(name);
      assertThat(before.getName()).isEqualTo(name);
      assertThat(before.getMostRecentFileName()).isNotEqualTo(name);

      reopenDatabase();

      assertThat(database.getSchema().existsIndex(name)).as("lookup by the name the user gave it").isTrue();
      final LSMTreeIndex after = (LSMTreeIndex) database.getSchema().getIndexByName(name);
      assertThat(after.getName()).isEqualTo(name);
      assertServes(name, keys);
    }

    private List<String> fill(final String name) {
      database.getSchema().buildManualIndex(name, new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();
      final List<String> keys = new ArrayList<>(RECORDS * 2);
      insert(name, keys);
      return keys;
    }

    private void insert(final String name, final List<String> keys) {
      final Index index = database.getSchema().getIndexByName(name);
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++) {
          final String key = UUID.randomUUID().toString();
          keys.add(key);
          index.put(new Object[] { key }, new RID[] { new RID(1, keys.size()) });
        }
      });
    }

    private void compact(final String name) throws Exception {
      final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getIndexByName(name);
      assertThat(index.getMutableIndex().getTotalPages()).as("the compaction needs at least 2 mutable pages to run")
          .isGreaterThanOrEqualTo(2);
      database.async().waitCompletion();
      index.scheduleCompaction();
      assertThat(index.compact()).as("the compaction must actually run").isTrue();
    }

    private void assertServes(final String name, final List<String> keys) {
      final Index index = database.getSchema().getIndexByName(name);
      assertThat(index.countEntries()).isEqualTo(keys.size());
      for (int i = 0; i < keys.size(); i += 97)
        assertThat(index.get(new Object[] { keys.get(i) }).hasNext()).as("key %s", keys.get(i)).isTrue();
    }
  }

  /**
   * Regression test for issue #9717: compacting an LSM index created the new mutable and the new compacted component with the
   * field-default null strategy (SKIP). The schema reads the strategy back from the mutable component, so after the first
   * compaction an ERROR index accepted null keys and an INDEX index was planned as SKIP, in memory and in schema.json.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#9717 compaction keeps the null strategy")
  class Issue9717CompactionNullStrategy {
    private static final String TYPE_NAME = "V9717";
    private static final int    PAGE_SIZE = 2048;
    private static final int    ROWS      = 5_000;

    @Test
    void compactionKeepsTheNullStrategyOfEveryComponent() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("k", Type.INTEGER);
        type.createProperty("f", Type.INTEGER);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
            .withPageSize(PAGE_SIZE).withNullStrategy(NULL_STRATEGY.ERROR).create();
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "f" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
            .withPageSize(PAGE_SIZE).withNullStrategy(NULL_STRATEGY.INDEX).create();
      });

      for (int batch = 0; batch < 5; batch++) {
        final int base = batch * (ROWS / 5);
        database.transaction(() -> {
          for (int i = base; i < base + ROWS / 5; i++) {
            final MutableDocument row = database.newDocument(TYPE_NAME).set("k", i);
            if (i % 10 != 0)
              row.set("f", i % 100);
            row.save();
          }
        });
      }

      compact(TYPE_NAME + "[k]");
      compact(TYPE_NAME + "[f]");

      assertStrategies();
      assertThat(bucketIndex(TYPE_NAME + "[f]").getMutableIndex().getSubIndex())
          .as("the compaction must have produced a compacted sub-index for the test to be meaningful").isNotNull();

      // A null key on the ERROR index is still refused after the compaction
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE_NAME).set("f", 1).save()))
          .rootCause().hasMessageContaining("cannot be NULL");

      // A row moving from null to a value must lose its null entry: with the strategy read as SKIP, the removal of the null key
      // was skipped and the row was then counted twice by a range query
      final AtomicReference<RID> nullRow = new AtomicReference<>();
      database.transaction(() -> {
        final MutableDocument row = database.newDocument(TYPE_NAME).set("k", ROWS + 1);
        row.save();
        nullRow.set(row.getIdentity());
      });
      database.transaction(() -> nullRow.get().asDocument().modify().set("f", 3).save());

      assertThat(countWhere("f < 5")).isEqualTo(countBelowFiveByScan());
      assertThat(countWhere("f is null")).isEqualTo(ROWS / 10);

      reopenDatabase();

      assertStrategies();
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE_NAME).set("f", 1).save()))
          .rootCause().hasMessageContaining("cannot be NULL");
      assertThat(countWhere("f < 5")).isEqualTo(countBelowFiveByScan());
      assertThat(countWhere("f is null")).isEqualTo(ROWS / 10);
    }

    private void assertStrategies() {
      assertThat(database.getSchema().getIndexByName(TYPE_NAME + "[k]").getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);
      assertThat(database.getSchema().getIndexByName(TYPE_NAME + "[f]").getNullStrategy()).isEqualTo(NULL_STRATEGY.INDEX);

      final LSMTreeIndex k = bucketIndex(TYPE_NAME + "[k]");
      assertThat(k.getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);
      if (k.getMutableIndex().getSubIndex() != null)
        assertThat(k.getMutableIndex().getSubIndex().getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);

      final LSMTreeIndex f = bucketIndex(TYPE_NAME + "[f]");
      assertThat(f.getNullStrategy()).isEqualTo(NULL_STRATEGY.INDEX);
      if (f.getMutableIndex().getSubIndex() != null)
        assertThat(f.getMutableIndex().getSubIndex().getNullStrategy()).isEqualTo(NULL_STRATEGY.INDEX);
    }

    private long countWhere(final String condition) {
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + TYPE_NAME + " WHERE " + condition)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private long countBelowFiveByScan() {
      long count = 0;
      try (final ResultSet rs = database.query("sql", "SELECT f FROM " + TYPE_NAME)) {
        while (rs.hasNext()) {
          final Integer f = rs.next().getProperty("f");
          if (f != null && f < 5)
            ++count;
        }
      }
      return count;
    }

    private LSMTreeIndex bucketIndex(final String indexName) {
      for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
        if (bucketIndex instanceof LSMTreeIndex lsmIndex)
          return lsmIndex;
      throw new IllegalStateException("No LSM bucket index found for " + indexName);
    }

    private void compact(final String indexName) {
      database.async().waitCompletion();
      for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
        try {
          for (int attempt = 0; attempt < 200 && !bucketIndex.scheduleCompaction(); ++attempt)
            Thread.sleep(25); // a background compaction owns the status; let it finish
          bucketIndex.compact();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
    }
  }

  /**
   * Issue #10188: a NOTUNIQUE LSM-tree index ended up with extra entries when documents were updated while the engine compacted
   * the index in the background. {@code TypeIndex.iterator()}, {@code countEntries()} and {@code SELECT k FROM T ORDER BY k} (which
   * reads the index in key order) returned more rows than the type has - most of the extra ones with a key the record no longer
   * holds - while the point lookups ({@code WHERE k = ?}, {@code TypeIndex.get(k)}) stayed right.
   * <p>
   * One thread, only updates, a compaction every two pages of the mutable index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#10188 no stale entries after background compaction")
  class Issue10188StaleEntriesAfterBackgroundCompaction {
    private static final int KEYS      = 50;
    private static final int DOCUMENTS = 50_000;
    private static final int PAGE_SIZE = 16_384;

    private final Object[] saved = new Object[2];

    @BeforeEach
    void configure() {
      saved[0] = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValue();
      saved[1] = GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.getValue();
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(2);
      GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.setValue(PAGE_SIZE);
    }

    @AfterEach
    void restore() {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(saved[0]);
      GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.setValue(saved[1]);
    }

    @Tag("slow")
    @Test
    void theIndexHoldsOneEntryPerRecordAfterUpdatesDuringBackgroundCompaction() throws InterruptedException {
      database.getSchema().createDocumentType("T").createProperty("k", Type.INTEGER);
      database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");

      final Random random = new Random(1);
      final RID[] rids = new RID[DOCUMENTS];
      database.transaction(() -> {
        for (int i = 0; i < DOCUMENTS; i++)
          rids[i] = database.newDocument("T").set("k", random.nextInt(KEYS)).save().getIdentity();
      });

      final long end = System.nanoTime() + 8_000_000_000L;
      while (System.nanoTime() < end) {
        database.begin();
        final RID rid = rids[random.nextInt(rids.length)];
        database.lookupByRID(rid, true).asDocument().modify().set("k", random.nextInt(KEYS)).save();
        database.commit();
      }
      // The compaction scheduled by the last commits runs in the background
      Thread.sleep(3_000);

      assertTheIndexMatchesTheRecords();
    }

    private void assertTheIndexMatchesTheRecords() {
      final Map<Long, Integer> keyOfRecord = new HashMap<>();
      final Iterator<Record> records = database.iterateType("T", true);
      while (records.hasNext()) {
        final Document document = records.next().asDocument();
        keyOfRecord.put(document.getIdentity().getPosition(), ((Number) document.get("k")).intValue());
      }

      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("T[k]");
      long entries = 0, stale = 0, duplicates = 0, orphans = 0;
      final Set<String> seen = new HashSet<>();
      for (final IndexCursor cursor = index.iterator(true); cursor.hasNext(); ) {
        final Identifiable rid = cursor.next();
        final Object[] keys = cursor.getKeys();
        ++entries;
        final Integer key = keyOfRecord.get(rid.getIdentity().getPosition());
        if (key == null)
          ++orphans;
        else if (key.intValue() != ((Number) keys[0]).intValue())
          ++stale;
        else if (!seen.add(rid.getIdentity() + ":" + key))
          ++duplicates;
      }

      long ordered = 0;
      try (final ResultSet resultSet = database.query("sql", "SELECT k FROM T ORDER BY k")) {
        while (resultSet.hasNext()) {
          resultSet.next();
          ++ordered;
        }
      }

      long byKey = 0, found = 0;
      for (int k = 0; k < KEYS; k++) {
        try (final ResultSet resultSet = database.query("sql", "SELECT k FROM T WHERE k = ?", k)) {
          while (resultSet.hasNext()) {
            resultSet.next();
            ++byKey;
          }
        }
        for (final IndexCursor cursor = index.get(new Object[] { k }); cursor.hasNext(); ) {
          cursor.next();
          ++found;
        }
      }

      final String state = "records " + keyOfRecord.size() + ", index iterated " + entries + " (stale key " + stale + ", duplicate "
          + duplicates + ", no record " + orphans + "), countEntries() " + index.countEntries() + ", ORDER BY rows " + ordered
          + ", WHERE k = ? rows " + byKey + ", TypeIndex.get rows " + found;
      assertThat(entries).as(state).isEqualTo(keyOfRecord.size());
      assertThat(index.countEntries()).as(state).isEqualTo(keyOfRecord.size());
      assertThat(ordered).as(state).isEqualTo(keyOfRecord.size());
      assertThat(byKey).as(state).isEqualTo(keyOfRecord.size());
      assertThat(found).as(state).isEqualTo(keyOfRecord.size());
    }
  }
}
