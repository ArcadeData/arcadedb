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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for UNIQUE (and composite) index behavior, one nested class per issue.
 */
class UniqueIndexTest extends TestHelper {
  /**
   * Contract guard for #5119: a {@code NOTUNIQUE} composite index on (word, lang) must return the same number of records
   * as a full scan for a key that has many duplicate entries, including when that single key's RID list overflows across
   * several compacted leaf pages (each indexed by its own root entry sharing the key).
   * <p>
   * The reporter observed the loss on 26.7.1, whose root cause is the compaction-atomicity bug fixed by #4946 (commit
   * b67973388, in 26.7.2): a failed compaction round left orphan leaf pages counted in the page series, and the next
   * successful round republished them, producing a malformed compacted series that the positional read logic walked
   * wrong - dropping live NOTUNIQUE duplicates. Heavy-duplicate keys make it easier to hit because they inflate the
   * compacted leaf-page count. The internal invariants of that fix are asserted by {@link LSMTreeCompactionAtomicityTest}
   * and the end-user failure sequence by {@link IndexCompactionRegressionTest.Issue5120CompactionDuplicates}; this test locks down the specific
   * composite-key, split-across-pages read path from the issue so a future regression there is caught.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5119 NOTUNIQUE composite key duplicates agree with a full scan")
  class Issue5119DuplicateCompositeKey {

    private static final String TYPE_NAME = "Tok";
    private static final int    DUPLICATES = 1000;

    private void createSchema() {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("word", String.class);
      type.createProperty("lang", String.class);
      type.createProperty("ref", String.class);
    }

    private void insertData() {
      database.transaction(() -> {
        // 1000 rows sharing the same (word, lang) key
        for (int i = 0; i < DUPLICATES; i++)
          database.newDocument(TYPE_NAME).set("word", "dup").set("lang", "xx").set("ref", String.format("r%04d", i)).save();

        // plus thousands of rows with distinct keys
        for (int i = 0; i < 5000; i++)
          database.newDocument(TYPE_NAME).set("word", "w" + i).set("lang", "xx").set("ref", String.format("d%04d", i)).save();
      });
    }

    private void createIndex() {
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "word", "lang" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    }

    private long scanCount() {
      final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word.trim() = 'dup' AND lang = 'xx'");
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
    }

    private long indexCount() {
      final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word = 'dup' AND lang = 'xx'");
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
    }

    @Test
    void indexAgreesWithScanRightAfterBuild() {
      createSchema();
      insertData();
      createIndex();

      assertThat(scanCount()).as("full scan sees all duplicates").isEqualTo(DUPLICATES);
      assertThat(indexCount()).as("index lookup must see all duplicates").isEqualTo(DUPLICATES);
    }

    @Test
    void indexAgreesWithScanAfterUnrelatedTransactions() {
      createSchema();
      insertData();
      createIndex();

      // "the loss grows over time as unrelated transactions run" - run many small unrelated transactions.
      for (int t = 0; t < 50; t++) {
        final int base = t;
        database.transaction(() -> {
          for (int i = 0; i < 20; i++)
            database.newDocument(TYPE_NAME).set("word", "u" + base + "_" + i).set("lang", "xx").set("ref", "x").save();
        });
      }

      assertThat(indexCount()).as("index lookup must still see all duplicates after unrelated writes").isEqualTo(DUPLICATES);
    }

    /**
     * Forces a single key's RID list to overflow across many compacted leaf pages (small page size, tens of thousands of
     * duplicates of one key) and then a successful compaction. Isolates any heavy-duplicate defect independent of the
     * #4946 compaction-atomicity fix.
     */
    @Test
    @Tag("slow")
    void heavyDuplicateKeySurvivesCompaction() {
      final int heavyDuplicates = 30_000;

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("word", String.class);
      type.createProperty("lang", String.class);

      // Small pages so one key's RID list overflows across several leaf pages during compaction.
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "word", "lang" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(4096).create();

      final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
          .getIndexesOnBuckets()[0];

      // Insert in batches so the mutable index accumulates many pages before compaction.
      for (int batch = 0; batch < 30; batch++) {
        final int base = batch;
        database.transaction(() -> {
          for (int i = 0; i < heavyDuplicates / 30; i++)
            database.newDocument(TYPE_NAME).set("word", "dup").set("lang", "xx").save();
        });
      }

      // Force a (successful) compaction: many mutable pages -> single-key RID list split across leaf pages.
      try {
        if (index.scheduleCompaction())
          index.compact();
      } catch (final Throwable t) {
        throw new RuntimeException("compaction failed", t);
      }

      // The compaction must actually have split the single "dup" key across several leaf pages, otherwise the
      // multi-root-entry read path this test targets is not exercised.
      assertThat(index.getMutableIndex().getSubIndex()).as("compaction created the sub-index").isNotNull();
      assertThat(index.getMutableIndex().getSubIndex().getTotalPages()).as("single key overflowed across many leaf pages").isGreaterThan(2);

      final ResultSet scan = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word.trim() = 'dup'");
      final long scanCount = scan.hasNext() ? ((Number) scan.next().getProperty("c")).longValue() : 0;

      final ResultSet idx = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word = 'dup' AND lang = 'xx'");
      final long idxCount = idx.hasNext() ? ((Number) idx.next().getProperty("c")).longValue() : 0;

      assertThat(scanCount).as("scan sees all heavy duplicates").isEqualTo(heavyDuplicates);
      assertThat(idxCount).as("index sees all heavy duplicates after compaction").isEqualTo(heavyDuplicates);
    }
  }

  /**
   * Regression guard for #8806: a lookup by a key PREFIX on a UNIQUE composite index built by many commits returned only part of
   * the matching entries. The unindexed scan over the same rows, and a bounded range over the whole key, were right.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#8806 prefix lookup on a UNIQUE composite index returns every entry")
  class Issue8806UniqueCompositePrefixLookup {
    private static final int HOSTS  = 10;
    private static final int POINTS = 30_000;
    private static final int COMMIT = 1_000;

    private void load(final String unique) throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Point");
      database.command("sql", "CREATE PROPERTY Point.host STRING");
      database.command("sql", "CREATE PROPERTY Point.ts LONG");
      database.command("sql", "CREATE INDEX ON Point (host, ts) " + unique);
      final int total = HOSTS * POINTS;
      database.begin();
      for (int i = 0; i < total; i++) {
        database.newDocument("Point").set("host", "host_" + (i % HOSTS), "ts", (long) (i / HOSTS) * 10).save();
        if ((i + 1) % COMMIT == 0) {
          database.commit();
          database.begin();
        }
      }
      database.commit();
      compactAndAssertSeries("Point[host,ts]");
    }

    /** The defect lives in the compacted series: compact now rather than wait for the background compaction. */
    private void compactAndAssertSeries(final String indexName) throws Exception {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
      for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
        bucketIndex.compact();
      assertThat(typeIndex.getIndexesOnBuckets()).anySatisfy(i -> assertThat(((LSMTreeIndex) i).getMutableIndex().getSubIndex()).isNotNull());
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private void check() {
      assertThat(count("SELECT count(*) AS c FROM Point WHERE host = 'host_7'")).isEqualTo(POINTS);
      assertThat(count("SELECT count(*) AS c FROM Point WHERE host.toUpperCase() = 'HOST_7'")).isEqualTo(POINTS);

      try (final ResultSet rs = database.query("sql", "SELECT ts FROM Point WHERE host = 'host_7' ORDER BY ts ASC LIMIT 1")) {
        assertThat(rs.next().<Number>getProperty("ts").longValue()).isZero();
      }

      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Point[host,ts]");
      for (final boolean ascending : new boolean[] { true, false }) {
        final IndexCursor cursor = index.range(ascending, new Object[] { "host_7" }, true, new Object[] { "host_7" }, true);
        long n = 0;
        while (cursor.hasNext()) {
          cursor.next();
          n++;
        }
        assertThat(n).as("range ascending=" + ascending).isEqualTo(POINTS);
      }
    }

    @Tag("slow")
    @Test
    void uniquePrefixLookupReturnsEveryEntry() throws Exception {
      load("UNIQUE");
      check();
    }

    @Tag("slow")
    @Test
    void uniquePrefixLookupAfterReopen() throws Exception {
      load("UNIQUE");
      reopenDatabase();
      check();
    }

    @Tag("slow")
    @Test
    void notUniquePrefixLookupReturnsEveryEntry() throws Exception {
      load("NOTUNIQUE");
      check();
    }

    @Tag("slow")
    @Test
    void uniqueFirstPropertyOfThreePropertyIndex() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE T3");
      database.command("sql", "CREATE PROPERTY T3.a INTEGER");
      database.command("sql", "CREATE PROPERTY T3.b INTEGER");
      database.command("sql", "CREATE PROPERTY T3.c INTEGER");
      database.command("sql", "CREATE INDEX ON T3 (a, b, c) UNIQUE");
      database.begin();
      for (int i = 0; i < 300_000; i++) {
        database.newDocument("T3").set("a", i % 10, "b", (i / 10) % 10, "c", i / 100).save();
        if ((i + 1) % COMMIT == 0) {
          database.commit();
          database.begin();
        }
      }
      database.commit();
      compactAndAssertSeries("T3[a,b,c]");
      assertThat(count("SELECT count(*) AS c FROM T3 WHERE a = 3")).isEqualTo(30_000);
      assertThat(count("SELECT count(*) AS c FROM T3 WHERE a = 3 AND b = 4")).isEqualTo(3_000);
    }
  }

  /**
   * Regression test for issue #8961: on a UNIQUE index, {@code REMOVE A} then {@code ADD B} collapse into
   * {@code REPLACE B (oldRid = A)}, and the next {@code REMOVE B} used to drop {@code oldRid}, so nothing removed A's
   * committed entry and a following {@code ADD C} failed the uniqueness check against the deleted A.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#8961 UNIQUE key deleted and re-inserted twice in one transaction")
  class Issue8961UniqueKeyDeleteTwice {

    @BeforeEach
    void createType() {
      database.command("sql", "CREATE DOCUMENT TYPE Item");
      database.command("sql", "CREATE PROPERTY Item.code STRING");
      database.command("sql", "CREATE INDEX ON Item (code) UNIQUE");
      database.transaction(() -> {
        database.newDocument("Item").set("code", "x").save();
        database.newDocument("Item").set("code", "y").save();
      });
    }

    @Test
    void deleteInsertDeleteInsertSameKeyCommits() {
      database.transaction(() -> {
        database.query("sql", "SELECT FROM Item WHERE code = 'x'").next().getRecord().get().asDocument().delete();
        final MutableDocument b = database.newDocument("Item").set("code", "x").save();
        b.delete();
        database.newDocument("Item").set("code", "x").save();
      });

      assertThat(database.query("sql", "SELECT FROM Item WHERE code = 'x'").stream().count()).isEqualTo(1);
      final IndexCursor cursor = database.getSchema().getIndexByName("Item[code]").get(new Object[] { "x" });
      assertThat(cursor.estimateSize()).isEqualTo(1);
      assertThat(database.existsRecord(cursor.next().getIdentity())).isTrue();
    }

    @Test
    void deleteInsertDeleteLeavesNoIndexEntry() {
      database.transaction(() -> {
        database.query("sql", "SELECT FROM Item WHERE code = 'y'").next().getRecord().get().asDocument().delete();
        final MutableDocument b = database.newDocument("Item").set("code", "y").save();
        b.delete();
      });

      final IndexCursor cursor = database.getSchema().getIndexByName("Item[code]").get(new Object[] { "y" });
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        assertThat(database.existsRecord(rid)).as("stale index entry " + rid).isTrue();
      }
      assertThat(database.getSchema().getIndexByName("Item[code]").get(new Object[] { "y" }).estimateSize()).isZero();
    }

    @Test
    void rollbackAfterTheSequenceLeavesTheCommittedEntryUntouched() {
      try {
        database.transaction(() -> {
          database.query("sql", "SELECT FROM Item WHERE code = 'x'").next().getRecord().get().asDocument().delete();
          final MutableDocument b = database.newDocument("Item").set("code", "x").save();
          b.delete();
          database.newDocument("Item").set("code", "x").save();
          throw new IllegalStateException("rollback");
        });
      } catch (final IllegalStateException expected) {
        // rolled back
      }

      assertThat(database.query("sql", "SELECT FROM Item WHERE code = 'x'").stream().count()).isEqualTo(1);
      final IndexCursor cursor = database.getSchema().getIndexByName("Item[code]").get(new Object[] { "x" });
      assertThat(cursor.estimateSize()).isEqualTo(1);
      assertThat(cursor.next().getIdentity().toString()).isEqualTo("#1:0");
    }

    @Test
    void longerChainEndingInADeleteLeavesNoEntry() {
      database.transaction(() -> {
        database.query("sql", "SELECT FROM Item WHERE code = 'x'").next().getRecord().get().asDocument().delete();
        final MutableDocument b = database.newDocument("Item").set("code", "x").save();
        b.delete();
        final MutableDocument c = database.newDocument("Item").set("code", "x").save();
        c.delete();
      });

      assertThat(database.getSchema().getIndexByName("Item[code]").get(new Object[] { "x" }).estimateSize()).isZero();
    }
  }

  /**
   * Issue #10265: on a UNIQUE index a transaction that took a key away from its committed holder and then gave it to
   * TWO records committed both. The first new claim was a {@code REPLACE} (a same-key REMOVE + ADD merged into one
   * entry), and the in-transaction duplicate check only treated a pending {@code ADD} as a claim, so the second ADD
   * displaced the first in the per-key map: two records held the key and the index kept one of them.
   * <p>
   * The comment cases H and I: a key claimed and withdrawn by the same uncommitted record left a pending {@code REMOVE}
   * of that record, which a read of a unique index treats as removing the whole key, hiding the committed holder from
   * the transaction's own lookups until commit.
   */
  @Nested
  @DisplayName("#10265 UNIQUE key moved to two records in one transaction")
  class Issue10265UniqueKeyMovedToTwoRecords {
    private static final AtomicInteger TYPES = new AtomicInteger();

    private String type;
    private RID    x;
    private RID    y;

    /** A fresh single-bucket type holding X (k = 1) and, when asked, Y (k = 2). */
    private void fresh(final String kind, final int buckets, final boolean withY) {
      type = "T" + TYPES.incrementAndGet();
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
      database.command("sql", "CREATE INDEX ON " + type + " (k) " + kind);
      database.transaction(() -> {
        x = insert(1, 0);
        if (withY)
          y = insert(2, 0);
      });
    }

    private String bucket(final int i) {
      return database.getSchema().getType(type).getBuckets(false).get(i).getName();
    }

    private RID insert(final Object k, final int bucket) {
      final MutableDocument d = database.newDocument(type).set("k", k);
      d.save(bucket(bucket));
      return d.getIdentity();
    }

    private RID insert(final Object k) {
      return insert(k, 0);
    }

    private void set(final RID rid, final Object k) {
      rid.asDocument().modify().set("k", k).save();
    }

    private int holders(final int k) {
      int holders = 0;
      for (final Iterator<Record> it = database.iterateType(type, true); it.hasNext(); ) {
        final Document d = it.next().asDocument();
        if (Objects.equals(d.get("k"), k))
          ++holders;
      }
      return holders;
    }

    private int indexGet(final int k) {
      int found = 0;
      final IndexCursor c = database.getSchema().getIndexByName(type + "[k]").get(new Object[] { k });
      while (c.hasNext()) {
        c.next();
        ++found;
      }
      return found;
    }

    private long sqlCount(final int k) {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE k = " + k)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    /** The records holding k, what the index answers for it and the SQL count through the index all agree on n. */
    private void assertKeyHeldBy(final int k, final int n) {
      assertThat(holders(k)).as("records holding k=" + k).isEqualTo(n);
      assertThat(indexGet(k)).as("index get(" + k + ")").isEqualTo(n);
      assertThat(sqlCount(k)).as("SELECT count(*) WHERE k = " + k).isEqualTo(n);
    }

    private void assertRefused(final Runnable tx) {
      assertThatThrownBy(() -> database.transaction(tx::run)).isInstanceOf(DuplicatedKeyException.class);
      // refused: the committed holder X still owns the key, and the index still finds it
      assertKeyHeldBy(1, 1);
      assertThat(indexGetRid(1)).isEqualTo(x);
    }

    private RID indexGetRid(final int k) {
      return database.getSchema().getIndexByName(type + "[k]").get(new Object[] { k }).next().getIdentity();
    }

    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void caseCReplacementStillCommits(final String kind) {
      fresh(kind, 1, false);
      database.transaction(() -> {
        x.asDocument().delete();
        insert(1);
      });
      assertKeyHeldBy(1, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void caseDDeleteHolderThenInsertTwiceIsRefused(final String kind) {
      fresh(kind, 1, false);
      assertRefused(() -> {
        x.asDocument().delete();
        insert(1);
        insert(1);
      });
    }

    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void caseEMoveHolderOffThenInsertTwiceIsRefused(final String kind) {
      fresh(kind, 1, false);
      assertRefused(() -> {
        set(x, 2);
        insert(1);
        insert(1);
      });
    }

    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void caseFRewriteOfTheNewHolderThenInsertIsRefused(final String kind) {
      fresh(kind, 1, false);
      assertRefused(() -> {
        x.asDocument().delete();
        final RID n = insert(1);
        set(n, 1);
        insert(1);
      });
    }

    /**
     * G: Z takes k = 1 while X still holds it (legal until commit, X then moves off), then Y moves onto k = 1 while Z
     * holds it - two uncommitted records claiming one key, the same shape as inserting k = 1 twice (case A), which is
     * refused at once. It used to be accepted and to drop Z's claim, so the commit left Z holding 1 and the index empty.
     */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void caseGSecondUncommittedClaimIsRefused(final String kind) {
      fresh(kind, 1, true);
      assertRefused(() -> {
        insert(1);
        set(x, null);
        set(y, 1);
        set(y, 3);
      });
      assertKeyHeldBy(2, 1);
    }

    /** G without the transient duplicate: the moves alone must keep the index on the record that ends with the key. */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void caseGWithoutTheTransientDuplicateCommitsConsistently(final String kind) {
      fresh(kind, 1, true);
      database.transaction(() -> {
        set(x, null);
        insert(1);
        set(y, 3);
      });
      assertKeyHeldBy(1, 1);
      assertKeyHeldBy(3, 1);
      assertKeyHeldBy(2, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void sqlScriptDeleteThenInsertTwiceIsRefused(final String kind) {
      fresh(kind, 1, false);
      assertThatThrownBy(() -> database.command("sqlscript",
          "BEGIN; DELETE FROM " + type + " WHERE k = 1; INSERT INTO " + type + " SET k = 1; INSERT INTO " + type + " SET k = 1; COMMIT;")
          .close()).isInstanceOf(DuplicatedKeyException.class);
      if (database.isTransactionActive())
        database.rollback();
      assertKeyHeldBy(1, 1);
    }

    /** The two new claims land in DIFFERENT buckets, so the duplicate sits in another sub-index's lane. */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void crossBucketDeleteThenInsertTwiceIsRefused(final String kind) {
      fresh(kind, 4, false);
      assertRefused(() -> {
        x.asDocument().delete();
        insert(1, 1);
        insert(1, 2);
      });
    }

    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void crossBucketMoveOffThenInsertTwiceIsRefused(final String kind) {
      fresh(kind, 4, false);
      assertRefused(() -> {
        set(x, 2);
        insert(1, 0);
        insert(1, 3);
      });
    }

    /** Comment case H: insert Y k = 1 (refused at commit), move it to 2: the committed X must stay visible inside the tx. */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void caseHWithdrawnClaimKeepsTheCommittedHolderVisible(final String kind) {
      fresh(kind, 1, false);
      database.transaction(() -> {
        final RID n = insert(1);
        set(n, 2);
        assertKeyHeldBy(1, 1);
        assertThat(indexGetRid(1)).isEqualTo(x);
      });
      assertKeyHeldBy(1, 1);
      assertKeyHeldBy(2, 1);
    }

    /** Comment case I: insert Y k = 1, delete it: the committed X must stay visible inside the tx. */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void caseIInsertedThenDeletedKeepsTheCommittedHolderVisible(final String kind) {
      fresh(kind, 1, false);
      database.transaction(() -> {
        insert(1).asDocument().delete();
        assertKeyHeldBy(1, 1);
        assertThat(indexGetRid(1)).isEqualTo(x);
      });
      assertKeyHeldBy(1, 1);
    }

    /** H then a real replacement: the withdrawn claim must not swallow the removal of X that follows. */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void withdrawnClaimThenReplacementCommits(final String kind) {
      fresh(kind, 1, false);
      database.transaction(() -> {
        final RID n = insert(1);
        n.asDocument().delete();
        x.asDocument().delete();
        insert(1);
      });
      assertKeyHeldBy(1, 1);
    }

    /**
     * The reporter's single-thread workload: random transactions of inserts, key updates and deletes over 16 keys, the
     * index checked against a scan after every transaction, committed or refused. It broke the index in 33 of 40 seeds.
     */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void randomSingleThreadWorkloadKeepsTheIndexInStepWithTheRecords(final String kind) {
      fresh(kind, 2, false);
      for (long seed = 0; seed < 40; seed++) {
        final Random random = new Random(seed);
        for (int t = 0; t < 30; t++) {
          final List<RID> committed = new ArrayList<>();
          for (final Iterator<Record> it = database.iterateType(type, true); it.hasNext(); )
            committed.add(it.next().getIdentity());
          // the same operations on every attempt, should the transaction be retried
          final long txSeed = random.nextLong();
          try {
            database.transaction(() -> {
              final Random ops0 = new Random(txSeed);
              final List<RID> live = new ArrayList<>(committed);
              final int ops = 1 + ops0.nextInt(5);
              for (int o = 0; o < ops; o++) {
                final int op = ops0.nextInt(3);
                if (op == 0 || live.isEmpty())
                  live.add(insert(ops0.nextInt(16), ops0.nextInt(2)));
                else if (op == 1)
                  set(live.get(ops0.nextInt(live.size())), ops0.nextInt(16));
                else
                  live.remove(ops0.nextInt(live.size())).asDocument().delete();
              }
            });
          } catch (final DuplicatedKeyException refused) {
            // refused as a whole: the check below still holds
          }

          final Map<Object, Integer> holders = new HashMap<>();
          for (final Iterator<Record> it = database.iterateType(type, true); it.hasNext(); )
            holders.merge(it.next().asDocument().get("k"), 1, Integer::sum);
          for (int k = 0; k < 16; k++) {
            final int held = holders.getOrDefault(k, 0);
            assertThat(held).as("seed %d tx %d: records holding k=%d", seed, t, k).isLessThanOrEqualTo(1);
            assertThat(indexGet(k)).as("seed %d tx %d: index get(%d)", seed, t, k).isEqualTo(held);
          }
        }
      }
    }

    /**
     * A withdrawn claim in one bucket and the committed holder's removal in another both read as "the key was deleted in
     * this transaction": the replacement in a third bucket must still commit, whichever removal is looked at first.
     */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void crossBucketWithdrawnClaimThenReplacementCommits(final String kind) {
      fresh(kind, 4, false);
      database.transaction(() -> {
        x.asDocument().delete();
        insert(1, 1).asDocument().delete();
        insert(1, 2);
      });
      assertKeyHeldBy(1, 1);

      fresh(kind, 4, false);
      database.transaction(() -> {
        insert(1, 1).asDocument().delete();
        x.asDocument().delete();
        insert(1, 2);
      });
      assertKeyHeldBy(1, 1);
    }

    /**
     * A removal naming a RID that does not hold the key is a no-op at commit, on HASH as on LSM: before #10265 the HASH bucket
     * ignored the RID of a unique key and dropped the holder's entry with it.
     */
    @ParameterizedTest
    @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
    void removalNamingAnotherRidLeavesTheHolderIndexed(final String kind) {
      fresh(kind, 1, true);
      database.transaction(() -> database.getSchema().getIndexByName(type + "[k]").remove(new Object[] { 1 }, y));
      assertKeyHeldBy(1, 1);
      assertThat(indexGetRid(1)).isEqualTo(x);
    }
  }
}
