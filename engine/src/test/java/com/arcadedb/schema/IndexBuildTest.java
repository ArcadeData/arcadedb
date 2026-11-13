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
package com.arcadedb.schema;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Regression tests, one nested class per fixed issue.
 */
class IndexBuildTest {

  /**
   * Regression for #10093: a sub type of an indexed type holds sub-indexes on its own buckets, and which type they are registered under was
   * not the same on a database that had just created them and on one that loaded the schema. The creation registers each one with the
   * type that OWNS the bucket (the sub type), under the wrapper of the type that declares the index. The load registered it with the
   * type whose {@code indexes} section listed it - the declaring type - so after any restart the super type held the sub type's
   * bucket indexes and the sub type held none. Under Raft HA the leader keeps what it created and every follower loads what it is
   * sent, so the comparison that ends every HA test reported the two as configured differently, and a node's layout depended on
   * whether it had restarted since.
   * <p>
   * The tests read the registration itself ({@code bucketIndexesByBucket}), which the schema JSON does not carry.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class InheritedSubIndexOwner extends TestHelper {

    /** The registration of every type: its name, then the bucket id and the sub-indexes registered under that bucket. */
    private Map<String, Map<Integer, List<String>>> registrations() {
      final Map<String, Map<Integer, List<String>>> result = new TreeMap<>();
      for (final DocumentType type : database.getSchema().getTypes()) {
        final Map<Integer, List<String>> byBucket = new TreeMap<>();
        for (final Map.Entry<Integer, List<IndexInternal>> entry : ((LocalDocumentType) type).bucketIndexesByBucket.entrySet()) {
          final List<String> names = new ArrayList<>();
          for (final IndexInternal index : entry.getValue())
            names.add(index.getName());
          // The order sub-indexes of one bucket were registered in is not part of the layout
          names.sort(null);
          byBucket.put(entry.getKey(), names);
        }
        result.put(type.getName(), byBucket);
      }
      return result;
    }

    private void createIndexedTypeWithSubType() {
      database.command("sql", "CREATE VERTEX TYPE Base");
      database.command("sql", "CREATE PROPERTY Base.id LONG");
      database.command("sql", "CREATE INDEX ON Base (id) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE Sub EXTENDS Base");
    }

    @Test
    void aSubIndexIsRegisteredWithTheTypeThatOwnsItsBucket() {
      createIndexedTypeWithSubType();

      final LocalDocumentType base = (LocalDocumentType) database.getSchema().getType("Base");
      final LocalDocumentType sub = (LocalDocumentType) database.getSchema().getType("Sub");
      assertThat(sub.getBuckets(false)).isNotEmpty();

      for (final Bucket bucket : sub.getBuckets(false)) {
        assertThat(sub.bucketIndexesByBucket).as("Sub owns bucket %s", bucket.getName()).containsKey(bucket.getFileId());
        assertThat(base.bucketIndexesByBucket).as("Base does not own bucket %s", bucket.getName())
            .doesNotContainKey(bucket.getFileId());
      }
    }

    @Test
    void theRegistrationIsTheSameAfterTheSchemaIsLoaded() {
      createIndexedTypeWithSubType();
      final Map<String, Map<Integer, List<String>>> created = registrations();

      reopenDatabase();

      assertThat(registrations()).as("what the database registered when it created the sub-indexes, against what it loads")
          .isEqualTo(created);
    }

    @Test
    void theWrapperOfTheDeclaringTypeStillCoversEverySubIndex() {
      createIndexedTypeWithSubType();
      reopenDatabase();

      final TypeIndex wrapper = database.getSchema().getType("Base").getIndexByProperties("id");
      assertThat(wrapper).isNotNull();
      final List<Integer> covered = new ArrayList<>();
      for (final Index index : wrapper.getIndexesOnBuckets())
        covered.add(index.getAssociatedBucketId());
      assertThat(covered).containsExactlyInAnyOrderElementsOf(database.getSchema().getType("Base").getBucketIds(true));

      // The sub type has no wrapper of its own: its sub-indexes belong to the one of Base
      assertThat(database.getSchema().getType("Sub").getIndexByProperties("id")).isNull();
      for (final IndexInternal index : wrapper.getIndexesOnBuckets())
        assertThat(index.getTypeIndex()).isSameAs(wrapper);
    }

    /** The index is declared on the root; the bucket of the grandchild is owned by the grandchild, not by the root or the middle. */
    @Test
    void aMultiLevelHierarchyBindsEverySubIndexToTheOwnerOfItsBucketOnCreationAndOnLoad() {
      database.command("sql", "CREATE VERTEX TYPE Root");
      database.command("sql", "CREATE PROPERTY Root.id LONG");
      database.command("sql", "CREATE INDEX ON Root (id) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE Middle EXTENDS Root");
      database.command("sql", "CREATE VERTEX TYPE Leaf EXTENDS Middle");
      final Map<String, Map<Integer, List<String>>> created = registrations();

      assertOwnsOnlyItsOwnBuckets("Root", "Middle", "Leaf");

      reopenDatabase();

      assertThat(registrations()).isEqualTo(created);
      assertOwnsOnlyItsOwnBuckets("Root", "Middle", "Leaf");
      database.transaction(() -> database.newVertex("Leaf").set("id", 11L).save());
      assertThat(database.query("sql", "SELECT FROM Root WHERE id = 11").stream().count()).isEqualTo(1L);
    }

    /** A type below two indexed types carries a sub-index of each on every bucket of its own. */
    @Test
    void multipleInheritanceBindsBothSubIndexesToTheOwnerOfTheBucketOnCreationAndOnLoad() {
      database.command("sql", "CREATE VERTEX TYPE Left");
      database.command("sql", "CREATE PROPERTY Left.id LONG");
      database.command("sql", "CREATE INDEX ON Left (id) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE Right");
      database.command("sql", "CREATE PROPERTY Right.code STRING");
      database.command("sql", "CREATE INDEX ON Right (code) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE Both EXTENDS Left, Right");
      final Map<String, Map<Integer, List<String>>> created = registrations();

      final LocalDocumentType both = (LocalDocumentType) database.getSchema().getType("Both");
      for (final Bucket bucket : both.getBuckets(false))
        assertThat(both.bucketIndexesByBucket.get(bucket.getFileId())).as("Both owns a sub-index of each parent on %s", bucket.getName())
            .hasSize(2);

      reopenDatabase();

      assertThat(registrations()).isEqualTo(created);
      database.transaction(() -> database.newVertex("Both").set("id", 5L).set("code", "five").save());
      assertThat(database.query("sql", "SELECT FROM Left WHERE id = 5").stream().count()).isEqualTo(1L);
      assertThat(database.query("sql", "SELECT FROM Right WHERE code = 'five'").stream().count()).isEqualTo(1L);
    }

    /** The files of a compacted sub-index no longer carry the name the schema first gave them, and are found again by the load. */
    @Test
    void aCompactedSubIndexIsBoundToTheOwnerOfItsBucketAfterTheLoad() throws Exception {
      createIndexedTypeWithSubType();
      database.transaction(() -> {
        for (int i = 0; i < 2_000; ++i)
          database.newVertex("Sub").set("id", (long) i).save();
      });
      final TypeIndex wrapper = database.getSchema().getType("Base").getIndexByProperties("id");
      wrapper.scheduleCompaction();
      wrapper.compact();

      reopenDatabase();

      final LocalDocumentType base = (LocalDocumentType) database.getSchema().getType("Base");
      final LocalDocumentType sub = (LocalDocumentType) database.getSchema().getType("Sub");
      for (final Bucket bucket : sub.getBuckets(false)) {
        assertThat(sub.bucketIndexesByBucket).containsKey(bucket.getFileId());
        assertThat(base.bucketIndexesByBucket).doesNotContainKey(bucket.getFileId());
      }
      assertThat(database.query("sql", "SELECT FROM Base WHERE id = 1999").stream().count()).isEqualTo(1L);
    }

    private void assertOwnsOnlyItsOwnBuckets(final String... typeNames) {
      for (final String typeName : typeNames) {
        final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
        final List<Integer> own = new ArrayList<>();
        for (final Bucket bucket : type.getBuckets(false))
          own.add(bucket.getFileId());
        assertThat(type.bucketIndexesByBucket.keySet()).as("%s registers sub-indexes on its own buckets and no others", typeName)
            .containsExactlyInAnyOrderElementsOf(own);
      }
    }

    @Test
    void aRecordOfTheSubTypeIsIndexedAfterTheSchemaIsLoaded() {
      createIndexedTypeWithSubType();
      reopenDatabase();

      database.transaction(() -> database.newVertex("Sub").set("id", 7L).save());

      assertThat(database.query("sql", "SELECT FROM Base WHERE id = 7").stream().count()).isEqualTo(1L);
      assertThat(database.getSchema().getType("Base").getIndexByProperties("id").countEntries()).isEqualTo(1L);
    }
  }

  /**
   * Regression for #10267: {@code CREATE INDEX ... NOTUNIQUE} over a populated type took the record-at-a-time build whatever the size, and
   * 23% of its time went to the transaction bookkeeping of every key (the lock on the key, the ordered set of keys of the
   * transaction, the commit of them), which a bulk build over existing records does not need. The sorted build skips it and, when the
   * keys fit its memory budget, takes about two thirds of the time. A NOTUNIQUE LSM_TREE index over a type holding at least
   * {@link GlobalConfiguration#INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS} records now takes it, under the same conditions as the UNIQUE
   * one of #10334, and gives the same index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NonUniqueSortedBuildChoice extends TestHelper {
    private static final int ROWS = 6_000;

    private final List<SortedIndexBuildMetrics> sortedBuilds = new ArrayList<>();

    @BeforeEach
    void captureSortedBuilds() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(sortedBuilds::add);
    }

    @AfterEach
    void releaseHook() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    @Test
    void typeAtTheThresholdTakesTheSortedBuildAndGivesTheIndexOfTheDefaultOne() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
      createPopulatedType("Sorted");
      createPopulatedType("Plain");

      database.command("sql", "CREATE INDEX ON Sorted (k) NOTUNIQUE").close();
      assertThat(sortedBuilds).hasSize(1);
      assertThat(sortedBuilds.getFirst().unique()).isFalse();
      assertThat(sortedBuilds.getFirst().logicalEntries()).isEqualTo(ROWS);

      // The same data, the same statement, the record-at-a-time build: the threshold is what tells them apart
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, 0L);
      database.command("sql", "CREATE INDEX ON Plain (k) NOTUNIQUE").close();
      assertThat(sortedBuilds).hasSize(1);

      final List<String> sorted = entries(database.getSchema().getType("Sorted").getIndexByProperties("k"));
      final List<String> plain = entries(database.getSchema().getType("Plain").getIndexByProperties("k"));
      assertThat(sorted).hasSize(ROWS);
      // The order of the records of ONE key is not part of what an index promises, the order of the keys and the set of entries are
      assertThat(keysOnly(sorted)).as("key order").isEqualTo(keysOnly(plain));
      assertThat(sorted.stream().sorted().toList()).as("entries").isEqualTo(plain.stream().sorted().toList());

      reopenDatabase();

      final List<String> reopened = entries(database.getSchema().getType("Sorted").getIndexByProperties("k"));
      assertThat(keysOnly(reopened)).isEqualTo(keysOnly(plain));
      assertThat(reopened.stream().sorted().toList()).isEqualTo(plain.stream().sorted().toList());
      database.transaction(() -> database.newDocument("Sorted").set("k", "after-reopen").save());
      assertThat(count(database.getSchema().getType("Sorted").getIndexByProperties("k").get(new Object[] { "after-reopen" })))
          .isEqualTo(1);
    }

    @Test
    void typeBelowTheThresholdKeepsTheRecordAtATimeBuild() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, ROWS + 1L);
      createPopulatedType("Work");

      database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();

      assertThat(sortedBuilds).isEmpty();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("k").countEntries()).isEqualTo(ROWS);
    }

    @Test
    void thresholdZeroDisablesTheSortedBuild() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, 0L);
      createPopulatedType("Work");

      database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();

      assertThat(sortedBuilds).isEmpty();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("k").countEntries()).isEqualTo(ROWS);
    }

    /** The two thresholds are independent: the one of UNIQUE indexes does not switch a NOTUNIQUE build, nor the other way round. */
    @Test
    void eachKindOfIndexFollowsItsOwnThreshold() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1L);
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, 0L);
      createPopulatedType("Work");
      database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();
      assertThat(sortedBuilds).isEmpty();

      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 0L);
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, 1L);
      final DocumentType other = database.getSchema().buildDocumentType().withName("Other").withTotalBuckets(1).create();
      other.createProperty("k", Type.STRING);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newDocument("Other").set("k", "distinct-%06d".formatted(i)).save();
      });
      database.command("sql", "CREATE INDEX ON Other (k) UNIQUE").close();
      assertThat(sortedBuilds).isEmpty();
    }

    @Test
    void recordsOfSubTypesCountTowardsTheThreshold() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
      database.getSchema().buildDocumentType().withName("Work").withTotalBuckets(1).create().createProperty("k", Type.STRING);
      database.getSchema().buildDocumentType().withName("Article").withSuperType("Work").withTotalBuckets(1).create();
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newDocument(i % 2 == 0 ? "Work" : "Article").set("k", key(i)).save();
      });

      database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();

      assertThat(sortedBuilds).hasSize(1);
      assertThat(database.getSchema().getType("Work").getIndexByProperties("k").countEntries()).isEqualTo(ROWS);
    }

    /** The shape of an HTTP command: CREATE INDEX inside a transaction that has written nothing yet is handed back usable. */
    @Test
    void emptyCallerTransactionIsHandedBackUsable() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
      createPopulatedType("Work");

      database.begin();
      database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();
      assertThat(sortedBuilds).hasSize(1);
      assertThat(database.isTransactionActive()).isTrue();
      database.newDocument("Work").set("k", "after-index").save();
      database.commit();

      final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("k");
      assertThat(count(index.get(new Object[] { "after-index" }))).isEqualTo(1);
      assertThat(index.countEntries()).isEqualTo(ROWS + 1L);
    }

    /** The sorted build cannot share the caller's transaction: the build has to see what that transaction wrote. */
    @Test
    void buildInsideATransactionWithPendingChangesKeepsTheRecordAtATimeBuild() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
      createPopulatedType("Work");

      database.transaction(() -> {
        database.newDocument("Work").set("k", "uncommitted").save();
        database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();
      });

      assertThat(sortedBuilds).isEmpty();
      final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("k");
      assertThat(index.countEntries()).isEqualTo(ROWS + 1L);
      assertThat(count(index.get(new Object[] { "uncommitted" }))).isEqualTo(1);
    }

    /** Repeated keys, many records on each: the shape of a low-cardinality column, the one the report measured. */
    @Test
    void lowCardinalityColumnKeepsEveryRecordOfEveryKey() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
      final DocumentType type = database.getSchema().buildDocumentType().withName("Lines").withTotalBuckets(3).create();
      type.createProperty("k", Type.STRING);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newDocument("Lines").set("k", "1994-01-%02d".formatted(1 + i % 28)).save();
      });

      database.command("sql", "CREATE INDEX ON Lines (k) NOTUNIQUE").close();

      assertThat(sortedBuilds).hasSize(1);
      final TypeIndex index = database.getSchema().getType("Lines").getIndexByProperties("k");
      assertThat(index.countEntries()).isEqualTo(ROWS);
      for (int day = 1; day <= 28; day++)
        assertThat(count(index.get(new Object[] { "1994-01-%02d".formatted(day) }))).isEqualTo(ROWS / 28 + (day <= ROWS % 28 ? 1 : 0));
    }

    private void createPopulatedType(final String typeName) {
      // One bucket, so two types loaded in the same order give the same record positions
      final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create();
      type.createProperty("k", Type.STRING);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newDocument(typeName).set("k", key(i)).save();
      });
    }

    /** Scrambled, so the keys never arrive sorted, and each one repeated by a handful of records. */
    private static String key(final int i) {
      return "key-%06d".formatted(Math.floorMod(i * 7_919, ROWS / 3));
    }

    private static List<String> keysOnly(final List<String> entries) {
      return entries.stream().map(e -> e.substring(0, e.indexOf('@'))).toList();
    }

    private static List<String> entries(final TypeIndex index) {
      final List<String> out = new ArrayList<>();
      final IndexCursor cursor = index.iterator(true);
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        out.add(Arrays.toString(cursor.getKeys()) + "@" + rid.getPosition());
      }
      return out;
    }

    private static int count(final IndexCursor cursor) {
      int total = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ++total;
      }
      return total;
    }
  }

  /**
   * Regression for #10267: the sorted build sized its runs at a fixed 3,072 bytes per entry, about twenty times what an entry of a short key
   * retains, so a table that fitted the budget many times over was still spilled and merged, and the sorted build gained nothing over
   * the default one (at 6,000,000 rows of TPC-H's {@code lineitem} it spilled three runs with a budget that holds the whole table in
   * one). A run is now sized by what its entries retain, measured from the keys, so a table that fits the budget is built in a single
   * pass, a table that does not is spilled in runs as large as the budget allows, and a key that is repeated or small costs less than
   * one that is long.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SortedBuildRunSizing extends TestHelper {
    private static final int  ROWS            = 50_000;
    private static final long ONE_PASS_BUDGET = 16L << 20;

    private final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();

    @BeforeEach
    void captureSortedBuild() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
    }

    @AfterEach
    void releaseHook() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    /** The shape of the report: ship dates as 'YYYY-MM-DD' strings over a few thousand distinct values, many rows each. */
    @Test
    void aLowCardinalityKeyThatFitsTheBudgetIsBuiltInOnePass() {
      final LocalDate first = LocalDate.of(1992, 1, 2);
      createType("LineItem", 8, Type.STRING);
      insert("LineItem", ROWS * 3, i -> first.plusDays(Math.floorMod(i * 7919L, 2_526L)).toString());

      final TypeIndex index = buildSorted("LineItem", ONE_PASS_BUDGET);

      assertOnePass(ROWS * 3);
      assertThat(index.countEntries()).isEqualTo(ROWS * 3L);
      try (final ResultSet resultSet = database.query("sql",
          "SELECT count() AS c FROM LineItem WHERE k >= '1994-01-01' AND k < '1995-01-01'")) {
        assertThat(resultSet.next().<Number>getProperty("c").longValue()).isEqualTo(expectedInYear(ROWS * 3, first));
      }
    }

    /** Every key distinct: the entries carry their own key, and still a run is measured by what they retain, not by a fixed 3 KB each. */
    @Test
    void aHighCardinalityKeyThatFitsTheBudgetIsBuiltInOnePass() {
      createType("Distinct", 3, Type.STRING);
      insert("Distinct", ROWS, i -> "key-%08d".formatted(Math.floorMod(i * 12_371L, ROWS)));

      final TypeIndex index = buildSorted("Distinct", ONE_PASS_BUDGET);

      // Before: 16 MB / 3,072 bytes = 5,461 entries a run, ten runs and a merge for 50,000 short keys
      assertOnePass(ROWS);
      assertThat(index.countEntries()).isEqualTo(ROWS);
      assertThat(index.get(new Object[] { "key-%08d".formatted(ROWS / 2) }).hasNext()).isTrue();
    }

    /** A table larger than the budget spills, in a handful of runs as large as the budget allows rather than dozens of tiny ones. */
    @Test
    void aTableLargerThanTheBudgetSpillsInFewLargeRuns() {
      createType("Spilled", 3, Type.STRING);
      insert("Spilled", ROWS, i -> "key-%08d".formatted(Math.floorMod(i * 12_371L, ROWS)));

      final TypeIndex index = buildSorted("Spilled", 4L << 20);

      // About 9 MB of entries at 4 MB a run is three runs; at 3,072 bytes an entry the same budget made 1,365 entries a run and 37 runs
      assertThat(captured.get()).isNotNull();
      assertThat(captured.get().initialRuns()).isBetween(2, 5);
      assertThat(index.countEntries()).isEqualTo(ROWS);
      assertThat(index.get(new Object[] { "key-%08d".formatted(17) }).hasNext()).isTrue();
    }

    /** The size of a run follows the size of the keys: the same row count with long keys needs runs where short ones fit in one pass. */
    @Test
    void aLongKeyFillsARunSoonerThanAShortOne() {
      createType("ShortKeys", 3, Type.STRING);
      createType("LongKeys", 3, Type.STRING);
      final String padding = "x".repeat(400);
      insert("ShortKeys", 10_000, i -> "key-%08d".formatted(Math.floorMod(i * 7_919L, 10_000L)));
      insert("LongKeys", 10_000, i -> "key-%08d-".formatted(Math.floorMod(i * 7_919L, 10_000L)) + padding);

      buildSorted("ShortKeys", 4L << 20);
      final int shortRuns = captured.get().initialRuns();
      buildSorted("LongKeys", 4L << 20);
      final int longRuns = captured.get().initialRuns();

      // 10,000 short entries retain about 2.4 MB and fit; 10,000 keys of 400 characters retain about 6.4 MB
      assertThat(shortRuns).isZero();
      assertThat(longRuns).isGreaterThanOrEqualTo(2);
    }

    /**
     * Short keys first, long keys later in the scan: the size of a key is followed as it changes, not averaged over the whole build.
     * An average over every key seen so far lags far behind a late jump and charges the long keys at a fraction of what they take, so
     * the run would grow past the budget.
     */
    @Test
    void keysThatGrowLateInTheScanAreChargedByTheirOwnSize() {
      createType("Growing", 1, Type.STRING);
      final String padding = "x".repeat(400);
      // 4,000 short keys retain about 1 MB and 8,000 keys of 400 characters about 5 MB: more than the 4 MB budget holds
      insert("Growing", 12_000, i -> i < 4_000 ? "key-%08d".formatted(i) : "key-%08d-".formatted(i) + padding);

      final TypeIndex index = buildSorted("Growing", 4L << 20);

      assertThat(captured.get().initialRuns()).isGreaterThanOrEqualTo(2);
      assertThat(index.countEntries()).isEqualTo(12_000);
      assertThat(index.get(new Object[] { "key-%08d-".formatted(11_999) + padding }).hasNext()).isTrue();
    }

    private TypeIndex buildSorted(final String typeName, final long budget) {
      captured.set(null);
      return database.getSchema().buildTypeIndex(typeName, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED).withBuildMemoryBudget(budget).withUnique(false).create();
    }

    private void assertOnePass(final long rows) {
      assertThat(captured.get()).as("the sorted build ran").isNotNull();
      assertThat(captured.get().initialRuns()).as("runs spilled").isZero();
      assertThat(captured.get().totalSpillBytes()).as("bytes spilled").isZero();
      assertThat(captured.get().logicalEntries()).isEqualTo(rows);
    }

    private void createType(final String typeName, final int buckets, final Type keyType) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
      type.createProperty("k", keyType);
    }

    private void insert(final String typeName, final int rows, final LongFunction<String> key) {
      for (int from = 0; from < rows; from += 10_000) {
        final int start = from;
        database.transaction(() -> {
          for (int i = start; i < Math.min(rows, start + 10_000); i++)
            database.newDocument(typeName).set("k", key.apply(i)).save();
        });
      }
    }

    private static long expectedInYear(final int rows, final LocalDate first) {
      long count = 0;
      for (long i = 0; i < rows; i++) {
        final String date = first.plusDays(Math.floorMod(i * 7919L, 2_526L)).toString();
        if (date.compareTo("1994-01-01") >= 0 && date.compareTo("1995-01-01") < 0)
          count++;
      }
      return count;
    }
  }

  /**
   * Regression for #10334: {@code CREATE INDEX ... UNIQUE} on a populated type was quadratic with the default build mode. Every
   * batch of the record-at-a-time build commits through the uniqueness check, which looks each key up in the index being
   * built, and that lookup walks every page of the mutable index - a number of pages that grows with every record added,
   * since nothing compacts an index while it is built. A UNIQUE LSM index over a type holding at least
   * {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS} records is now built by the sorted pipeline (an
   * external sort that refuses a duplicate key in the stream) unless the build cannot take it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UniqueIndexSortedBuild extends TestHelper {
    private final List<SortedIndexBuildMetrics> sortedBuilds = new ArrayList<>();

    @BeforeEach
    void captureSortedBuilds() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(sortedBuilds::add);
    }

    @AfterEach
    void releaseHook() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    @Test
    void uniqueIndexOnPopulatedTypeIsBuiltWithoutWalkingTheIndexPerRecord() {
      // The symptom of the issue, as a count rather than a clock: the record-at-a-time build read 30k/130k/530k index pages
      // for 20k/40k/80k records (each new key looked up in every page written so far), the sorted build reads a few dozen
      final int records = 40_000;
      createPopulatedType("Work", 3, records);

      final PageManager.PPageManagerStats before = PageManager.INSTANCE.getStats();
      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
      final PageManager.PPageManagerStats after = PageManager.INSTANCE.getStats();

      // The counters are JVM-wide, so the bound is deliberately loose: the quadratic build reads ~3x the record count here, the
      // sorted one a few dozen pages, and records/4 sits between them with room for anything else running. Do not tighten it
      final long pageAccesses = (after.cacheHits + after.cacheMiss) - (before.cacheHits + before.cacheMiss);
      assertThat(pageAccesses).as("index pages read by the build of a UNIQUE index over %d records", records)
          .isLessThan(records / 4);

      assertThat(sortedBuilds).hasSize(1);
      assertThat(sortedBuilds.getFirst().unique()).isTrue();

      final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("id");
      assertThat(index.isUnique()).isTrue();
      assertThat(index.countEntries()).isEqualTo(records);
      assertThat(count(index.get(new Object[] { workId(12_345) }))).isEqualTo(1);

      // The index is live and keeps refusing duplicates after the build
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Work").set("id", workId(7)).save()))
          .isInstanceOf(DuplicatedKeyException.class);
      database.transaction(() -> database.newDocument("Work").set("id", "fresh").save());
      assertThat(count(index.get(new Object[] { "fresh" }))).isEqualTo(1);
    }

    @Test
    void duplicateKeyIsRefusedAndLeavesNoIndex() throws IOException {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 3, 5_000);
      database.transaction(() -> database.newDocument("Work").set("id", workId(2_500)).save());

      // Reported as the record-at-a-time build reports it: the index, the key and the record already holding it
      assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close())
          .hasRootCauseInstanceOf(DuplicatedKeyException.class)
          .rootCause().hasMessageContaining("Duplicated key [" + workId(2_500) + "] found on index 'Work[id]' already assigned to record #");

      assertThat(sortedBuilds).isEmpty();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id")).isNull();
      assertThat(database.getSchema().getIndexes()).isEmpty();
      // Nothing of the refused build is left on disk: no index file, no spill run, no recovery marker
      try (final Stream<Path> files = Files.list(Path.of(database.getDatabasePath()))) {
        assertThat(files.map(path -> path.getFileName().toString())
            .filter(name -> name.endsWith("mtidx") || name.endsWith("ctidx") || name.startsWith(".arcadedb-sorted-index-build-")
                || name.startsWith(".arcadedb-index-sort-"))
            .toList()).isEmpty();
      }

      // Refused inside an empty caller transaction, that transaction is handed back active and usable
      database.begin();
      assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close())
          .hasRootCauseInstanceOf(DuplicatedKeyException.class);
      assertThat(database.isTransactionActive()).isTrue();
      database.newDocument("Work").set("id", "after-refusal").save();
      database.commit();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id")).isNull();

      // Fixing the data lets the same statement succeed
      database.transaction(() -> database.command("sql", "DELETE FROM Work WHERE id = ? LIMIT 1", workId(2_500)).close());
      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
      assertThat(sortedBuilds).hasSize(1);
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id").countEntries()).isEqualTo(5_001);
    }

    @Test
    void nullAndCompositeKeysGiveTheSameIndexAsTheRecordAtATimeBuild() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      final DocumentType type = database.getSchema().buildDocumentType().withName("Paper").withTotalBuckets(3).create();
      type.createProperty("id", Type.STRING);
      type.createProperty("a", Type.INTEGER);
      type.createProperty("b", Type.INTEGER);
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          // Every other id is NULL, which a UNIQUE index accepts any number of times; "a" repeats, the (a, b) pair does not
          database.newDocument("Paper").set("id", i % 2 == 0 ? null : workId(i)).set("a", i % 10).set("b", i).save();
      });

      final long[] sorted = buildBoth("Paper");
      assertThat(sortedBuilds).hasSize(2);

      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 0L);
      final long[] recordAtATime = buildBoth("Paper");
      assertThat(sortedBuilds).hasSize(2);

      assertThat(sorted).containsExactly(recordAtATime);

      // A duplicated (a, b) pair is refused by both builds
      database.transaction(() -> database.newDocument("Paper").set("id", "extra").set("a", 7).set("b", 7).save());
      for (final long minRecords : new long[] { 1_000L, 0L }) {
        database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, minRecords);
        assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Paper (a, b) UNIQUE").close())
            .hasRootCauseInstanceOf(DuplicatedKeyException.class);
        assertThat(database.getSchema().getType("Paper").getIndexByProperties("a", "b")).isNull();
      }
    }

    @Test
    void emptyCallerTransactionIsHandedBackUsable() {
      createPopulatedType("Work", 2, 12_000);

      // The shape of an HTTP command: CREATE INDEX inside a transaction that has written nothing yet
      database.begin();
      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
      assertThat(sortedBuilds).hasSize(1);
      assertThat(database.isTransactionActive()).isTrue();

      database.newDocument("Work").set("id", "after-index").save();
      database.commit();

      final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("id");
      assertThat(count(index.get(new Object[] { "after-index" }))).isEqualTo(1);
      assertThat(index.countEntries()).isEqualTo(12_001);

      // ...and the same transaction shape keeps refusing duplicates through the new index
      database.begin();
      database.newDocument("Work").set("id", workId(42)).save();
      assertThatThrownBy(() -> database.commit()).isInstanceOf(DuplicatedKeyException.class);
    }

    @Test
    void pendingChangesInAnOuterTransactionKeepTheRecordAtATimeBuild() {
      createPopulatedType("Work", 2, 12_000);

      // The sorted build sets the WHOLE transaction stack aside, so a nested empty transaction must not hide what the outer one
      // wrote: the scan would miss the outer record, and nothing would add it to the index when the outer transaction commits
      database.begin();
      database.newDocument("Work").set("id", "outer-pending").save();
      database.begin();
      assertThat(((TransactionContext) ((DatabaseInternal) database).getTransaction()).hasChanges()).isFalse();

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Work", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED).withUnique(true).create())
          .hasMessageContaining("Sorted build requires no pending changes");

      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
      assertThat(sortedBuilds).isEmpty();

      database.commit();
      database.commit();
    }

    @Test
    void writerStartedDuringTheSortedBuildCannotSlipADuplicateIn() throws Exception {
      createPopulatedType("Work", 2, 12_000);

      // The sorted build reads outside any transaction, so it relies on nobody committing while it runs. A writer started from
      // inside the scan inserts a key the type already holds: it has to wait for the build and then be refused by the new index.
      // Had it committed during the scan, the duplicate would either fail the build or end up in the index undetected.
      final AtomicReference<Throwable> writerOutcome = new AtomicReference<>();
      final CountDownLatch writerAtInsert = new CountDownLatch(1);
      final Thread[] writer = new Thread[1];
      database.getSchema().buildTypeIndex("Work", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withCallback((document, totalIndexed) -> {
            if (writer[0] == null) {
              writer[0] = new Thread(() -> {
                writerAtInsert.countDown();
                try {
                  database.transaction(() -> database.newDocument("Work").set("id", workId(9_999)).save());
                } catch (final Throwable t) {
                  writerOutcome.set(t);
                }
              });
              writer[0].start();
              // The scan goes on only once the writer is at its insert, so the insert is attempted while the build runs
              try {
                assertThat(writerAtInsert.await(30, TimeUnit.SECONDS)).isTrue();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              }
            }
          }).create();

      assertThat(sortedBuilds).hasSize(1);
      writer[0].join(60_000);
      assertThat(writer[0].isAlive()).isFalse();
      assertThat(writerOutcome.get()).isInstanceOf(DuplicatedKeyException.class);

      final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("id");
      assertThat(index.countEntries()).isEqualTo(12_000);
      assertThat(count(index.get(new Object[] { workId(9_999) }))).isEqualTo(1);
    }

    @Test
    void typeBelowTheThresholdKeepsTheRecordAtATimeBuild() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 2, 999);

      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();

      assertThat(sortedBuilds).isEmpty();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id").countEntries()).isEqualTo(999);
    }

    @Test
    void thresholdZeroDisablesTheSortedBuild() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 0L);
      createPopulatedType("Work", 2, 12_000);

      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();

      assertThat(sortedBuilds).isEmpty();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id").countEntries()).isEqualTo(12_000);
    }

    @Test
    void recordsOfSubTypesCountTowardsTheThreshold() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 1, 600);
      database.getSchema().buildDocumentType().withName("Article").withSuperType("Work").withTotalBuckets(1).create();
      database.transaction(() -> {
        for (int i = 600; i < 1_200; i++)
          database.newDocument("Article").set("id", workId(i)).save();
      });

      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();

      assertThat(sortedBuilds).hasSize(1);
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id").countEntries()).isEqualTo(1_200);
    }

    @Test
    void notUniqueIndexBelowItsOwnThresholdKeepsTheRecordAtATimeBuild() {
      // A NOTUNIQUE build has a threshold of its own (issue #10267), far above the 12,000 records here
      createPopulatedType("Work", 2, 12_000);

      database.command("sql", "CREATE INDEX ON Work (id) NOTUNIQUE").close();

      assertThat(sortedBuilds).isEmpty();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id").countEntries()).isEqualTo(12_000);
    }

    @Test
    void buildInsideATransactionKeepsTheRecordAtATimeBuild() {
      createPopulatedType("Work", 2, 12_000);

      // The sorted build cannot share the caller's transaction: the build has to see what that transaction wrote
      database.transaction(() -> {
        database.newDocument("Work").set("id", "uncommitted").save();
        database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
      });

      assertThat(sortedBuilds).isEmpty();
      final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("id");
      assertThat(index.countEntries()).isEqualTo(12_001);
      assertThat(count(index.get(new Object[] { "uncommitted" }))).isEqualTo(1);
    }

    /** Builds the UNIQUE indexes on Paper.id and Paper(a, b), answers their entry counts and drops them again. */
    private long[] buildBoth(final String typeName) {
      database.command("sql", "CREATE INDEX ON " + typeName + " (id) UNIQUE").close();
      database.command("sql", "CREATE INDEX ON " + typeName + " (a, b) UNIQUE").close();
      final DocumentType type = database.getSchema().getType(typeName);
      final TypeIndex byId = type.getIndexByProperties("id");
      final TypeIndex byPair = type.getIndexByProperties("a", "b");
      final long[] entries = { byId.countEntries(), byPair.countEntries(), count(byPair.get(new Object[] { 7, 1_007 })) };
      database.getSchema().dropIndex(byId.getName());
      database.getSchema().dropIndex(byPair.getName());
      return entries;
    }

    private void createPopulatedType(final String typeName, final int buckets, final int records) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
      type.createProperty("id", Type.STRING);
      for (int from = 0; from < records; from += 10_000) {
        final int first = from;
        database.transaction(() -> {
          for (int i = first; i < Math.min(records, first + 10_000); i++)
            database.newDocument(typeName).set("id", workId(i)).save();
        });
      }
    }

    /** OpenAlex-style ids in a scrambled order, as in the report: the keys never arrive sorted. */
    private static String workId(final int i) {
      return "https://openalex.org/W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
    }

    private static int count(final IndexCursor cursor) {
      int total = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ++total;
      }
      return total;
    }
  }

  /**
   * Regression for #10349 (follow-up of #10334): {@code CREATE INDEX ... UNIQUE} on a populated type stayed quadratic wherever the sorted build
   * cannot run - on a replicated (HA) database, inside a transaction with pending writes to the buckets it indexes. The
   * record-at-a-time build commits every batch through the uniqueness check, which looks each key up in the index being built, and
   * that lookup walks every page of the mutable index: nothing compacts an index while it is built, and a replicated build cannot
   * compact it either.
   * <p>
   * The build now leaves the check to ONE ordered pass over the finished index (the index's own order, across every bucket, so a
   * duplicate in another bucket is found too) when the type holds at least
   * {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS} records. Nobody else can commit while the build holds the
   * database write lock, so the keys of the build are the only ones the index holds until the pass has run.
   * <p>
   * A build the sorted build cannot take is made here by a transaction with a pending record in the type being indexed, the same
   * refusal a replicated database gives (issue #10466: a write to another type no longer is one).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeferredUniqueVerification extends TestHelper {
    private final List<SortedIndexBuildMetrics> sortedBuilds   = new ArrayList<>();
    private       int                           pendingRecords = 0;

    @BeforeEach
    void captureSortedBuilds() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(sortedBuilds::add);
    }

    @AfterEach
    void releaseHook() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    @Test
    void theBuildOfALargeUniqueIndexTheSortedBuildCannotTakeIsNotQuadratic() {
      createPopulatedType("Small", 3, 30_000);
      createPopulatedType("Large", 3, 60_000);

      final long small = pageAccessesOfTheBuild("Small");
      final long large = pageAccessesOfTheBuild("Large");

      // TWICE THE RECORDS: A LINEAR BUILD READS ABOUT TWICE THE PAGES, A QUADRATIC ONE ABOUT FOUR TIMES (30k, 130k and 530k pages for
      // 20k, 40k and 80k records, as measured in #10334). The counters are JVM-wide, hence a ratio and a bound with room to spare
      assertThat(sortedBuilds).isEmpty();
      assertThat((double) large / small).as("pages read for 60k records (%d) over pages read for 30k (%d)", large, small)
          .isLessThan(2.8);

      for (final String typeName : new String[] { "Small", "Large" }) {
        final TypeIndex index = database.getSchema().getType(typeName).getIndexByProperties("id");
        assertThat(index.isUnique()).isTrue();
        // THE RECORDS OF THE TYPE AND THE ONE THE CALLER WROTE, QUEUED ON ITS TRANSACTION AND COMMITTED WITH IT
        assertThat(index.countEntries()).isEqualTo(typeName.equals("Small") ? 30_001 : 60_001);
        assertThat(count(index.get(new Object[] { workId(12_345) }))).isEqualTo(1);
      }
    }

    @Test
    void aDuplicateInTheSameBucketIsRefusedAndLeavesNoIndex() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 1, 5_000);
      database.transaction(() -> database.newDocument("Work").set("id", workId(2_500)).save());

      assertDuplicateRefused("Work", workId(2_500));
      assertNothingOfTheBuildIsLeft("Work", workId(2_500));
    }

    @Test
    void aDuplicateInAnotherBucketIsRefusedAndLeavesNoIndex() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 4, 5_000);
      // THE SECOND RECORD AND THE LAST ONE: INSERTED IN TURN, SO INTO DIFFERENT BUCKETS
      database.transaction(() -> database.newDocument("Work").set("id", workId(1)).save());
      final Set<Integer> buckets = new HashSet<>();
      try (final ResultSet rows = database.query("sql", "SELECT @rid AS rid FROM Work WHERE id = ?", workId(1))) {
        while (rows.hasNext())
          buckets.add(((RID) rows.next().getProperty("rid")).getBucketId());
      }
      assertThat(buckets).as("the two records holding the key sit in different buckets").hasSize(2);

      assertDuplicateRefused("Work", workId(1));
      assertNothingOfTheBuildIsLeft("Work", workId(1));
    }

    @Test
    void aDuplicateUnderTheCaseInsensitiveCollationIsRefused() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      final DocumentType type = database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(3).create();
      type.createProperty("k", Type.STRING);
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          database.newDocument("Doc").set("k", "key-" + i).save();
        // THE SAME KEY AS "key-1500" TO A CASE-INSENSITIVE INDEX: A DIFFERENT STRING, THE SAME KEY
        database.newDocument("Doc").set("k", "KEY-1500").save();
      });

      database.begin();
      database.newDocument("Doc").set("k", "pending").save();
      assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Doc (k COLLATE CI) UNIQUE").close())
          .hasRootCauseInstanceOf(DuplicatedKeyException.class);
      database.rollback();
      assertThat(database.getSchema().getType("Doc").getIndexByProperties("k")).isNull();

      // WITHOUT THE COLLATION THE TWO STRINGS ARE TWO KEYS
      database.begin();
      database.newDocument("Doc").set("k", "pending").save();
      database.command("sql", "CREATE INDEX ON Doc (k) UNIQUE").close();
      database.commit();
      assertThat(database.getSchema().getType("Doc").getIndexByProperties("k").countEntries()).isEqualTo(3_002);
    }

    @Test
    void nullAndCompositeKeysGiveTheSameIndexAsTheClassicBuild() {
      final DocumentType type = database.getSchema().buildDocumentType().withName("Paper").withTotalBuckets(3).create();
      type.createProperty("id", Type.STRING);
      type.createProperty("a", Type.INTEGER);
      type.createProperty("b", Type.INTEGER);
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          // EVERY OTHER ID IS NULL, WHICH A UNIQUE INDEX ACCEPTS ANY NUMBER OF TIMES; "a" REPEATS, THE (a, b) PAIR DOES NOT
          database.newDocument("Paper").set("id", i % 2 == 0 ? null : workId(i)).set("a", i % 10).set("b", i).save();
      });

      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      final long[] deferred = buildBothWithPendingChange("Paper");

      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 0L);
      final long[] classic = buildBothWithPendingChange("Paper");

      assertThat(deferred).containsExactly(classic);

      // A DUPLICATED (a, b) PAIR IS REFUSED BY BOTH
      database.transaction(() -> database.newDocument("Paper").set("id", "extra").set("a", 7).set("b", 7).save());
      for (final long minRecords : new long[] { 1_000L, 0L }) {
        database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, minRecords);
        database.begin();
        database.newDocument("Paper").set("id", "pending-" + minRecords).set("a", -1).set("b", -1).save();
        assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Paper (a, b) UNIQUE").close())
            .hasRootCauseInstanceOf(DuplicatedKeyException.class);
        database.rollback();
        assertThat(database.getSchema().getType("Paper").getIndexByProperties("a", "b")).isNull();
      }
    }

    @Test
    void theIndexRefusesDuplicatesOnceBuilt() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 2, 5_000);

      database.begin();
      pendingRecordIn("Work");
      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
      database.commit();

      // THE CHECK THE BUILD LEFT TO ITS FINAL PASS IS BACK FOR EVERY WRITE AFTERWARDS
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Work").set("id", workId(7)).save()))
          .isInstanceOf(DuplicatedKeyException.class);
      database.transaction(() -> database.newDocument("Work").set("id", "fresh").save());
      assertThat(count(database.getSchema().getType("Work").getIndexByProperties("id").get(new Object[] { "fresh" }))).isEqualTo(1);
    }

    @Test
    void aRecordTheCallerWroteInTheSameTransactionIsStillRefusedAtItsCommit() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 2, 5_000);

      database.begin();
      // THE CALLER'S PENDING RECORD REPEATS A KEY THE TYPE HOLDS: ITS ENTRY QUEUES ON THE CALLER'S TRANSACTION, WHICH THE BUILD DOES NOT
      // CHECK - THE CHECK OF ITS COMMIT IS BACK BY THEN, WHATEVER THE BUILD LEFT TO ITS FINAL PASS
      database.newDocument("Work").set("id", workId(7)).save();
      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();

      assertThatThrownBy(() -> database.commit()).isInstanceOf(DuplicatedKeyException.class);
      if (database.isTransactionActive())
        database.rollback();

      // The index the build made over the committed records stands, and holds none of the refused record
      final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("id");
      assertThat(index).isNotNull();
      assertThat(index.countEntries()).isEqualTo(5_000);
      assertThat(count(index.get(new Object[] { workId(7) }))).isEqualTo(1);
    }

    @Test
    void theDeferralHoldsForTheBuildAloneAndIsGivenBackWithIt() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 2, 5_000);
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Work");
      final List<String> properties = List.of("id");
      final List<Boolean> deferredWhileBuilding = new ArrayList<>();

      // NOT BEFORE THE BUILD HAS THE WRITE LOCK (A CREATOR WAITING FOR IT WOULD SKIP THE CHECK OF AN INDEX ANOTHER ONE JUST PUBLISHED)...
      assertThat(type.isUniqueCheckDeferred(properties)).isFalse();

      database.begin();
      pendingRecordIn("Work");
      database.getSchema().buildTypeIndex("Work", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withCallback((document, totalIndexed) -> {
            if (deferredWhileBuilding.isEmpty())
              deferredWhileBuilding.add(type.isUniqueCheckDeferred(properties));
          }).create();
      database.commit();

      // ...ON FOR THE BUILD, AND OFF AGAIN ONCE IT HAS RETURNED, WHATEVER THE SCHEMA SESSION DOES AFTER THE LOCK IS RELEASED
      assertThat(deferredWhileBuilding).containsExactly(true);
      assertThat(type.isUniqueCheckDeferred(properties)).isFalse();
      assertThat(type.getIndexByProperties("id").countEntries()).isEqualTo(5_001);
    }

    @Test
    void recordsDeletedBeforeTheBuildLeaveNothingInTheIndex() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 3, 5_000);
      // A HUNDRED RECORDS GONE, AND A DUPLICATE OF A KEY THE TYPE HOLDS ADDED THEN DELETED: THE INDEX IS MADE OF THE LIVE RECORDS ALONE
      database.transaction(() -> database.command("sql", "DELETE FROM Work LIMIT 100").close());
      database.transaction(() -> database.newDocument("Work").set("id", workId(4_999)).save());
      database.transaction(() -> database.command("sql", "DELETE FROM Work WHERE id = ? LIMIT 1", workId(4_999)).close());
      final long live = database.countType("Work", true);

      database.begin();
      pendingRecordIn("Work");
      database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
      database.commit();

      assertThat(sortedBuilds).isEmpty();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id").countEntries()).isEqualTo(live + 1);
    }

    @Test
    void aWriterStartedDuringTheBuildCannotSlipADuplicateIn() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 2, 5_000);

      // THE BUILD LEAVES THE CHECK TO ITS FINAL PASS, SO IT RELIES ON NOBODY COMMITTING WHILE IT RUNS: A WRITER STARTED FROM INSIDE
      // THE SCAN, INSERTING A KEY THE TYPE ALREADY HOLDS, HAS TO WAIT FOR THE BUILD AND THEN BE REFUSED BY THE NEW INDEX
      final AtomicReference<Throwable> writerOutcome = new AtomicReference<>();
      final CountDownLatch writerAtInsert = new CountDownLatch(1);
      final Thread[] writer = new Thread[1];

      database.begin();
      pendingRecordIn("Work");
      database.getSchema().buildTypeIndex("Work", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withCallback((document, totalIndexed) -> {
            if (writer[0] == null) {
              writer[0] = new Thread(() -> {
                writerAtInsert.countDown();
                try {
                  database.transaction(() -> database.newDocument("Work").set("id", workId(4_999)).save());
                } catch (final Throwable t) {
                  writerOutcome.set(t);
                }
              });
              writer[0].start();
              try {
                assertThat(writerAtInsert.await(30, TimeUnit.SECONDS)).isTrue();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              }
            }
          }).create();
      database.commit();

      writer[0].join(60_000);
      assertThat(writer[0].isAlive()).isFalse();
      assertThat(writerOutcome.get()).isInstanceOf(DuplicatedKeyException.class);

      final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("id");
      assertThat(index.countEntries()).isEqualTo(5_001);
      assertThat(count(index.get(new Object[] { workId(4_999) }))).isEqualTo(1);
    }

    @Test
    void typeBelowTheThresholdKeepsTheCheckOfEveryBatch() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Work", 2, 999);
      database.transaction(() -> database.newDocument("Work").set("id", workId(500)).save());

      // THE SAME REFUSAL AS EVER, WITH THE RECORD ALREADY HOLDING THE KEY
      database.begin();
      pendingRecordIn("Work");
      assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close())
          .hasRootCauseInstanceOf(DuplicatedKeyException.class)
          .rootCause().hasMessageContaining("Duplicated key [" + workId(500) + "] found on index 'Work[id]' already assigned to record #");
      database.rollback();
    }

    private void assertDuplicateRefused(final String typeName, final String key) {
      // REPORTED AS THE CLASSIC BUILD REPORTS IT: THE INDEX, THE KEY AND THE RECORD ALREADY HOLDING IT
      database.begin();
      pendingRecordIn(typeName);
      assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON " + typeName + " (id) UNIQUE").close())
          .hasRootCauseInstanceOf(DuplicatedKeyException.class)
          .rootCause().hasMessageContaining("Duplicated key [" + key + "] found on index '" + typeName + "[id]' already assigned to record #");
      // THE CALLER'S TRANSACTION IS HANDED BACK ACTIVE AND USABLE
      assertThat(database.isTransactionActive()).isTrue();
      database.newDocument(typeName).set("id", "after-refusal").save();
      database.commit();
      assertThat(sortedBuilds).isEmpty();
    }

    private void assertNothingOfTheBuildIsLeft(final String typeName, final String duplicatedKey) throws Exception {
      assertThat(database.getSchema().getType(typeName).getIndexByProperties("id")).isNull();
      assertThat(database.getSchema().getIndexes()).isEmpty();
      try (final Stream<Path> files = Files.list(Path.of(database.getDatabasePath()))) {
        assertThat(files.map(path -> path.getFileName().toString())
            .filter(name -> name.endsWith("mtidx") || name.endsWith("ctidx")).toList()).isEmpty();
      }

      // FIXING THE DATA LETS THE SAME STATEMENT SUCCEED
      database.transaction(() -> database.command("sql", "DELETE FROM " + typeName + " WHERE id = ? LIMIT 1", duplicatedKey).close());
      database.begin();
      pendingRecordIn(typeName);
      database.command("sql", "CREATE INDEX ON " + typeName + " (id) UNIQUE").close();
      database.commit();
      assertThat(database.getSchema().getType(typeName).getIndexByProperties("id")).isNotNull();
    }

    /** The page accesses of one CREATE INDEX that the sorted build cannot take: a transaction with a pending record in the type. */
    private long pageAccessesOfTheBuild(final String typeName) {
      database.begin();
      pendingRecordIn(typeName);

      final PageManager.PPageManagerStats before = PageManager.INSTANCE.getStats();
      database.command("sql", "CREATE INDEX ON " + typeName + " (id) UNIQUE").close();
      final PageManager.PPageManagerStats after = PageManager.INSTANCE.getStats();

      database.commit();
      return (after.cacheHits + after.cacheMiss) - (before.cacheHits + before.cacheMiss);
    }

    /** Builds the UNIQUE indexes on id and (a, b) inside a transaction with a pending record, answers their entry counts. */
    private long[] buildBothWithPendingChange(final String typeName) {
      database.begin();
      database.newDocument(typeName).set("id", "pending").set("a", -1).set("b", -1).save();
      database.command("sql", "CREATE INDEX ON " + typeName + " (id) UNIQUE").close();
      database.command("sql", "CREATE INDEX ON " + typeName + " (a, b) UNIQUE").close();
      final DocumentType type = database.getSchema().getType(typeName);
      final TypeIndex byId = type.getIndexByProperties("id");
      final TypeIndex byPair = type.getIndexByProperties("a", "b");
      final long[] entries = { byId.countEntries(), byPair.countEntries(), count(byPair.get(new Object[] { 7, 1_007 })) };
      database.commit();
      database.getSchema().dropIndex(byId.getName());
      database.getSchema().dropIndex(byPair.getName());
      // THE PENDING RECORD IS COMMITTED NOW: THE NEXT CALL OF THIS METHOD WRITES ITS OWN, WITH ANOTHER KEY
      database.transaction(() -> database.command("sql", "DELETE FROM " + typeName + " WHERE id = 'pending'").close());
      return entries;
    }

    /** A record the caller writes in the type's own buckets, with a key no other record holds: what refuses the sorted build. */
    private void pendingRecordIn(final String typeName) {
      // A KEY OF ITS OWN EVERY TIME: A TEST THAT COMMITS ONE PENDING RECORD AND WRITES ANOTHER MUST NOT REPEAT IT
      database.newDocument(typeName).set("id", "pending-" + typeName + "-" + (++pendingRecords)).save();
    }

    private void createPopulatedType(final String typeName, final int buckets, final int records) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
      type.createProperty("id", Type.STRING);
      for (int from = 0; from < records; from += 10_000) {
        final int first = from;
        database.transaction(() -> {
          for (int i = first; i < Math.min(records, first + 10_000); i++)
            database.newDocument(typeName).set("id", workId(i)).save();
        });
      }
    }

    /** OpenAlex-style ids in a scrambled order, as in the report: the keys never arrive sorted. */
    private static String workId(final int i) {
      return "https://openalex.org/W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
    }

    private static int count(final IndexCursor cursor) {
      int total = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ++total;
      }
      return total;
    }
  }

  /**
   * Regression for #10382: the sorted build of a UNIQUE index (chosen automatically from
   * {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS} records) wrapped the {@link DuplicatedKeyException}
   * it raises in a reason-less {@link IndexException}, so {@code getMessage()} of what the caller catches named neither the
   * duplicated key nor the record already holding it, whereas the record-at-a-time build of the same statement did (issue
   * #5607). The message is what the embedded API, the console and every log line report, so it is what these tests read:
   * the root cause carrying the key was already covered by {@code IndexBuildTest.UniqueIndexSortedBuild}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UniqueIndexRefusalMessage extends TestHelper {
    private final List<SortedIndexBuildMetrics> sortedBuilds = new ArrayList<>();

    @BeforeEach
    void captureSortedBuilds() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(sortedBuilds::add);
    }

    @AfterEach
    void releaseHook() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    /**
     * One duplicated key, the same statement, either side of the sorted-build threshold: the message names the same
     * things. {@code 0} keeps the record-at-a-time build, {@code 1000} routes the 5,000 records to the sorted one.
     */
    @ParameterizedTest(name = "sorted build from {0} records")
    @ValueSource(longs = { 0L, 1_000L })
    void duplicateKeyIsNamedInTheMessageOfTheRefusal(final long sortedBuildMinRecords) {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, sortedBuildMinRecords);
      createPopulatedType("Work", 3, 5_000);
      database.transaction(() -> database.newDocument("Work").set("id", workId(2_500)).save());

      assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close())
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("Error on creating index on type 'Work', properties [id]")
          .hasMessageContaining("Duplicated key [" + workId(2_500) + "] found on index 'Work[id]' already assigned to record #")
          .hasRootCauseInstanceOf(DuplicatedKeyException.class);

      assertThat(sortedBuilds).isEmpty();
      assertThat(database.getSchema().getType("Work").getIndexByProperties("id")).isNull();
    }

    /** A composite key is reported whole, in the sorted build as in the record-at-a-time one. */
    @ParameterizedTest(name = "sorted build from {0} records")
    @ValueSource(longs = { 0L, 1_000L })
    void duplicateCompositeKeyIsNamedInTheMessageOfTheRefusal(final long sortedBuildMinRecords) {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, sortedBuildMinRecords);
      final DocumentType type = database.getSchema().buildDocumentType().withName("Pair").withTotalBuckets(2).create();
      type.createProperty("a", Type.INTEGER);
      type.createProperty("b", Type.INTEGER);
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          database.newDocument("Pair").set("a", i % 10).set("b", i).save();
        database.newDocument("Pair").set("a", 7).set("b", 1_007).save();
      });

      assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Pair (a, b) UNIQUE").close())
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("Duplicated key [7, 1007] found on index 'Pair[a,b]' already assigned to record #");
    }

    private void createPopulatedType(final String typeName, final int buckets, final int records) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
      type.createProperty("id", Type.STRING);
      for (int from = 0; from < records; from += 10_000) {
        final int first = from;
        database.transaction(() -> {
          for (int i = first; i < Math.min(records, first + 10_000); i++)
            database.newDocument(typeName).set("id", workId(i)).save();
        });
      }
    }

    /** OpenAlex-style ids in a scrambled order, as in the report: the keys never arrive sorted. */
    private static String workId(final int i) {
      return "https://openalex.org/W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
    }
  }

  /**
   * Regression for #10420: from 10,000 records on, {@code CREATE INDEX ... UNIQUE} switches to the sorted build, and a duplicate key was
   * refused with "Cannot build sorted index" instead of the message of the record-at-a-time build below that count, so an embedded
   * caller no longer saw which key and which record clash. #10382 fixed the sorted build (and tests it against a lowered
   * threshold); this pins the exact shape of the report on the DEFAULT threshold, one record either side of it: the same statement
   * is refused with the same message, for a plain key, a composite key and a duplicate held by a sub type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UniqueIndexRefusalAtDefaultThreshold extends TestHelper {
    /** One record either side of {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS}, which is 10,000 by default. */
    private static final long ONE_BELOW_THE_THRESHOLD = 9_999L;
    private static final long AT_THE_THRESHOLD        = 10_000L;

    @ParameterizedTest(name = "{0} records")
    @ValueSource(longs = { ONE_BELOW_THE_THRESHOLD, AT_THE_THRESHOLD })
    void duplicateKeyIsNamedInTheMessage(final long records) {
      assertThat(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS.getValueAsLong()).isEqualTo(AT_THE_THRESHOLD);
      createType("P", null);
      insertDistinct("P", records - 1);
      database.transaction(() -> database.newDocument("P").set("k", key(0)).save());

      assertRefusedNaming("P", "CREATE INDEX ON P (k) UNIQUE", "properties [k]", "Duplicated key [" + key(0) + "] found on index 'P[k]'");
      assertThat(database.getSchema().getType("P").getIndexByProperties("k")).isNull();
    }

    @ParameterizedTest(name = "{0} records")
    @ValueSource(longs = { ONE_BELOW_THE_THRESHOLD, AT_THE_THRESHOLD })
    void duplicateHeldByASubTypeIsNamedInTheMessage(final long records) {
      createType("P", null);
      createType("Q", "P");
      insertDistinct("P", records - 1);
      database.transaction(() -> database.newDocument("Q").set("k", key(5)).save());

      assertRefusedNaming("P", "CREATE INDEX ON P (k) UNIQUE", "properties [k]", "Duplicated key [" + key(5) + "] found on index 'P[k]'");
    }

    @ParameterizedTest(name = "{0} records")
    @ValueSource(longs = { ONE_BELOW_THE_THRESHOLD, AT_THE_THRESHOLD })
    void duplicateCompositeKeyIsNamedInTheMessage(final long records) {
      final DocumentType type = database.getSchema().buildDocumentType().withName("C").withTotalBuckets(3).create();
      type.createProperty("a", Type.INTEGER);
      type.createProperty("b", Type.INTEGER);
      database.transaction(() -> {
        for (int i = 0; i < records - 1; i++)
          database.newDocument("C").set("a", i % 10).set("b", i).save();
        database.newDocument("C").set("a", 7).set("b", 7).save();
      });

      assertRefusedNaming("C", "CREATE INDEX ON C (a, b) UNIQUE", "properties [a, b]", "Duplicated key [7, 7] found on index 'C[a,b]'");
    }

    private void assertRefusedNaming(final String typeName, final String statement, final String properties, final String duplicate) {
      assertThatThrownBy(() -> database.command("sql", statement).close())
          .isInstanceOf(IndexException.class)
          .hasMessageStartingWith("Error on creating index on type '" + typeName + "', " + properties)
          .hasMessageContaining(duplicate + " already assigned to record #")
          .hasMessageNotContaining("Cannot build sorted index")
          .hasRootCauseInstanceOf(DuplicatedKeyException.class);
    }

    private void createType(final String typeName, final String superTypeName) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(3).create();
      if (superTypeName != null)
        type.addSuperType(superTypeName);
      else
        type.createProperty("k", Type.STRING);
    }

    private void insertDistinct(final String typeName, final long records) {
      for (long from = 0; from < records; from += 10_000L) {
        final long first = from;
        database.transaction(() -> {
          for (long i = first; i < Math.min(records, first + 10_000L); i++)
            database.newDocument(typeName).set("k", key(i)).save();
        });
      }
    }

    private static String key(final long i) {
      return "k" + i;
    }
  }

  /**
   * Regression for #10450: since #10438 an index build joined the caller's transaction whenever that
   * transaction held ANY pending write, so a {@code CREATE INDEX} over a large committed bucket, in a script that had also
   * written something unrelated, queued one entry per record in that transaction until the commit instead of taking the
   * chunked build of its own transaction. A build now joins the caller's transaction only when that transaction has
   * written to a bucket the index covers.
   * <p>
   * The observable difference is what a ROLLBACK of the caller leaves behind: a build that ran in a transaction of its own
   * has committed its entries, one that joined the caller's transaction loses them with it. One test per entry point that
   * makes the decision (type-level builder, bucket-level builder, super-type propagation), plus the cases that must still
   * share.
   */
  @Nested
  class IndexBuildWithUnrelatedPendingWrites extends TestHelper {

    /** SQL {@code CREATE INDEX ... LSM_VECTOR}: the case the issue names. */
    @Test
    void vectorIndexBuildDoesNotJoinATransactionThatWroteOnlyElsewhere() {
      createCommittedVectorRecords("Vec", 3);
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");

      database.begin();
      database.command("sql", "INSERT INTO Unrelated SET x = 1");
      database.command("sql", "CREATE INDEX ON Vec (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      database.rollback();

      assertThat(database.getSchema().getIndexByName("Vec[behavior]").countEntries()).isEqualTo(3L);
      assertThat(neighbors("Vec[behavior]")).hasSize(3);
      assertThat(database.countType("Unrelated", false)).isZero();

      reopenDatabase();
      assertThat(database.getSchema().getIndexByName("Vec[behavior]").countEntries()).isEqualTo(3L);
      assertThat(neighbors("Vec[behavior]")).hasSize(3);
    }

    /** The Java type-level builder reports where it built. */
    @Test
    void typeBuilderReportsItDidNotBuildInTheCallerTransaction() {
      database.command("sql", "CREATE DOCUMENT TYPE KeyedApi");
      database.command("sql", "CREATE PROPERTY KeyedApi.id INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated2");
      database.transaction(() -> {
        database.newDocument("KeyedApi").set("id", 1).save();
        database.newDocument("KeyedApi").set("id", 2).save();
      });

      final TypeIndexBuilder[] builder = new TypeIndexBuilder[1];
      database.transaction(() -> {
        database.newDocument("Unrelated2").set("x", 1).save();
        builder[0] = database.getSchema().buildTypeIndex("KeyedApi", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE);
        builder[0].create();
      });

      assertThat(builder[0].isBuiltInCallerTransaction()).isFalse();
      assertThat(database.getSchema().getIndexByName("KeyedApi[id]").countEntries()).isEqualTo(2L);
    }

    /** ... and a write to the indexed type still makes it join the caller's transaction. */
    @Test
    void typeBuilderReportsItBuiltInTheCallerTransactionAfterAWriteToTheIndexedType() {
      database.command("sql", "CREATE DOCUMENT TYPE KeyedApi2");
      database.command("sql", "CREATE PROPERTY KeyedApi2.id INTEGER");

      final TypeIndexBuilder[] builder = new TypeIndexBuilder[1];
      database.transaction(() -> {
        database.newDocument("KeyedApi2").set("id", 1).save();
        builder[0] = database.getSchema().buildTypeIndex("KeyedApi2", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE);
        builder[0].create();
      });

      assertThat(builder[0].isBuiltInCallerTransaction()).isTrue();
      assertThat(database.getSchema().getIndexByName("KeyedApi2[id]").countEntries()).isEqualTo(1L);
    }

    /** Every family follows the same rule: an LSM_TREE index is built in its own transaction too. */
    @Test
    void lsmTreeIndexBuildDoesNotJoinATransactionThatWroteOnlyElsewhere() {
      database.command("sql", "CREATE DOCUMENT TYPE Keyed");
      database.command("sql", "CREATE PROPERTY Keyed.id INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated3");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("Keyed").set("id", i).save();
      });

      database.begin();
      database.command("sql", "INSERT INTO Unrelated3 SET x = 1");
      database.command("sql", "CREATE INDEX ON Keyed (id) UNIQUE");
      database.rollback();

      assertThat(database.getSchema().getIndexByName("Keyed[id]").countEntries()).isEqualTo(10L);
      try (final ResultSet rs = database.query("sql", "SELECT FROM Keyed WHERE id = 7")) {
        assertThat(rs.stream().count()).isEqualTo(1L);
      }
    }

    /** A write to ANY bucket the index covers - here a subtype's, reached polymorphically - still joins the transaction. */
    @Test
    void writeToASubtypeBucketStillJoinsTheTransaction() {
      createCommittedVectorRecords("VecParent", 2);
      database.command("sql", "CREATE DOCUMENT TYPE VecChild EXTENDS VecParent");

      database.transaction(() -> {
        database.command("sql", "INSERT INTO VecChild SET behavior=[0.8,0.2,0.0]");
        database.command("sql",
            "CREATE INDEX ON VecParent (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      });

      assertThat(database.getSchema().getIndexByName("VecParent[behavior]").countEntries()).isEqualTo(3L);
      assertThat(neighbors("VecParent[behavior]")).hasSize(3);
    }

    /**
     * A pending update of a committed record in the indexed type still joins the transaction: the entries of the pending
     * versions are queued on it, and the rollback undoes them. The committed records are indexed on their own (issue #10490),
     * so the index still describes them afterwards.
     */
    @Test
    void updateInTheIndexedTypeStillJoinsTheTransaction() {
      createCommittedVectorRecords("VecUpd", 2);

      database.begin();
      database.command("sql", "UPDATE VecUpd SET behavior = [0.1,0.9,0.0]");
      database.command("sql", "CREATE INDEX ON VecUpd (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      database.rollback();

      assertThat(database.getSchema().getIndexByName("VecUpd[behavior]").countEntries())
          .as("the committed records are indexed whatever the caller does").isEqualTo(2L);
      assertThat(neighbors("VecUpd[behavior]")).hasSize(2);
    }

    /** Bucket-level builder: a write to ANOTHER bucket of the same type is unrelated to this bucket's index. */
    @Test
    void bucketIndexBuildDoesNotJoinATransactionThatWroteToAnotherBucket() {
      final DocumentType type = database.getSchema().createDocumentType("BucketVec", 2);
      type.createProperty("behavior", Type.ARRAY_OF_FLOATS);
      final String indexed = type.getBuckets(false).getFirst().getName();
      final String other = type.getBuckets(false).get(1).getName();
      database.transaction(() -> {
        database.newDocument("BucketVec").set("behavior", new float[] { 0.9f, 0.1f, 0.0f }).save(indexed);
        database.newDocument("BucketVec").set("behavior", new float[] { 0.1f, 0.9f, 0.0f }).save(indexed);
      });

      final Index[] index = new Index[1];
      database.begin();
      database.newDocument("BucketVec").set("behavior", new float[] { 0.5f, 0.5f, 0.0f }).save(other);
      final BucketLSMVectorIndexBuilder builder = (BucketLSMVectorIndexBuilder) database.getSchema()
          .buildBucketIndex("BucketVec", indexed, new String[] { "behavior" })
          .withType(Schema.INDEX_TYPE.LSM_VECTOR);
      builder.withDimensions(3).withSimilarity("COSINE");
      index[0] = builder.create();
      database.rollback();

      assertThat(index[0].countEntries()).isEqualTo(2L);
      // an index on one of the two buckets of the type is reported by CHECK DATABASE (run after each test) as lacking the other one
      database.getSchema().dropIndex(index[0].getName());
    }

    /** Bucket-level builder: a write to the indexed bucket itself still joins the transaction. */
    @Test
    void bucketIndexBuildJoinsATransactionThatWroteToItsBucket() {
      final DocumentType type = database.getSchema().createDocumentType("BucketVec2", 1);
      type.createProperty("behavior", Type.ARRAY_OF_FLOATS);
      final String indexed = type.getBuckets(false).getFirst().getName();

      final Index[] index = new Index[1];
      database.transaction(() -> {
        database.newDocument("BucketVec2").set("behavior", new float[] { 0.9f, 0.1f, 0.0f }).save();
        final BucketLSMVectorIndexBuilder builder = (BucketLSMVectorIndexBuilder) database.getSchema()
            .buildBucketIndex("BucketVec2", indexed, new String[] { "behavior" })
            .withType(Schema.INDEX_TYPE.LSM_VECTOR);
        builder.withDimensions(3).withSimilarity("COSINE");
        index[0] = builder.create();
      });

      assertThat(index[0].countEntries()).isEqualTo(1L);
    }

    /** Super-type propagation: the subtype's buckets are what is indexed, so a write elsewhere does not join. */
    @Test
    void propagatedIndexBuildDoesNotJoinATransactionThatWroteOnlyElsewhere() {
      database.command("sql", "CREATE DOCUMENT TYPE PParent");
      database.command("sql", "CREATE PROPERTY PParent.behavior ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON PParent (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      createCommittedVectorRecords("PChild", 2);
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated4");

      database.begin();
      database.command("sql", "INSERT INTO Unrelated4 SET x = 1");
      database.getSchema().getType("PChild").addSuperType("PParent");
      database.rollback();

      assertThat(database.getSchema().getIndexByName("PParent[behavior]").countEntries()).isEqualTo(2L);
      assertThat(neighbors("PParent[behavior]")).hasSize(2);
    }

    /** The transaction-side half of the decision, asked directly: which files a transaction has written to. */
    @Test
    void transactionReportsTheFilesItWroteTo() {
      final DocumentType written = database.getSchema().createDocumentType("WrittenTo", 1);
      final DocumentType updated = database.getSchema().createDocumentType("UpdatedOnly", 1);
      final DocumentType untouched = database.getSchema().createDocumentType("Untouched", 1);
      final int writtenBucket = written.getBuckets(false).getFirst().getFileId();
      final int updatedBucket = updated.getBuckets(false).getFirst().getFileId();
      final int untouchedBucket = untouched.getBuckets(false).getFirst().getFileId();

      final RID[] committed = new RID[1];
      database.transaction(() -> committed[0] = database.newDocument("UpdatedOnly").set("x", 1).save().getIdentity());

      database.transaction(() -> {
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        assertThat(tx.hasChangesInFiles(setOf(writtenBucket, updatedBucket, untouchedBucket))).isFalse();

        database.newDocument("WrittenTo").set("x", 1).save();
        assertThat(tx.hasChangesInFiles(setOf(writtenBucket))).isTrue();
        assertThat(tx.hasChangesInFiles(setOf(updatedBucket, untouchedBucket))).isFalse();

        database.lookupByRID(committed[0], true).asDocument().modify().set("x", 2).save();
        assertThat(tx.hasChangesInFiles(setOf(updatedBucket))).isTrue();
        assertThat(tx.hasChangesInFiles(setOf(untouchedBucket))).isFalse();
        assertThat(tx.hasChangesInFiles(new IntHashSet())).isFalse();
      });
    }

    private static IntHashSet setOf(final int... values) {
      final IntHashSet set = new IntHashSet();
      for (final int value : values)
        set.add(value);
      return set;
    }

    private void createCommittedVectorRecords(final String typeName, final int count) {
      database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
      database.command("sql", "CREATE PROPERTY " + typeName + ".behavior ARRAY_OF_FLOATS");
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          database.newDocument(typeName).set("behavior", new float[] { 0.9f, 0.1f, i / 10f }).save();
      });
    }

    private List<Result> neighbors(final String indexName) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(vectorNeighbors('" + indexName + "', [0.9,0.1,0.0], 10))")) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Regression for #10466: the sorted index build was refused whenever ANY transaction stacked on the thread held ANY write, so
   * {@code BEGIN; INSERT INTO Unrelated ...; CREATE INDEX ON Big (id) UNIQUE; COMMIT} paid the quadratic record-at-a-time
   * build of #10334 again. The sorted build only needs the buckets it scans to be free of pending writes, in every
   * transaction of the stack, since it sets the whole stack aside.
   */
  @Nested
  class SortedBuildBesideUnrelatedWrites extends TestHelper {
    private static final int RECORDS = 2_000;

    private final List<SortedIndexBuildMetrics> sortedBuilds = new ArrayList<>();

    @BeforeEach
    void captureSortedBuilds() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(sortedBuilds::add);
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1_000L);
      createPopulatedType("Big", 2, RECORDS);
      final DocumentType unrelated = database.getSchema().buildDocumentType().withName("Unrelated").withTotalBuckets(1).create();
      unrelated.createProperty("name", Type.STRING);
      unrelated.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    }

    @AfterEach
    void releaseHook() {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    @Test
    void writeToAnotherTypeKeepsTheSortedBuild() {
      database.begin();
      database.newDocument("Unrelated").set("name", "pending").save();

      database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
      assertThat(sortedBuilds).hasSize(1);

      // The caller's transaction is handed back with its own write still in it, and commits it
      assertThat(database.isTransactionActive()).isTrue();
      assertThat(((TransactionContext) ((DatabaseInternal) database).getTransaction()).hasChanges()).isTrue();
      database.newDocument("Big").set("id", "after-index").save();
      database.commit();

      assertThat(database.query("sql", "SELECT FROM Unrelated WHERE name = 'pending'").stream().count()).isEqualTo(1);
      final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
      assertThat(index.countEntries()).isEqualTo(RECORDS + 1);
      assertThat(count(index.get(new Object[] { "after-index" }))).isEqualTo(1);
      assertThat(count(index.get(new Object[] { id(7) }))).isEqualTo(1);
      // The Unrelated index committed its pending entry along with the record
      assertThat(count(database.getSchema().getType("Unrelated").getIndexByProperties("name").get(new Object[] { "pending" })))
          .isEqualTo(1);

      // The new index is live and refuses a duplicate
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Big").set("id", id(3)).save()))
          .isInstanceOf(DuplicatedKeyException.class);
    }

    @Test
    void rollingBackTheUnrelatedWriteKeepsTheIndex() {
      database.begin();
      database.newDocument("Unrelated").set("name", "pending").save();
      database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
      assertThat(sortedBuilds).hasSize(1);
      database.rollback();

      // The index is DDL and survives the rollback of the caller's transaction, as the record-at-a-time build's does
      assertThat(database.query("sql", "SELECT FROM Unrelated").stream().count()).isZero();
      final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
      assertThat(index).isNotNull();
      assertThat(index.countEntries()).isEqualTo(RECORDS);
    }

    @Test
    void writeToAnotherTypeInAnOuterTransactionKeepsTheSortedBuild() {
      database.begin();
      database.newDocument("Unrelated").set("name", "outer").save();
      database.begin();
      database.newDocument("Unrelated").set("name", "inner").save();

      database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
      assertThat(sortedBuilds).hasSize(1);

      database.commit();
      database.commit();
      assertThat(database.query("sql", "SELECT FROM Unrelated").stream().count()).isEqualTo(2);
      assertThat(database.getSchema().getType("Big").getIndexByProperties("id").countEntries()).isEqualTo(RECORDS);
    }

    @Test
    void explicitSortedModeAcceptsAnUnrelatedWrite() {
      database.begin();
      database.newDocument("Unrelated").set("name", "pending").save();

      database.getSchema().buildTypeIndex("Big", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED).withUnique(true).create();
      assertThat(sortedBuilds).hasSize(1);
      database.commit();

      assertThat(database.getSchema().getType("Big").getIndexByProperties("id").countEntries()).isEqualTo(RECORDS);
    }

    @Test
    void insertIntoTheIndexedTypeInAnOuterTransactionStillRefusesTheSortedBuild() {
      // Outer wrote to Big, inner wrote elsewhere: the inner one is NOT the one that matters, every frame is asked
      database.begin();
      database.newDocument("Big").set("id", "outer-pending").save();
      database.begin();
      database.newDocument("Unrelated").set("name", "inner").save();

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Big", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED).withUnique(true).create())
          .hasMessageContaining("Sorted build requires no pending changes");

      database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
      assertThat(sortedBuilds).isEmpty();

      database.commit();
      database.commit();
    }

    @Test
    void updateOfAnIndexedRecordStillRefusesTheSortedBuild() {
      final RID rid = database.query("sql", "SELECT FROM Big LIMIT 1").next().getIdentity().get();

      database.begin();
      database.newDocument("Unrelated").set("name", "pending").save();
      final MutableDocument doc = rid.asDocument().modify();
      doc.set("id", "updated");
      doc.save();

      database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
      assertThat(sortedBuilds).isEmpty();
      database.commit();

      final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
      assertThat(index.countEntries()).isEqualTo(RECORDS);
      assertThat(count(index.get(new Object[] { "updated" }))).isEqualTo(1);
    }

    @Test
    void deleteOfAnIndexedRecordStillRefusesTheSortedBuild() {
      final RID rid = database.query("sql", "SELECT FROM Big LIMIT 1").next().getIdentity().get();
      final String deletedId = rid.asDocument().getString("id");

      database.begin();
      database.newDocument("Unrelated").set("name", "pending").save();
      rid.asDocument().delete();

      database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
      assertThat(sortedBuilds).isEmpty();
      database.commit();

      final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
      assertThat(index.countEntries()).isEqualTo(RECORDS - 1);
      assertThat(count(index.get(new Object[] { deletedId }))).isZero();
    }

    @Test
    void insertIntoASubTypeStillRefusesTheSortedBuild() {
      // The build is polymorphic, so a sub-type's bucket is one it scans
      database.getSchema().buildDocumentType().withName("BigChild").withSuperType("Big").withTotalBuckets(1).create();

      database.begin();
      database.newDocument("BigChild").set("id", "child-pending").save();

      database.command("sql", "CREATE INDEX ON Big (id) UNIQUE").close();
      assertThat(sortedBuilds).isEmpty();
      database.commit();

      final TypeIndex index = database.getSchema().getType("Big").getIndexByProperties("id");
      assertThat(count(index.get(new Object[] { "child-pending" }))).isEqualTo(1);
    }

    private void createPopulatedType(final String typeName, final int buckets, final int records) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
      type.createProperty("id", Type.STRING);
      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          database.newDocument(typeName).set("id", id(i)).save();
      });
    }

    private static String id(final int i) {
      return "W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
    }

    private static int count(final IndexCursor cursor) {
      int total = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ++total;
      }
      return total;
    }
  }

  /**
   * Regression for #10490: a {@code CREATE INDEX} inside a transaction that had already written to the indexed type
   * joined that transaction, and the scan - which reads the caller's pending pages first and the committed ones underneath -
   * queued one entry per record, the COMMITTED ones included, on the transaction the caller then rolled back. The index
   * component itself is committed on its own, so after the rollback the schema had the index and its file had no entries: a
   * lookup found nothing, and a UNIQUE index accepted a duplicate of a committed key.
   * <p>
   * The committed records are indexed in a transaction of their own now, and only what the caller itself created, updated or
   * deleted is queued on the caller's transaction, so a rollback leaves the index describing exactly the committed state and a
   * commit leaves it describing the caller's.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IndexBuildRollbackKeepsCommittedRecords extends TestHelper {

    /** One row per index family the issue lists: how to create it, how to ask it for key 1, and what a scan must find. */
    private record Family(String name, String create, String lookupOne, String lookupNine) {
    }

    private static final List<Family> FAMILIES = List.of(
        new Family("LSM_TREE", "CREATE INDEX ON %s (k) NOTUNIQUE", "SELECT FROM %s WHERE k = 1", "SELECT FROM %s WHERE k = 9"),
        new Family("UNIQUE", "CREATE INDEX ON %s (k) UNIQUE", "SELECT FROM %s WHERE k = 1", "SELECT FROM %s WHERE k = 9"),
        new Family("FULL_TEXT", "CREATE INDEX ON %s (s) FULL_TEXT", "SELECT FROM %s WHERE SEARCH_INDEX('%s[s]', 'w1') = true",
            "SELECT FROM %s WHERE SEARCH_INDEX('%s[s]', 'w9') = true"),
        new Family("LSM_TREE BY ITEM", "CREATE INDEX ON %s (tags BY ITEM) NOTUNIQUE", "SELECT FROM %s WHERE tags CONTAINS 'a1'",
            "SELECT FROM %s WHERE tags CONTAINS 'a9'"),
        new Family("HASH", "CREATE INDEX ON %s (k) UNIQUE_HASH", "SELECT FROM %s WHERE k = 1", "SELECT FROM %s WHERE k = 9"));

    @Test
    void rollbackAfterAWriteToTheIndexedTypeKeepsEveryCommittedRecordIndexed() {
      int n = 0;
      for (final Family family : FAMILIES) {
        final String type = createTypeWithThreeCommittedRecords("Ins" + n++);

        database.begin();
        database.command("sql", "INSERT INTO " + type + " SET k = 9, s = 'w9', tags = ['a9']");
        database.command("sql", family.create().formatted(type));
        database.rollback();

        assertThat(find(family.lookupOne(), type)).as(family.name() + " finds the committed record").isEqualTo(1);
        assertThat(find(family.lookupNine(), type)).as(family.name() + " does not find the rolled back record").isZero();
        assertThat(entries(type)).as(family.name() + " entries after the rollback").isGreaterThanOrEqualTo(3L);
      }
    }

    @Test
    void commitAfterAWriteToTheIndexedTypeIndexesTheCommittedAndTheNewRecord() {
      int n = 0;
      for (final Family family : FAMILIES) {
        final String type = createTypeWithThreeCommittedRecords("Commit" + n++);

        database.begin();
        database.command("sql", "INSERT INTO " + type + " SET k = 9, s = 'w9', tags = ['a9']");
        database.command("sql", family.create().formatted(type));
        database.commit();

        assertThat(find(family.lookupOne(), type)).as(family.name() + " finds the committed record").isEqualTo(1);
        assertThat(find(family.lookupNine(), type)).as(family.name() + " finds the record committed with the index").isEqualTo(1);
      }
    }

    @Test
    void anUpdateOfACommittedRecordIsAppliedOnCommitAndTakenBackOnRollback() {
      int n = 0;
      for (final Family family : FAMILIES) {
        final String rolledBack = createTypeWithThreeCommittedRecords("UpdRb" + n);
        final String committed = createTypeWithThreeCommittedRecords("UpdCm" + n++);

        database.begin();
        database.command("sql", "UPDATE " + rolledBack + " SET k = 9, s = 'w9', tags = ['a9'] WHERE k = 1");
        database.command("sql", family.create().formatted(rolledBack));
        database.rollback();
        assertThat(find(family.lookupOne(), rolledBack)).as(family.name() + " rollback keeps the old key").isEqualTo(1);
        assertThat(find(family.lookupNine(), rolledBack)).as(family.name() + " rollback drops the new key").isZero();

        database.begin();
        database.command("sql", "UPDATE " + committed + " SET k = 9, s = 'w9', tags = ['a9'] WHERE k = 1");
        database.command("sql", family.create().formatted(committed));
        database.commit();
        assertThat(find(family.lookupOne(), committed)).as(family.name() + " commit drops the old key").isZero();
        assertThat(find(family.lookupNine(), committed)).as(family.name() + " commit has the new key").isEqualTo(1);
      }
    }

    @Test
    void aDeleteOfACommittedRecordIsAppliedOnCommitAndTakenBackOnRollback() {
      int n = 0;
      for (final Family family : FAMILIES) {
        final String rolledBack = createTypeWithThreeCommittedRecords("DelRb" + n);
        final String committed = createTypeWithThreeCommittedRecords("DelCm" + n++);

        database.begin();
        database.command("sql", "DELETE FROM " + rolledBack + " WHERE k = 1");
        database.command("sql", family.create().formatted(rolledBack));
        database.rollback();
        assertThat(find(family.lookupOne(), rolledBack)).as(family.name() + " rollback restores the record").isEqualTo(1);

        database.begin();
        database.command("sql", "DELETE FROM " + committed + " WHERE k = 1");
        database.command("sql", family.create().formatted(committed));
        database.commit();
        assertThat(find(family.lookupOne(), committed)).as(family.name() + " commit removes the record").isZero();
      }
    }

    @Test
    void aUniqueIndexRefusesADuplicateOfACommittedKeyAfterTheRollback() {
      final String type = createTypeWithThreeCommittedRecords("Dup");

      database.begin();
      database.command("sql", "INSERT INTO " + type + " SET k = 9, s = 'w9', tags = ['a9']");
      database.command("sql", "CREATE INDEX ON " + type + " (k) UNIQUE");
      database.rollback();

      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET k = 1").close()))
          .isInstanceOf(DuplicatedKeyException.class);
      assertThat(scan("SELECT FROM " + type + " WHERE k = 1")).isEqualTo(1);

      reopenDatabase();
      assertThat(find("SELECT FROM %s WHERE k = 1", type)).isEqualTo(1);
    }

    /**
     * A pending duplicate of a committed key is queued on the caller's transaction like any other write, so a UNIQUE index built
     * over it is created and the duplicate is refused where every unique violation is, at the caller's commit. The committed phase
     * of the build has committed on its own by then, and what the refused commit leaves behind is a complete index that goes on
     * refusing duplicates, not one the next write cannot trust.
     */
    @Test
    void aPendingDuplicateOfACommittedKeyIsRefusedAtTheCommitAndLeavesACompleteUniqueIndex() {
      final String type = createTypeWithThreeCommittedRecords("PendingDup");

      database.begin();
      database.command("sql", "INSERT INTO " + type + " SET k = 1, s = 'dup', tags = ['dup']");
      database.command("sql", "CREATE INDEX ON " + type + " (k) UNIQUE");
      assertThatThrownBy(() -> database.commit()).isInstanceOf(DuplicatedKeyException.class);
      if (database.isTransactionActive())
        database.rollback();

      assertThat(scan("SELECT FROM " + type + " WHERE k = 1")).as("only the committed record is left").isEqualTo(1);
      assertThat(entries(type)).as("the index holds every committed record").isEqualTo(3L);
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET k = 1").close()))
          .isInstanceOf(DuplicatedKeyException.class);
    }

    /** A record written before the index and updated after it, whichever way the transaction ends. */
    @Test
    void aRecordInsertedBeforeTheIndexAndUpdatedAfterItFollowsTheOutcomeOfTheTransaction() {
      final String committed = createTypeWithThreeCommittedRecords("InsUpdCm");
      final String rolledBack = createTypeWithThreeCommittedRecords("InsUpdRb");

      database.begin();
      database.command("sql", "INSERT INTO " + committed + " SET k = 9, s = 'w9', tags = ['a9']");
      database.command("sql", "CREATE INDEX ON " + committed + " (k) NOTUNIQUE");
      database.command("sql", "UPDATE " + committed + " SET k = 10 WHERE k = 9");
      database.commit();
      assertThat(find("SELECT FROM %s WHERE k = 9", committed)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 10", committed)).isEqualTo(1);
      assertThat(entries(committed)).isEqualTo(4L);

      database.begin();
      database.command("sql", "INSERT INTO " + rolledBack + " SET k = 9, s = 'w9', tags = ['a9']");
      database.command("sql", "CREATE INDEX ON " + rolledBack + " (k) NOTUNIQUE");
      database.command("sql", "UPDATE " + rolledBack + " SET k = 10 WHERE k = 9");
      database.rollback();
      assertThat(find("SELECT FROM %s WHERE k = 9", rolledBack)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 10", rolledBack)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 1", rolledBack)).isEqualTo(1);
      assertThat(entries(rolledBack)).isEqualTo(3L);
    }

    /** The caller deletes a committed row and inserts another with the same key before the UNIQUE index exists. */
    @Test
    void aUniqueIndexOverADeletedAndReinsertedKeyFollowsTheOutcomeOfTheTransaction() {
      final String committed = createTypeWithThreeCommittedRecords("ReinsertCm");
      final String rolledBack = createTypeWithThreeCommittedRecords("ReinsertRb");

      database.begin();
      database.command("sql", "DELETE FROM " + committed + " WHERE k = 1");
      database.command("sql", "INSERT INTO " + committed + " SET k = 1, s = 'again', tags = ['again']");
      database.command("sql", "CREATE INDEX ON " + committed + " (k) UNIQUE");
      database.commit();
      assertThat(scan("SELECT FROM " + committed + " WHERE k = 1 AND s = 'again'")).as("the new row answers the key").isEqualTo(1);
      assertThat(scan("SELECT FROM " + committed + " WHERE k = 1")).isEqualTo(1);
      assertThat(entries(committed)).isEqualTo(3L);

      database.begin();
      database.command("sql", "DELETE FROM " + rolledBack + " WHERE k = 1");
      database.command("sql", "INSERT INTO " + rolledBack + " SET k = 1, s = 'again', tags = ['again']");
      database.command("sql", "CREATE INDEX ON " + rolledBack + " (k) UNIQUE");
      database.rollback();
      assertThat(scan("SELECT FROM " + rolledBack + " WHERE k = 1 AND s = 'w1'")).as("the committed row answers the key").isEqualTo(1);
      assertThat(scan("SELECT FROM " + rolledBack + " WHERE k = 1")).isEqualTo(1);
      assertThat(entries(rolledBack)).isEqualTo(3L);
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO " + rolledBack + " SET k = 1").close()))
          .isInstanceOf(DuplicatedKeyException.class);
    }

    /**
     * A failure while the caller's pending records are being queued leaves nothing behind: the committed entries are already
     * committed by then, so the index has to be dropped by the builder that created it, whichever builder that is, and a second
     * attempt must not find it "already there". The failure is raised by the progress callback on a record only the caller wrote.
     */
    @Test
    void aFailureWhileQueueingTheCallersRecordsDropsTheTypeIndexAndAllowsARetry() {
      final String type = createTypeWithThreeCommittedRecords("FailType");

      database.begin();
      database.command("sql", "INSERT INTO " + type + " SET k = 9, s = 'w9', tags = ['a9']");
      assertThatThrownBy(() -> database.getSchema().buildTypeIndex(type, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false).withCallback((document, total) -> failOnTheCallersRecord(document)).create())
          .hasStackTraceContaining("injected failure");
      database.rollback();

      assertThat(database.getSchema().getType(type).getAllIndexes(true)).as("no index left behind").isEmpty();

      final Index retried = database.getSchema().buildTypeIndex(type, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false).create();
      assertThat(retried.countEntries()).isEqualTo(3L);
    }

    @Test
    void aFailureWhileQueueingTheCallersRecordsDropsTheBucketIndexAndAllowsARetry() {
      final DocumentType type = database.getSchema().createDocumentType("FailBucket", 1);
      type.createProperty("k", Type.INTEGER);
      final String bucket = type.getBuckets(false).getFirst().getName();
      database.transaction(() -> {
        for (int i = 1; i <= 3; i++)
          database.newDocument("FailBucket").set("k", i).save();
      });

      database.begin();
      database.newDocument("FailBucket").set("k", 9).save();
      assertThatThrownBy(() -> database.getSchema().buildBucketIndex("FailBucket", bucket, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withCallback((document, total) -> failOnTheCallersRecord(document))
          .create()).hasStackTraceContaining("injected failure");
      database.rollback();

      assertThat(database.getSchema().getType("FailBucket").getAllIndexes(true)).as("no index left behind").isEmpty();

      final Index retried = database.getSchema().buildBucketIndex("FailBucket", bucket, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      assertThat(retried.countEntries()).isEqualTo(3L);
    }

    private static void failOnTheCallersRecord(final Document document) {
      if (document.getInteger("k") == 9)
        throw new IllegalStateException("injected failure");
    }

    /**
     * A record updated before the index existed and again after it: the second update diffs against the last indexed state of
     * the record, and the reconciliation of the first one has to leave that state behind, or the entry it queued for the
     * intermediate key stays in the index for good.
     */
    @Test
    void aKeyUpdatedBeforeAndAgainAfterTheIndexWasCreatedLeavesNoStaleEntry() {
      final String type = createTypeWithThreeCommittedRecords("TwiceUpd");

      database.begin();
      database.command("sql", "UPDATE " + type + " SET k = 5 WHERE k = 1");
      database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
      database.command("sql", "UPDATE " + type + " SET k = 7 WHERE k = 5");
      database.commit();

      assertThat(find("SELECT FROM %s WHERE k = 1", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 5", type)).as("the intermediate key").isZero();
      assertThat(find("SELECT FROM %s WHERE k = 7", type)).isEqualTo(1);
      assertThat(entries(type)).isEqualTo(3L);
    }

    /** The same for a record the transaction created: created, updated, indexed, updated again. */
    @Test
    void aCreatedKeyUpdatedBeforeAndAgainAfterTheIndexWasCreatedLeavesNoStaleEntry() {
      final String type = createTypeWithThreeCommittedRecords("TwiceNew");

      database.begin();
      database.command("sql", "INSERT INTO " + type + " SET k = 4, s = 'w4', tags = ['a4']");
      database.command("sql", "UPDATE " + type + " SET k = 5 WHERE k = 4");
      database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
      database.command("sql", "UPDATE " + type + " SET k = 7 WHERE k = 5");
      database.commit();

      assertThat(find("SELECT FROM %s WHERE k = 4", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 5", type)).as("the intermediate key").isZero();
      assertThat(find("SELECT FROM %s WHERE k = 7", type)).isEqualTo(1);
      assertThat(entries(type)).isEqualTo(4L);
    }

    /** An index that already existed keeps the diff base it had: the reconciliation of the new one must not replace it. */
    @Test
    void anIndexThatAlreadyExistedStillDiffsAgainstItsOwnLastState() {
      final String type = createTypeWithThreeCommittedRecords("OtherIdx");
      database.command("sql", "CREATE INDEX ON " + type + " (s) UNIQUE");

      database.begin();
      database.command("sql", "UPDATE " + type + " SET k = 5, s = 'x1' WHERE k = 1");
      database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
      database.command("sql", "UPDATE " + type + " SET k = 7, s = 'x2' WHERE k = 5");
      database.commit();

      assertThat(find("SELECT FROM %s WHERE k = 5", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 7", type)).isEqualTo(1);
      assertThat(scan("SELECT FROM " + type + " WHERE s = 'w1'")).as("the first value of s").isZero();
      assertThat(scan("SELECT FROM " + type + " WHERE s = 'x1'")).as("the intermediate value of s").isZero();
      assertThat(scan("SELECT FROM " + type + " WHERE s = 'x2'")).isEqualTo(1);
      for (final Index index : database.getSchema().getType(type).getAllIndexes(true))
        assertThat(index.countEntries()).as(index.getName()).isEqualTo(3L);
    }

    /** More writes than one chunk of the reconciliation holds: the chunk boundaries must not lose or repeat a record. */
    @Test
    void aTransactionWritingMoreThanOneChunkIsReconciledInFull() {
      final String type = createTypeWithThreeCommittedRecords("Chunks");
      final int inserted = IndexBuilder.BUILD_BATCH_SIZE + 123;

      database.begin();
      for (int i = 0; i < inserted; i++)
        database.newDocument(type).set("k", 1000 + i).set("s", "w").save();
      database.command("sql", "UPDATE " + type + " SET k = 9 WHERE k = 1");
      database.command("sql", "DELETE FROM " + type + " WHERE k = 2");
      database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
      database.commit();

      assertThat(find("SELECT FROM %s WHERE k = 1", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 2", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 9", type)).isEqualTo(1);
      assertThat(find("SELECT FROM %s WHERE k = 3", type)).isEqualTo(1);
      assertThat(find("SELECT FROM %s WHERE k = " + (1000 + inserted - 1), type)).isEqualTo(1);
      assertThat(find("SELECT FROM %s WHERE k = 1000", type)).isEqualTo(1);
      assertThat(entries(type)).isEqualTo(2L + inserted);
    }

    @Test
    void aTransactionThatCreatedAndThenUpdatedOrDeletedRecordsLeavesTheIndexConsistent() {
      final String type = createTypeWithThreeCommittedRecords("Mixed");

      database.begin();
      database.command("sql", "INSERT INTO " + type + " SET k = 7, s = 'w7', tags = ['a7']");
      database.command("sql", "INSERT INTO " + type + " SET k = 8, s = 'w8', tags = ['a8']");
      database.command("sql", "UPDATE " + type + " SET k = 70 WHERE k = 7");
      database.command("sql", "DELETE FROM " + type + " WHERE k = 8");
      database.command("sql", "UPDATE " + type + " SET k = 20 WHERE k = 2");
      database.command("sql", "DELETE FROM " + type + " WHERE k = 3");
      // created, updated and deleted in the one transaction: nothing of it reaches the index
      database.command("sql", "INSERT INTO " + type + " SET k = 6, s = 'w6', tags = ['a6']");
      database.command("sql", "UPDATE " + type + " SET k = 60 WHERE k = 6");
      database.command("sql", "DELETE FROM " + type + " WHERE k = 60");
      database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
      database.commit();

      assertThat(find("SELECT FROM %s WHERE k = 6", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 60", type)).isZero();

      assertThat(find("SELECT FROM %s WHERE k = 1", type)).isEqualTo(1);
      assertThat(find("SELECT FROM %s WHERE k = 2", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 20", type)).isEqualTo(1);
      assertThat(find("SELECT FROM %s WHERE k = 3", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 7", type)).isZero();
      assertThat(find("SELECT FROM %s WHERE k = 70", type)).isEqualTo(1);
      assertThat(find("SELECT FROM %s WHERE k = 8", type)).isZero();
      assertThat(entries(type)).isEqualTo(3L);
    }

    @Test
    void aSuperTypeIndexPropagatedToATypeWrittenInTheTransactionKeepsTheCommittedRecordsOnRollback() {
      database.command("sql", "CREATE DOCUMENT TYPE PParent");
      database.command("sql", "CREATE PROPERTY PParent.k INTEGER");
      database.command("sql", "CREATE INDEX ON PParent (k) UNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE PChild");
      database.command("sql", "CREATE PROPERTY PChild.k INTEGER");
      database.transaction(() -> {
        for (int i = 1; i <= 3; i++)
          database.command("sql", "INSERT INTO PChild SET k = " + i).close();
      });

      database.begin();
      database.command("sql", "INSERT INTO PChild SET k = 9");
      database.getSchema().getType("PChild").addSuperType("PParent");
      database.rollback();

      assertThat(database.getSchema().getIndexByName("PParent[k]").countEntries()).isEqualTo(3L);
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO PChild SET k = 1").close()))
          .isInstanceOf(DuplicatedKeyException.class);
    }

    /** The propagation entry point with updates and deletes of the subtype's committed records, whichever way the transaction ends. */
    @Test
    void aSuperTypeIndexPropagatedAfterUpdatesAndDeletesFollowsTheOutcomeOfTheTransaction() {
      for (final boolean commit : new boolean[] { true, false }) {
        final String suffix = commit ? "Cm" : "Rb";
        final String parent = "Par" + suffix;
        final String child = "Chi" + suffix;
        database.command("sql", "CREATE DOCUMENT TYPE " + parent);
        database.command("sql", "CREATE PROPERTY " + parent + ".k INTEGER");
        database.command("sql", "CREATE INDEX ON " + parent + " (k) UNIQUE");
        database.command("sql", "CREATE DOCUMENT TYPE " + child);
        database.command("sql", "CREATE PROPERTY " + child + ".k INTEGER");
        database.transaction(() -> {
          for (int i = 1; i <= 4; i++)
            database.command("sql", "INSERT INTO " + child + " SET k = " + i).close();
        });

        database.begin();
        database.command("sql", "UPDATE " + child + " SET k = 20 WHERE k = 2");
        database.command("sql", "DELETE FROM " + child + " WHERE k = 3");
        database.command("sql", "INSERT INTO " + child + " SET k = 5");
        database.getSchema().getType(child).addSuperType(parent);
        if (commit)
          database.commit();
        else
          database.rollback();

        final String index = parent + "[k]";
        if (commit) {
          assertThat(database.getSchema().getIndexByName(index).countEntries()).as("committed: 1, 20, 4, 5").isEqualTo(4L);
          assertThat(scan("SELECT FROM " + parent + " WHERE k = 20")).isEqualTo(1);
          assertThat(scan("SELECT FROM " + parent + " WHERE k = 2")).isZero();
          assertThat(scan("SELECT FROM " + parent + " WHERE k = 3")).isZero();
          assertThat(scan("SELECT FROM " + parent + " WHERE k = 5")).isEqualTo(1);
        } else {
          assertThat(database.getSchema().getIndexByName(index).countEntries()).as("rolled back: 1, 2, 3, 4").isEqualTo(4L);
          assertThat(scan("SELECT FROM " + parent + " WHERE k = 2")).isEqualTo(1);
          assertThat(scan("SELECT FROM " + parent + " WHERE k = 3")).isEqualTo(1);
          assertThat(scan("SELECT FROM " + parent + " WHERE k = 20")).isZero();
          assertThat(scan("SELECT FROM " + parent + " WHERE k = 5")).isZero();
        }
      }
    }

    /**
     * The geospatial family counts one entry per covering cell, so its count is compared with an index built the plain way over
     * the same final records instead of with a number written down.
     */
    @Test
    void aGeospatialIndexBuiltInAWritingTransactionHoldsWhatTheOneBuiltAfterwardsHolds() {
      for (final boolean commit : new boolean[] { true, false }) {
        final String suffix = commit ? "Cm" : "Rb";
        final String type = "Geo" + suffix;
        final String twin = "GeoTwin" + suffix;
        for (final String name : new String[] { type, twin }) {
          database.command("sql", "CREATE DOCUMENT TYPE " + name);
          database.command("sql", "CREATE PROPERTY " + name + ".loc STRING");
          database.transaction(() -> {
            database.command("sql", "INSERT INTO " + name + " SET loc = 'POINT (10.0 45.0)'").close();
            database.command("sql", "INSERT INTO " + name + " SET loc = 'POINT (11.0 46.0)'").close();
            database.command("sql", "INSERT INTO " + name + " SET loc = 'POINT (12.0 47.0)'").close();
          });
        }

        database.begin();
        database.command("sql", "INSERT INTO " + type + " SET loc = 'POINT (13.0 48.0)'");
        database.command("sql", "UPDATE " + type + " SET loc = 'POINT (20.0 50.0)' WHERE loc = 'POINT (10.0 45.0)'");
        database.command("sql", "DELETE FROM " + type + " WHERE loc = 'POINT (11.0 46.0)'");
        database.command("sql", "CREATE INDEX ON " + type + " (loc) GEOSPATIAL");
        if (commit)
          database.commit();
        else
          database.rollback();

        if (commit) {
          // the twin gets the same final records, then the index
          database.transaction(() -> {
            database.command("sql", "INSERT INTO " + twin + " SET loc = 'POINT (13.0 48.0)'").close();
            database.command("sql", "UPDATE " + twin + " SET loc = 'POINT (20.0 50.0)' WHERE loc = 'POINT (10.0 45.0)'").close();
            database.command("sql", "DELETE FROM " + twin + " WHERE loc = 'POINT (11.0 46.0)'").close();
          });
        }
        database.command("sql", "CREATE INDEX ON " + twin + " (loc) GEOSPATIAL");

        assertThat(entries(type)).as("geospatial entries, " + (commit ? "committed" : "rolled back")).isEqualTo(entries(twin));
        assertThat(entries(type)).isGreaterThan(0L);
      }
    }

    @Test
    void aBucketIndexBuiltAfterAWriteToItsBucketKeepsTheCommittedRecordsOnRollback() {
      final DocumentType type = database.getSchema().createDocumentType("BucketIdx", 1);
      type.createProperty("k", Type.INTEGER);
      final String bucket = type.getBuckets(false).getFirst().getName();
      database.transaction(() -> {
        for (int i = 1; i <= 3; i++)
          database.newDocument("BucketIdx").set("k", i).save();
      });

      database.begin();
      database.newDocument("BucketIdx").set("k", 9).save();
      final Index index = database.getSchema().buildBucketIndex("BucketIdx", bucket, new String[] { "k" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      database.rollback();

      assertThat(index.countEntries()).isEqualTo(3L);
    }

    private String createTypeWithThreeCommittedRecords(final String name) {
      database.command("sql", "CREATE DOCUMENT TYPE " + name);
      database.command("sql", "CREATE PROPERTY " + name + ".k INTEGER");
      database.command("sql", "CREATE PROPERTY " + name + ".s STRING");
      database.command("sql", "CREATE PROPERTY " + name + ".tags LIST");
      database.transaction(() -> {
        for (int i = 1; i <= 3; i++)
          database.command("sql", "INSERT INTO " + name + " SET k = " + i + ", s = 'w" + i + "', tags = ['a" + i + "']").close();
      });
      return name;
    }

    /** Rows a lookup answers; the SEARCH_INDEX spelling carries the type twice. */
    private int find(final String sql, final String type) {
      return scan(sql.contains("%s[") ? sql.formatted(type, type) : sql.formatted(type));
    }

    private int scan(final String sql) {
      final List<Object> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows.size();
    }

    private long entries(final String type) {
      long entries = 0;
      for (final Index index : database.getSchema().getType(type).getAllIndexes(true))
        entries = Math.max(entries, index.countEntries());
      return entries;
    }
  }

  /**
   * Regression for #10588: {@code CREATE INDEX} silently skipped a record another session had written and not yet committed. The build scans
   * the committed state, and the writer's pages are invisible to it; the writer's commit staged no entry either, because the index
   * did not exist when the record was saved. The record stayed invisible to every index-backed query, and a UNIQUE index accepted a
   * duplicate of its key - with {@code CREATE INDEX} reporting success. The same hole opened for an OUTER frame of the caller's own
   * stack (an empty nested transaction over an outer one that wrote a record, which is what a {@code sqlscript} with its own
   * {@code BEGIN} looks like inside an open transaction).
   * <p>
   * The rule: every record that is in the type when a build ends has an entry in the index that build created. A transaction that
   * began before an index was registered on a bucket it wrote makes up the entries of that index at its own commit, under the same
   * read lock that keeps a {@code CREATE INDEX} out, whatever the build did.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreateIndexBesideUncommittedWriter extends TestHelper {

    private static final int COMMITTED = 20;

    // ---- shape A: another session holds the write

    @Test
    void aUniqueIndexBuiltBesideAnotherSessionsUncommittedWriteStillEnforcesItsKey() throws Exception {
      createPopulatedType("T", "p");

      runWriterAcrossIndexCreation(() -> database.newDocument("T").set("p", 999).save(), () -> database.command("sql", "CREATE INDEX ON T (p) UNIQUE"));

      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 999)).isEqualTo(1L);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("T").set("p", 999).save())).isInstanceOf(
          DuplicatedKeyException.class);
      assertThat(database.countType("T", true)).isEqualTo(COMMITTED + 1);
    }

    @Test
    void aNotUniqueIndexBuiltBesideAnotherSessionsUncommittedWriteFindsTheRecord() throws Exception {
      createPopulatedType("T", "p");

      runWriterAcrossIndexCreation(() -> database.newDocument("T").set("p", 999).save(), () -> database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE"));

      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 999)).as("through the index").isEqualTo(1L);
    }

    @Test
    void anUpdateAndADeleteMadeBesideTheBuildAreReflectedInTheIndex() throws Exception {
      createPopulatedType("T", "p");

      runWriterAcrossIndexCreation(() -> {
        // an update that moves a key, a delete, a record created and deleted, and a record created and updated
        final Document moved = database.query("sql", "SELECT FROM T WHERE p = 5").next().toElement();
        moved.modify().set("p", 500).save();
        database.query("sql", "SELECT FROM T WHERE p = 7").next().toElement().delete();
        database.newDocument("T").set("p", 777).save().delete();
        final Document created = database.newDocument("T").set("p", 888).save();
        created.modify().set("p", 889).save();
      }, () -> database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE"));

      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 5)).isZero();
      assertThat(lookup("T", "p", 500)).isEqualTo(1L);
      assertThat(lookup("T", "p", 7)).isZero();
      assertThat(lookup("T", "p", 777)).isZero();
      assertThat(lookup("T", "p", 888)).isZero();
      assertThat(lookup("T", "p", 889)).isEqualTo(1L);
      assertThat(database.countType("T", true)).isEqualTo(COMMITTED - 1 + 1);
    }

    @Test
    void aWriterThatRollsBackLeavesNoEntry() throws Exception {
      createPopulatedType("T", "p");

      final CountDownLatch written = new CountDownLatch(1);
      final CountDownLatch indexed = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.begin();
          database.newDocument("T").set("p", 999).save();
          written.countDown();
          indexed.await(30, TimeUnit.SECONDS);
          database.rollback();
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      writer.start();
      assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
      database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
      indexed.countDown();
      writer.join(30_000);
      assertThat(failure.get()).isNull();

      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 999)).isZero();
      database.transaction(() -> database.newDocument("T").set("p", 999).save());
      assertThat(lookup("T", "p", 999)).isEqualTo(1L);
    }

    @Test
    void aWriterThatCommitsADuplicateOfAKeyTakenAfterTheBuildIsRefusedAtItsCommit() throws Exception {
      createPopulatedType("T", "p");

      final CountDownLatch written = new CountDownLatch(1);
      final CountDownLatch indexed = new CountDownLatch(1);
      final AtomicReference<Throwable> commitFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.begin();
          database.newDocument("T").set("p", 999).save();
          written.countDown();
          indexed.await(30, TimeUnit.SECONDS);
          try {
            database.commit();
          } catch (final Throwable t) {
            commitFailure.set(t);
            if (database.isTransactionActive())
              database.rollback();
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      writer.start();
      assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
      database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
      // another session takes the key while the writer's record is still pending: the index is live, so this commit wins the key
      database.transaction(() -> database.newDocument("T").set("p", 999).save());
      indexed.countDown();
      writer.join(30_000);

      assertThat(commitFailure.get()).as("the pending record would be a second holder of the key").isInstanceOf(DuplicatedKeyException.class);
      assertThat(lookup("T", "p", 999)).isEqualTo(1L);
      assertThat(database.countType("T", true)).isEqualTo(COMMITTED + 1);
      assertIndexCompleteFor("T", "p");
    }

    @Test
    void aWriterThatSavedBothBeforeAndAfterTheIndexExistedIsRefusedRetryablyAndTheRetryIsIndexedOnce() throws Exception {
      savedBeforeAndAfterTheIndexExisted("UNIQUE");
    }

    @Test
    void aWriterThatSavedBothBeforeAndAfterANotUniqueIndexExistedIsRefusedRetryablyAndTheRetryIsIndexedOnce() throws Exception {
      savedBeforeAndAfterTheIndexExisted("NOTUNIQUE");
    }

    /**
     * The entries of what the writer saved AFTER the index existed are already queued on its transaction, and an entry it owes for what
     * it saved BEFORE cannot be told from them: queueing the owed ones from the committed state would queue the others twice, which an
     * index that counts what it is given (the corpus counters of a full-text index) takes for two documents. So the commit is refused
     * with a retryable exception, and the retry finds the index there from its first write.
     */
    private void savedBeforeAndAfterTheIndexExisted(final String kind) throws Exception {
      // one bucket, so the writes before and after the index land in the same one: in a type of several buckets each bucket is
      // judged by what the transaction wrote to it, and writes to two different buckets are both exact
      createPopulatedType("T", "p", 1);

      final CountDownLatch written = new CountDownLatch(1);
      final CountDownLatch indexed = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicReference<Throwable> commitFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.begin();
          database.newDocument("T").set("p", 900).save();
          written.countDown();
          indexed.await(30, TimeUnit.SECONDS);
          database.newDocument("T").set("p", 901).save();
          database.query("sql", "SELECT FROM T WHERE p = 3").next().toElement().modify().set("p", 303).save();
          try {
            database.commit();
          } catch (final Throwable t) {
            commitFailure.set(t);
            if (database.isTransactionActive())
              database.rollback();
          }
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      writer.start();
      assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
      database.command("sql", "CREATE INDEX ON T (p) " + kind);
      indexed.countDown();
      writer.join(30_000);
      assertThat(failure.get()).isNull();

      assertThat(commitFailure.get()).isInstanceOf(NeedRetryException.class).hasMessageContaining("was created on type 'T'");
      // nothing of the refused transaction is there, and the index describes the committed state
      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 900)).isZero();
      assertThat(lookup("T", "p", 901)).isZero();
      assertThat(lookup("T", "p", 3)).isEqualTo(1L);

      // the retry
      database.transaction(() -> {
        database.newDocument("T").set("p", 900).save();
        database.newDocument("T").set("p", 901).save();
        database.query("sql", "SELECT FROM T WHERE p = 3").next().toElement().modify().set("p", 303).save();
      });
      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 900)).isEqualTo(1L);
      assertThat(lookup("T", "p", 901)).isEqualTo(1L);
      assertThat(lookup("T", "p", 3)).isZero();
      assertThat(lookup("T", "p", 303)).isEqualTo(1L);
    }

    /** The refusal of a writer on both sides of the index is retried by the retry loop of {@code Database.transaction()}, and succeeds. */
    @Test
    void theRetryLoopOfTransactionAbsorbsTheRefusalOfAWriterOnBothSidesOfTheIndex() throws Exception {
      createPopulatedType("T", "p", 1);

      final CountDownLatch written = new CountDownLatch(1);
      final CountDownLatch indexed = new CountDownLatch(1);
      final AtomicInteger attempts = new AtomicInteger();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.transaction(() -> {
            final boolean firstAttempt = attempts.incrementAndGet() == 1;
            database.newDocument("T").set("p", 900).save();
            if (firstAttempt) {
              written.countDown();
              try {
                indexed.await(30, TimeUnit.SECONDS);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
            database.newDocument("T").set("p", 901).save();
          });
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      writer.start();
      assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
      database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
      indexed.countDown();
      writer.join(30_000);

      assertThat(failure.get()).isNull();
      assertThat(attempts.get()).as("the first attempt was refused, the second committed").isEqualTo(2);
      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 900)).isEqualTo(1L);
      assertThat(lookup("T", "p", 901)).isEqualTo(1L);
    }

    @Test
    void aFullTextIndexBuiltBesideAnotherSessionsWriteCountsItsDocumentsOnce() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2").close();
      database.command("sql", "CREATE PROPERTY Doc.content STRING").close();
      database.transaction(() -> {
        for (int i = 0; i < COMMITTED; i++)
          database.newDocument("Doc").set("content", "alpha beta " + i).save();
      });

      runWriterAcrossIndexCreation(() -> database.newDocument("Doc").set("content", "alpha gamma pending").save(),
          () -> database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT"));

      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'pending') = true")) {
        assertThat(rs.stream().count()).as("the pending record, found through the full-text index").isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true")) {
        final String plan = rs.next().getProperty("executionPlanAsString");
        assertThat(plan).as("the corpus counters hold each document once").contains("\"totalDocs\":" + (COMMITTED + 1) + ",");
      }
    }

    @Test
    void aFullTextIndexBuiltInTheSameTransactionCountsItsDocumentsOnce() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2").close();
      database.command("sql", "CREATE PROPERTY Doc.content STRING").close();
      database.transaction(() -> {
        for (int i = 0; i < COMMITTED; i++)
          database.newDocument("Doc").set("content", "alpha beta " + i).save();
      });

      database.begin();
      database.newDocument("Doc").set("content", "alpha gamma pending").save();
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      database.commit();

      try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'alpha') = true")) {
        final String plan = rs.next().getProperty("executionPlanAsString");
        assertThat(plan).as("the build queued the caller's record, and the commit did not queue it again")
            .contains("\"totalDocs\":" + (COMMITTED + 1) + ",");
      }
    }

    @Test
    void aWriterThatBeganAfterTheIndexExistedIsNotTouched() throws Exception {
      createPopulatedType("T", "p");
      database.command("sql", "CREATE INDEX ON T (p) UNIQUE");

      database.transaction(() -> database.newDocument("T").set("p", 999).save());

      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 999)).isEqualTo(1L);
    }

    @Test
    void aWriterThatOnlyWroteToTheTypeAfterTheIndexExistedIsNotRefusedNorQueuedTwice() throws Exception {
      createPopulatedType("T", "p");
      createPopulatedType("U", "p");

      final CountDownLatch written = new CountDownLatch(1);
      final CountDownLatch indexed = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          // the transaction began before the index existed and wrote to ANOTHER type, so it owes the index nothing: what it writes to T
          // from here on is queued as it is written
          database.begin();
          database.newDocument("U").set("p", 777).save();
          written.countDown();
          indexed.await(30, TimeUnit.SECONDS);
          database.newDocument("T").set("p", 999).save();
          database.commit();
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      writer.start();
      assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
      database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
      indexed.countDown();
      writer.join(30_000);
      assertThat(failure.get()).isNull();

      assertIndexCompleteFor("T", "p");
      assertThat(lookup("T", "p", 999)).isEqualTo(1L);
      assertThat(database.countType("U", true)).isEqualTo(COMMITTED + 1);
    }

    @Test
    void aTypeRenamedBetweenTheIndexAndTheCommitStillGetsTheEntriesTheWriterOwes() throws Exception {
      createPopulatedType("T", "p");

      runWriterAcrossIndexCreation(() -> database.newDocument("T").set("p", 999).save(), () -> {
        database.command("sql", "CREATE INDEX ON T (p) UNIQUE");
        // the sub-indexes are renamed with their type, and the registration of the index has to follow
        database.command("sql", "ALTER TYPE T NAME T2");
      });

      assertIndexCompleteFor("T2", "p");
      assertThat(lookup("T2", "p", 999)).isEqualTo(1L);
    }

    @Test
    void aTransactionCommittingUnderneathAnotherOneThatOwesEntriesIsRefusedNotSkipped() {
      createPopulatedType("V", "id");

      database.begin();
      database.newDocument("V").set("id", 999).save();
      database.begin();
      try {
        database.command("sql", "CREATE INDEX ON V (id) UNIQUE");

        final TransactionContext outer = DatabaseContext.INSTANCE.getContext(((DatabaseInternal) database).getDatabasePath()).transactions.get(0);
        assertThatThrownBy(outer::commit).isInstanceOf(TransactionException.class).hasMessageContaining("innermost");
      } finally {
        if (database.isTransactionActive())
          database.rollbackAllNested();
      }

      assertThat(database.isTransactionActive()).isFalse();
      assertIndexCompleteFor("V", "id");
      assertThat(lookup("V", "id", 999)).isZero();
      assertThat(database.countType("V", true)).isEqualTo(COMMITTED);
    }

    @Test
    void aWriterOfAnotherTypeIsNotTouched() throws Exception {
      createPopulatedType("T", "p");
      createPopulatedType("U", "p");

      runWriterAcrossIndexCreation(() -> database.newDocument("U").set("p", 999).save(), () -> database.command("sql", "CREATE INDEX ON T (p) UNIQUE"));

      assertIndexCompleteFor("T", "p");
      assertThat(database.countType("U", true)).isEqualTo(COMMITTED + 1);
    }

    // ---- shape B: an outer frame of the caller's own stack holds the write

    @Test
    void anOuterFrameOfTheCallersOwnStackThatWroteIsIndexedAtItsCommit() {
      createPopulatedType("V", "id");

      database.begin();
      database.newDocument("V").set("id", 999).save();
      database.command("sqlscript", "BEGIN;\nCREATE INDEX ON V (id) UNIQUE;\nCOMMIT;");
      database.commit();

      assertIndexCompleteFor("V", "id");
      assertThat(lookup("V", "id", 999)).isEqualTo(1L);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("V").set("id", 999).save())).isInstanceOf(
          DuplicatedKeyException.class);
    }

    @Test
    void aNotUniqueIndexBuiltFromAnEmptyInnerFrameFindsTheOuterFramesRecord() {
      createPopulatedType("V", "id");

      database.begin();
      database.newDocument("V").set("id", 999).save();
      database.command("sqlscript", "BEGIN;\nCREATE INDEX ON V (id) NOTUNIQUE;\nCOMMIT;");
      database.commit();

      assertIndexCompleteFor("V", "id");
      assertThat(lookup("V", "id", 999)).isEqualTo(1L);
    }

    @Test
    void aBucketIndexBuiltFromAnEmptyInnerFrameFindsTheOuterFramesRecord() {
      createPopulatedType("V", "id");
      final DocumentType type = database.getSchema().getType("V");

      database.begin();
      final RID rid = database.newDocument("V").set("id", 999).save().getIdentity();
      database.begin();
      for (final var bucket : type.getBuckets(false))
        database.getSchema().buildBucketIndex("V", bucket.getName(), new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false).create();
      database.commit();
      database.commit();

      assertThat(rid).isNotNull();
      assertIndexCompleteFor("V", "id");
      assertThat(lookup("V", "id", 999)).isEqualTo(1L);
    }

    @Test
    void anInheritedIndexPropagatedFromAnEmptyInnerFrameFindsTheOuterFramesRecord() {
      database.command("sql", "CREATE DOCUMENT TYPE P").close();
      database.command("sql", "CREATE PROPERTY P.id INTEGER").close();
      database.command("sql", "CREATE INDEX ON P (id) UNIQUE").close();
      database.command("sql", "CREATE DOCUMENT TYPE C").close();
      database.transaction(() -> {
        for (int i = 0; i < COMMITTED; i++)
          database.newDocument("C").set("id", 10_000 + i).save();
      });

      database.begin();
      database.newDocument("C").set("id", 999).save();
      database.begin();
      database.getSchema().getType("C").addSuperType("P");
      database.commit();
      database.commit();

      // the index of P now covers the buckets of C, and the record the outer frame wrote is in one of them
      assertThat(database.getSchema().getType("P").getAllIndexes(true)).isNotEmpty();
      assertThat(lookup("P", "id", 999)).isEqualTo(1L);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("C").set("id", 999).save())).isInstanceOf(
          DuplicatedKeyException.class);
    }

    @Test
    void oneFrameOnlyStaysCleanAsItWas() {
      createPopulatedType("V", "id");

      database.begin();
      database.newDocument("V").set("id", 999).save();
      database.command("sql", "CREATE INDEX ON V (id) UNIQUE");
      database.commit();

      assertIndexCompleteFor("V", "id");
      assertThat(lookup("V", "id", 999)).isEqualTo(1L);
    }

    // ---- helpers

    private void createPopulatedType(final String type, final String property) {
      createPopulatedType(type, property, 4);
    }

    private void createPopulatedType(final String type, final String property, final int buckets) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets).close();
      database.command("sql", "CREATE PROPERTY " + type + "." + property + " INTEGER").close();
      database.transaction(() -> {
        for (int i = 0; i < COMMITTED; i++)
          database.newDocument(type).set(property, i).save();
      });
    }

    /**
     * The writer thread does its writes and holds the transaction open; this thread runs the DDL; then the writer commits. The write
     * is staged before the index exists and committed after it was built.
     */
    private void runWriterAcrossIndexCreation(final Runnable writes, final Runnable ddl) throws Exception {
      final CountDownLatch written = new CountDownLatch(1);
      final CountDownLatch indexed = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.begin();
          writes.run();
          written.countDown();
          indexed.await(30, TimeUnit.SECONDS);
          database.commit();
        } catch (final Throwable t) {
          failure.set(t);
          written.countDown();
        }
      }, "writer-beside-create-index");
      writer.start();
      assertThat(written.await(30, TimeUnit.SECONDS)).isTrue();
      try {
        ddl.run();
      } finally {
        indexed.countDown();
      }
      writer.join(30_000);
      assertThat(writer.isAlive()).isFalse();
      assertThat(failure.get()).isNull();
    }

    /** Every record of the type has an entry in the index, and a lookup finds each of the records a scan finds. */
    private void assertIndexCompleteFor(final String type, final String property) {
      final TypeIndex index = database.getSchema().getType(type).getPolymorphicIndexByProperties(property);
      assertThat(index).as("an index on %s.%s", type, property).isNotNull();
      assertThat(index.countEntries()).as("entries of the index vs records of the type").isEqualTo(database.countType(type, true));

      final List<Integer> values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT " + property + " FROM " + type)) {
        while (rs.hasNext())
          values.add(rs.next().getProperty(property));
      }
      for (final Integer value : values)
        assertThat(lookup(type, property, value)).as("%s.%s = %d through the index", type, property, value).isEqualTo(1L);
    }

    /** Records found by an index-backed equality, as opposed to a scan. */
    private long lookup(final String type, final String property, final int value) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE " + property + " = ?", value)) {
        return rs.stream().count();
      }
    }
  }

  /**
   * Regression for #10589: a refused {@code DROP INDEX} left the index half dropped. Dropping a {@link TypeIndex} walks its bucket sub-indexes and
   * deletes one file per sub-index; nothing asked first whether those files could be deleted (issue #10421 added that probe to
   * {@code DROP TYPE}, {@code ALTER TYPE ... BUCKET} and {@code ... SUPERTYPE} only), so the first file that refused aborted the statement
   * with every sub-index before it already dropped, unregistered and saved. The wrapper stayed registered over a SUBSET of the buckets,
   * and every record of a bucket whose sub-index was gone answered 0 rows through the index, across a restart. When the refusing file
   * was the last one the wrapper was left empty, which wedged every later DDL.
   * <p>
   * The failure is injected as {@code SchemaDdlAtomicityTest.SchemaStatementsAreAtomic} does: the file of a sub-index is moved aside and a
   * non-empty directory takes its path, which {@code Files.delete} refuses. Every statement that reaches {@code dropIndexInternal} is
   * covered, and the ones that drop several indexes are checked with the refusing file on each of them in turn, so the index dropped
   * BEFORE the refusal is the one that proves nothing was changed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DropIndexIsAtomic extends TestHelper {

    private static final int BUCKETS = 4;
    private static final int RECORDS = 12;

    /** A directory with something in it where a file of the schema is: it cannot be deleted. */
    private record Fault(Path file, Path aside) {
      void remove() throws IOException {
        Files.deleteIfExists(file.resolve("blocker"));
        Files.deleteIfExists(file);
        Files.move(aside, file);
      }
    }

    private Fault injectFault(final IndexInternal subIndex) throws IOException {
      assumeFalse(System.getProperty("os.name", "").toLowerCase().contains("win"), "an open file cannot be moved on Windows");
      final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getFilePath());
      final Path aside = file.resolveSibling(file.getFileName() + ".aside");
      Files.move(file, aside);
      Files.createDirectory(file);
      Files.writeString(file.resolve("blocker"), "x");
      return new Fault(file, aside);
    }

    @Test
    void aDropIndexThatCannotDeleteAMiddleFileChangesNothing() throws IOException {
      createIndexedType();
      refusedDropLeavesTheIndexWhole("DROP INDEX `T[p]`", 1);
    }

    @Test
    void aDropIndexThatCannotDeleteTheLastFileChangesNothing() throws IOException {
      createIndexedType();
      refusedDropLeavesTheIndexWhole("DROP INDEX `T[p]`", BUCKETS - 1);
    }

    @Test
    void aDropIndexThatCannotDeleteTheFirstFileChangesNothing() throws IOException {
      createIndexedType();
      refusedDropLeavesTheIndexWhole("DROP INDEX `T[p]`", 0);
    }

    @Test
    void aDropOfTheWrapperFromTheJavaApiChangesNothingEither() throws IOException {
      createIndexedType();
      final Fault fault = injectFault(subIndexOf("T[p]", 2));
      try {
        final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("T[p]");
        assertThatThrownBy(index::drop).hasStackTraceContaining("Nothing was changed");

        assertIndexWhole("T[p]");
      } finally {
        fault.remove();
      }
      reopenDatabase();
      assertIndexWhole("T[p]");
    }

    @Test
    void aDropPropertyForceThatCannotDeleteAFileKeepsThePropertyAndItsIndex() throws IOException {
      createIndexedType();
      refusedDropLeavesTheIndexWhole("DROP PROPERTY T.p FORCE", 1);
      assertThat(database.getSchema().getType("T").existsProperty("p")).isTrue();
    }

    @Test
    void aRebuildIndexThatCannotDeleteAFileKeepsTheIndexWhole() throws IOException {
      createIndexedType();
      refusedDropLeavesTheIndexWhole("REBUILD INDEX `T[p]`", 1);
    }

    @Test
    void aTruncateTypeThatCannotDeleteAFileKeepsTheIndexAndTheRecords() throws IOException {
      createIndexedType();
      refusedDropLeavesTheIndexWhole("TRUNCATE TYPE T", 1);
      assertThat(database.countType("T", true)).isEqualTo(RECORDS);
    }

    @Test
    void aTruncateTypeWithoutIndexesIsNotASchemaChange() {
      database.command("sql", "CREATE DOCUMENT TYPE Plain").close();
      database.transaction(() -> database.newDocument("Plain").set("x", 1).save());
      final long versionBefore = database.getSchema().getEmbedded().getVersion();

      database.command("sql", "TRUNCATE TYPE Plain").close();

      assertThat(database.countType("Plain", true)).isZero();
      assertThat(database.getSchema().getEmbedded().getVersion()).as("no index to drop, no schema.json saved").isEqualTo(versionBefore);
    }

    @Test
    void aTruncateTypeOverSeveralIndexesDropsNoneWhenTheFirstDroppedWouldNotBeTheRefusedOne() throws IOException {
      for (final String refusing : new String[] { "p", "q" }) {
        createTwoIndexedType();
        final Fault fault = injectFault(subIndexOf("T[" + refusing + "]", 1));
        try {
          assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE T")).hasStackTraceContaining("Nothing was changed");

          assertIndexWhole("T[p]");
          assertIndexWhole("T[q]");
          assertThat(database.countType("T", true)).isEqualTo(RECORDS);
        } finally {
          fault.remove();
        }
        reopenDatabase();
        assertIndexWhole("T[p]");
        assertIndexWhole("T[q]");
        database.command("sql", "DROP TYPE T");
      }
    }

    @Test
    void aDropPropertyForceOverSeveralIndexesDropsNoneWhenOneRefuses() throws IOException {
      for (final String refusing : new String[] { "T[p]", "T[p,q]" }) {
        database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS " + BUCKETS);
        database.command("sql", "CREATE PROPERTY T.p STRING");
        database.command("sql", "CREATE PROPERTY T.q STRING");
        database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON T (p, q) NOTUNIQUE");
        insertRecords();

        final Fault fault = injectFault(subIndexOf(refusing, 1));
        try {
          assertThatThrownBy(() -> database.command("sql", "DROP PROPERTY T.p FORCE")).hasStackTraceContaining("Nothing was changed");

          assertIndexWhole("T[p]");
          assertIndexWhole("T[p,q]");
          assertThat(database.getSchema().getType("T").existsProperty("p")).isTrue();
        } finally {
          fault.remove();
        }
        reopenDatabase();
        assertIndexWhole("T[p]");
        assertIndexWhole("T[p,q]");
        database.command("sql", "DROP TYPE T");
      }
    }

    @Test
    void anEdgeTypeThatCannotDeleteTheFileOfItsUniqueIndexKeepsTheConstraint() throws IOException {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E UNIQUE");
      final TypeIndex unique = (TypeIndex) database.getSchema().getIndexByName(LocalEdgeType.uniqueIndexName("E"));
      final int subIndexes = unique.getIndexesOnBuckets().length;
      final Fault fault = injectFault(unique.getIndexesOnBuckets()[subIndexes - 1]);
      try {
        assertThatThrownBy(() -> database.command("sql", "ALTER TYPE E WITH unique = false")).hasStackTraceContaining("Nothing was changed");

        assertThat(((TypeIndex) database.getSchema().getIndexByName(LocalEdgeType.uniqueIndexName("E"))).getIndexesOnBuckets()).hasSize(
            subIndexes);
        assertThat(((LocalEdgeType) database.getSchema().getType("E")).isUnique()).isTrue();
      } finally {
        fault.remove();
      }
    }

    // ---- helpers

    private void refusedDropLeavesTheIndexWhole(final String statement, final int refusingSubIndex) throws IOException {
      final Fault fault = injectFault(subIndexOf("T[p]", refusingSubIndex));
      try {
        assertThatThrownBy(() -> database.command("sql", statement)).hasStackTraceContaining("Nothing was changed");

        assertIndexWhole("T[p]");
        // a later DDL of any kind is not wedged by a wrapper without sub-indexes
        database.command("sql", "CREATE DOCUMENT TYPE Z");
        database.command("sql", "DROP TYPE Z");
      } finally {
        fault.remove();
      }

      reopenDatabase();

      assertIndexWhole("T[p]");

      // and the statement completes once nothing refuses it
      if (statement.startsWith("DROP INDEX")) {
        database.command("sql", statement);
        assertThat(database.getSchema().existsIndex("T[p]")).isFalse();
        assertThat(database.countType("T", true)).isEqualTo(RECORDS);
      }
    }

    /** The index has a sub-index on every bucket of the type, and every record is found through it. */
    private void assertIndexWhole(final String indexName) {
      assertThat(database.getSchema().existsIndex(indexName)).isTrue();
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
      assertThat(index.getIndexesOnBuckets()).hasSize(BUCKETS);
      assertThat(index.countEntries()).isEqualTo(RECORDS);
      for (final IndexInternal subIndex : index.getIndexesOnBuckets())
        assertThat(database.getSchema().existsIndex(subIndex.getName())).isTrue();

      // a lookup through the index finds every record, including the ones of the buckets a half dropped index would have lost
      final boolean onQ = indexName.equals("T[q]");
      for (int i = 0; i < RECORDS; i++)
        assertThat(database.query("sql", "SELECT FROM T WHERE " + (onQ ? "q" : "p") + " = ?", (onQ ? "w" : "v") + i).stream().count()).as(
            "record %d through %s", i, indexName).isEqualTo(1L);
    }

    private IndexInternal subIndexOf(final String indexName, final int position) {
      return ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets()[position];
    }

    private void createIndexedType() {
      database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY T.p STRING");
      database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE");
      insertRecords();
    }

    private void createTwoIndexedType() {
      database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY T.p STRING");
      database.command("sql", "CREATE PROPERTY T.q STRING");
      database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON T (q) NOTUNIQUE");
      insertRecords();
    }

    private void insertRecords() {
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument("T").set("p", "v" + i).set("q", "w" + i).save();
      });
    }
  }
}
