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
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10349 (follow-up of #10334): {@code CREATE INDEX ... UNIQUE} on a populated type stayed quadratic wherever the sorted build
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
class Issue10349DeferredUniqueVerificationTest extends TestHelper {
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
