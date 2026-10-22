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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10334: {@code CREATE INDEX ... UNIQUE} on a populated type was quadratic with the default build mode. Every
 * batch of the record-at-a-time build commits through the uniqueness check, which looks each key up in the index being
 * built, and that lookup walks every page of the mutable index - a number of pages that grows with every record added,
 * since nothing compacts an index while it is built. A UNIQUE LSM index over a type holding at least
 * {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS} records is now built by the sorted pipeline (an
 * external sort that refuses a duplicate key in the stream) unless the build cannot take it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10334UniqueIndexSortedBuildTest extends TestHelper {
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
