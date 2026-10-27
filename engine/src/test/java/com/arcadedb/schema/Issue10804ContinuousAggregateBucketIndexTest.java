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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10804: every ingest request refreshes the continuous aggregates over its type, and a refresh deleted the buckets at or
 * after the watermark before it recomputed them. The backing type had no index, so that delete read every row the aggregate ever
 * wrote: measured with {@code Issue10804IngestRefreshCostBenchmark}, an autocommit INSERT into the open bucket of an aggregate holding
 * 100k rows cost 9 ms, 400 times the INSERT alone, and the cost grew with the history of the aggregate, not with the bucket. The open
 * bucket itself, the cost the issue was filed for, is 10 to 30 times the INSERT and bounded by the bucket.
 * <p>
 * The backing type is indexed on its bucket column now and both reads of the rows a refresh replaces are served by it. The rows are
 * rewritten in place rather than deleted and inserted again: with the index, the delete and insert of the open bucket's rows under one
 * key on every request left a tombstone and an entry per request that every lookup of the bucket walked (35 ms a request after 20k).
 * An aggregate created before is named at open with the statements that index it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10804ContinuousAggregateBucketIndexTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private void createSourceAndAggregate() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  private void insert(final long ts, final String tag, final double v) {
    database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", ts, tag, v);
  }

  private record Bucket(long start, String tag, double total) {
  }

  private List<Bucket> buckets() {
    final List<Bucket> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM A ORDER BY hr, h")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), row.getProperty("h"),
            ((Number) row.getProperty("total")).doubleValue()));
      }
    }
    return result;
  }

  @Test
  void theBackingTypeIsIndexedOnItsBucketColumn() {
    createSourceAndAggregate();

    final DocumentType backing = aggregate().getBackingType();
    assertThat(backing.getProperty("hr").getType()).isEqualTo(Type.DATETIME);
    final TypeIndex index = backing.getPolymorphicIndexByProperties("hr");
    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(index.isUnique()).isFalse();
    assertThat(aggregate().getUpgradeWarning()).isNull();
  }

  /** Both reads of the rows a refresh replaces are the index's, so neither reads the buckets the refresh has no reason to look at. */
  @Test
  void bothReadsOfTheReplacedRowsAreServedByTheIndex() {
    createSourceAndAggregate();

    assertThat(plan(ContinuousAggregateRefresher.bucketsFromSelect(aggregate()), new Date(H0))).contains("FETCH FROM INDEX");
    assertThat(plan(ContinuousAggregateRefresher.bucketRangeSelect(aggregate()), new Date(H0), new Date(H0 + HOUR)))
        .contains("FETCH FROM INDEX");
  }

  /**
   * The row of the open bucket is the same record however many requests recompute it, and so is the row of a bucket a late sample
   * recomputes: its bucket key never changes, so the index on it is not written, and no request leaves a hole in the backing type.
   */
  @Test
  void aRecomputedBucketRewritesItsRowInPlace() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 1);
    final RID openBucketRow = rowOf(H0);
    final long deletedBefore = statistic("deleteRecord");
    final long createdBefore = statistic("createRecord");
    for (int i = 0; i < 50; i++)
      insert(H0 + 2_000 + i, "a", 1);
    // a delete and an insert of the row would reuse its freed slot as well: the RID alone does not tell the two apart
    assertThat(statistic("deleteRecord") - deletedBefore).as("rows deleted by 50 refreshes of the open bucket").isZero();
    assertThat(statistic("createRecord") - createdBefore).as("rows created by 50 refreshes of the open bucket").isZero();
    assertThat(rowOf(H0)).isEqualTo(openBucketRow);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 51));

    // a new bucket moves the watermark; a late sample then recomputes the closed bucket, through the range of the late samples
    insert(H0 + 2 * HOUR, "a", 7);
    final long deletedBeforeLate = statistic("deleteRecord");
    insert(H0 + 3_000_000, "a", 9);
    assertThat(statistic("deleteRecord") - deletedBeforeLate).as("rows deleted by the recompute of a late sample's bucket").isZero();
    assertThat(rowOf(H0)).isEqualTo(openBucketRow);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 60), new Bucket(H0 + 2 * HOUR, "a", 7));
    assertThat(database.countType("A", false)).isEqualTo(2L);
  }

  /**
   * A recomputed range whose buckets gain rows and lose rows: a late sample of a tag new to a closed bucket adds a row to it (no
   * replaced row left to take over, so it is inserted), a delete of every sample of a tag in another closed bucket removes one (a
   * replaced row nothing took over, so it is deleted), and the open bucket does both in turn.
   */
  @Test
  void aRecomputedBucketThatGainsOrLosesRowsInsertsOrDeletesOnlyThose() {
    createSourceAndAggregate();
    for (int hour = 0; hour < 3; hour++) {
      insert(H0 + hour * HOUR + 1_000, "a", 1);
      insert(H0 + hour * HOUR + 2_000, "b", 10);
    }
    insert(H0 + 4 * HOUR + 1_000, "a", 100);

    // grows: tag c is new to hour 0, behind the watermark
    insert(H0 + 5_000, "c", 1000);
    // shrinks: every sample of tag b in hour 1 is deleted
    database.command("sql", "DELETE FROM S WHERE ts >= ? AND ts < ? AND h = 'b'", H0 + HOUR, H0 + 2 * HOUR);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 1), new Bucket(H0, "b", 10), new Bucket(H0, "c", 1000),
        new Bucket(H0 + HOUR, "a", 1), new Bucket(H0 + 2 * HOUR, "a", 1), new Bucket(H0 + 2 * HOUR, "b", 10),
        new Bucket(H0 + 4 * HOUR, "a", 100));

    // the open bucket gains a tag, then loses it
    insert(H0 + 4 * HOUR + 2_000, "d", 5);
    assertThat(buckets().subList(6, 8)).containsExactly(new Bucket(H0 + 4 * HOUR, "a", 100), new Bucket(H0 + 4 * HOUR, "d", 5));
    database.command("sql", "DELETE FROM S WHERE ts >= ? AND h = 'd'", H0 + 4 * HOUR);
    assertThat(buckets()).hasSize(7).last().isEqualTo(new Bucket(H0 + 4 * HOUR, "a", 100));

    // and a full refresh agrees with what the incremental ones left
    final List<Bucket> incremental = buckets();
    database.command("sql", "DROP CONTINUOUS AGGREGATE A");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
    assertThat(buckets()).isEqualTo(incremental);
  }

  /** A creation that fails after its backing type was made, here in the first refresh, leaves neither the aggregate nor the type. */
  @Test
  void aFailedCreationLeavesNoBackingTypeBehind() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
    insert(H0 + 1_000, "not a number", 1);

    final Throwable failure = catchThrowable(() -> database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h.asLong() AS n, sum(v) AS total FROM S GROUP BY hr, n"));

    assertThat(failure).as("the first refresh has to fail, or this test proves nothing").isNotNull();
    assertThat(database.getSchema().existsContinuousAggregate("A")).isFalse();
    assertThat(database.getSchema().existsType("A")).isFalse();
    assertThat(database.getSchema().getIndexes()).noneMatch(index -> index.getName().startsWith("A["));
  }

  private long statistic(final String name) {
    return ((Number) ((DatabaseInternal) database).getStats().get(name)).longValue();
  }

  private RID rowOf(final long bucketStart) {
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM A WHERE hr = ?", new Date(bucketStart))) {
      return rs.next().getProperty("rid");
    }
  }

  /**
   * What the aggregate holds is the same as before: the open bucket is recomputed by every INSERT, a new bucket is added, a late
   * sample recomputes its own bucket (the range replacement), a delete of the source too, and all of it survives a reopen.
   */
  @Test
  void theRefreshKeepsEveryBucketItHeld() {
    createSourceAndAggregate();
    for (int hour = 0; hour < 5; hour++)
      insert(H0 + hour * HOUR + 1_000, "a", hour + 1);
    insert(H0 + 4 * HOUR + 2_000, "a", 10);
    insert(H0 + 4 * HOUR + 3_000, "b", 100);
    // late, behind the watermark
    insert(H0 + HOUR + 5_000, "a", 20);
    database.command("sql", "DELETE FROM S WHERE ts >= ? AND ts < ?", H0 + 2 * HOUR, H0 + 3 * HOUR);

    final List<Bucket> expected = List.of(new Bucket(H0, "a", 1), new Bucket(H0 + HOUR, "a", 22), new Bucket(H0 + 3 * HOUR, "a", 4),
        new Bucket(H0 + 4 * HOUR, "a", 15), new Bucket(H0 + 4 * HOUR, "b", 100));
    assertThat(buckets()).isEqualTo(expected);

    reopenDatabase();
    assertThat(buckets()).isEqualTo(expected);
    assertThat(aggregate().getBackingType().getPolymorphicIndexByProperties("hr")).isNotNull();
    insert(H0 + 4 * HOUR + 4_000, "b", 1);
    assertThat(buckets().getLast()).isEqualTo(new Bucket(H0 + 4 * HOUR, "b", 101));
  }

  /**
   * An aggregate created before #10804 has neither the property nor the index. It is not altered at open (under HA every node would
   * make that schema change on its own), it is named, with the statements that index it, and they work as written.
   */
  @Test
  void anAggregateWithoutTheIndexIsNamedWithTheStatementsThatIndexIt() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 1);
    // the shape of an aggregate created before #10804
    database.getSchema().dropIndex(aggregate().getBackingType().getPolymorphicIndexByProperties("hr").getName());
    database.getSchema().getType("A").dropProperty("hr");

    // as an older version left it on disk: the open names it, and the refresh still works without the index
    reopenDatabase();
    final String warning = aggregate().getUpgradeWarning();
    assertThat(warning).contains("'A'").contains("'hr'")
        .endsWith("Run: CREATE PROPERTY `A`.`hr` DATETIME; CREATE INDEX ON `A` (`hr`) NOTUNIQUE");
    insert(H0 + 2_000, "a", 2);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3));

    for (final String statement : warning.substring(warning.indexOf("Run: ") + 5).split("; "))
      database.command("sql", statement);

    assertThat(aggregate().getUpgradeWarning()).isNull();
    assertThat(plan(ContinuousAggregateRefresher.bucketsFromSelect(aggregate()), new Date(H0))).contains("FETCH FROM INDEX");
    // the rows written before the property was declared are found through the index
    insert(H0 + 3_000, "a", 4);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 7));
    assertThat(database.query("sql", "SELECT count(*) AS n FROM A WHERE hr >= ?", new Date(H0)).next().<Long>getProperty("n"))
        .isEqualTo(1L);
  }

  /** An index other than the one the creation builds serves the deletes as well, when the bucket column leads it. */
  @Test
  void anIndexLedByTheBucketColumnIsEnough() {
    createSourceAndAggregate();
    database.getSchema().dropIndex(aggregate().getBackingType().getPolymorphicIndexByProperties("hr").getName());
    assertThat(aggregate().getUpgradeWarning()).endsWith("Run: CREATE INDEX ON `A` (`hr`) NOTUNIQUE");

    database.command("sql", "CREATE PROPERTY A.h STRING");
    database.command("sql", "CREATE INDEX ON A (hr, h) NOTUNIQUE");
    assertThat(aggregate().getUpgradeWarning()).isNull();

    database.command("sql", "DROP INDEX `A[hr,h]`");
    database.command("sql", "CREATE INDEX ON A (h, hr) NOTUNIQUE");
    assertThat(aggregate().getUpgradeWarning()).as("an index the bucket column does not lead cannot serve a range on it").isNotNull();
  }

  /**
   * The index is built inside the schema frame of the creation, and an index build waits for the async workers to drain, which need
   * the write lock of that frame to commit their batch. A hang detector, not a latency bound.
   */
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void theCreationDoesNotStallWhileTheAsyncExecutorHoldsABatch() {
    database.getSchema().createDocumentType("Batch");
    for (int i = 0; i < 100; i++)
      database.async().createRecord(database.newDocument("Batch").set("i", i), null);

    createSourceAndAggregate();

    database.async().waitCompletion();
    assertThat(database.countType("Batch", false)).isEqualTo(100L);
    assertThat(aggregate().getBackingType().getPolymorphicIndexByProperties("hr")).isNotNull();
  }

  private String plan(final String statement, final Object... params) {
    try (final ResultSet rs = database.command("sql", "EXPLAIN " + statement, params)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
