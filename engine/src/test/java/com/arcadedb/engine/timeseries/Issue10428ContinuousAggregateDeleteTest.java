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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10428: {@code DELETE FROM <timeseries type> WHERE ...} and {@code TRUNCATE TYPE <timeseries type>} removed samples from the
 * type, but a continuous aggregate over it kept the buckets it had already materialized, and a refresh did not rebuild them: the
 * incremental refresh only recomputes the bucket open at the watermark and the newer ones, so every bucket behind the watermark kept
 * the deleted samples for good.
 * <p>
 * The delete now records, on every aggregate over the type, the time range it removed samples from. The next refresh recomputes the
 * buckets of that range - and only those, the ones behind it and the ones between it and the watermark are not touched - and the
 * statement schedules that refresh as an INSERT does. The record is part of the aggregate's definition in the schema, so it survives
 * a restart and, under HA, is shipped with the delete.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10428ContinuousAggregateDeleteTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  /** An hour boundary, so that the buckets of the default grid start where the test says they do. */
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, String tag, double total) {
  }

  private void createSourceAndAggregate() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private void insert(final long ts, final String tag, final double v) {
    database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", ts, tag, v).close();
  }

  private void refresh() {
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
  }

  private int delete(final String where) {
    try (final ResultSet rs = database.command("sql", "DELETE FROM S" + (where != null ? " WHERE " + where : ""))) {
      return ((Number) rs.next().getProperty("count")).intValue();
    }
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

  // ---- the repro of the issue

  @Test
  void aDeleteBehindTheWatermarkIsReflectedByTheNextRefresh() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20); // two hours later, so the first bucket is behind the watermark
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 20));

    assertThat(delete("ts = " + H0)).isEqualTo(1);
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void aDeleteRefreshesTheAggregateByItselfLikeAnInsertDoes() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();

    delete("ts = " + H0);

    // no REFRESH here
    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void aDeleteInsideATransactionRefreshesWhenItCommits() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();

    database.transaction(() -> delete("ts = " + H0));

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void aDeleteOfSomeSamplesOfABucketRecomputesItsTotal() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 10);
    insert(H0 + 2_000, "a", 5);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    assertThat(buckets().get(0).total()).isEqualTo(15.0);

    delete("ts = " + (H0 + 1_000));

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 5), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void aDeleteByTagKeepsTheRowsOfTheOtherTags() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 10);
    insert(H0 + 2_000, "b", 7);
    insert(H0 + 3 * HOUR, "a", 20);
    refresh();

    delete("h = 'a'");

    assertThat(buckets()).containsExactly(new Bucket(H0, "b", 7));
  }

  @Test
  void aDeleteOfEverySampleOfABucketRemovesTheBucket() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + HOUR, "a", 20);
    insert(H0 + 2 * HOUR, "a", 30);
    refresh();

    delete("ts >= " + H0 + " AND ts < " + (H0 + HOUR));

    assertThat(buckets()).containsExactly(new Bucket(H0 + HOUR, "a", 20), new Bucket(H0 + 2 * HOUR, "a", 30));
  }

  // ---- TRUNCATE TYPE

  @Test
  void truncateTypeEmptiesTheAggregate() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    assertThat(buckets()).hasSize(2);

    database.command("sql", "TRUNCATE TYPE S").close();

    assertThat(buckets()).isEmpty();
    // and it keeps working: a sample written afterwards is aggregated
    insert(H0 + 5 * HOUR, "a", 3);
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0 + 5 * HOUR, "a", 3));
  }

  // ---- only the buckets the delete reached are rebuilt

  @Test
  void onlyTheBucketsOfTheDeletedRangeAreRebuilt() {
    createSourceAndAggregate();
    for (int hour = 0; hour < 4; hour++)
      insert(H0 + hour * HOUR + 1_000, "a", 10 + hour);
    refresh();
    // buckets 0..3, the watermark is at bucket 3
    final String[] before = new String[4];
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, hr FROM A ORDER BY hr")) {
      for (int i = 0; rs.hasNext(); i++)
        before[i] = rs.next().getProperty("rid").toString();
    }

    delete("ts = " + (H0 + HOUR + 1_000)); // bucket 1

    final String[] after = new String[3];
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, hr FROM A ORDER BY hr")) {
      for (int i = 0; rs.hasNext(); i++)
        after[i] = rs.next().getProperty("rid").toString();
    }
    assertThat(buckets().stream().map(Bucket::total)).containsExactly(10.0, 12.0, 13.0);
    // bucket 0 is behind the range, bucket 2 is between it and the watermark: neither is rewritten
    assertThat(after[0]).isEqualTo(before[0]);
    assertThat(after[1]).isEqualTo(before[2]);
  }

  @Test
  void aDeleteThatRemovesNothingLeavesTheAggregateAlone() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    final String firstRid;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM A ORDER BY hr")) {
      firstRid = rs.next().getProperty("rid").toString();
    }

    assertThat(delete("ts = " + (H0 + HOUR))).isZero();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 20));
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM A ORDER BY hr")) {
      assertThat(rs.next().getProperty("rid").toString()).isEqualTo(firstRid);
    }
  }

  // ---- a grid that does not start at the epoch

  @Test
  void aBucketOfAShiftedGridIsRebuiltWholeNotFromAPartOfItsSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    // days that start at 08:00 UTC
    database.command("sql", "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1d', ts, {'offset': '8h'}) AS hr, h, sum(v) AS total "
        + "FROM S GROUP BY hr, h");
    final long day0 = H0 - (H0 % (24 * HOUR)) + 8 * HOUR; // a bucket start of that grid
    insert(day0 + 1 * HOUR, "a", 1);
    insert(day0 + 5 * HOUR, "a", 2);
    insert(day0 + 20 * HOUR, "a", 4);
    insert(day0 + 24 * HOUR + HOUR, "a", 8);
    insert(day0 + 48 * HOUR + HOUR, "a", 16);
    refresh();
    assertThat(buckets().stream().map(Bucket::total)).containsExactly(7.0, 8.0, 16.0);

    // a delete that starts in the middle of the first bucket of the grid
    delete("ts = " + (day0 + 5 * HOUR));

    assertThat(buckets()).containsExactly(new Bucket(day0, "a", 5), new Bucket(day0 + 24 * HOUR, "a", 8),
        new Bucket(day0 + 48 * HOUR, "a", 16));
  }

  @Test
  void aBucketOfATimezoneGridIsRebuiltWholeNotFromAPartOfItsSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    // days that start at local midnight in UTC+08:00, which is 16:00 UTC
    database.command("sql", "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1d', ts, {'timezone': '+08:00'}) AS hr, h, "
        + "sum(v) AS total FROM S GROUP BY hr, h");
    final long day0 = H0 - (H0 % (24 * HOUR)) + 16 * HOUR; // a bucket start of that grid
    insert(day0 + 1 * HOUR, "a", 1);
    insert(day0 + 5 * HOUR, "a", 2);
    insert(day0 + 20 * HOUR, "a", 4);
    insert(day0 + 24 * HOUR + HOUR, "a", 8);
    insert(day0 + 48 * HOUR + HOUR, "a", 16);
    refresh();
    assertThat(buckets().stream().map(Bucket::total)).containsExactly(7.0, 8.0, 16.0);

    // a delete that starts in the middle of the first bucket of the grid
    delete("ts = " + (day0 + 5 * HOUR));

    assertThat(buckets()).containsExactly(new Bucket(day0, "a", 5), new Bucket(day0 + 24 * HOUR, "a", 8),
        new Bucket(day0 + 48 * HOUR, "a", 16));
  }

  // ---- failures

  @Test
  void aDeleteInATransactionThatRollsBackStillRecordsTheRangeForTheNextRefresh() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();

    // the samples are not part of the transaction, so the rollback does not bring them back; it does drop the refresh that was to
    // follow the commit
    database.begin();
    delete("ts = " + H0);
    database.rollback();

    assertThat(buckets()).as("not refreshed yet").containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(pendingFrom()).isEqualTo(H0);

    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(pendingFrom()).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void aRefreshThatFailsWhileRecomputingTheRangeLeavesTheBucketsAndTheRecordAsTheyWere() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 10);
    insert(H0 + 2_000, "a", 8);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 18), new Bucket(H0 + 2 * HOUR, "a", 20));
    // the backing type refuses a total under 15
    database.command("sql", "CREATE PROPERTY A.total DOUBLE").close();
    database.command("sql", "ALTER PROPERTY A.total MIN 15").close();

    // the delete leaves the first bucket with 8: the refresh it schedules deletes the old row of the bucket and then fails to insert
    // the new one, so it must roll back as a whole
    delete("ts = " + (H0 + 1_000));

    assertThat(buckets()).as("the old row of the bucket is still there").containsExactly(new Bucket(H0, "a", 18),
        new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(pendingFrom()).as("and the range is still recorded").isEqualTo(H0 + 1_000);

    // once the backing type accepts the row, the next refresh finishes the job
    database.command("sql", "ALTER PROPERTY A.total MIN 1").close();
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 8), new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(pendingFrom()).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void aDeleteThatLandsWhileARefreshRunsIsAppliedWhenThatRefreshEnds() throws Exception {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + HOUR, "a", 20);
    insert(H0 + 2 * HOUR, "a", 30);
    refresh();

    // a first delete whose own refresh stands aside, so that the refresh below has a range to recompute
    final ContinuousAggregateImpl aggregate = (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
    assertThat(aggregate.tryBeginRefresh()).isTrue();
    delete("ts = " + H0);
    aggregate.endRefresh();

    // the refresh stops when it deletes the old row of the first bucket, after it read the range of the first delete
    final CountDownLatch inRefresh = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    final AtomicBoolean first = new AtomicBoolean(true);
    database.getSchema().getType("A").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (first.getAndSet(false)) {
        inRefresh.countDown();
        try {
          resume.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    });
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread refresher = new Thread(() -> {
      try {
        refresh();
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    refresher.start();
    try {
      assertThat(inRefresh.await(30, TimeUnit.SECONDS)).isTrue();

      // a second delete, in a bucket the refresh in progress did not read: its own refresh finds the aggregate busy
      delete("ts = " + (H0 + HOUR));
      assertThat(pendingFrom()).as("recorded, and not yet applied").isEqualTo(H0);
    } finally {
      resume.countDown();
      refresher.join(30_000);
    }

    assertThat(failure.get()).isNull();
    // the refresh that was in progress ran again when it ended: nothing asked for a third
    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 30));
    assertThat(pendingFrom()).isEqualTo(Long.MIN_VALUE);
  }

  /** The start of the range the aggregate records as invalidated, or {@code Long.MIN_VALUE} when it records none. */
  private long pendingFrom() {
    final JSONObject definition = ((LocalSchema) database.getSchema()).toJSON().getJSONObject("continuousAggregates").getJSONObject("A");
    return definition.getLong("invalidatedFromTs", Long.MIN_VALUE);
  }

  // ---- the record lives in the schema

  @Test
  void theRangeIsPartOfTheDefinitionOfTheAggregateAndSurvivesARestart() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();

    // a refresh in flight makes the one the delete schedules stand aside, so the record is still there to be seen
    final ContinuousAggregateImpl aggregate = (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
    assertThat(aggregate.tryBeginRefresh()).isTrue();
    try {
      delete("ts = " + H0);
      final JSONObject definition = ((LocalSchema) database.getSchema()).toJSON().getJSONObject("continuousAggregates").getJSONObject("A");
      assertThat(definition.getLong("invalidatedFromTs", Long.MIN_VALUE)).isEqualTo(H0);
      assertThat(definition.getLong("invalidatedToTs", Long.MIN_VALUE)).isEqualTo(H0);
    } finally {
      aggregate.endRefresh();
    }

    reopenDatabase();
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
    // and the refresh that rebuilt it cleared the record
    final JSONObject definition = ((LocalSchema) database.getSchema()).toJSON().getJSONObject("continuousAggregates").getJSONObject("A");
    assertThat(definition.has("invalidatedFromTs")).isFalse();
    assertThat(definition.has("invalidatedToTs")).isFalse();
  }

  @Test
  void everyAggregateOverTheTypeIsRebuilt() {
    createSourceAndAggregate();
    database.command("sql", "CREATE CONTINUOUS AGGREGATE B AS SELECT ts.timeBucket('1h', ts) AS hr, count(*) AS n FROM S GROUP BY hr");
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE B").close();

    delete("ts = " + H0);

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM B")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    }
  }
}
