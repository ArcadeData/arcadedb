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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10342: {@code DELETE FROM <timeseries type> WHERE ...} and {@code TRUNCATE TYPE} remove samples from the mutable pages and
 * the sealed blocks of every shard, and under HA the leader ships both - the pages it rewrote and the sealed bytes of every shard it
 * changed - in one replication unit, as it ships a compaction. The follower must hold exactly the samples the leader holds, in both
 * layers, and must not see a gap in the page versions of the mutable bucket (the sealed store has no WAL, the pages do).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue10342TimeSeriesDeleteReplicationIT extends BaseRaftHATest {
  private static final long BASE = 1_700_000_000_000L;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void cleanupHooks() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
  }

  @Test
  void aDeleteAndATruncateReachTheFollower() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2").close();
    waitForAllServers();
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    // 90 samples sealed by a compaction, then 60 left in the mutable pages
    insert(leaderDb, 0, 90);
    ((LocalTimeSeriesType) leaderDb.getSchema().getType("sensor")).getEngine().compactAll();
    insert(leaderDb, 90, 150);
    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, 150L)).as("the follower holds the samples before the delete").isEqualTo(150L);
    assertThat(sealedSamples(followerIndex)).as("the follower holds the sealed blocks").isEqualTo(sealedSamples(leaderIndex)).isPositive();

    // a follower never holds the replication session: a delete that reaches the engine of one (a client-managed transaction runs on the
    // follower it is open on) is refused at once, as not worth retrying on that node, and removes nothing
    assertThatThrownBy(() -> engine(followerIndex).deleteSamples(Long.MIN_VALUE, Long.MAX_VALUE, null))
        .isInstanceOfSatisfying(NeedRetryException.class, e -> assertThat(e.isRetryableOnThisNode()).isFalse())
        .hasMessageContaining("not the leader");
    assertThat(count(followerIndex)).as("the refused delete removed nothing").isEqualTo(150L);

    // a delete that drops sealed blocks, rebuilds one and rewrites the mutable pages
    try (final ResultSet rs = leaderDb.command("sql",
        "DELETE FROM sensor WHERE host = 'h1' AND ts >= " + (BASE + 20_000L) + " AND ts < " + (BASE + 130_000L))) {
      // the samples 22, 25, ... 127 of the series h1
      assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(36L);
    }
    waitForAllServers();

    assertThat(awaitCountOn(leaderIndex, 114L)).as("leader").isEqualTo(114L);
    assertThat(awaitCountOn(followerIndex, 114L)).as("follower").isEqualTo(114L);
    assertThat(sum(followerIndex)).as("follower sum(v)").isEqualTo(sum(leaderIndex));
    assertThat(sealedSamples(followerIndex)).as("follower sealed samples").isEqualTo(sealedSamples(leaderIndex));
    assertThat(mutableSamples(followerIndex)).as("follower mutable samples").isEqualTo(mutableSamples(leaderIndex));
    assertThat(countOfHost(followerIndex, "h1")).as("follower samples of the deleted series").isEqualTo(countOfHost(leaderIndex, "h1"));

    // a truncate empties both layers on both nodes, and the type takes samples again
    try (final ResultSet rs = leaderDb.command("sql", "TRUNCATE TYPE sensor")) {
      assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(114L);
    }
    waitForAllServers();
    assertThat(awaitCountOn(leaderIndex, 0L)).as("leader after truncate").isZero();
    assertThat(awaitCountOn(followerIndex, 0L)).as("follower after truncate").isZero();
    assertThat(sealedSamples(followerIndex)).isZero();
    assertThat(mutableSamples(followerIndex)).isZero();

    insert(leaderDb, 200, 210);
    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, 10L)).as("follower after the type took samples again").isEqualTo(10L);

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get()).as("page-version gaps seen by the follower").isZero();
  }

  /**
   * A delete that fails on the second shard of the leader has already rewritten the first one: its sealed file, and its mutable pages,
   * which the transaction committed into the replication session's buffer. The session must end normally and publish them, or the
   * followers would keep samples the leader deleted while the wrapper quarantined the leader to resync it from them. The statement
   * reports the failure, the follower holds what the leader holds, and running it again finishes the job on both.
   */
  @Test
  void aDeleteThatFailedOnALaterShardStillPublishesTheShardsBeforeIt() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2").close();
    waitForAllServers();
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    // 60 samples sealed, 40 more in the mutable pages of both shards
    insert(leaderDb, 0, 60);
    engine(leaderIndex).compactAll();
    insert(leaderDb, 60, 100);
    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, 100L)).isEqualTo(100L);

    // the temp file of the second shard's sealed rewrite cannot be created on the leader
    final Path obstruction = Path.of(leaderDb.getDatabasePath(), "sensor_shard_1.ts.sealed.tmp");
    Files.createDirectory(obstruction);
    try {
      assertThatThrownBy(() -> leaderDb.command("sql", "DELETE FROM sensor WHERE ts >= " + (BASE + 10_000L)).close())
          .hasMessageContaining("TIMESERIES type 'sensor'");
    } finally {
      Files.deleteIfExists(obstruction);
    }
    final long afterTheFailure = count(leaderIndex);
    assertThat(afterTheFailure).as("the first shard of the leader was rewritten").isLessThan(100L);
    assertThat(afterTheFailure).as("the second shard is as it was").isGreaterThan(10L);

    // published without waiting for a rerun: the follower holds what the leader holds, in both layers, and the leader was not
    // quarantined and given the follower's older state back
    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, afterTheFailure)).as("follower after the failed statement").isEqualTo(afterTheFailure);
    assertThat(count(leaderIndex)).as("leader after the failed statement").isEqualTo(afterTheFailure);
    assertThat(sealedSamples(followerIndex)).as("follower sealed samples").isEqualTo(sealedSamples(leaderIndex));
    assertThat(mutableSamples(followerIndex)).as("follower mutable samples").isEqualTo(mutableSamples(leaderIndex));

    leaderDb.command("sql", "DELETE FROM sensor WHERE ts >= " + (BASE + 10_000L)).close();
    waitForAllServers();

    final long expected = 10L;
    assertThat(awaitCountOn(leaderIndex, expected)).as("leader").isEqualTo(expected);
    assertThat(awaitCountOn(followerIndex, expected)).as("the follower after the statement was run again").isEqualTo(expected);
    assertThat(sealedSamples(followerIndex)).isEqualTo(sealedSamples(leaderIndex));
    assertThat(mutableSamples(followerIndex)).isEqualTo(mutableSamples(leaderIndex));
    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get()).as("page-version gaps seen by the follower").isZero();
  }

  /**
   * A delete cannot take the replication session while another maintenance pass (a compaction, a retention, a DDL) holds it: it waits
   * a few seconds, then is refused with an error that is worth retrying and says nothing was deleted, and it removes nothing on either
   * node. Once the session is free the same delete goes through.
   */
  @Test
  void aDeleteRefusedForWantOfTheReplicationSessionRemovesNothing() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2").close();
    waitForAllServers();
    insert(leaderDb, 0, 30);
    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, 30L)).isEqualTo(30L);

    final FileManager fileManager = ((DatabaseInternal) leaderDb).getFileManager();
    assertThat(fileManager.startRecordingChanges()).as("the replication session is free").isTrue();
    try {
      assertThatThrownBy(() -> engine(leaderIndex).deleteSamples(BASE + 10_000L, Long.MAX_VALUE, null))
          .isInstanceOfSatisfying(NeedRetryException.class, e -> assertThat(e.isRetryableOnThisNode()).isTrue())
          .hasMessageContaining("holds the replication session").hasMessageContaining("Nothing was deleted");
    } finally {
      fileManager.stopRecordingChanges();
    }
    assertThat(count(leaderIndex)).as("the refused delete removed nothing on the leader").isEqualTo(30L);
    assertThat(count(followerIndex)).as("the refused delete removed nothing on the follower").isEqualTo(30L);

    leaderDb.command("sql", "DELETE FROM sensor WHERE ts >= " + (BASE + 10_000L)).close();
    waitForAllServers();
    assertThat(awaitCountOn(leaderIndex, 10L)).as("leader").isEqualTo(10L);
    assertThat(awaitCountOn(followerIndex, 10L)).as("follower").isEqualTo(10L);
  }

  /**
   * Issue #10428: a delete records, on the continuous aggregates over the type, the range it removed samples from, and the record is
   * part of the replication unit of the delete. A follower promoted to leader after the delete refreshes the aggregate with it, and
   * recomputes the buckets the delete reached instead of keeping the samples for good. The leader's own refresh is held off here, so
   * that the record is still pending when the follower is looked at.
   */
  @Test
  void aDeleteShipsTheRangeItInvalidatedOnTheContinuousAggregatesWithIt() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final long hour = 3_600_000L;
    final long base = BASE - (BASE % hour);
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2").close();
    leaderDb.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, count(*) AS n FROM sensor GROUP BY hr").close();
    waitForAllServers();
    for (int h = 0; h < 3; h++)
      leaderDb.command("sql", "INSERT INTO sensor SET ts = ?, host = 'h0', v = 1", base + h * hour).close();
    leaderDb.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
    waitForAllServers();
    assertThat(awaitValue(3L, () -> aggregateRows(followerIndex))).as("the follower holds the three buckets").isEqualTo(3L);

    final ContinuousAggregateImpl aggregate = (ContinuousAggregateImpl) leaderDb.getSchema().getContinuousAggregate("hourly");
    assertThat(aggregate.tryBeginRefresh()).isTrue();
    try {
      leaderDb.command("sql", "DELETE FROM sensor WHERE ts = " + base).close();
      waitForAllServers();

      assertThat(awaitValue(base, () -> invalidatedFromOn(followerIndex)))
          .as("the follower holds the range the delete invalidated, shipped with the delete").isEqualTo(base);
    } finally {
      aggregate.endRefresh();
    }

    leaderDb.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
    waitForAllServers();
    assertThat(awaitValue(2L, () -> aggregateRows(leaderIndex))).as("leader").isEqualTo(2L);
    assertThat(awaitValue(2L, () -> aggregateRows(followerIndex))).as("follower").isEqualTo(2L);
  }

  private long aggregateRows(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM hourly")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  /** The start of the range the aggregate records as invalidated on that server, or {@code Long.MIN_VALUE} when it records none. */
  private long invalidatedFromOn(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    final JSONObject definition = db.getSchema().getEmbedded().toJSON().getJSONObject("continuousAggregates").getJSONObject("hourly");
    return definition.getLong("invalidatedFromTs", Long.MIN_VALUE);
  }

  private void insert(final Database db, final int from, final int to) {
    for (int i = from; i < to; i++)
      db.command("sql", "INSERT INTO sensor SET ts = ?, host = ?, v = ?", BASE + i * 1_000L, "h" + (i % 3), (double) i).close();
  }

  private long awaitCountOn(final int serverIndex, final long expected) throws InterruptedException {
    return awaitValue(expected, () -> count(serverIndex));
  }

  private long count(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM sensor")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long countOfHost(final int serverIndex, final String host) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM sensor WHERE host = ?", host)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private double sum(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT sum(v) AS s FROM sensor")) {
      final Number sum = rs.next().getProperty("s");
      return sum != null ? sum.doubleValue() : 0.0;
    }
  }

  private long sealedSamples(final int serverIndex) {
    final TimeSeriesEngine engine = engine(serverIndex);
    long total = 0;
    for (int s = 0; s < engine.getShardCount(); s++)
      total += engine.getShard(s).getSealedStore().getTotalSampleCount();
    return total;
  }

  private long mutableSamples(final int serverIndex) throws Exception {
    final TimeSeriesEngine engine = engine(serverIndex);
    long total = 0;
    for (int s = 0; s < engine.getShardCount(); s++)
      total += engine.getShard(s).getMutableBucket().getSampleCount();
    return total;
  }

  private TimeSeriesEngine engine(final int serverIndex) {
    return ((LocalTimeSeriesType) getServerDatabase(serverIndex, getDatabaseName()).getSchema().getType("sensor")).getEngine();
  }
}
