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
import com.arcadedb.engine.timeseries.TimeSeriesShard;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10673: the record of a sample appended behind the watermark of a continuous aggregate (#10502) was shipped to the followers
 * (#10634) in a schema session that ran right AFTER the append committed. A leader lost between the two left the followers with the
 * sample and without the record, and a follower promoted then never aggregated it. The record now ships before the samples, so a
 * follower that holds the samples holds the record too.
 * <p>
 * The test looks at the follower from the point a leader lost right after the commit would stop at: the hook runs once the samples
 * committed and before anything else the append does, and waits there for the follower to apply them.
 */
@Tag("slow")
class Issue10673LateSampleRecordShippedBeforeTheSamplesIT extends BaseRaftHATest {
  private static final long HOUR = 3_600_000L;
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void aFollowerHoldingTheLateSampleHoldsItsRecord() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1").close();
    leaderDb.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM sensor GROUP BY hr").close();
    waitForAllServers();
    for (int h = 0; h < 3; h++)
      leaderDb.command("sql", "INSERT INTO sensor SET ts = ?, host = 'h0', v = 1", H0 + h * HOUR).close();
    leaderDb.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
    waitForAllServers();
    assertThat(awaitValue(3L, () -> samplesOn(followerIndex))).isEqualTo(3L);
    assertThat(invalidatedFromOn(followerIndex)).as("nothing is recorded yet").isEqualTo(Long.MIN_VALUE);

    final AtomicLong samplesSeen = new AtomicLong(-1);
    final AtomicLong recordSeen = new AtomicLong(Long.MIN_VALUE);
    final AtomicReference<Throwable> hookFailure = new AtomicReference<>();
    TimeSeriesShard.TEST_AFTER_APPEND_COMMIT_HOOK = () -> {
      // another thread: the appending one is inside the leader's command, and must not open the follower's database
      final Thread observer = new Thread(() -> {
        try {
          samplesSeen.set(awaitValue(4L, () -> samplesOn(followerIndex)));
          recordSeen.set(invalidatedFromOn(followerIndex));
        } catch (final Throwable t) {
          hookFailure.set(t);
        }
      }, "issue-10673-follower-observer");
      observer.start();
      try {
        observer.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    try {
      // behind the watermark (the start of the third hour): the bucket of the first hour
      leaderDb.command("sql", "INSERT INTO sensor SET ts = ?, host = 'h0', v = 5", H0 + 60_000L).close();
    } finally {
      TimeSeriesShard.TEST_AFTER_APPEND_COMMIT_HOOK = null;
    }

    assertThat(hookFailure.get()).isNull();
    assertThat(samplesSeen.get()).as("the follower applied the late sample before the append went on").isEqualTo(4L);
    assertThat(recordSeen.get()).as("and already held its record, shipped before the sample").isEqualTo(H0);

    // the aggregate ends up right on both, as before
    leaderDb.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
    waitForAllServers();
    assertThat(awaitValue(6L, () -> firstBucketTotal(leaderIndex))).as("leader").isEqualTo(6L);
    assertThat(awaitValue(6L, () -> firstBucketTotal(followerIndex))).as("follower").isEqualTo(6L);
  }

  private long samplesOn(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM sensor")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long firstBucketTotal(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT total FROM hourly ORDER BY hr LIMIT 1")) {
      return rs.hasNext() ? rs.next().<Number>getProperty("total").longValue() : 0L;
    }
  }

  /** The start of the range the aggregate records as invalidated on that server, or {@code Long.MIN_VALUE} when it records none. */
  private long invalidatedFromOn(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    final JSONObject definition = db.getSchema().getEmbedded().toJSON().getJSONObject("continuousAggregates").getJSONObject("hourly");
    return definition.getLong("invalidatedFromTs", Long.MIN_VALUE);
  }
}
