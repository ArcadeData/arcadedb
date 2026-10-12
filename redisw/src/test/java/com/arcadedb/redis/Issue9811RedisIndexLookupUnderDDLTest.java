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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9811: the Redis wrapper looked keys up with a bare {@code index.get(...)}, without the guard every other module
 * gives an index read during a concurrent CREATE, REBUILD or DROP INDEX. A drop takes the sub-indexes away one at a time
 * while the index still answers as valid, so a lookup in that window came back empty - and on this protocol an empty answer
 * is "the key does not exist", a wrong answer reported as a success. Every read and delete by key now goes through
 * {@link RedisIndexKeys#lookup}: the record, or a key that really is not there, or a {@code TRYAGAIN} error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9811RedisIndexLookupUnderDDLTest extends BaseRedisServerTest {
  private static final int RECORDS = 2_000;

  @Test
  void anIndexThatCannotAnswerIsATryAgainErrorNotAMissingKey() {
    final Database database = createType();
    database.command("sql", "CREATE INDEX ON K (k) UNIQUE");
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("K[k]");
    final String name = getDatabaseName() + ".K[k]";

    try (final Jedis jedis = newClient()) {
      assertThat(jedis.hget(name, "7")).contains("\"k\":7");

      // the state a concurrent drop leaves for a moment: the index still valid, its sub-indexes gone
      final IndexInternal[] subIndexes = index.getIndexesOnBuckets();
      for (final IndexInternal subIndex : subIndexes)
        index.removeIndexOnBucket(subIndex);
      try {
        assertThatThrownBy(() -> jedis.hget(name, "7")).isInstanceOf(JedisDataException.class).hasMessageStartingWith("TRYAGAIN");
        assertThatThrownBy(() -> jedis.hexists(name, "7")).isInstanceOf(JedisDataException.class).hasMessageStartingWith("TRYAGAIN");
        assertThatThrownBy(() -> jedis.hmget(name, "7", "8")).isInstanceOf(JedisDataException.class)
            .hasMessageStartingWith("TRYAGAIN");
        assertThatThrownBy(() -> jedis.hdel(name, "7")).as("not counted as nothing to delete")
            .isInstanceOf(JedisDataException.class).hasMessageStartingWith("TRYAGAIN");
        assertThatThrownBy(() -> database.query("redis", "HGET K[k] 7")).hasStackTraceContaining("Retry the command");
      } finally {
        for (final IndexInternal subIndex : subIndexes)
          index.addIndexOnBucket(subIndex);
      }

      assertThat(jedis.hget(name, "7")).contains("\"k\":7");
      assertThat(jedis.hexists(name, "7")).isTrue();
      assertThat(jedis.hget(name, "999999")).as("a key that is not there is still nil").isNull();
    }
  }

  /** The issue's repro over the wire: one thread dropping and re-creating the index, clients reading a key that exists. */
  @Test
  @Tag("slow")
  void underConcurrentDropAndCreateALookupNeverAnswersAKeyThatExistsAsMissing() throws Exception {
    final Database database = createType();
    database.command("sql", "CREATE INDEX ON K (k) UNIQUE");
    final String name = getDatabaseName() + ".K[k]";

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicLong found = new AtomicLong();
    final AtomicLong silentlyMissing = new AtomicLong();

    final Thread ddl = new Thread(() -> {
      try {
        while (!stop.get()) {
          database.command("sql", "DROP INDEX `K[k]` IF EXISTS");
          database.command("sql", "CREATE INDEX IF NOT EXISTS ON K (k) UNIQUE");
          Thread.sleep(2);
        }
      } catch (final Throwable t) {
        failure.compareAndSet(null, t);
      }
    });

    final List<Thread> readers = new ArrayList<>();
    for (int r = 0; r < 3; r++) {
      final String key = String.valueOf(100 + r * 300);
      readers.add(new Thread(() -> {
        try (final Jedis jedis = newClient()) {
          while (!stop.get()) {
            try {
              if (jedis.hget(name, key) != null)
                found.incrementAndGet();
              else
                silentlyMissing.incrementAndGet();
              if (!jedis.hexists(name, key))
                silentlyMissing.incrementAndGet();
            } catch (final JedisDataException e) {
              // visible: TRYAGAIN, or the index not there at this instant. A client can retry those
            }
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      }));
    }

    ddl.start();
    readers.forEach(Thread::start);
    // at least 3 s of churn, and until the clients found the key a few times: a loaded runner may leave few windows open
    await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(60)).until(() -> found.get() >= 20 || failure.get() != null);
    stop.set(true);
    ddl.join(60_000);
    for (final Thread reader : readers)
      reader.join(60_000);

    assertThat(failure.get()).isNull();
    assertThat(found.get()).as("the clients did find the key between the DDLs").isPositive();
    assertThat(silentlyMissing.get()).as("a key that exists answered as missing").isZero();
  }

  private Database createType() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE K BUCKETS 4");
    database.command("sql", "CREATE PROPERTY K.k LONG");
    database.transaction(() -> {
      for (long i = 0; i < RECORDS; i++)
        database.newDocument("K").set("k", i).save();
    });
    return database;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  private Jedis newClient() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    return jedis;
  }
}
