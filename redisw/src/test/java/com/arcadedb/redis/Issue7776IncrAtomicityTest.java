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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7776: INCR/DECR/INCRBY/INCRBYFLOAT read the current value with {@code getVariable()} and
 * wrote the new one with {@code setVariable()} as two separate operations, with no CAS and no lock. Real Redis'
 * single-threaded command loop makes INCR atomic, and a counter is the single most common thing INCR is used for, so
 * a client has no reason to guard it: two connections incrementing the same key interleaved and silently lost
 * updates, each having been answered a plausible {@code :<n>}.
 * <p>
 * Fixed by giving the store a {@code compute} entry point - {@code DatabaseInternal.computeGlobalVariable} - and
 * routing INCR/DECR through it, so the read, the arithmetic and the write are ONE operation. That is the
 * read-modify-write sibling of the {@code setVariableIfAbsent}/{@code setVariableIfPresent} pair SET NX/XX already
 * had.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7776IncrAtomicityTest extends BaseRedisServerTest {

  private static final int THREADS     = 4;
  private static final int PER_THREAD  = 500;

  @Test
  void concurrentIncrementsOnAPersistentKeyAreNotLost() throws Exception {
    assertNoIncrementIsLost(getDatabaseName() + ".issue7776counter");
  }

  @Test
  void concurrentIncrementsOnAConnectionLocalKeyAreNotLostEither() throws Exception {
    // No database prefix and no SELECT: the connection-local bucket. Every connection has its own map here, so the
    // shared-state race is not reachable - but the command must still answer correctly, which is what this pins.
    final Jedis jedis = connect();
    try {
      for (int i = 0; i < 100; i++)
        jedis.incr("issue7776local");
      assertThat(jedis.get("issue7776local")).isEqualTo("100");
    } finally {
      jedis.close();
    }
  }

  @Test
  void theArithmeticAndItsRefusalsAreUnchanged() {
    final Jedis jedis = connect();
    try {
      final String key = getDatabaseName() + ".issue7776mixed";

      assertThat(jedis.incrBy(key, 10)).isEqualTo(10);
      assertThat(jedis.decrBy(key, 4)).isEqualTo(6);
      assertThat(jedis.decr(key)).isEqualTo(5);
      assertThat(jedis.incr(key)).isEqualTo(6);

      // A value INCR cannot operate on is still refused, and refusing it must leave the key exactly as it was: the
      // computation throws from INSIDE the atomic update, which is precisely where a partial write would show.
      final String text = getDatabaseName() + ".issue7776text";
      jedis.set(text, "not-a-number");
      assertThatThrownBy(() -> jedis.incr(text)).isInstanceOf(JedisDataException.class);
      assertThat(jedis.get(text)).isEqualTo("not-a-number");

      // INCRBYFLOAT promotes the integral value it finds and keeps working through the same path.
      assertThat(jedis.incrByFloat(key, 0.5)).isEqualTo(6.5);
    } finally {
      jedis.close();
    }
  }

  private void assertNoIncrementIsLost(final String key) throws Exception {
    final Jedis setup = connect();
    try {
      setup.set(key, "0");
    } finally {
      setup.close();
    }

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(THREADS);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < THREADS; t++) {
      final Thread thread = new Thread(() -> {
        // One connection per thread: the race lives between connections, and a Jedis instance is not shareable.
        final Jedis jedis = connect();
        try {
          start.await();
          for (int i = 0; i < PER_THREAD; i++)
            jedis.incr(key);
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          jedis.close();
          done.countDown();
        }
      }, "issue7776-incr-" + t);
      threads.add(thread);
      thread.start();
    }

    start.countDown();
    assertThat(done.await(60, TimeUnit.SECONDS)).as("the increment threads must all finish").isTrue();
    for (final Thread thread : threads)
      thread.join();

    assertThat(failure.get()).isNull();

    final Jedis reader = connect();
    try {
      assertThat(reader.get(key))
          .as("%d threads x %d INCR must add up exactly: an INCR that is a read followed by a write loses the "
              + "increments that interleave with it", THREADS, PER_THREAD)
          .isEqualTo(String.valueOf((long) THREADS * PER_THREAD));
    } finally {
      reader.close();
    }
  }

  private Jedis connect() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    return jedis;
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
