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
import com.arcadedb.server.BaseGraphServerTest;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Defensive regression test for protocol labelling on the Redis connection thread.
 *
 * Redis wire commands do not route through database.query/command, so
 * arcadedb.query.duration will never fire for Redis traffic. The important
 * invariant is that if in the future any Redis command path does reach the
 * query engine, it must be tagged protocol="redis" and not "internal". This
 * test guards that invariant: it snapshot-counts the internal-protocol timer
 * before Redis commands run, drives several commands, then asserts the count
 * did not increase (i.e. no Redis-thread work was mis-labelled as "internal").
 */
public class RedisQueryMetricsIT extends BaseGraphServerTest {

  private static final int DEF_PORT = GlobalConfiguration.REDIS_PORT.getValueAsInteger();

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

  @Test
  void redisCommandsDoNotLeakInternalProtocolTag() {
    // Snapshot the internal-protocol timer count before any Redis commands execute.
    // The timer may already exist if other tests in the suite created it.
    final long beforeCount = internalTimerCount();

    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      // PING
      jedis.ping();

      // SET / GET / EXISTS / GETDEL via default (connection-local) bucket
      jedis.set("metricsKey", "metricsValue");
      jedis.get("metricsKey");
      jedis.exists("metricsKey");
      jedis.getDel("metricsKey");

      // SELECT then INCR / DECR via database globalVariables
      jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());
      jedis.incr("metricsCounter");
      jedis.decr("metricsCounter");
    }

    // The internal-protocol count must not have grown: no Redis-thread engine
    // query should be labelled "internal".
    final long afterCount = internalTimerCount();
    assertThat(afterCount)
        .as("arcadedb.query.duration{protocol=internal} count must not increase due to Redis commands")
        .isEqualTo(beforeCount);
  }

  private long internalTimerCount() {
    final Timer t = Metrics.globalRegistry
        .find("arcadedb.query.duration")
        .tag("protocol", "internal")
        .timer();
    return t == null ? 0L : t.count();
  }

  @Override
  protected void populateDatabase() {
  }
}
