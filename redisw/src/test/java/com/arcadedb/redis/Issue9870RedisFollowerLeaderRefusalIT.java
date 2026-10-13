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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9870: a real follower answering a real Redis client with a leader refusal. Issue #9845 made the refusal a
 * {@code READONLY} reply naming the leader, tested on the mapping helpers alone; this drives it through Jedis, the
 * executor's error arm and the RESP framing of a follower in a running cluster.
 * <p>
 * Unlike Bolt, Postgres and MongoDB, the Redis wire has no command a follower refuses on its own: an {@code HSET}
 * commits on the node it reached and the commit travels through Raft, so it succeeds on a follower. The refusal is
 * therefore raised on that follower's own save, by a listener registered on its copy of the type only, with the
 * leader's address as the follower itself resolves it. Everything between the save and the client is the production
 * path. A Redis command that changes the schema, or one routed through the follower's forward, would let this test drop
 * the listener and use a refusal the cluster raises itself, as the MongoDB and Postgres tests do.
 */
@Tag("slow")
class Issue9870RedisFollowerLeaderRefusalIT extends BaseRaftHATest {
  private static final String TYPE = "Issue9870Redis";

  // One Redis port per server, drawn free from the same ledger as the Raft ports
  private final int[] redisPorts = allocateFixturePorts(3);

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final int index = serverIndexOf(config);
    config.setValue(GlobalConfiguration.SERVER_PLUGINS.getKey(), "Redis:com.arcadedb.redis.RedisProtocolPlugin");
    config.setValue(GlobalConfiguration.REDIS_PORT.getKey(), String.valueOf(redisPorts[index]));
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(120)
  void aRefusedWriteOnAFollowerIsReadOnlyNamingTheLeader() {
    final int leader = requireLeaderIndex();
    getServerDatabase(leader, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    waitForAllServers();

    final int follower = firstFollowerOf(leader);
    final String leaderAddress = getRaftPlugin(follower).getRaftHAServer().getLeaderHttpAddress();
    assertThat(leaderAddress).as("the follower must know where the leader is").isNotBlank();

    final Database followerDb = getServerDatabase(follower, getDatabaseName());
    final BeforeRecordCreateListener refuse = record -> {
      throw new ServerIsNotTheLeaderException("Changes must be executed on the leader server", leaderAddress);
    };
    followerDb.getSchema().getType(TYPE).getEvents().registerListener(refuse);
    try (final Jedis jedis = new Jedis("localhost", redisPorts[follower])) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);

      final Throwable refused = catchThrowable(
          () -> jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{\"name\":\"refused\"}"));
      assertThat(refused).as("the follower must refuse the write").isInstanceOf(JedisDataException.class);
      // Jedis hands back the RESP error line without its leading '-': the kind word, then the text
      assertThat(refused.getMessage())
          .as("a Sentinel-aware client reconnects to the master on READONLY, and retries the replica on TRYAGAIN (issue #9845)")
          .startsWith("READONLY ")
          .contains("The leader is server '" + leaderAddress + "'");

      assertThat(jedis.ping()).as("the connection must still serve the next command after the refusal").isEqualTo("PONG");
    } finally {
      followerDb.getSchema().getType(TYPE).getEvents().unregisterListener(refuse);
    }
    assertThat(followerDb.countType(TYPE, false)).as("the refused write must not stand").isZero();
  }
}
