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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A connection reuses the database context its previous command bound instead of rebuilding it per command. A command that
 * fails inside its transaction must not leave anything behind for the next one: the next commands on the same connection see
 * a clean transaction and the committed state only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisContextReuseTest extends BaseRedisServerTest {
  private static final String TYPE = "ContextReuse";

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
  void aFailedCommandLeavesNothingForTheNextOneOnTheSameConnection() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".id INTEGER");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (id) UNIQUE");
    final String index = getDatabaseName() + "." + TYPE + "[id]";

    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
      jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());

      assertThat(jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{\"id\":1,\"v\":\"first\"}")).isEqualTo(1L);

      // fails inside its transaction, after writing the first of its two records: the transaction rolls back whole
      assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{\"id\":2}", "{\"id\":1}"))
          .isInstanceOf(JedisDataException.class);
      // fails before any write
      assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{not json"))
          .isInstanceOf(JedisDataException.class);

      // the same connection, and so the same reused context: nothing of the failed commands is visible or pending
      assertThat(jedis.sendCommand(Protocol.Command.HEXISTS, index, "2")).isEqualTo(0L);
      assertThat(jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{\"id\":2,\"v\":\"second\"}")).isEqualTo(1L);
      assertThat(jedis.set("reuse:key", "value")).isEqualTo("OK");
      assertThat(jedis.get("reuse:key")).isEqualTo("value");
      assertThat(new String((byte[]) jedis.sendCommand(Protocol.Command.HGET, index, "2"))).contains("\"v\":\"second\"");
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(2);
  }
}
