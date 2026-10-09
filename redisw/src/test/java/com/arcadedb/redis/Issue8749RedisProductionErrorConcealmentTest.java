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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #8749: production mode conceals the engine's error text on HTTP, gRPC, PostgreSQL, MongoDB and Gremlin, but the
 * RESP protocol answered every failure with the raw exception message, so a persistent HSET refused on a unique index
 * handed the stored key value to any authenticated client. The kind word ({@code ERR}, {@code TRYAGAIN},
 * {@code NOPERM}) is what a client branches on and stays, and so does the RESP text the executor words itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8749RedisProductionErrorConcealmentTest extends BaseRedisServerTest {
  private static final String TYPE   = "Account8749";
  private static final String SECRET = "stored-secret-8749@example.com";

  @Test
  void duplicatedKeyIsConcealedInProductionMode() throws Exception {
    final String reply = withMode("production", this::duplicatedHset);

    assertThat(reply).startsWith("ERR ");
    assertThat(reply).isEqualTo("ERR " + ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(reply).doesNotContain(SECRET);
  }

  @Test
  void developmentModeKeepsTheFullMessage() throws Exception {
    final String reply = withMode("development", this::duplicatedHset);

    assertThat(reply).startsWith("ERR ");
    assertThat(reply).contains(SECRET);
  }

  /** RESP text the executor words itself carries no stored data and is what the client needs: kept in production. */
  @Test
  void redisProtocolTextIsKeptInProductionMode() throws Exception {
    withMode("production", () -> {
      try (final Jedis jedis = connect()) {
        final String key = getDatabaseName() + ".issue8749overflow";
        jedis.set(key, String.valueOf(Long.MAX_VALUE));
        assertThatThrownBy(() -> jedis.incr(key)).isInstanceOf(JedisDataException.class)
            .hasMessageContaining("increment or decrement would overflow");
      }
      return null;
    });
  }

  /** The error reply of a persistent HSET whose unique key is taken. */
  private String duplicatedHset() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType(TYPE)) {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
      database.command("sql", "CREATE PROPERTY " + TYPE + ".email STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE + " (email) UNIQUE");
      database.transaction(() -> database.newDocument(TYPE).set("email", SECRET).save());
    }

    try (final Jedis jedis = connect()) {
      final JedisDataException refused = catchThrowableOfType(JedisDataException.class,
          () -> jedis.hset(getDatabaseName(), TYPE, "{'email':'" + SECRET + "'}"));
      assertThat(refused).isNotNull();
      return refused.getMessage();
    }
  }

  private <T> T withMode(final String mode, final Callable<T> work) throws Exception {
    final Object previous = getServer(0).getConfiguration().getValue(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, mode);
    try {
      return work.call();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
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
