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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test for GHSA-m46c-jh3x-xwrp: the Redis wire-protocol plugin must require authentication
 * before accepting any data command, mirroring the Postgres and MongoDB wire-protocol wrappers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisAuthenticationTest extends BaseGraphServerTest {

  private static final int    DEF_PORT = GlobalConfiguration.REDIS_PORT.getValueAsInteger();
  private static final String USER     = "root";
  private static final String PASSWORD = DEFAULT_PASSWORD_FOR_TESTS;

  @Test
  void unauthenticatedCommandsAreRejected() {
    final DatabaseInternal serverDatabase = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    serverDatabase.command("sql", "CREATE DOCUMENT TYPE V");

    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      // PING before AUTH must be rejected with NOAUTH.
      JedisDataException error = catchThrowableOfType(JedisDataException.class, jedis::ping);
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("NOAUTH");

      // A read against an arbitrary database must be rejected.
      error = catchThrowableOfType(JedisDataException.class, () -> jedis.get(getDatabaseName() + ".anyKey"));
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("NOAUTH");

      // A write into an arbitrary database/type must be rejected...
      error = catchThrowableOfType(JedisDataException.class,
          () -> jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), "V", "{\"injected\":\"marker\"}"));
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("NOAUTH");

      // ...and must not have durably mutated the database.
      assertThat(serverDatabase.countType("V", false)).isEqualTo(0L);

      // A transient write must be rejected too.
      error = catchThrowableOfType(JedisDataException.class, () -> jedis.set(getDatabaseName() + ".foo", "bar"));
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("NOAUTH");
    }
  }

  @Test
  void wrongCredentialsAreRejected() {
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      final JedisDataException error = catchThrowableOfType(JedisDataException.class, () -> jedis.auth(USER, "wrong-password"));
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("WRONGPASS");

      // Still unauthenticated: commands remain rejected.
      final JedisDataException stillDenied = catchThrowableOfType(JedisDataException.class, jedis::ping);
      assertThat(stillDenied).isNotNull();
      assertThat(stillDenied.getMessage()).contains("NOAUTH");
    }
  }

  @Test
  void authenticatedCommandsSucceed() {
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");

      // Now the normal command flow works.
      assertThat(jedis.ping()).isEqualTo("PONG");

      jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());
      jedis.set("authKey", "authValue");
      assertThat(jedis.get("authKey")).isEqualTo("authValue");

      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
      assertThat(database.getGlobalVariable("authKey")).isEqualTo("authValue");
    }
  }

  @Test
  void singleArgumentAuthIsRejected() {
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      // ArcadeDB has no anonymous default user: single-argument AUTH must be rejected.
      try {
        jedis.auth(PASSWORD);
        fail("Single-argument AUTH should be rejected");
      } catch (final JedisDataException e) {
        assertThat(e.getMessage()).contains("WRONGPASS");
      }
    }
  }

  @Test
  void helloWithAuthOptionAuthenticates() {
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      // HELLO carrying the AUTH option authenticates and negotiates the protocol in one round-trip.
      final Object reply = jedis.sendCommand(Protocol.Command.HELLO, "2", "AUTH", USER, PASSWORD);
      assertThat(reply).isNotNull();

      // The connection is now authenticated: the normal command flow works.
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  void helloWithoutAuthIsRejectedBeforeAuthentication() {
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      final JedisDataException error = catchThrowableOfType(JedisDataException.class,
          () -> jedis.sendCommand(Protocol.Command.HELLO, "2"));
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("NOAUTH");

      // Still unauthenticated.
      final JedisDataException stillDenied = catchThrowableOfType(JedisDataException.class, jedis::ping);
      assertThat(stillDenied).isNotNull();
      assertThat(stillDenied.getMessage()).contains("NOAUTH");
    }
  }

  @Test
  void helloWithWrongCredentialsIsRejected() {
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      final JedisDataException error = catchThrowableOfType(JedisDataException.class,
          () -> jedis.sendCommand(Protocol.Command.HELLO, "2", "AUTH", USER, "wrong-password"));
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("WRONGPASS");
    }
  }

  @Test
  void userCannotAccessUnauthorizedDatabase() {
    // Create a user that can only access a non-existent "otherdb", not the test database.
    final var security = getServer(0).getSecurity();
    final String encoded = security.encodePassword("limitedPassword1");
    security.createUser(new JSONObject()
        .put("name", "limited")
        .put("password", encoded)
        .put("databases", new JSONObject().put("otherdb", new JSONArray().put("admin"))));

    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth("limited", "limitedPassword1")).isEqualTo("OK");

      // Addressing the test database (which this user is NOT authorized for) must be rejected with NOPERM.
      JedisDataException error = catchThrowableOfType(JedisDataException.class,
          () -> jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName()));
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("NOPERM");

      error = catchThrowableOfType(JedisDataException.class, () -> jedis.get(getDatabaseName() + ".anyKey"));
      assertThat(error).isNotNull();
      assertThat(error.getMessage()).contains("NOPERM");
    } finally {
      security.dropUser("limited");
    }
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
