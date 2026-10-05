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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * An open Redis connection must stop working when its user is deleted, loses the database grant or has its password
 * rotated: it used to keep the access it had at AUTH time while HTTP refused the same credentials.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisStaleSessionRevalidationTest extends BaseRedisServerTest {
  private static final String USER     = "redisStaleUser";
  private static final String PASSWORD = "redisStalePassword1";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(USER) != null)
      security.dropUser(USER);
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void deletedUserIsCutOff() {
    try (final Jedis jedis = connect()) {
      insert(jedis, "before");
      getServer(0).getSecurity().dropUser(USER);
      assertDenied(jedis);
    }
  }

  @Test
  void revokedDatabaseGrantIsCutOff() {
    try (final Jedis jedis = connect()) {
      insert(jedis, "before");
      update(new JSONObject().put("name", USER).put("password", encoded()).put("databases", new JSONObject()));
      assertDenied(jedis);
    }
  }

  @Test
  void rotatedPasswordIsCutOff() {
    try (final Jedis jedis = connect()) {
      insert(jedis, "before");
      update(new JSONObject().put("name", USER).put("password", getServer(0).getSecurity().encodePassword("another-Password-2"))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"))));
      assertDenied(jedis);
    }
  }

  private Jedis connect() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", USER).put("password", encoded())
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"))));
    getServerDatabase(0, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE StaleDoc IF NOT EXISTS");
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth(USER, PASSWORD);
    return jedis;
  }

  private String encoded() {
    return getServer(0).getSecurity().encodePassword(PASSWORD);
  }

  private void update(final JSONObject configuration) {
    getServer(0).getSecurity().updateUser(configuration);
  }

  private void insert(final Jedis jedis, final String tag) {
    jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), "StaleDoc", "{\"name\":\"" + tag + "\"}");
  }

  private void assertDenied(final Jedis jedis) {
    final JedisDataException error = catchThrowableOfType(JedisDataException.class, () -> insert(jedis, "after"));
    assertThat(error).isNotNull();
    assertThat(error.getMessage()).contains("NOAUTH");
    assertThat(getServerDatabase(0, getDatabaseName()).countType("StaleDoc", false)).isEqualTo(1L);
  }
}
