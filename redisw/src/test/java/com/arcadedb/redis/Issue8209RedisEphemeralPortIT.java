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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8209: the Redis tests used to start the plugin on, and connect to, the production default
 * port 6379, so anything already listening there failed the module or answered the tests' connections. This test holds
 * 6379 itself (when it is free) before the server starts, and checks the plugin still comes up on an OS-assigned port
 * that the tests can reach, while the production default stays 6379.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8209RedisEphemeralPortIT extends BaseRedisServerTest {

  private static final int DEFAULT_PORT = 6379;

  private ServerSocket squatter;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
    try {
      squatter = new ServerSocket(DEFAULT_PORT, 50, InetAddress.getByName("0.0.0.0"));
    } catch (final IOException e) {
      // SOMETHING ELSE ALREADY HOLDS THE DEFAULT PORT: THAT IS THE SCENARIO UNDER TEST ANYWAY
      squatter = null;
    }
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void pluginStartsOnAnEphemeralPortWhileTheDefaultIsTaken() {
    assertThat(getServer(0).isStarted()).isTrue();

    final int port = getServerRedisPort();
    assertThat(port).isGreaterThan(0).isNotEqualTo(DEFAULT_PORT);

    try (final Jedis jedis = new Jedis("localhost", port)) {
      assertThat(jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
      jedis.set("issue8209", "ephemeral");
      assertThat(jedis.get("issue8209")).isEqualTo("ephemeral");
    }

    assertThat(GlobalConfiguration.REDIS_PORT.getDefValue()).isEqualTo(DEFAULT_PORT);
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      super.endTest();
    } finally {
      if (squatter != null)
        try {
          squatter.close();
        } catch (final IOException ignored) {
          // NOTHING TO DO
        }
    }
  }
}
