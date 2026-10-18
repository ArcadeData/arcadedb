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
import com.arcadedb.server.AdmissionSlotConformance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10235 over RESP: a command that gives its admission slot back before a wait finds the slot on the thread that
 * runs it. A Redis command cannot issue a statement that waits, so the hook is the record its {@code HSET} creates. See
 * {@link AdmissionSlotConformance}.
 */
public class AdmissionSlotConformanceRedisTest extends BaseRedisServerTest {
  private static final String TYPE = "Conformance10235";

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
  void aCommandGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".id INTEGER");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (id) UNIQUE");

    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);

      AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "Redis HSET",
          () -> jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), TYPE, "{\"id\":1}"));
    }
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }
}
