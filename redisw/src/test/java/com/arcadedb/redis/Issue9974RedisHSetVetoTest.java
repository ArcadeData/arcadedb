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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9974: the integer an HSET replies is the number of records it STORED. {@code MutableDocument.save()} discards the engine's
 * answer, so a create a {@code BeforeRecordCreateListener} vetoed was counted on both surfaces: the RESP wire path replied the number
 * of elements it was given, the {@code redis} query language incremented after every save. Both now count what the engine created.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9974RedisHSetVetoTest extends BaseRedisServerTest {
  private static final String TYPE = "W";

  @Test
  void wireHSetDoesNotCountAVetoedCreate() {
    final Database database = createVetoingType();
    try (final Jedis jedis = newClient()) {
      assertThat(persistentHSet(jedis, "{\"pid\":\"k1\"}", "{\"pid\":\"k2\"}", "{\"pid\":\"refused\"}")).isEqualTo(2L);
    }
    assertThat(countRows(database)).isEqualTo(2L);
  }

  @Test
  void queryHSetDoesNotCountAVetoedCreate() {
    final Database database = createVetoingType();
    try (final ResultSet rs = database.command("redis", "HSET " + TYPE + " {\"pid\":\"k1\"} {\"pid\":\"refused\"} {\"pid\":\"k2\"}")) {
      assertThat(((Number) rs.next().getProperty("value")).intValue()).isEqualTo(2);
    }
    assertThat(countRows(database)).isEqualTo(2L);
  }

  @Test
  void anHSetWhoseEveryElementIsVetoedRepliesZero() {
    final Database database = createVetoingType();
    try (final Jedis jedis = newClient()) {
      assertThat(persistentHSet(jedis, "{\"pid\":\"refused\"}", "{\"pid\":\"refused\"}")).isZero();
    }
    try (final ResultSet rs = database.command("redis", "HSET " + TYPE + " {\"pid\":\"refused\"}")) {
      assertThat(((Number) rs.next().getProperty("value")).intValue()).isZero();
    }
    assertThat(countRows(database)).isZero();
  }

  private Database createVetoingType() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.getSchema().getType(TYPE).getEvents()
        .registerListener((BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("pid")));
    return database;
  }

  private static long countRows(final Database database) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long persistentHSet(final Jedis jedis, final String... elements) {
    final String[] args = new String[elements.length + 2];
    args[0] = getDatabaseName();
    args[1] = TYPE;
    System.arraycopy(elements, 0, args, 2, elements.length);
    return ((Number) jedis.sendCommand(() -> "HSET".getBytes(StandardCharsets.UTF_8), args)).longValue();
  }

  @Override
  protected void populateDatabase() {
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
