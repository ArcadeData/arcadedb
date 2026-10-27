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
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.SetParams;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10790: a Redis key of a database's key space is the client's key, whatever SQL thinks of it as a variable name. The
 * names SQL reserves for its context ({@code parent}, {@code current}, {@code depth}, {@code path}, {@code stack},
 * {@code history}) were refused, with or without a leading {@code $}, and a key starting with {@code $} was the key without it
 * (issue #10694), because the key went through the SQL accessors of the global variables. The connection's own bucket took them
 * all, so a key behaved differently by the half of the key space it landed in.
 * <p>
 * What SQL can name stays visible to it as {@code $key}; a reserved name stays the context's, never answered from a Redis key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10790RedisReservedKeyNamesTest extends BaseRedisServerTest {

  @Test
  void everyReservedNameIsAKeyOnTheWire() {
    try (final Jedis jedis = connect()) {
      for (final String name : SQLQueryEngine.RESERVED_VARIABLE_NAMES)
        for (final String key : List.of(name, "$" + name)) {
          final String prefixed = getDatabaseName() + "." + key;
          assertThat(jedis.set(prefixed, "v-" + key)).as(prefixed).isEqualTo("OK");
          assertThat(jedis.get(prefixed)).as(prefixed).isEqualTo("v-" + key);
          assertThat(jedis.exists(prefixed)).isTrue();
          assertThat(jedis.set(prefixed, "x", SetParams.setParams().xx())).isEqualTo("OK");
          assertThat(jedis.getDel(prefixed)).isEqualTo("x");
          assertThat(jedis.set(prefixed, "nx", SetParams.setParams().nx())).isEqualTo("OK");
          assertThat(jedis.getDel(prefixed)).isEqualTo("nx");
          assertThat(jedis.incr(prefixed)).isEqualTo(1L);
          assertThat(jedis.decrBy(prefixed, 3)).isEqualTo(-2L);
          assertThat(jedis.getDel(prefixed)).isEqualTo("-2");
        }

      // the same through SELECT, and through the transient hash form
      jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());
      assertThat(jedis.set("depth", "selected")).isEqualTo("OK");
      assertThat(jedis.hget(getDatabaseName(), "depth")).isEqualTo("selected");
      assertThat(jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), "{\"id\":\"history\",\"v\":1}")).isEqualTo(1L);
      assertThat(jedis.hexists(getDatabaseName(), "history")).isTrue();
      assertThat(jedis.hdel(getDatabaseName(), "depth", "history")).isEqualTo(2L);
    }
  }

  @Test
  void aKeyStartingWithDollarIsNotTheKeyWithoutIt() {
    try (final Jedis jedis = connect()) {
      final String plain = getDatabaseName() + ".k10790";
      final String dollar = getDatabaseName() + ".$k10790";
      jedis.set(plain, "plain");
      jedis.set(dollar, "dollar");
      assertThat(jedis.get(plain)).isEqualTo("plain");
      assertThat(jedis.get(dollar)).isEqualTo("dollar");
      assertThat(jedis.getDel(dollar)).isEqualTo("dollar");
      assertThat(jedis.get(plain)).as("the GETDEL of $k left k").isEqualTo("plain");
      assertThat(jedis.set(getDatabaseName() + ".$", "lone")).isEqualTo("OK");
      assertThat(jedis.get(getDatabaseName() + ".")).as("$ is not the empty key").isNull();
    }
  }

  @Test
  void sqlSeesWhatItCanNameAndNeverAReservedName() {
    try (final Jedis jedis = connect()) {
      jedis.set(getDatabaseName() + ".shared10790", "shared");
      jedis.set(getDatabaseName() + ".parent", "fromRedis");
      jedis.set(getDatabaseName() + ".depth", "fromRedis");
    }
    final Database database = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = database.query("sql", "SELECT $shared10790 AS shared, $parent AS parent, $depth AS depth")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("shared")).as("a Redis key SQL can name is $key").isEqualTo("shared");
      assertThat(row.<Object>getProperty("parent")).as("$parent stays the context's").isNull();
      assertThat(row.<Object>getProperty("depth")).as("$depth stays the context's").isNull();
    }
  }

  @Test
  void theQueryLanguageAndTheWireNameTheSameKeys() {
    final Database database = getServerDatabase(0, getDatabaseName());
    for (final String name : SQLQueryEngine.RESERVED_VARIABLE_NAMES) {
      try (final ResultSet rs = database.command("redis", "SET " + name + " ql-" + name)) {
        assertThat(rs.next().<Object>getProperty("value")).isEqualTo("OK");
      }
      assertThat(get(database, name)).isEqualTo("ql-" + name);
    }
    try (final ResultSet rs = database.command("redis", "MULTI\nSET $parent block\nINCR $history\nGET $parent\nEXEC")) {
      final List<?> replies = (List<?>) rs.next().getProperty("value");
      assertThat(((Number) replies.get(1)).longValue()).isEqualTo(1L);
      assertThat(replies.get(2)).isEqualTo("block");
    }
    assertThat(get(database, "parent")).as("$parent is its own key").isEqualTo("ql-parent");

    try (final Jedis jedis = connect()) {
      assertThat(jedis.get(getDatabaseName() + ".parent")).isEqualTo("ql-parent");
      assertThat(jedis.get(getDatabaseName() + ".$parent")).isEqualTo("block");
      assertThat(jedis.get(getDatabaseName() + ".history")).isEqualTo("ql-history");
      assertThat(jedis.get(getDatabaseName() + ".$history")).isEqualTo("1");
    }
  }

  private static Object get(final Database database, final String key) {
    try (final ResultSet rs = database.command("redis", "GET " + key)) {
      return rs.hasNext() ? rs.next().getProperty("value") : null;
    }
  }

  private Jedis connect() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    return jedis;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
