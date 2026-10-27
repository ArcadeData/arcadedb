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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.SetParams;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10694: a database-backed Redis key is stored in the database's global-variable map, whose setters and getters
 * strip a leading {@code $} because, for SQL, {@code $name} and {@code name} are two spellings of one variable. A Redis
 * key is opaque client data, so {@code <db>.$k} and {@code <db>.k} were one key: a write to one overwrote the other,
 * {@code GETDEL <db>.$k} deleted {@code <db>.k}, {@code INCR <db>.$ctr} incremented {@code <db>.ctr}, and {@code ""} and
 * {@code $} were one key. The {@code redis} query language stripped the same {@code $} on purpose, so both surfaces
 * agreed on the wrong answer.
 * <p>
 * The round trip below writes a distinct value to every key of a set that includes the aliasing pairs and reads them all
 * back, in each form a key can take on the wire - database-prefixed, under SELECT, and connection-local - so the
 * database-backed forms are pinned against the connection-local one, which never aliased.
 */
class Issue10694RedisDollarKeyAliasingTest extends BaseRedisServerTest {

  private static final List<String> KEYS = List.of("k", "$k", "$$k", "", "$", "$$", "a.b", "$a.b", "ключ-€-鍵", "$ключ",
      "x".repeat(4096), "$" + "x".repeat(4096));

  @Test
  void databasePrefixedKeysRoundTripWithoutAliasing() {
    try (final Jedis jedis = newJedis()) {
      roundTrip(jedis, getDatabaseName() + ".");
    }
  }

  @Test
  void selectedDatabaseKeysRoundTripWithoutAliasing() {
    try (final Jedis jedis = newJedis()) {
      jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());
      // "a.b" is not database-prefixed ("a" is no database), so under SELECT it is a key of the selected database too
      roundTrip(jedis, "");
    }
  }

  @Test
  void connectionLocalKeysRoundTripWithoutAliasing() {
    try (final Jedis jedis = newJedis()) {
      roundTrip(jedis, "");
    }
  }

  @Test
  void writeThroughOneNameNeverReachesTheOther() {
    final String db = getDatabaseName() + ".";
    try (final Jedis jedis = newJedis()) {
      jedis.set(db + "sess", "user-A");
      jedis.set(db + "$sess", "user-B");
      assertThat(jedis.get(db + "sess")).isEqualTo("user-A");
      assertThat(jedis.get(db + "$sess")).isEqualTo("user-B");

      jedis.set(db + "ctr", "5");
      assertThat(jedis.incr(db + "$ctr")).isEqualTo(1L);
      assertThat(jedis.get(db + "ctr")).isEqualTo("5");
      assertThat(jedis.decr(db + "$$ctr")).isEqualTo(-1L);
      assertThat(jedis.get(db + "$ctr")).isEqualTo("1");

      jedis.set(db + "k1", "one");
      jedis.set(db + "$k1", "two");
      assertThat(jedis.getDel(db + "$k1")).isEqualTo("two");
      assertThat(jedis.get(db + "k1")).isEqualTo("one");
      assertThat(jedis.exists(db + "$k1")).isFalse();
      assertThat(jedis.exists(db + "k1")).isTrue();

      assertThat(jedis.set(db + "$nx", "first", SetParams.setParams().nx())).isEqualTo("OK");
      assertThat(jedis.set(db + "nx", "second", SetParams.setParams().nx())).isEqualTo("OK");
      assertThat(jedis.set(db + "$xx", "never", SetParams.setParams().xx())).isNull();
      jedis.set(db + "xx", "plain");
      assertThat(jedis.get(db + "$xx")).isNull();
      assertThat(jedis.get(db + "$nx")).isEqualTo("first");
      assertThat(jedis.get(db + "nx")).isEqualTo("second");

      jedis.set(db, "DOT");
      assertThat(jedis.get(db + "$")).isNull();
      assertThat(jedis.get(db)).isEqualTo("DOT");
    }
  }

  @Test
  void transientHashKeysDoNotAlias() {
    final String db = getDatabaseName();
    try (final Jedis jedis = newJedis()) {
      final Object stored = jedis.sendCommand(Protocol.Command.HSET, db, "{\"id\":\"u\",\"n\":1}", "{\"id\":\"$u\",\"n\":2}");
      assertThat(((Long) stored).intValue()).isEqualTo(2);

      assertThat(jedis.hget(db, "u")).contains("\"n\":1");
      assertThat(jedis.hget(db, "$u")).contains("\"n\":2");
      assertThat(jedis.hdel(db, "$u")).isEqualTo(1L);
      assertThat(jedis.hexists(db, "$u")).isFalse();
      assertThat(jedis.hexists(db, "u")).isTrue();
    }
  }

  /**
   * The {@code redis} query language and the wire are two surfaces over one key space: they must name the same key by the
   * same string, and both must keep {@code $k} apart from {@code k}.
   */
  @Test
  void queryLanguageAndWireAgreeOnDollarKeys() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String db = getDatabaseName() + ".";
    try (final Jedis jedis = newJedis()) {
      database.command("redis", "SET qk plain");
      database.command("redis", "SET $qk dollar");
      assertThat(redisGet(database, "qk")).isEqualTo("plain");
      assertThat(redisGet(database, "$qk")).isEqualTo("dollar");
      assertThat(jedis.get(db + "qk")).isEqualTo("plain");
      assertThat(jedis.get(db + "$qk")).isEqualTo("dollar");

      jedis.set(db + "$wk", "from-wire");
      assertThat(redisGet(database, "$wk")).isEqualTo("from-wire");
      assertThat(redisGet(database, "wk")).isNull();

      database.command("redis", "SET qc 10");
      database.command("redis", "INCR $qc");
      assertThat(redisGet(database, "qc")).isEqualTo("10");
      assertThat(jedis.get(db + "$qc")).isEqualTo("1");

      database.command("redis", "GETDEL $qk");
      assertThat(redisGet(database, "$qk")).isNull();
      assertThat(jedis.get(db + "qk")).isEqualTo("plain");

      try (final ResultSet rs = database.command("redis", "EXISTS qk $qk")) {
        assertThat(((Number) rs.next().getProperty("value")).intValue()).isEqualTo(1);
      }
    }
  }

  /** The buffered writes of a MULTI/EXEC block are keyed by the client's key too, inside the block and once published. */
  @Test
  void queryLanguageTransactionKeepsDollarKeysApart() {
    final Database database = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = database.command("redis", """
        MULTI
        SET seq plain
        SET $seq dollar
        GET seq
        GET $seq
        EXEC
        """)) {
      final List<?> replies = (List<?>) rs.next().getProperty("value");
      assertThat(replies.get(2)).isEqualTo("plain");
      assertThat(replies.get(3)).isEqualTo("dollar");
    }
    assertThat(redisGet(database, "seq")).isEqualTo("plain");
    assertThat(redisGet(database, "$seq")).isEqualTo("dollar");
  }

  /** A plain key is still the SQL variable of the same name: only the aliasing of {@code $k} onto {@code k} goes away. */
  @Test
  void plainKeyIsStillVisibleToSqlAsAVariable() {
    final Database database = getServerDatabase(0, getDatabaseName());
    try (final Jedis jedis = newJedis()) {
      jedis.set(getDatabaseName() + ".shared", "plain");
      jedis.set(getDatabaseName() + ".$shared", "dollar");
    }
    try (final ResultSet rs = database.query("sql", "SELECT $shared as val")) {
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("plain");
    }
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Redis Protocol Plugin needed for the module to be loaded
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  private void roundTrip(final Jedis jedis, final String prefix) {
    for (int i = 0; i < KEYS.size(); i++)
      assertThat(jedis.set(prefix + KEYS.get(i), "v" + i)).isEqualTo("OK");

    for (int i = 0; i < KEYS.size(); i++)
      assertThat(jedis.get(prefix + KEYS.get(i))).as("key '%s' read back", abbreviate(KEYS.get(i))).isEqualTo("v" + i);

    // delete every other key through its own name: its alias, if it had one, would vanish with it
    for (int i = 0; i < KEYS.size(); i += 2)
      assertThat(jedis.getDel(prefix + KEYS.get(i))).isEqualTo("v" + i);
    for (int i = 0; i < KEYS.size(); i++)
      assertThat(jedis.get(prefix + KEYS.get(i))).as("key '%s' after deleting the even keys", abbreviate(KEYS.get(i)))
          .isEqualTo(i % 2 == 0 ? null : "v" + i);
  }

  private Jedis newJedis() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    return jedis;
  }

  private static Object redisGet(final Database database, final String key) {
    try (final ResultSet rs = database.command("redis", "GET " + key)) {
      return rs.hasNext() ? rs.next().getProperty("value") : null;
    }
  }

  private static String abbreviate(final String key) {
    return key.length() > 20 ? key.substring(0, 20) + "...(" + key.length() + ")" : key;
  }
}
