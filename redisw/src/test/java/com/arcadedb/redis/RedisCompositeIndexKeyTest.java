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
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every index lookup the Redis wire protocol and the {@code redis} query language serve - HGET, HMGET, HEXISTS,
 * HDEL - reads the key through the same {@link RedisIndexKeys#parse(String)}, so a key shape that works for one
 * of them works for all of them.
 * <p>
 * It did not use to: the four call sites each carried their own copy of the parsing and the copies drifted.
 * HMGET's cast the key to {@code String[]} instead of parsing it, so a bracketed composite key came back as a
 * {@code ClassCastException} while HGET on the very same index and key came back with the record (#6757), and the
 * query language's narrowed a numeric-looking key to a {@code Long} before the index could narrow it to the
 * property's ACTUAL declared type, losing the record behind a {@code STRING} key like {@code 007}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/6757">Issue #6757</a>
 */
public class RedisCompositeIndexKeyTest extends BaseGraphServerTest {

  private static final int DEF_PORT = GlobalConfiguration.REDIS_PORT.getValueAsInteger();

  @Test
  void hmgetReadsABracketedCompositeKeyJustLikeHget() {
    try (final Jedis jedis = newClient()) {
      createAccounts(10);

      final String index = getDatabaseName() + ".CompositeAccount[firstName,lastName]";

      // #6757: THIS IS THE COMBINATION THAT THREW ClassCastException
      final List<String> single = jedis.hmget(index, "[\"first_3\",\"last_3\"]");
      assertThat(single).hasSize(1);
      assertAccount(single.get(0), 3);

      // HMGET'S WHOLE POINT IS MORE THAN ONE KEY, AND A MISSING ONE MUST STILL HOLD ITS POSITION IN THE REPLY
      final List<String> many = jedis.hmget(index, "[\"first_1\",\"last_1\"]", "[\"nobody\",\"here\"]", "[\"first_7\",\"last_7\"]");
      assertThat(many).hasSize(3);
      assertAccount(many.get(0), 1);
      assertThat(many.get(1)).isNull();
      assertAccount(many.get(2), 7);

      // THE SINGLE-KEY SIBLING THAT ALWAYS WORKED, PINNED SO THE TWO CANNOT DRIFT APART AGAIN
      assertAccount(jedis.hget(index, "[\"first_3\",\"last_3\"]"), 3);
      assertThat(jedis.hexists(index, "[\"first_3\",\"last_3\"]")).isTrue();
      assertThat(jedis.hexists(index, "[\"nobody\",\"here\"]")).isFalse();

      assertThat(jedis.hdel(index, "[\"first_0\",\"last_0\"]", "[\"first_9\",\"last_9\"]")).isEqualTo(2);
      assertThat(jedis.hexists(index, "[\"first_0\",\"last_0\"]")).isFalse();
    }
  }

  @Test
  void aNumericLookingStringKeyKeepsItsExactCharacters() {
    try (final Jedis jedis = newClient()) {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Badge;\
          CREATE PROPERTY Badge.code STRING;\
          CREATE INDEX ON Badge (code) UNIQUE;""");

      // 007 AND THE 30-DIGIT ONE ARE BOTH VALID STRING KEYS, AND NEITHER SURVIVES BEING GUESSED INTO A Long
      final String hugeCode = "123456789012345678901234567890";
      for (final String code : new String[] { "007", "7", hugeCode })
        jedis.hset(getDatabaseName(), "Badge", "{\"code\":\"" + code + "\"}");

      final String index = getDatabaseName() + ".Badge[code]";
      assertThat(new JSONObject(jedis.hget(index, "007")).getString("code")).isEqualTo("007");
      assertThat(new JSONObject(jedis.hget(index, "7")).getString("code")).isEqualTo("7");
      assertThat(new JSONObject(jedis.hget(index, hugeCode)).getString("code")).isEqualTo(hugeCode);

      // ...INCLUDING THROUGH THE `redis` QUERY LANGUAGE, WHICH USED TO DO EXACTLY THAT GUESS
      assertThat(firstCode(database, "HGET Badge[code] 007")).isEqualTo("007");
      assertThat(firstCode(database, "HGET Badge[code] 7")).isEqualTo("7");
      assertThat(firstCode(database, "HGET Badge[code] " + hugeCode)).isEqualTo(hugeCode);

      // AND A LONG KEY IS STILL FOUND BY ITS PLAIN DECIMAL TEXT: NARROWING IS THE INDEX'S JOB, NOT THE PARSER'S
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Ticket;\
          CREATE PROPERTY Ticket.id LONG;\
          CREATE INDEX ON Ticket (id) UNIQUE;""");
      jedis.hset(getDatabaseName(), "Ticket", "{\"id\":42}");
      assertThat(new JSONObject(jedis.hget(getDatabaseName() + ".Ticket[id]", "42")).getInt("id")).isEqualTo(42);
    }
  }

  @Test
  void aMalformedCompositeKeyIsAnErrorReplyNotACrash() {
    try (final Jedis jedis = newClient()) {
      createAccounts(1);

      final String index = getDatabaseName() + ".CompositeAccount[firstName,lastName]";

      for (final String badKey : new String[] { "[\"first_0\"", "[]" })
        assertThatThrownBy(() -> jedis.hmget(index, badKey)).isInstanceOf(JedisDataException.class)
            .hasMessageContaining("Composite index key");

      // A LONE QUOTE IS A ONE-CHARACTER VALUE, NOT AN UNTERMINATED QUOTING: IT MUST NOT BLOW UP THE CONNECTION
      assertThat(jedis.hmget(getDatabaseName() + ".CompositeAccount[firstName]", "\"")).containsExactly((String) null);
    }
  }

  private void createAccounts(final int total) {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sqlscript", """
        CREATE DOCUMENT TYPE CompositeAccount;\
        CREATE PROPERTY CompositeAccount.firstName STRING;\
        CREATE PROPERTY CompositeAccount.lastName STRING;\
        CREATE INDEX ON CompositeAccount (firstName, lastName) UNIQUE;\
        CREATE INDEX ON CompositeAccount (firstName) NOTUNIQUE;""");

    try (final Jedis jedis = newClient()) {
      for (int i = 0; i < total; ++i)
        jedis.hset(getDatabaseName(), "CompositeAccount", "{\"firstName\":\"first_" + i + "\",\"lastName\":\"last_" + i + "\"}");
    }
  }

  private static void assertAccount(final String json, final int i) {
    assertThat(json).isNotNull();
    final JSONObject doc = new JSONObject(json);
    assertThat(doc.getString("firstName")).isEqualTo("first_" + i);
    assertThat(doc.getString("lastName")).isEqualTo("last_" + i);
  }

  private static String firstCode(final Database database, final String redisCommand) {
    try (final ResultSet rs = database.query("redis", redisCommand)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("code");
    }
  }

  private Jedis newClient() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);
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
