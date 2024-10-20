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
import com.arcadedb.database.RID;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class RedisWTest extends BaseGraphServerTest {

  private static final int DEF_PORT         = GlobalConfiguration.REDIS_PORT.getValueAsInteger();
  private static final int TOTAL_RAM        = 10_000;
  private static final int TOTAL_PERSISTENT = 1_000;

  @Test
  public void testRAMCommands() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // PING
    assertThat(jedis.ping()).isEqualTo("PONG");
    assertThat(jedis.ping("This is a test")).isEqualTo("This is a test");

    // SET
    long beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      jedis.set("foo" + i, String.valueOf(i));
    System.out.println("SET " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // EXISTS
    assertThat(jedis.exists("fooNotFound")).isFalse();

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.exists("foo" + i)).isTrue();
    System.out.println("EXISTS " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    assertThat(jedis.exists("fooNotFound", "eitherThis")).isEqualTo(0);

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; i += 10) {
      final String[] keyChunk = new String[10];
      for (int k = 0; k < 10; ++k)
        keyChunk[k] = "foo" + (i + k);
      final Long result = jedis.exists(keyChunk);
      assertThat(result).isEqualTo(10);
    }
    System.out.println(
        "MULTI EXISTS (chunk of 10 keys) " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // GET
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.get("foo" + i)).isEqualTo(String.valueOf(i));
    System.out.println("GET " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCR
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.incr("foo" + i)).isEqualTo(i + 1L);
    System.out.println("INCR " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // DECR
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.decr("foo" + i)).isEqualTo(i);
    System.out.println("DECR " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCRBY
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.incrBy("foo" + i, 3)).isEqualTo(i + 3L);
    System.out.println("INCRBY " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // DECRBY
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.decrBy("foo" + i, 3)).isEqualTo(i);
    System.out.println("DECRBY " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCRBYFLOAT
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.incrByFloat("foo" + i, 3.3)).isEqualTo(i + 3.3D);
    System.out.println("INCRBYFLOAT " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // GETDEL
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.getDel("foo" + i)).isEqualTo(String.valueOf(i + 3.3D));
    System.out.println("GETDEL " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.get("foo" + i)).isNull();
  }

  @Test
  public void testPersistentCommands() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    final Database database = getServerDatabase(0, getDatabaseName());

    database.command("sqlscript", "CREATE DOCUMENT TYPE Account;" +//
        "CREATE PROPERTY Account.id LONG;" +//
        "CREATE INDEX ON Account (id) UNIQUE;" +//
        "CREATE PROPERTY Account.email STRING;" +//
        "CREATE INDEX ON Account (email) UNIQUE;");

    // HSET
    long beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; ++i)
      jedis.hset(getDatabaseName(), "Account", "{'id':" + i + ",'email':'jay.miner" + i + "@commodore.com','firstName':'Jay','lastName':'Miner'}");
    System.out.println("HSET " + TOTAL_PERSISTENT + " items to the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HEXISTS
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      // RETRIEVE BY ID (LONG)
      assertThat(jedis.hexists(getDatabaseName() + ".Account[id]", String.valueOf(i))).isTrue();
      assertThat(jedis.hexists(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com")).isTrue();
    }
    System.out.println("HEXISTS " + TOTAL_PERSISTENT + " items to the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HGET
    beginTime = System.currentTimeMillis();
    final JSONObject expectedJson = new JSONObject("{'firstName':'Jay','lastName':'Miner'}");

    final List<RID> rids = new ArrayList<>();

    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      expectedJson.put("id", i);
      expectedJson.put("email", "jay.miner" + i + "@commodore.com");

      // RETRIEVE BY ID (LONG)
      JSONObject doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[id]", String.valueOf(i)));
      assertThat(doc.getString("@rid")).isNotNull();
      assertThat(doc.getString("@type")).isEqualTo("Account");
      doc.remove("@type");
      doc.remove("@rid");
      doc.remove("@cat");

      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());

      // RETRIEVE BY EMAIL (STRING)
      doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com"));
      assertThat(doc.getString("@rid")).isNotNull();
      assertThat(doc.getString("@type")).isEqualTo("Account");
      doc.remove("@type");
      doc.remove("@rid");
      doc.remove("@cat");

      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());

      // RETRIEVE BY EMAIL (STRING)
      doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com"));
      assertThat(doc.getString("@rid")).isNotNull();
      assertThat(doc.getString("@type")).isEqualTo("Account");
      doc.remove("@type");
      doc.remove("@cat");

      // SAVE THE RID TO BE RETRIEVED IN THE MGET
      final Object rid = doc.remove("@rid");
      rids.add(new RID(database, rid.toString()));

      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());

      // RETRIEVE BY RID
      doc = new JSONObject(jedis.hget(getDatabaseName(), rid.toString()));
      assertThat(doc.getString("@rid")).isNotNull();
      assertThat(doc.getString("@type")).isEqualTo("Account");
      doc.remove("@rid");
      doc.remove("@type");
      doc.remove("@cat");

      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());
    }
    System.out.println("HGET " + TOTAL_PERSISTENT + " items by 2 keys + rid from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    assertThat(rids.size()).isEqualTo(TOTAL_PERSISTENT);

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; i += 10) {
      final String[] ridChunk = new String[10];
      for (int k = 0; k < 10; ++k)
        ridChunk[k] = rids.get(i + k).toString();

      // RETRIEVE BY CHUNK OF 10 RIDS
      final List<String> result = jedis.hmget(getDatabaseName(), ridChunk);

      assertThat(result.size()).isEqualTo(10);

      for (int k = 0; k < 10; ++k) {
        final JSONObject doc = new JSONObject(result.get(k));
        assertThat(doc.getString("@type")).isEqualTo("Account");
      }
    }

    System.out.println(
        "HMGET " + TOTAL_PERSISTENT + " items by chunks of 10 rids from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HDEL
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; i += 2) {
      // DELETE BY ID (LONG)
      assertThat(jedis.hdel(getDatabaseName() + ".Account[id]", String.valueOf(i), String.valueOf(i + 1))).isEqualTo(2);
    }
    System.out.println("HDEL " + TOTAL_PERSISTENT + " items from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");
  }

  @Test
  public void testCommandNotSupported() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);
    try {
      jedis.aclList();
      fail("");
    } catch (final JedisDataException e) {
      // EXPECTED
      assertThat(e.getMessage()).isEqualTo("Command not found");
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
