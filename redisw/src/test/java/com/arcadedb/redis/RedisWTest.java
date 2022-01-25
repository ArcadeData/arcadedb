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
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;

public class RedisWTest extends BaseGraphServerTest {

  private static final int DEF_PORT         = GlobalConfiguration.REDIS_PORT.getValueAsInteger();
  private static final int TOTAL_RAM        = 10_000;
  private static final int TOTAL_PERSISTENT = 1_000;

  @Test
  public void testRAMCommands() {
    Jedis jedis = new Jedis("localhost", DEF_PORT);

    // PING
    Assertions.assertEquals("PONG", jedis.ping());
    Assertions.assertEquals("This is a test", jedis.ping("This is a test"));

    // SET
    long beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      jedis.set("foo" + i, String.valueOf(i));
    System.out.println("SET " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // EXISTS
    Assertions.assertFalse(jedis.exists("fooNotFound"));

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertTrue(jedis.exists("foo" + i));
    System.out.println("EXISTS " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    Assertions.assertEquals(0, jedis.exists("fooNotFound", "eitherThis"));

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; i += 10) {
      final String[] keyChunk = new String[10];
      for (int k = 0; k < 10; ++k)
        keyChunk[k] = "foo" + (i + k);
      Long result = jedis.exists(keyChunk);
      Assertions.assertEquals(10, result);
    }
    System.out.println(
        "MULTI EXISTS (chunk of 10 keys) " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // GET
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertEquals(String.valueOf(i), jedis.get("foo" + i));
    System.out.println("GET " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCR
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertEquals(i + 1L, jedis.incr("foo" + i));
    System.out.println("INCR " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // DECR
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertEquals(i, jedis.decr("foo" + i));
    System.out.println("DECR " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCRBY
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertEquals(i + 3L, jedis.incrBy("foo" + i, 3));
    System.out.println("INCRBY " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // DECRBY
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertEquals(i, jedis.decrBy("foo" + i, 3));
    System.out.println("DECRBY " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCRBYFLOAT
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertEquals(i + 3.3D, jedis.incrByFloat("foo" + i, 3.3));
    System.out.println("INCRBYFLOAT " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // GETDEL
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertEquals(String.valueOf(i + 3.3D), jedis.getDel("foo" + i));
    System.out.println("GETDEL " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    for (int i = 0; i < TOTAL_RAM; ++i)
      Assertions.assertNull(jedis.get("foo" + i));
  }

  @Test
  public void testPersistentCommands() {
    Jedis jedis = new Jedis("localhost", DEF_PORT);

    Database database = getServerDatabase(0, getDatabaseName());

    database.execute("sql", "CREATE DOCUMENT TYPE Account;" +//
        "CREATE PROPERTY Account.id LONG;" +//
        "CREATE INDEX `Account[id]` ON Account (id) UNIQUE;" +//
        "CREATE PROPERTY Account.email STRING;" +//
        "CREATE INDEX `Account[email]` ON Account (email) UNIQUE;");

    // HSET
    long beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; ++i)
      jedis.hset(getDatabaseName(), "Account", "{'id':" + i + ",'email':'jay.miner" + i + "@commodore.com','firstName':'Jay','lastName':'Miner'}");
    System.out.println("HSET " + TOTAL_PERSISTENT + " items to the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HEXISTS
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      // RETRIEVE BY ID (LONG)
      Assertions.assertTrue(jedis.hexists(getDatabaseName() + ".Account[id]", String.valueOf(i)));
      Assertions.assertTrue(jedis.hexists(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com"));
    }
    System.out.println("HEXISTS " + TOTAL_PERSISTENT + " items to the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HGET
    beginTime = System.currentTimeMillis();
    JSONObject expectedJson = new JSONObject("{'firstName':'Jay','lastName':'Miner'}");

    final List<RID> rids = new ArrayList<>();

    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      expectedJson.put("id", i);
      expectedJson.put("email", "jay.miner" + i + "@commodore.com");

      // RETRIEVE BY ID (LONG)
      JSONObject doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[id]", String.valueOf(i)));
      Assertions.assertNotNull(doc.getString("@rid"));
      Assertions.assertEquals("Account", doc.getString("@type"));
      doc.remove("@type");
      doc.remove("@rid");

      Assertions.assertEquals(expectedJson.toMap(), doc.toMap());

      // RETRIEVE BY EMAIL (STRING)
      doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com"));
      Assertions.assertNotNull(doc.getString("@rid"));
      Assertions.assertEquals("Account", doc.getString("@type"));
      doc.remove("@type");
      doc.remove("@rid");

      Assertions.assertEquals(expectedJson.toMap(), doc.toMap());

      // RETRIEVE BY EMAIL (STRING)
      doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com"));
      Assertions.assertNotNull(doc.getString("@rid"));
      Assertions.assertEquals("Account", doc.getString("@type"));
      doc.remove("@type");

      // SAVE THE RID TO BE RETRIEVED IN THE MGET
      final Object rid = doc.remove("@rid");
      rids.add(new RID(database, rid.toString()));

      Assertions.assertEquals(expectedJson.toMap(), doc.toMap());

      // RETRIEVE BY RID
      doc = new JSONObject(jedis.hget(getDatabaseName(), rid.toString()));
      Assertions.assertNotNull(doc.getString("@rid"));
      Assertions.assertEquals("Account", doc.getString("@type"));
      doc.remove("@rid");
      doc.remove("@type");

      Assertions.assertEquals(expectedJson.toMap(), doc.toMap());
    }
    System.out.println("HGET " + TOTAL_PERSISTENT + " items by 2 keys + rid from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    Assertions.assertEquals(TOTAL_PERSISTENT, rids.size());

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; i += 10) {
      final String[] ridChunk = new String[10];
      for (int k = 0; k < 10; ++k)
        ridChunk[k] = rids.get(i + k).toString();

      // RETRIEVE BY CHUNK OF 10 RIDS
      final List<String> result = jedis.hmget(getDatabaseName(), ridChunk);

      Assertions.assertEquals(10, result.size());

      for (int k = 0; k < 10; ++k) {
        final JSONObject doc = new JSONObject(result.get(k));
        Assertions.assertEquals("Account", doc.getString("@type"));
      }
    }

    System.out.println(
        "HMGET " + TOTAL_PERSISTENT + " items by chunks of 10 rids from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HDEL
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; i += 2) {
      // DELETE BY ID (LONG)
      Assertions.assertEquals(2, jedis.hdel(getDatabaseName() + ".Account[id]", String.valueOf(i), String.valueOf(i + 1)));
    }
    System.out.println("HDEL " + TOTAL_PERSISTENT + " items from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");
  }

  @Test
  public void testCommandNotSupported() {
    Jedis jedis = new Jedis("localhost", DEF_PORT);
    try {
      jedis.aclList();
      Assertions.fail();
    } catch (JedisDataException e) {
      // EXPECTED
      Assertions.assertEquals("Command not found", e.getMessage());
    }
  }

  @Override
  protected boolean isPopulateDatabase() {
    return false;
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
