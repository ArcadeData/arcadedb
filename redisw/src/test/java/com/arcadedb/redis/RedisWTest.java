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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.test.BaseGraphServerTest;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.PROPERTY_TYPES_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class RedisWTest extends BaseGraphServerTest {

  private static final int DEF_PORT         = GlobalConfiguration.REDIS_PORT.getValueAsInteger();
  private static final int TOTAL_RAM        = 10_000;
  private static final int TOTAL_PERSISTENT = 1_000;

  @Test
  void ramCommands() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // PING
    assertThat(jedis.ping()).isEqualTo("PONG");
    assertThat(jedis.ping("This is a test")).isEqualTo("This is a test");

    // SET
    long beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      jedis.set("foo" + i, String.valueOf(i));
    // System.out.println("SET " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // EXISTS
    assertThat(jedis.exists("fooNotFound")).isFalse();

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.exists("foo" + i)).isTrue();
    // System.out.println("EXISTS " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    assertThat(jedis.exists("fooNotFound", "eitherThis")).isEqualTo(0);

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; i += 10) {
      final String[] keyChunk = new String[10];
      for (int k = 0; k < 10; ++k)
        keyChunk[k] = "foo" + (i + k);
      final Long result = jedis.exists(keyChunk);
      assertThat(result).isEqualTo(10);
    }
    // System.out.println(
        "MULTI EXISTS (chunk of 10 keys) " + TOTAL_RAM + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // GET
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.get("foo" + i)).isEqualTo(String.valueOf(i));
    // System.out.println("GET " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCR
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.incr("foo" + i)).isEqualTo(i + 1L);
    // System.out.println("INCR " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // DECR
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.decr("foo" + i)).isEqualTo(i);
    // System.out.println("DECR " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCRBY
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.incrBy("foo" + i, 3)).isEqualTo(i + 3L);
    // System.out.println("INCRBY " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // DECRBY
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.decrBy("foo" + i, 3)).isEqualTo(i);
    // System.out.println("DECRBY " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCRBYFLOAT
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.incrByFloat("foo" + i, 3.3)).isEqualTo(i + 3.3D);
    // System.out.println("INCRBYFLOAT " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // GETDEL
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.getDel("foo" + i)).isEqualTo(String.valueOf(i + 3.3D));
    // System.out.println("GETDEL " + TOTAL_RAM + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    for (int i = 0; i < TOTAL_RAM; ++i)
      assertThat(jedis.get("foo" + i)).isNull();
  }

  @Test
  void persistentCommands() {
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
    // System.out.println("HSET " + TOTAL_PERSISTENT + " items to the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HEXISTS
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      // RETRIEVE BY ID (LONG)
      assertThat(jedis.hexists(getDatabaseName() + ".Account[id]", String.valueOf(i))).isTrue();
      assertThat(jedis.hexists(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com")).isTrue();
    }
    // // System.out.println("HEXISTS " + TOTAL_PERSISTENT + " items to the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HGET
    beginTime = System.currentTimeMillis();
    final JSONObject expectedJson = new JSONObject("{'firstName':'Jay','lastName':'Miner'}");

    final List<RID> rids = new ArrayList<>();

    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      expectedJson.put("id", i);
      expectedJson.put("email", "jay.miner" + i + "@commodore.com");

      // RETRIEVE BY ID (LONG)
      JSONObject doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[id]", String.valueOf(i)));
      assertThat(doc.getString(RID_PROPERTY)).isNotNull();
      assertThat(doc.getString(TYPE_PROPERTY)).isEqualTo("Account");
      doc.remove(TYPE_PROPERTY);
      doc.remove(RID_PROPERTY);
      doc.remove(CAT_PROPERTY);
      doc.remove(PROPERTY_TYPES_PROPERTY);

      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());

      // RETRIEVE BY EMAIL (STRING)
      doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com"));
      assertThat(doc.getString(RID_PROPERTY)).isNotNull();
      assertThat(doc.getString(TYPE_PROPERTY)).isEqualTo("Account");
      doc.remove(TYPE_PROPERTY);
      doc.remove(RID_PROPERTY);
      doc.remove(CAT_PROPERTY);
      doc.remove(PROPERTY_TYPES_PROPERTY);

      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());

      // RETRIEVE BY EMAIL (STRING)
      doc = new JSONObject(jedis.hget(getDatabaseName() + ".Account[email]", "jay.miner" + i + "@commodore.com"));
      assertThat(doc.getString(RID_PROPERTY)).isNotNull();
      assertThat(doc.getString(TYPE_PROPERTY)).isEqualTo("Account");
      doc.remove(TYPE_PROPERTY);
      doc.remove(CAT_PROPERTY);
      doc.remove(PROPERTY_TYPES_PROPERTY);

      // SAVE THE RID TO BE RETRIEVED IN THE MGET
      final Object rid = doc.remove(RID_PROPERTY);
      rids.add(new RID(database, rid.toString()));

      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());

      // RETRIEVE BY RID
      doc = new JSONObject(jedis.hget(getDatabaseName(), rid.toString()));
      assertThat(doc.getString(RID_PROPERTY)).isNotNull();
      assertThat(doc.getString(TYPE_PROPERTY)).isEqualTo("Account");
      doc.remove(RID_PROPERTY);
      doc.remove(TYPE_PROPERTY);
      doc.remove(CAT_PROPERTY);
      doc.remove(PROPERTY_TYPES_PROPERTY);

      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());
    }
    // System.out.println("HGET " + TOTAL_PERSISTENT + " items by 2 keys + rid from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

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
        assertThat(doc.getString(TYPE_PROPERTY)).isEqualTo("Account");
      }
    }

    // System.out.println(
        "HMGET " + TOTAL_PERSISTENT + " items by chunks of 10 rids from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HDEL
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; i += 2) {
      // DELETE BY ID (LONG)
      assertThat(jedis.hdel(getDatabaseName() + ".Account[id]", String.valueOf(i), String.valueOf(i + 1))).isEqualTo(2);
    }
    // System.out.println("HDEL " + TOTAL_PERSISTENT + " items from the database. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");
  }

  @Test
  void transientCommands() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // HSET transient (JSON objects to globalVariables)
    long beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(),
          "{\"id\":\"user" + i + "\",\"email\":\"jay.miner" + i + "@commodore.com\",\"firstName\":\"Jay\",\"lastName\":\"Miner\"}");
    }
    // System.out.println("HSET transient " + TOTAL_PERSISTENT + " items to globalVariables. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HEXISTS transient
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      assertThat(jedis.hexists(getDatabaseName(), "user" + i)).isTrue();
    }
    // System.out.println("HEXISTS transient " + TOTAL_PERSISTENT + " items from globalVariables. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HGET transient
    beginTime = System.currentTimeMillis();
    final JSONObject expectedJson = new JSONObject("{\"firstName\":\"Jay\",\"lastName\":\"Miner\"}");

    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      expectedJson.put("id", "user" + i);
      expectedJson.put("email", "jay.miner" + i + "@commodore.com");

      final String jsonStr = jedis.hget(getDatabaseName(), "user" + i);
      assertThat(jsonStr).isNotNull();
      final JSONObject doc = new JSONObject(jsonStr);
      assertThat(doc.toMap()).isEqualTo(expectedJson.toMap());
    }
    // System.out.println("HGET transient " + TOTAL_PERSISTENT + " items from globalVariables. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HMGET transient (chunks of 10)
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; i += 10) {
      final String[] keyChunk = new String[10];
      for (int k = 0; k < 10; ++k)
        keyChunk[k] = "user" + (i + k);

      final List<String> result = jedis.hmget(getDatabaseName(), keyChunk);
      assertThat(result.size()).isEqualTo(10);

      for (int k = 0; k < 10; ++k) {
        final JSONObject doc = new JSONObject(result.get(k));
        assertThat(doc.getString("firstName")).isEqualTo("Jay");
      }
    }
    // System.out.println("HMGET transient " + TOTAL_PERSISTENT + " items by chunks of 10 from globalVariables. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // HDEL transient
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_PERSISTENT; i += 2) {
      assertThat(jedis.hdel(getDatabaseName(), "user" + i, "user" + (i + 1))).isEqualTo(2);
    }
    // System.out.println("HDEL transient " + TOTAL_PERSISTENT + " items from globalVariables. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // Verify all deleted
    for (int i = 0; i < TOTAL_PERSISTENT; ++i) {
      assertThat(jedis.hexists(getDatabaseName(), "user" + i)).isFalse();
    }
  }

  @Test
  void commandNotSupported() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);
    try {
      jedis.aclList();
      fail("");
    } catch (final JedisDataException e) {
      // EXPECTED
      assertThat(e.getMessage()).isEqualTo("Command not found");
    }
  }

  @Test
  void selectCommandSwitchesDatabase() {
    // Test SELECT command to switch database context
    // Issue #3246: Redis commands should use database's globalVariables
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Select the test database (using sendCommand because Jedis select() expects int)
    final Object selectResult = jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());
    // sendCommand returns byte[] for simple strings
    final String resultStr = selectResult instanceof byte[] ? new String((byte[]) selectResult) : selectResult.toString();
    assertThat(resultStr).isEqualTo("OK");

    // Set a value via Redis wire protocol
    jedis.set("wireKey", "wireValue");

    // Get it back
    assertThat(jedis.get("wireKey")).isEqualTo("wireValue");

    // Verify the value is in the database's globalVariables (accessible via SQL)
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    assertThat(database.getGlobalVariable("wireKey")).isEqualTo("wireValue");
  }

  @Test
  void keyPrefixOverridesSelectedDatabase() {
    // Test that key prefix (dbname.key) takes priority over SELECT
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Set value using key prefix (no SELECT needed)
    jedis.set(getDatabaseName() + ".prefixKey", "prefixValue");

    // Get it back using prefix
    assertThat(jedis.get(getDatabaseName() + ".prefixKey")).isEqualTo("prefixValue");

    // Verify it's in the database's globalVariables
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    assertThat(database.getGlobalVariable("prefixKey")).isEqualTo("prefixValue");
  }

  @Test
  void selectThenIncrDecr() {
    // Test INCR/DECR with database context
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Select database
    jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());

    // INCR on non-existent key should start from 0
    assertThat(jedis.incr("newCounter")).isEqualTo(1L);
    assertThat(jedis.incr("newCounter")).isEqualTo(2L);

    // Verify in database globalVariables
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    assertThat(((Number) database.getGlobalVariable("newCounter")).longValue()).isEqualTo(2L);

    // DECR
    assertThat(jedis.decr("newCounter")).isEqualTo(1L);
    assertThat(((Number) database.getGlobalVariable("newCounter")).longValue()).isEqualTo(1L);
  }

  @Test
  void globalVariablesSharedWithSQL() {
    // Test that Redis values are accessible via SQL $variable syntax
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Select database and set a value
    jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());
    jedis.set("sqlSharedVar", "hello_from_redis_wire");

    // Access via SQL
    final Database database = getServerDatabase(0, getDatabaseName());
    try (final var rs = database.query("sql", "SELECT $sqlSharedVar as val")) {
      assertThat(rs.hasNext()).isTrue();
      final var result = rs.next();
      assertThat((String) result.getProperty("val")).isEqualTo("hello_from_redis_wire");
    }
  }

  @Test
  void existsAndGetDelWithDatabaseContext() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Select database
    jedis.sendCommand(Protocol.Command.SELECT, getDatabaseName());

    // Set some values
    jedis.set("existKey1", "value1");
    jedis.set("existKey2", "value2");

    // EXISTS
    assertThat(jedis.exists("existKey1")).isTrue();
    assertThat(jedis.exists("existKey1", "existKey2")).isEqualTo(2);
    assertThat(jedis.exists("nonExistent")).isFalse();

    // GETDEL
    assertThat(jedis.getDel("existKey1")).isEqualTo("value1");
    assertThat(jedis.exists("existKey1")).isFalse();

    // Verify in database globalVariables
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    assertThat(database.getGlobalVariable("existKey1")).isNull();
    assertThat(database.getGlobalVariable("existKey2")).isEqualTo("value2");
  }

  @Test
  void transientHSetAndHGet() {
    // Test HSET in transient mode (type omitted, JSON as second argument)
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Store JSON objects in globalVariables (transient mode)
    // For transient: HSET <database> <json> where json starts with '{'
    // Use sendCommand because Jedis hset() expects (key, field, value) format
    final Object result = jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(),
        "{\"id\":\"user1\",\"name\":\"John\",\"age\":30}",
        "{\"id\":\"123\",\"email\":\"test@example.com\"}");
    // Result is the count of stored items
    assertThat(((Long) result).intValue()).isEqualTo(2);

    // Retrieve using HGET in transient mode (no dot, key doesn't start with #)
    final String user1 = jedis.hget(getDatabaseName(), "user1");
    assertThat(user1).isNotNull();
    final JSONObject user1Json = new JSONObject(user1);
    assertThat(user1Json.getString("name")).isEqualTo("John");
    assertThat(user1Json.getInt("age")).isEqualTo(30);

    // Retrieve numeric id (stored as string key "123")
    final String item123 = jedis.hget(getDatabaseName(), "123");
    assertThat(item123).isNotNull();
    final JSONObject item123Json = new JSONObject(item123);
    assertThat(item123Json.getString("email")).isEqualTo("test@example.com");

    // Verify also accessible via database globalVariables
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    assertThat(database.getGlobalVariable("user1")).isNotNull();
  }

  @Test
  void transientHExists() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Store JSON object using sendCommand for transient mode
    jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(), "{\"id\":\"existsTest\",\"value\":\"test\"}");

    // HEXISTS in transient mode
    assertThat(jedis.hexists(getDatabaseName(), "existsTest")).isTrue();
    assertThat(jedis.hexists(getDatabaseName(), "nonExistent")).isFalse();
  }

  @Test
  void transientHDel() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Store JSON objects using sendCommand
    jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(),
        "{\"id\":\"delTest1\",\"value\":\"v1\"}",
        "{\"id\":\"delTest2\",\"value\":\"v2\"}");

    // Verify they exist
    assertThat(jedis.hexists(getDatabaseName(), "delTest1")).isTrue();
    assertThat(jedis.hexists(getDatabaseName(), "delTest2")).isTrue();

    // HDEL in transient mode (no dot in bucket name)
    final long deleted = jedis.hdel(getDatabaseName(), "delTest1");
    assertThat(deleted).isEqualTo(1);

    // Verify delTest1 is gone but delTest2 remains
    assertThat(jedis.hexists(getDatabaseName(), "delTest1")).isFalse();
    assertThat(jedis.hexists(getDatabaseName(), "delTest2")).isTrue();
  }

  @Test
  void transientHMGet() {
    final Jedis jedis = new Jedis("localhost", DEF_PORT);

    // Store multiple JSON objects using sendCommand
    jedis.sendCommand(Protocol.Command.HSET, getDatabaseName(),
        "{\"id\":\"mget1\",\"name\":\"First\"}",
        "{\"id\":\"mget2\",\"name\":\"Second\"}",
        "{\"id\":\"mget3\",\"name\":\"Third\"}");

    // HMGET in transient mode
    final List<String> results = jedis.hmget(getDatabaseName(), "mget1", "mget2", "mget3", "nonExistent");
    assertThat(results).hasSize(4);

    final JSONObject first = new JSONObject(results.get(0));
    assertThat(first.getString("name")).isEqualTo("First");

    final JSONObject second = new JSONObject(results.get(1));
    assertThat(second.getString("name")).isEqualTo("Second");

    final JSONObject third = new JSONObject(results.get(2));
    assertThat(third.getString("name")).isEqualTo("Third");

    assertThat(results.get(3)).isNull();
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
