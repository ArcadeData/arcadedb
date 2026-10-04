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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for:
 * <ul>
 *   <li>#9058 - INCRBYFLOAT answered Java's {@code Double.toString} text and stored a {@code Double}, so INCR failed on an
 *   integral result.</li>
 *   <li>#9059 - no argument-count check, and {@code Long.parseLong} leniency ({@code +5}, {@code 05}) for amounts and values.</li>
 *   <li>#9056 - {@code HDEL <db> <rid>} answered 0 and deleted nothing.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9058And9059And9056RedisParityTest extends BaseRedisServerTest {

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

  @Test
  void incrByFloatReplyAndStoredValueAreRedisDecimalText() {
    try (final Jedis jedis = connect()) {
      assertThat(text(jedis, "INCRBYFLOAT", "f1", "1")).isEqualTo("1");
      assertThat(jedis.get("f1")).isEqualTo("1");
      assertThat(jedis.incr("f1")).isEqualTo(2L);

      assertThat(jedis.incrByFloat("f2", 1.5)).isEqualTo(1.5);
      assertThat(text(jedis, "INCRBYFLOAT", "f2", "1.5")).isEqualTo("3");
      assertThat(jedis.incr("f2")).isEqualTo(4L);

      text(jedis, "INCRBYFLOAT", "f3", "0.1");
      assertThat(text(jedis, "INCRBYFLOAT", "f3", "0.2")).isEqualTo("0.3");
      assertThat(text(jedis, "INCRBYFLOAT", "f4", "1e3")).isEqualTo("1000");
      assertThat(text(jedis, "INCRBYFLOAT", "f5", "1e21")).isEqualTo("1000000000000000000000");
      assertThat(text(jedis, "INCRBYFLOAT", "f6", "1e-7")).isEqualTo("0.0000001");

      jedis.set("f8", "10");
      assertThat(text(jedis, "INCRBYFLOAT", "f8", "5")).isEqualTo("15");
      assertThat(jedis.get("f8")).isEqualTo("15");
      jedis.set("f7", "10");
      assertThat(text(jedis, "INCRBYFLOAT", "f7", "0.5")).isEqualTo("10.5");

      // a fractional result is still refused by INCR, as in Redis
      assertThatThrownBy(() -> jedis.incr("f7")).isInstanceOf(JedisDataException.class)
          .hasMessageContaining("value is not an integer or out of range");

      // a hostile exponent is refused instead of allocating a gigantic number
      assertThatThrownBy(() -> text(jedis, "INCRBYFLOAT", "f9", "1e999999999")).isInstanceOf(JedisDataException.class)
          .hasMessageContaining("increment would produce NaN or Infinity");

      // a tiny exponent must not make the exact addition align gigantic numbers (increment and stored-value paths)
      assertThat(text(jedis, "INCRBYFLOAT", "t1", "1e-999999999")).isEqualTo("0");
      jedis.set("t2", "1e-999999999");
      assertThat(text(jedis, "INCRBYFLOAT", "t2", "1")).isEqualTo("1");

      // an operand of 5 KB or more is refused, as a stored value and as an increment
      final String huge = "1".repeat(5120);
      jedis.set("h1", huge);
      refused(jedis, "value is not a valid float", "INCRBYFLOAT", "h1", "1");
      refused(jedis, "value is not a valid float", "INCRBYFLOAT", "h2", huge);
    }
  }

  @Test
  void wrongArgumentCountIsRefusedAndTheKeyIsUnchanged() {
    try (final Jedis jedis = connect()) {
      jedis.set("n1", "10");
      refused(jedis, "wrong number of arguments for 'incr' command", "INCR", "n1", "5");
      refused(jedis, "wrong number of arguments for 'decr' command", "DECR", "n1", "100");
      refused(jedis, "wrong number of arguments for 'incrby' command", "INCRBY", "n1");
      refused(jedis, "wrong number of arguments for 'decrby' command", "DECRBY", "n1");
      refused(jedis, "wrong number of arguments for 'incr' command", "INCR");
      refused(jedis, "wrong number of arguments for 'get' command", "GET");
      refused(jedis, "wrong number of arguments for 'get' command", "GET", "n1", "n2");
      refused(jedis, "wrong number of arguments for 'set' command", "SET", "n2");
      assertThat(jedis.get("n1")).isEqualTo("10");
    }
  }

  @Test
  void nonCanonicalIntegersAreRefused() {
    try (final Jedis jedis = connect()) {
      jedis.set("n1", "10");
      for (final String amount : new String[] { "+5", "05", "-05", "abc", "-0", "-", "1.5", " 5", "9223372036854775808", "" }) {
        refused(jedis, "value is not an integer or out of range", "INCRBY", "n1", amount);
        refused(jedis, "value is not an integer or out of range", "DECRBY", "n1", amount);
      }
      assertThat(jedis.get("n1")).isEqualTo("10");

      for (final String stored : new String[] { "+10", "007", "-0" }) {
        jedis.set("z", stored);
        refused(jedis, "value is not an integer or out of range", "INCR", "z");
        assertThat(jedis.get("z")).isEqualTo(stored);
      }

      assertThat(jedis.incrBy("n1", -5)).isEqualTo(5L);
      assertThat(jedis.decrBy("n1", 0)).isEqualTo(5L);
      jedis.set("min", String.valueOf(Long.MIN_VALUE));
      assertThat(jedis.incr("min")).isEqualTo(Long.MIN_VALUE + 1);
    }
  }

  @Test
  void hdelDeletesRecordsByRid() {
    final Database db = getServer(0).getDatabase(getDatabaseName());
    db.getSchema().createDocumentType("Item");
    final String[] rid = new String[4];
    db.transaction(() -> {
      for (int i = 0; i < 4; i++)
        rid[i] = db.newDocument("Item").set("n", i).save().getIdentity().toString();
    });

    try (final Jedis jedis = connect()) {
      final String bucket = getDatabaseName();
      assertThat(jedis.hdel(bucket, rid[0])).isEqualTo(1L);
      assertThat(count(db)).isEqualTo(3L);
      assertThat(db.existsRecord(new RID(rid[0]))).isFalse();

      // already gone: nothing deleted, nothing raised
      assertThat(jedis.hdel(bucket, rid[0])).isEqualTo(0L);

      // a malformed RID in the bucket argument is refused before anything is deleted
      refused(jedis, "invalid RID", "HDEL", bucket + ".#abc", rid[1]);
      assertThat(count(db)).isEqualTo(3L);

      // a duplicate RID is deleted once and counted once
      assertThat(jedis.hdel(bucket, rid[1], rid[1])).isEqualTo(1L);
      assertThat(count(db)).isEqualTo(2L);
      // a key that only starts with '#' is a variable name, not a RID
      jedis.sendCommand(() -> "HSET".getBytes(StandardCharsets.UTF_8), bucket, "{\"id\":\"#tag\"}");
      assertThat(jedis.hdel(bucket, "#tag")).isEqualTo(1L);

      // several RIDs in one call, mixed with a global variable name
      jedis.sendCommand(() -> "HSET".getBytes(StandardCharsets.UTF_8), bucket, "{\"id\":\"var\"}");
      assertThat(jedis.hexists(bucket, "var")).isTrue();
      // the record and the variable are counted, the RID of a bucket that does not exist is 0
      assertThat(jedis.hdel(bucket, rid[2], "#999:99", "var")).isEqualTo(2L);
      assertThat(count(db)).isEqualTo(1L);
      assertThat(jedis.hexists(bucket, "var")).isFalse();

      // the dotted form deletes the bucket RID and every RID among the keys
      assertThat(jedis.hdel(bucket + "." + rid[3], rid[3])).isEqualTo(1L);
      assertThat(count(db)).isEqualTo(0L);
    }
  }

  @Test
  void otherCommandsRefuseAWrongArgumentCount() {
    try (final Jedis jedis = connect()) {
      refused(jedis, "wrong number of arguments for 'hdel' command", "HDEL", "db");
      refused(jedis, "wrong number of arguments for 'hget' command", "HGET", "db");
      refused(jedis, "wrong number of arguments for 'hget' command", "HGET", "db", "k", "extra");
      refused(jedis, "wrong number of arguments for 'hexists' command", "HEXISTS", "db", "k", "extra");
      refused(jedis, "wrong number of arguments for 'hmget' command", "HMGET", "db");
      refused(jedis, "wrong number of arguments for 'hset' command", "HSET", "db");
      refused(jedis, "wrong number of arguments for 'hset' command", "HSET", "db", "Item");
      refused(jedis, "wrong number of arguments for 'hmset' command", "HMSET", "db");
      refused(jedis, "wrong number of arguments for 'exists' command", "EXISTS");
      refused(jedis, "wrong number of arguments for 'getdel' command", "GETDEL");
    }
  }

  @Test
  void hdelByRidIsRefusedForAUserWithoutAccessToTheDatabase() {
    final Database db = getServer(0).getDatabase(getDatabaseName());
    db.getSchema().createDocumentType("Item");
    final String[] rid = new String[1];
    db.transaction(() -> rid[0] = db.newDocument("Item").set("n", 1).save().getIdentity().toString());

    final var security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", "nodb").put("password", security.encodePassword("noDbPassword1"))
        .put("databases", new JSONObject().put("otherdb", new JSONArray().put("admin"))));
    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("nodb", "noDbPassword1");
      refused(jedis, "NOPERM", "HDEL", getDatabaseName(), rid[0]);
      refused(jedis, "NOPERM", "HDEL", getDatabaseName() + "." + rid[0], rid[0]);
      assertThat(db.existsRecord(new RID(rid[0]))).isTrue();
    } finally {
      security.dropUser("nodb");
    }
  }

  @Override
  protected void populateDatabase() {
  }

  private Jedis connect() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    return jedis;
  }

  private static String text(final Jedis jedis, final String command, final String... args) {
    return new String((byte[]) jedis.sendCommand(() -> command.getBytes(StandardCharsets.UTF_8), args));
  }

  private static void refused(final Jedis jedis, final String message, final String command, final String... args) {
    assertThatThrownBy(() -> jedis.sendCommand(() -> command.getBytes(StandardCharsets.UTF_8), args)).isInstanceOf(JedisDataException.class)
        .hasMessageContaining(message);
  }

  private static long count(final Database db) {
    try (final var rs = db.query("sql", "SELECT count(*) AS n FROM Item")) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
