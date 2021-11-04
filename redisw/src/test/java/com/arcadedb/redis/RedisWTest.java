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
 */
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisWTest extends BaseGraphServerTest {

  private static final int DEF_PORT = 6379;
  private static final int TOTAL    = 10_000;

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
  public void testDefaultBucket() {
    Jedis jedis = new Jedis("localhost", DEF_PORT);

    // SET
    long beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL; ++i)
      jedis.set("foo" + i, String.valueOf(i));
    System.out.println("Inserted " + TOTAL + " items in the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // GET
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL; ++i)
      Assertions.assertEquals(String.valueOf(i), jedis.get("foo" + i));
    System.out.println("Retrieved " + TOTAL + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");

    // INCR
    beginTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL; ++i)
      Assertions.assertEquals(i + 1L, jedis.incr("foo" + i));
    System.out.println("Incremented " + TOTAL + " items from the default bucket. Elapsed " + (System.currentTimeMillis() - beginTime) + "ms");
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
}
