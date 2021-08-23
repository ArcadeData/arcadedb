/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

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
  public void testSet() {
    Jedis jedis = new Jedis("localhost", DEF_PORT);

    long beginTime = System.currentTimeMillis();

    for (int i = 0; i < TOTAL; ++i) {
      jedis.set("foo" + i, String.valueOf(i));
    }

    System.out.println("Inserted  " + TOTAL + " items. Elapsed" + (System.currentTimeMillis() - beginTime) + "ms");

    beginTime = System.currentTimeMillis();

    for (int i = 0; i < TOTAL; ++i) {
      jedis.get("foo" + i);
      //Assertions.assertEquals(String.valueOf(i), jedis.get("foo" + i));
    }

    System.out.println("Retrieved  " + TOTAL + " items. Elapsed" + (System.currentTimeMillis() - beginTime) + "ms");

  }

}
