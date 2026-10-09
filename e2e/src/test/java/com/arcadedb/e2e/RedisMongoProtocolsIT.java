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
package com.arcadedb.e2e;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** The Redis and MongoDB wire protocols, each through its own client driver. */
class RedisMongoProtocolsIT extends ArcadeContainerTemplate {

  @Test
  void redisPingSetGet() {
    try (final Jedis jedis = new Jedis(host, redisPort)) {
      jedis.auth("root", "playwithdata");
      assertThat(jedis.ping()).isEqualTo("PONG");
      try {
        jedis.set("e2eKey", "e2eValue");
        assertThat(jedis.get("e2eKey")).isEqualTo("e2eValue");
      } finally {
        // the container is shared by the whole battery; ArcadeDB's Redis wrapper has GETDEL but no DEL
        jedis.getDel("e2eKey");
      }
    }
  }

  @Test
  void mongoPingAndQuery() {
    try (final MongoClient client = new MongoClient(new ServerAddress(host, mongoPort),
        MongoCredential.createPlainCredential("root", "beer", "playwithdata".toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(10_000).build())) {
      assertThat(client.getDatabase("beer").runCommand(new Document("ping", 1)).get("ok")).isIn(1, 1.0);
      assertThat(client.getDatabase("beer").getCollection("Beer").find().limit(5).into(new ArrayList<>())).hasSize(5);
    }
  }
}
