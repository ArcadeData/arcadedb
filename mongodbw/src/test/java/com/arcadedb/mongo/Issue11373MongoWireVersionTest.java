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
package com.arcadedb.mongo;

import com.arcadedb.GlobalConfiguration;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11373: PyMongo 4.18 dropped MongoDB 4.2 and refuses a server whose {@code isMaster} reply says {@code maxWireVersion}
 * is below 9 (MongoDB 4.4). {@code mongo-java-server} 1.47.0 answers 8 for its newest {@code ServerVersion}, so every current
 * PyMongo was locked out at the handshake. The reply and {@code buildInfo} must agree on the same MongoDB release.
 */
class Issue11373MongoWireVersionTest extends BaseMongoServerTest {
  private MongoClient client;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  @Test
  void handshakeReportsWireVersionOfMongoDb44() {
    // isMaster and buildInfo are answered before any login, as a driver sends them while it connects
    final Document hello = client.getDatabase("admin").runCommand(new Document("isMaster", 1));
    assertThat(hello.getInteger("maxWireVersion")).isEqualTo(9);
    assertThat(hello.getInteger("minWireVersion")).isLessThanOrEqualTo(hello.getInteger("maxWireVersion"));

    final Document buildInfo = client.getDatabase("admin").runCommand(new Document("buildInfo", 1));
    assertThat(buildInfo.getString("version")).isEqualTo("4.4.0");
    assertThat(buildInfo.getList("versionArray", Integer.class)).containsExactly(4, 4, 0);
  }
}
