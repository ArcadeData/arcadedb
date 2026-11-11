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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11373: the handshake advertises wire version 9 (MongoDB 4.4), the lowest current drivers accept. It advertised the
 * library's 8, and every PyMongo 4.x refused to connect.
 */
class Issue11373MongoWireVersionTest extends BaseMongoServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void handshakeAdvertisesWireVersionCurrentDriversAccept() {
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final Document reply = client.getDatabase("admin").runCommand(new Document("isMaster", 1));
      assertThat(reply.getInteger("maxWireVersion")).isGreaterThanOrEqualTo(9);
      assertThat(reply.getInteger("minWireVersion")).isZero();
    }
  }
}
