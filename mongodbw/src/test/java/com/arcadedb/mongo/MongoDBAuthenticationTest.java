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
import com.arcadedb.server.BaseGraphServerTest;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Verifies SASL PLAIN authentication on the MongoDB wire-protocol plugin (issue #4746).
 */
public class MongoDBAuthenticationTest extends BaseGraphServerTest {

  private static final int DEF_PORT = 27017;

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
  void plainAuthenticationWithValidCredentials() {
    final MongoCredential credential = MongoCredential.createPlainCredential("root", getDatabaseName(),
        DEFAULT_PASSWORD_FOR_TESTS.toCharArray());

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT), credential,
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoDatabase db = client.getDatabase(getDatabaseName());
      final Document result = db.runCommand(new Document("ping", 1));
      assertThat(result.getDouble("ok")).isEqualTo(1.0);
    }
  }

  @Test
  void plainAuthenticationWithInvalidPassword() {
    final MongoCredential credential = MongoCredential.createPlainCredential("root", getDatabaseName(),
        "wrong-password".toCharArray());

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT), credential,
        MongoClientOptions.builder().serverSelectionTimeout(3000).build())) {
      final MongoDatabase db = client.getDatabase(getDatabaseName());
      final Throwable thrown = catchThrowable(() -> db.runCommand(new Document("ping", 1)));
      assertThat(thrown).isInstanceOf(MongoSecurityException.class);
    }
  }
}
