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
import com.arcadedb.server.AdmissionSlotConformance;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10235 over the MongoDB protocol: a command that gives its admission slot back before a wait finds the slot on the
 * thread that runs it. A MongoDB command cannot issue a statement that waits, so the hook is the record its insert
 * creates. See {@link AdmissionSlotConformance}.
 */
public class AdmissionSlotConformanceMongoTest extends BaseMongoServerTest {
  private static final String TYPE = "Conformance10235";

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
  void anInsertGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    getDatabase(0);
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      client.getDatabase(getDatabaseName()).createCollection(TYPE);
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection(TYPE);

      AdmissionSlotConformance.assertGivesItsSlotBack(getServerDatabase(0, getDatabaseName()), TYPE, "MongoDB insert", () -> {
        collection.insertOne(new Document("id", 1));
        return null;
      });
      assertThat(collection.countDocuments()).isEqualTo(1);
    }
  }
}
