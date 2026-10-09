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
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9518 over the MongoDB protocol: its commands run on the Netty channel threads, which also serve other
 * connections, so a command the query admission gate cannot start at once is refused with {@code ExceededTimeLimit}
 * (262), a code the drivers retry, without waiting even when the gate's queue would let other protocols wait; and every
 * command gives its slot back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryAdmissionGateMongoIssue9518Test extends BaseMongoServerTest {
  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aCommandTheGateCannotStartAtOnceIsRefusedWithoutWaiting() throws Exception {
    getDatabase(0);
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    // THE OTHER PROTOCOLS WOULD WAIT TWO MINUTES: A MONGODB COMMAND THAT WAITED WOULD NOT COMPLETE WITHIN THE ONE BELOW
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(120_000L);

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      client.getDatabase(getDatabaseName()).createCollection("Gate9518");
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("Gate9518");

      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        final CompletableFuture<Throwable> refused = CompletableFuture.supplyAsync(() -> {
          try {
            collection.insertOne(new Document("id", 1));
            return null;
          } catch (final Throwable t) {
            return t;
          }
        });
        final Throwable error = refused.get(60, TimeUnit.SECONDS);
        assertThat(error).isInstanceOf(MongoException.class);
        assertThat(((MongoException) error).getCode()).isEqualTo(262);
      }

      // EVERY COMMAND GIVES ITS SLOT BACK: WITH ONE SLOT, A LEAKED ONE WOULD REFUSE THE NEXT
      collection.insertOne(new Document("id", 2));
      assertThat(collection.countDocuments()).isEqualTo(1);
    }
    assertThat(gate.getRunning()).isZero();
  }
}
