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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13052 over the query admission gate (#9518): a {@code getMore} of the cursor of a {@code find} reads, filters and converts
 * the rows of its batch, as the {@code find} did, so it takes a slot of the gate and is refused without waiting when there is none
 * ({@code ExceededTimeLimit}, 262), leaving the cursor where it was. A {@code killCursors} only releases, and is never refused.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class Issue13052MongoGetMoreAdmissionTest extends BaseMongoServerTest {
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
  void aGetMoreTheGateCannotStartAtOnceIsRefusedAndTheCursorStaysWhereItWas() throws Exception {
    getDatabase(0);
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoDatabase db = client.getDatabase(getDatabaseName());
      db.createCollection("Gate13052");
      final MongoCollection<Document> collection = db.getCollection("Gate13052");
      final List<Document> documents = new ArrayList<>();
      for (int i = 0; i < 50; i++)
        documents.add(new Document("_id", i).append("n", i));
      collection.insertMany(documents);

      final Document first = db.runCommand(new Document("find", "Gate13052").append("batchSize", 5));
      final long id = ((Number) ((Document) first.get("cursor")).get("id")).longValue();
      assertThat(id).isNotZero();

      GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
      // THE OTHER PROTOCOLS WOULD WAIT TWO MINUTES: A MONGODB COMMAND THAT WAITED WOULD NOT COMPLETE WITHIN THE ONE BELOW
      GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(120_000L);

      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        final CompletableFuture<Throwable> refused = CompletableFuture.supplyAsync(() -> {
          try {
            db.runCommand(new Document("getMore", id).append("collection", "Gate13052").append("batchSize", 5));
            return null;
          } catch (final Throwable t) {
            return t;
          }
        });
        final Throwable error = refused.get(60, TimeUnit.SECONDS);
        assertThat(error).as("the batch of a getMore reads rows: it needs a slot").isInstanceOf(MongoCommandException.class);
        assertThat(((MongoCommandException) error).getErrorCode()).isEqualTo(262);

        // releasing a cursor reads nothing, and is never refused
        final Document killedElsewhere = db.runCommand(new Document("killCursors", "Gate13052").append("cursors", List.of(123456L)));
        assertThat(killedElsewhere.get("cursorsNotFound")).isEqualTo(List.of(123456L));
      }

      // THE REFUSED getMore DID NOT MOVE THE CURSOR, AND GAVE NO SLOT AWAY: WITH ONE SLOT, A LEAKED ONE WOULD REFUSE THE NEXT
      final Document more = db.runCommand(new Document("getMore", id).append("collection", "Gate13052").append("batchSize", 5));
      final List<?> batch = (List<?>) ((Document) more.get("cursor")).get("nextBatch");
      assertThat(batch).extracting(d -> ((Document) d).get("n")).containsExactly(5, 6, 7, 8, 9);

      final Document killed = db.runCommand(new Document("killCursors", "Gate13052").append("cursors", List.of(id)));
      assertThat(killed.get("cursorsKilled")).isEqualTo(List.of(id));
    }
    assertThat(gate.getRunning()).isZero();
  }
}
