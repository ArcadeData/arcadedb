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
import com.arcadedb.database.Database;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.query.RunningQuery;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9689 over the MongoDB protocol: a command is registered in the server's running statements while it runs - so
 * {@code list queries} and {@code SHOW TRANSACTIONS} list it - and a terminate stops it with MongoDB's own code for an
 * interrupted operation, without its writes. The entry is captured on the command's own thread, by a record listener,
 * so nothing depends on timing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9689MongoRunningQueryTest extends BaseMongoServerTest {
  private static final String COLLECTION = "Running9689";

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
  void aCommandIsListedWhileItRunsAndATerminateStopsIt() {
    getDatabase(0);
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      client.getDatabase(getDatabaseName()).createCollection(COLLECTION);
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection(COLLECTION);

      final Database database = getServerDatabase(0, getDatabaseName());
      final AtomicReference<RunningQuery> seen = new AtomicReference<>();
      final AtomicReference<String> listedText = new AtomicReference<>();
      final AtomicReference<Boolean> terminate = new AtomicReference<>(false);
      final BeforeRecordCreateListener listener = record -> {
        final RunningQuery current = RunningQuery.current();
        seen.set(current);
        if (current != null) {
          listedText.set(current.getText());
          if (terminate.get())
            current.terminate("root");
        }
        return true;
      };
      database.getSchema().getType(COLLECTION).getEvents().registerListener(listener);
      try {
        collection.insertOne(new Document("name", "first").append("password", "s3cr3t"));
        final RunningQuery entry = seen.get();
        assertThat(entry).as("the command must run under its registry entry").isNotNull();
        assertThat(entry.getProtocol()).isEqualTo("mongodb");
        assertThat(entry.getUser()).isEqualTo("root");
        assertThat(entry.getDatabase()).isEqualTo(getDatabaseName());
        assertThat(entry.getConnectionId()).isNotNull();
        assertThat(listedText.get()).startsWith("insert").contains(COLLECTION).doesNotContain("s3cr3t");
        assertThat(entry.isEnded()).as("the entry goes once the command is over").isTrue();
        assertThat(getServer(0).getRunningQueries().size()).isZero();

        // Terminated while it runs: the write does not stand, and the client is told the operation was interrupted
        terminate.set(true);
        assertThatThrownBy(() -> collection.insertOne(new Document("name", "second"))).isInstanceOf(MongoException.class)
            .satisfies(e -> assertThat(((MongoException) e).getCode()).isEqualTo(11601));
        assertThat(seen.get().getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
        assertThat(collection.countDocuments()).isEqualTo(1);
      } finally {
        database.getSchema().getType(COLLECTION).getEvents().unregisterListener(listener);
      }
    }
  }
}
