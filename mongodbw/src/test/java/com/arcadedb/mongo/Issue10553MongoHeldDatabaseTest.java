/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10553, through a real driver: a MongoDB client that names a database an {@code overwrite} restore holds, or that a
 * {@code close database} closed, is answered {@code 365 TemporarilyUnavailable} - the database exists and cannot serve now - and
 * not the uncoded "Unknown error" the library reports for a bare exception, which a driver reads as a server fault. Once nothing
 * holds the name the same command goes through.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10553MongoHeldDatabaseTest extends BaseMongoServerTest {
  private static final String HELD = "mongoheld10553";
  private static final int    TEMPORARILY_UNAVAILABLE = 365;

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
    getDatabase(0);
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
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
  void aCommandOnADatabaseARestoreHoldsIsTemporarilyUnavailable() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.reserveDatabaseNameForRestore(HELD);
    try {
      assertThatThrownBy(() -> ping()).isInstanceOfSatisfying(MongoCommandException.class, e -> {
        assertThat(e.getErrorCode()).isEqualTo(TEMPORARILY_UNAVAILABLE);
        assertThat(e.getErrorCodeName()).isEqualTo("TemporarilyUnavailable");
      });
      assertThat(server.existsDatabase(HELD)).as("the command opened nothing").isFalse();
    } finally {
      server.releaseDatabaseNameReservedForRestore(HELD);
    }

    try {
      assertThat(ping().getDouble("ok")).as("a released reservation holds nothing").isEqualTo(1.0);
    } finally {
      dropIfPresent(server);
    }
  }

  @Test
  void aCommandOnADatabaseACloseCommandClosedIsTemporarilyUnavailable() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.markClosedByCommand(HELD);
    try {
      assertThatThrownBy(() -> ping()).isInstanceOfSatisfying(MongoCommandException.class,
          e -> assertThat(e.getErrorCode()).isEqualTo(TEMPORARILY_UNAVAILABLE));
    } finally {
      server.unmarkClosedByCommand(HELD);
      dropIfPresent(server);
    }
  }

  @Test
  void aNameThatIsNotADatabaseKeepsItsAnswer() {
    assertThatThrownBy(() -> client.getDatabase("mongonothing10553").runCommand(new Document("ping", 1)))
        .isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorCode()).isNotEqualTo(TEMPORARILY_UNAVAILABLE));
  }

  private Document ping() {
    return client.getDatabase(HELD).runCommand(new Document("ping", 1));
  }

  /** Leaves {@link #HELD} on disk, closed and deregistered: what an {@code overwrite} restore finds on its target. */
  private static void onDiskNotOpen(final ArcadeDBServer server) {
    final ServerDatabase created = server.createDatabase(HELD, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(HELD);
    assertThat(server.existsDatabase(HELD)).isFalse();
  }

  private static void dropIfPresent(final ArcadeDBServer server) {
    if (server.existsDatabase(HELD))
      server.dropLocalDatabase(server.getDatabase(HELD).getEmbedded(), HELD);
    FileUtils.deleteRecursively(
        new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), HELD));
  }
}
