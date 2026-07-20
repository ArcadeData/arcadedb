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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for GHSA-fq9c-x968-g278: the MongoDB wire protocol must enforce authentication on the
 * data path and authorize the target database - an unauthenticated client, and an authenticated user
 * without a grant on a database, must not be able to read or write it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBAuthorizationTest extends BaseGraphServerTest {

  private static final int    DEF_PORT          = 27017;
  private static final String LIMITED_USER      = "limitedMongoUser";
  private static final String LIMITED_PASSWORD  = "limitedPassword1";

  @Test
  void unauthenticatedClientCannotWrite() {
    getDatabase(0);
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("Victim");

      final Throwable thrown = catchThrowable(() -> collection.insertOne(new Document("injected", "marker")));
      assertThat(thrown).as("unauthenticated write must be rejected").isNotNull();
      assertThat(thrown.getMessage()).containsIgnoringCase("auth");
    }

    // Nothing must have been written.
    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.getSchema().existsType("Victim")).isFalse();
  }

  @Test
  void unauthenticatedClientCannotRead() {
    seedVictimData();
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("Victim");

      final Throwable thrown = catchThrowable(() -> collection.find().first());
      assertThat(thrown).as("unauthenticated read must be rejected").isNotNull();
      assertThat(thrown.getMessage()).containsIgnoringCase("auth");
    }
  }

  @Test
  void authenticatedUserCannotAccessUnauthorizedDatabase() {
    seedVictimData();

    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(LIMITED_USER) == null)
      security.createUser(new JSONObject()
          .put("name", LIMITED_USER)
          .put("password", security.encodePassword(LIMITED_PASSWORD))
          .put("databases", new JSONObject().put("otherdb", new JSONArray().put("admin"))));

    final MongoCredential credential = MongoCredential.createPlainCredential(LIMITED_USER, "$external",
        LIMITED_PASSWORD.toCharArray());

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT), credential,
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("Victim");

      final Throwable thrown = catchThrowable(() -> collection.find().first());
      assertThat(thrown).as("a user without a grant on the database must be rejected").isNotNull();
      assertThat(thrown.getMessage()).containsIgnoringCase("not authorized");
    }
  }

  @Test
  void authenticatedRootCanReadAndWrite() {
    getDatabase(0);

    final MongoCredential credential = MongoCredential.createPlainCredential("root", "$external",
        DEFAULT_PASSWORD_FOR_TESTS.toCharArray());

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", DEF_PORT), credential,
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      client.getDatabase(getDatabaseName()).createCollection("RootType");
      final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("RootType");
      collection.insertOne(new Document("value", 1));
      assertThat(collection.find().first().getInteger("value")).isEqualTo(1);
    }
  }

  private void seedVictimData() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sqlscript", "CREATE DOCUMENT TYPE Victim IF NOT EXISTS;\nINSERT INTO Victim SET secret = 'topsecret';");
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ServerSecurity security = getServer(0).getSecurity();
      if (security != null && security.getUser(LIMITED_USER) != null)
        security.dropUser(LIMITED_USER);
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
