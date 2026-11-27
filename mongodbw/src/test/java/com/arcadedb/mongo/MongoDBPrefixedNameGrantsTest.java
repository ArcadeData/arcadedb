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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityTestAccess;
import com.arcadedb.utility.FileUtils;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Only the name {@code admin} is the administrative namespace of the MongoDB wire protocol. A database whose name starts with
 * {@code $} is a database like any other: its commands need a login and a grant on it, and run with the permissions the user's
 * group gives on each type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBPrefixedNameGrantsTest extends BaseMongoServerTest {
  private static final String PREFIXED      = "$mongoprefixed";
  private static final String PASSWORD      = "prefixedPassword1";
  private static final String NO_GRANT_USER = "prefixedNoGrantUser";
  private static final String FULL_USER     = "prefixedFullUser";
  private static final String READER_USER   = "prefixedReaderUser";
  private static final String READER_GROUP  = "prefixedReadOnly";
  private static final int    UNAUTHORIZED  = 13;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    final ArcadeDBServer server = getServer(0);
    final ServerDatabase database = server.createDatabase(PREFIXED, ComponentFile.MODE.READ_WRITE);
    database.command("sqlscript", "CREATE DOCUMENT TYPE Notes;\nINSERT INTO Notes SET name = 'first';");

    final ServerSecurity security = server.getSecurity();
    ServerSecurityTestAccess.databaseGroups(security, PREFIXED).put(READER_GROUP, new JSONObject().put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();

    createUser(NO_GRANT_USER, new JSONObject().put("otherdb", new JSONArray().put("admin")));
    createUser(FULL_USER, new JSONObject().put(PREFIXED, new JSONArray().put("admin")));
    createUser(READER_USER, new JSONObject().put(PREFIXED, new JSONArray().put(READER_GROUP)));
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ArcadeDBServer server = getServer(0);
      final ServerSecurity security = server.getSecurity();
      for (final String user : new String[] { NO_GRANT_USER, FULL_USER, READER_USER })
        if (security.getUser(user) != null)
          security.dropUser(user);
      ServerSecurityTestAccess.databaseGroups(security, PREFIXED).remove(READER_GROUP);
      security.saveGroups();

      if (server.existsDatabase(PREFIXED))
        server.dropLocalDatabase(server.getDatabaseForOperator(PREFIXED).getEmbedded(), PREFIXED);
      FileUtils.deleteRecursively(
          new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), PREFIXED));
    } finally {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      super.endTest();
    }
  }

  @Test
  void aUserWithoutAGrantOnTheDatabaseIsRefused() {
    try (final MongoClient client = loggedInClient(NO_GRANT_USER)) {
      final MongoCollection<Document> notes = client.getDatabase(PREFIXED).getCollection("Notes");

      assertThat(catchThrowable(() -> notes.find().first())).as("a read without a grant").isNotNull()
          .hasMessageContaining("not authorized");
      assertThat(catchThrowable(() -> notes.insertOne(new Document("name", "added")))).as("a write without a grant").isNotNull()
          .hasMessageContaining("not authorized");
    }

    assertThat(notesCount()).as("nothing was written").isEqualTo(1L);
  }

  @Test
  void aUserWithAGrantReadsAndWritesTheDatabase() {
    try (final MongoClient client = loggedInClient(FULL_USER)) {
      final MongoCollection<Document> notes = client.getDatabase(PREFIXED).getCollection("Notes");

      assertThat(notes.find().first().getString("name")).isEqualTo("first");
      notes.insertOne(new Document("name", "second"));
    }

    assertThat(notesCount()).isEqualTo(2L);
  }

  @Test
  void thePermissionsOfTheGroupOnEachTypeApply() {
    try (final MongoClient client = loggedInClient(READER_USER)) {
      final MongoCollection<Document> notes = client.getDatabase(PREFIXED).getCollection("Notes");

      assertThat(notes.find().first().getString("name")).as("the group reads").isEqualTo("first");
      assertThat(catchThrowable(() -> notes.insertOne(new Document("name", "added")))).as("the group does not create").isNotNull();
    }

    assertThat(notesCount()).as("nothing was written").isEqualTo(1L);
  }

  @Test
  void aClientThatNeverLoggedInOpensNoDatabase() {
    final ArcadeDBServer server = getServer(0);
    // on disk and not open: what a command on the name has to open
    final ServerDatabase created = server.getDatabaseForOperator(PREFIXED);
    created.getEmbedded().close();
    server.removeDatabase(PREFIXED);
    assertThat(server.existsDatabase(PREFIXED)).isFalse();

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      assertThat(catchThrowable(() -> client.getDatabase(PREFIXED).runCommand(new Document("getLog", "*")))).isInstanceOfSatisfying(
          MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(UNAUTHORIZED));
    }

    assertThat(server.existsDatabase(PREFIXED)).as("the command opened nothing").isFalse();
  }

  private long notesCount() {
    final Database database = getServer(0).getDatabaseForOperator(PREFIXED);
    return database.countType("Notes", false);
  }

  private MongoClient loggedInClient(final String user) {
    return new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential(user, "$external", PASSWORD.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
  }

  private void createUser(final String name, final JSONObject databases) {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(name) != null)
      security.dropUser(name);
    security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD)).put("databases", databases));
  }
}
