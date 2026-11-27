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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.FileUtils;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The databases a MongoDB client is told about are the ones its user has a grant on, whichever other databases the server has
 * open for other connections.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class MongoDBDatabaseListingGrantsTest extends BaseMongoServerTest {
  private static final String OTHER_DATABASE      = "mongolistingother";
  private static final String PASSWORD            = "listingPassword1";
  private static final String ONE_DATABASE_USER   = "listingOneDatabaseUser";
  private static final String EVERY_DATABASE_USER = "listingEveryDatabaseUser";
  private static final String NO_GRANT_USER       = "listingNoGrantUser";

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
    getServerDatabase(0, getDatabaseName()).command("sqlscript", "CREATE DOCUMENT TYPE Notes IF NOT EXISTS;\nINSERT INTO Notes SET name = 'first';");
    final ServerDatabase other = server.createDatabase(OTHER_DATABASE, ComponentFile.MODE.READ_WRITE);
    other.command("sqlscript", "CREATE DOCUMENT TYPE Notes;\nINSERT INTO Notes SET name = 'first';");

    createUser(ONE_DATABASE_USER, new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));
    createUser(EVERY_DATABASE_USER, new JSONObject().put("*", new JSONArray().put("admin")));
    createUser(NO_GRANT_USER, new JSONObject().put("someotherdatabase", new JSONArray().put("admin")));

    // both databases are in use by another connection before anybody lists them
    try (final MongoClient root = loggedInClient("root", DEFAULT_PASSWORD_FOR_TESTS)) {
      for (final String database : new String[] { getDatabaseName(), OTHER_DATABASE })
        assertThat(root.getDatabase(database).getCollection("Notes").find().first()).isNotNull();
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ArcadeDBServer server = getServer(0);
      final ServerSecurity security = server.getSecurity();
      for (final String user : new String[] { ONE_DATABASE_USER, EVERY_DATABASE_USER, NO_GRANT_USER })
        if (security.getUser(user) != null)
          security.dropUser(user);

      if (server.existsDatabase(OTHER_DATABASE))
        server.dropLocalDatabase(server.getDatabaseForOperator(OTHER_DATABASE).getEmbedded(), OTHER_DATABASE);
      FileUtils.deleteRecursively(
          new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), OTHER_DATABASE));
    } finally {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      super.endTest();
    }
  }

  @Test
  void aUserGrantedOneDatabaseListsOnlyThatDatabase() {
    try (final MongoClient client = loggedInClient(ONE_DATABASE_USER, PASSWORD)) {
      assertThat(listedNames(client)).containsExactly(getDatabaseName());
    }
  }

  @Test
  void aUserGrantedEveryDatabaseListsEveryOpenDatabase() {
    try (final MongoClient client = loggedInClient(EVERY_DATABASE_USER, PASSWORD)) {
      assertThat(listedNames(client)).contains(getDatabaseName(), OTHER_DATABASE);
    }
    try (final MongoClient client = loggedInClient("root", DEFAULT_PASSWORD_FOR_TESTS)) {
      assertThat(listedNames(client)).contains(getDatabaseName(), OTHER_DATABASE);
    }
  }

  @Test
  void aUserGrantedNoOpenDatabaseListsNone() {
    try (final MongoClient client = loggedInClient(NO_GRANT_USER, PASSWORD)) {
      assertThat(listedNames(client)).doesNotContain(getDatabaseName(), OTHER_DATABASE);
    }
  }

  @Test
  void theDriverListingOfDatabaseNamesFollowsTheGrants() {
    try (final MongoClient client = loggedInClient(ONE_DATABASE_USER, PASSWORD)) {
      final List<String> names = new ArrayList<>();
      client.listDatabaseNames().into(names);
      assertThat(names).containsExactly(getDatabaseName());
    }
  }

  @Test
  void theListingKeepsItsReplyShape() {
    try (final MongoClient client = loggedInClient(EVERY_DATABASE_USER, PASSWORD)) {
      final Document reply = client.getDatabase("admin").runCommand(new Document("listDatabases", 1));

      assertThat(((Number) reply.get("ok")).intValue()).isEqualTo(1);
      final List<String> names = new ArrayList<>();
      for (final Document database : reply.getList("databases", Document.class)) {
        names.add(database.getString("name"));
        assertThat(database.get("empty")).isEqualTo(Boolean.FALSE);
      }
      assertThat(names).isSorted().contains(getDatabaseName(), OTHER_DATABASE);
    }
  }

  private static List<String> listedNames(final MongoClient client) {
    final Document reply = client.getDatabase("admin").runCommand(new Document("listDatabases", 1));
    final List<String> names = new ArrayList<>();
    for (final Document database : reply.getList("databases", Document.class))
      names.add(database.getString("name"));
    return names;
  }

  private MongoClient loggedInClient(final String user, final String password) {
    return new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential(user, "$external", password.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
  }

  private void createUser(final String name, final JSONObject databases) {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(name) != null)
      security.dropUser(name);
    security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD)).put("databases", databases));
  }
}
