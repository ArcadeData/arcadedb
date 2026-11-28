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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityTestAccess;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The permissions of a security group on each type apply to the commands that answer about a collection over the MongoDB wire
 * protocol: a user whose group may use the database but not read a type is refused by the commands that count its documents,
 * read them or report its indexes, whether the type holds documents or not, and the types the group reads are answered.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class MongoCommandGrantsOnCollectionTest extends BaseMongoServerTest {
  private static final String PASSWORD      = "grantsPassword1";
  private static final String READER_USER   = "grantsReaderUser";
  private static final String SCHEMA_USER   = "grantsSchemaUser";
  private static final String READER_GROUP  = "grantsReaders";
  private static final String SCHEMA_GROUP  = "grantsSchemaChangers";
  private static final String OPEN          = "OpenNotes";
  private static final String SECRET        = "SecretNotes";
  private static final String EMPTY_SECRET  = "EmptySecretNotes";
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
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sqlscript", "CREATE DOCUMENT TYPE " + OPEN + ";\nCREATE DOCUMENT TYPE " + SECRET + ";\nCREATE DOCUMENT TYPE " + EMPTY_SECRET
        + ";\nINSERT INTO " + OPEN + " SET name = 'open';\nINSERT INTO " + SECRET + " SET name = 'one';\nINSERT INTO " + SECRET + " SET name = 'two';");

    final ServerSecurity security = getServer(0).getSecurity();
    final JSONObject groups = ServerSecurityTestAccess.databaseGroups(security, getDatabaseName());
    groups.put(READER_GROUP, group(new JSONArray()));
    groups.put(SCHEMA_GROUP, group(new JSONArray().put("updateSchema")));
    security.saveGroups();

    createUser(READER_USER, READER_GROUP);
    createUser(SCHEMA_USER, SCHEMA_GROUP);
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ServerSecurity security = getServer(0).getSecurity();
      for (final String user : new String[] { READER_USER, SCHEMA_USER })
        if (security.getUser(user) != null)
          security.dropUser(user);
      final JSONObject groups = ServerSecurityTestAccess.databaseGroups(security, getDatabaseName());
      groups.remove(READER_GROUP);
      groups.remove(SCHEMA_GROUP);
      security.saveGroups();
    } finally {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      super.endTest();
    }
  }

  @Test
  void aUserWhoseGroupMayNotReadATypeIsRefusedByTheCommandsAboutIt() {
    try (final MongoClient client = loggedInClient(READER_USER)) {
      for (final String type : List.of(SECRET, EMPTY_SECRET)) {
        assertRefused(client, new Document("count", type));
        assertRefused(client, new Document("count", type).append("query", new Document("name", "one")));
        assertRefused(client, new Document("find", type));
        assertRefused(client, new Document("aggregate", type).append("pipeline", List.of()).append("cursor", new Document()));
        assertRefused(client, new Document("createIndexes", type).append("indexes", List.of()));
        assertRefused(client, new Document("createIndexes", type));
        assertRefused(client, index(type));
      }
    }
  }

  @Test
  void aUserWhoseGroupMayChangeTheSchemaButNotReadATypeIsRefusedToo() {
    try (final MongoClient client = loggedInClient(SCHEMA_USER)) {
      for (final String type : List.of(SECRET, EMPTY_SECRET)) {
        assertRefused(client, new Document("createIndexes", type).append("indexes", List.of()));
        assertRefused(client, new Document("createIndexes", type));
        assertRefused(client, index(type));
      }
    }
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType(SECRET).getAllIndexes(false)).isEmpty();
  }

  @Test
  void theTypesTheGroupReadsAreAnswered() {
    try (final MongoClient client = loggedInClient(READER_USER)) {
      final MongoDatabase database = client.getDatabase(getDatabaseName());
      assertThat(database.runCommand(new Document("count", OPEN)).get("n")).isEqualTo(1);
      assertThat(database.runCommand(new Document("find", OPEN)).get("cursor", Document.class).getList("firstBatch", Document.class)).hasSize(1);
    }
    try (final MongoClient client = loggedInClient(SCHEMA_USER)) {
      final MongoDatabase database = client.getDatabase(getDatabaseName());
      assertThat(database.runCommand(index(OPEN)).get("ok")).isEqualTo(1.0);
      assertThat(catchThrowable(() -> database.runCommand(new Document("createIndexes", OPEN).append("indexes", List.of())))).as("no index named")
          .isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(2));
    }
  }

  private static Document index(final String type) {
    return new Document("createIndexes", type).append("indexes", List.of(new Document("key", new Document("name", 1)).append("name", "name_1")));
  }

  private void assertRefused(final MongoClient client, final Document command) {
    assertThat(catchThrowable(() -> client.getDatabase(getDatabaseName()).runCommand(command))).as(command.toJson())
        .isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorCode()).as(e.getMessage()).isEqualTo(UNAUTHORIZED));
  }

  /** The group reads one type and names the others with no access, so nothing depends on the defaults of the server. */
  private JSONObject group(final JSONArray databaseAccess) {
    return new JSONObject().put("access", databaseAccess).put("types", new JSONObject()
        .put(OPEN, new JSONObject().put("access", new JSONArray().put("readRecord")))
        .put(SECRET, new JSONObject().put("access", new JSONArray()))
        .put(EMPTY_SECRET, new JSONObject().put("access", new JSONArray())));
  }

  private MongoClient loggedInClient(final String user) {
    return new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential(user, "$external", PASSWORD.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
  }

  private void createUser(final String name, final String group) {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(name) != null)
      security.dropUser(name);
    security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(group))));
  }
}
