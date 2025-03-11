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
package com.arcadedb.server.security;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.network.HostUtil;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ServerProfilingIT {
  private static       ArcadeDBServer SERVER;
  private static       ServerSecurity SECURITY;
  private final static String         DATABASE_NAME = "ServerProfilingIT";

  @Test
  void userDefaultAccessCannotAccessDatabase() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein")));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      checkJohnUser(setCurrentUser("albert", database));

      createSchemaNotAllowed(database);

      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      final RID validRID = createSomeRecords(database, true);

      // SWITCH BACK TO ALBERT
      checkJohnUser(setCurrentUser("albert", database));

      expectedSecurityException(() -> database.newVertex("Vertex1").save());
      expectedSecurityException(() -> database.newDocument("Document1").save());
      expectedSecurityException(() -> database.iterateType("Document1", true));
      expectedSecurityException(() -> database.lookupByRID(validRID, true));

      expectedSecurityException(() -> {
        executeRemoteCommand("INSERT INTO Vertex1 set name = 'invalid'", "albert", "einstein");
      });
      expectedSecurityException(() -> {
        executeRemoteCommand("INSERT INTO Document1 set name = 'invalid'", "albert", "einstein");
      });
      expectedSecurityException(() -> {
        executeRemoteCommand("SELECT FROM Document1", "albert", "einstein");
      });
      expectedSecurityException(() -> {
        executeRemoteCommand("SELECT FROM " + validRID, "albert", "einstein");
      });

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  private static void executeRemoteCommand(final String command, final String userName, final String userPassword) {
    final String[] address = HostUtil.parseHostAddress(SERVER.getHttpServer().getListeningAddress(),
        HostUtil.CLIENT_DEFAULT_PORT);
    final RemoteDatabase remoteDatabase = new RemoteDatabase(address[0], Integer.parseInt(address[1]), DATABASE_NAME, userName,
        userPassword);
    remoteDatabase.command("sql", command);
  }

  @Test
  void notRootAdminAccess() {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "admin" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      checkJohnUser(setCurrentUser("albert", database));

      createSchema(database);

      final RID validRID = createSomeRecords(database, true);

      database.newVertex("Vertex1").save();
      database.newDocument("Document1").save();
      database.iterateType("Document1", true);
      database.lookupByRID(validRID, true);

      dropSchema(database);
    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void testMultipleGroupsAnyType() {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein")).put("databases",
        new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "creator", "reader", "updater", "deleter" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      setCurrentUser("root", database);
      createSchema(database);

      checkJohnUser(setCurrentUser("albert", database));

      final RID validRID = createSomeRecords(database, true);

      database.newVertex("Vertex1").save();
      database.newDocument("Document1").save();
      database.iterateType("Document1", true);
      database.lookupByRID(validRID, true);

      setCurrentUser("root", database);
      dropSchema(database);
    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void testMultipleGroupsSpecificType() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein")).put("databases",
        new JSONObject().put(DATABASE_NAME, new JSONArray(
            new String[] { "creatorOfDocuments", "readerOfDocuments", "updaterOfDocuments", "deleterOfDocuments" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      setCurrentUser("root", database);
      createSchema(database);

      checkJohnUser(setCurrentUser("albert", database));

      expectedSecurityException(() -> database.newVertex("Vertex1").save());
      database.newDocument("Document1").save();
      database.iterateType("Document1", true);
      database.transaction(() -> database.iterateType("Document1", true).next().asDocument().modify().set("modified", true).save());
      database.transaction(() -> database.iterateType("Document1", true).next().asDocument().delete());

      setCurrentUser("root", database);
      dropSchema(database);
    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void createOnlyAccess() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "creator" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      checkJohnUser(setCurrentUser("albert", database));

      createSchemaNotAllowed(database);

      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      // SWITCH BACK TO ALBERT
      checkJohnUser(setCurrentUser("albert", database));

      final RID validRID = createSomeRecords(database, false);

      MutableVertex v1 = database.newVertex("Vertex1").save();
      MutableVertex v2 = database.newVertex("Vertex1").save();

      // NEW EDGE IS TECHNICALLY A 2-STEP OPERATION: CREATE THE EDGE AND UPDATE THE VERTICES
      expectedSecurityException(() -> v1.newEdge("Edge1", v2, true).save());

      database.newDocument("Document1").save();
      expectedSecurityException(() -> database.iterateType("Document1", true));
      expectedSecurityException(() -> database.lookupByRID(validRID, true));

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void createEdgeAccess() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "createOnlyGraph" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      checkJohnUser(setCurrentUser("albert", database));

      createSchemaNotAllowed(database);

      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      // SWITCH BACK TO ALBERT
      checkJohnUser(setCurrentUser("albert", database));

      MutableVertex v1 = database.newVertex("Vertex1").save();
      MutableVertex v2 = database.newVertex("Vertex1").save();

      // NEW EDGE IS TECHNICALLY A 2-STEP OPERATION: CREATE THE EDGE AND UPDATE THE VERTICES
      v1.newEdge("Edge1", v2, true);

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void readOnlyAccess() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "reader" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      checkJohnUser(setCurrentUser("albert", database));

      createSchemaNotAllowed(database);

      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      final RID validRID = createSomeRecords(database, true);

      // SWITCH BACK TO ALBERT
      checkJohnUser(setCurrentUser("albert", database));

      expectedSecurityException(() -> database.newVertex("Vertex1").save());
      expectedSecurityException(() -> database.newDocument("Document1").save());
      database.iterateType("Document1", true);
      database.lookupByRID(validRID, true);

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void readAndUpdateOnlyAccess() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "reader", "updater" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      checkJohnUser(setCurrentUser("albert", database));

      createSchemaNotAllowed(database);

      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      final RID validRID = createSomeRecords(database, true);

      final Vertex v = validRID.getRecord(true).asVertex();

      // SWITCH BACK TO ALBERT
      checkJohnUser(setCurrentUser("albert", database));

      database.iterateType("Document1", true);
      database.lookupByRID(validRID, true);

      expectedSecurityException(() -> database.newVertex("Vertex1").save());
      expectedSecurityException(() -> database.newDocument("Document1").save());

      database.transaction(() -> v.modify().set("justModified", true).save());

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void deleteOnlyAccess() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "deleter" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      final ServerSecurityUser albert = setCurrentUser("albert", database);
      checkJohnUser(albert);
      assertThat(albert.getAuthorizedDatabases().contains(database.getName())).isTrue();

      createSchemaNotAllowed(database);

      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      final ServerSecurityUser root = setCurrentUser("root", database);
      assertThat(root.getAuthorizedDatabases().contains("*")).isTrue();

      createSchema(database);

      createSomeRecords(database, true);
      final Document doc = database.iterateType("Document1", true).next().asDocument();

      // SWITCH BACK TO ALBERT
      checkJohnUser(setCurrentUser("albert", database));

      expectedSecurityException(() -> database.newVertex("Vertex1").save());
      expectedSecurityException(() -> database.newDocument("Document1").save());
      expectedSecurityException(() -> database.iterateType("Document1", true));
      expectedSecurityException(() -> database.lookupByRID(doc.getIdentity(), true));

      database.transaction(() -> database.deleteRecord(doc));

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void testResultSetLimit() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "readerOfDocumentsCapped" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      checkJohnUser(setCurrentUser("albert", database));

      createSchemaNotAllowed(database);

      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      createSomeRecords(database, true);
      for (int i = 0; i < 14; i++) {
        createSomeRecords(database, true);
      }

      // SWITCH BACK TO ALBERT
      checkJohnUser(setCurrentUser("albert", database));

      expectedSecurityException(() -> database.newVertex("Vertex1").save());
      expectedSecurityException(() -> database.newDocument("Document1").save());

      int count = 0;
      for (final Iterator<Record> iter = database.iterateType("Document1", true); iter.hasNext(); ) {
        iter.next();
        ++count;
      }

      assertThat(count).isEqualTo(10);

      count = 0;
      for (final ResultSet iter = database.query("sql", "select from Document1"); iter.hasNext(); ) {
        iter.next();
        ++count;
      }

      assertThat(count).isEqualTo(10);

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void testReadTimeout() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "readerOfDocumentsShortTimeout" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      checkJohnUser(setCurrentUser("albert", database));

      createSchemaNotAllowed(database);

      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      database.transaction(() -> {
        for (int i = 0; i < 10000; i++) {
          database.newDocument("Document1").save();
        }
      });

      // SWITCH BACK TO ALBERT
      checkJohnUser(setCurrentUser("albert", database));

      expectedSecurityException(() -> database.newVertex("Vertex1").save());
      expectedSecurityException(() -> database.newDocument("Document1").save());

      try {
        for (final Iterator<Record> iter = database.iterateType("Document1", true); iter.hasNext(); ) {
          iter.next();
        }
        fail("");
      } catch (final TimeoutException e) {
        // EXPECTED
      }

      try {
        for (final ResultSet iter = database.query("sql", "select from Document1"); iter.hasNext(); ) {
          iter.next();
        }
        fail("");
      } catch (final TimeoutException e) {
        // EXPECTED
      }

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void testGroupsReload() throws Throwable {
    final File file = new File("./target/config/" + SecurityGroupFileRepository.FILE_NAME);
    assertThat(file.exists()).isTrue();

    final JSONObject json = new JSONObject(FileUtils.readFileAsString(file));

    final byte[] original = json.toString(2).getBytes();

    json.getJSONObject("databases").getJSONObject("*").getJSONObject("groups").put("reloaded", true);

    try {
      FileUtils.writeContentToStream(file, json.toString(2).getBytes());

      Thread.sleep(300);

      assertThat(SECURITY.getDatabaseGroupsConfiguration("*").getBoolean("reloaded")).isTrue();
    } finally {
      // RESTORE THE ORIGINAL FILE AND WAIT FOR TO RELOAD
      FileUtils.writeContentToStream(file, original);
      Thread.sleep(200);
      createSecurity();
    }
  }

  @Test
  public void reloadWhileUsingDatabase() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "reader", "updater" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);
      checkJohnUser(setCurrentUser("albert", database));
      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      final int CYCLES = 30;
      final CountDownLatch semaphore = new CountDownLatch(CYCLES);

      final Thread cfgUpdaterThread = new Thread(() -> {
        final File file = new File("./target/config/" + SecurityGroupFileRepository.FILE_NAME);
        assertThat(file.exists()).isTrue();

        try {
          for (int i = 0; i < CYCLES; i++) {
            final JSONObject json = new JSONObject(FileUtils.readFileAsString(file));
            json.getJSONObject("databases").getJSONObject("*").getJSONObject("groups").put("reloaded", i);
            FileUtils.writeContentToStream(file, json.toString(2).getBytes());
            Thread.sleep(150);
            assertThat(SECURITY.getDatabaseGroupsConfiguration("*").getInt("reloaded")).isEqualTo(i);

            semaphore.countDown();
          }

        } catch (Exception e) {
          e.printStackTrace();
          fail("", e);
        }
      });

      cfgUpdaterThread.start();

      int i = 0;
      while (semaphore.getCount() > 0 && cfgUpdaterThread.isAlive())
        database.getSchema().createVertexType("Test" + (i++));

      cfgUpdaterThread.join();

      assertThat(SECURITY.getDatabaseGroupsConfiguration("*").getInt("reloaded")).isEqualTo(CYCLES - 1);

      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  private void createSchemaNotAllowed(final DatabaseInternal database) throws Throwable {
    expectedSecurityException(() -> database.getSchema().createBucket("Bucket1"));
    expectedSecurityException(() -> database.getSchema().createVertexType("Vertex1"));
    expectedSecurityException(() -> database.getSchema().createEdgeType("Edge1"));
    expectedSecurityException(() -> database.getSchema().createDocumentType("Document1"));

    expectedSecurityException(
        () -> database.getSchema().buildBucketIndex("Document1", "Bucket1", new String[] { "id" }).withUnique(true)
            .withType(Schema.INDEX_TYPE.LSM_TREE).withPageSize(10_000).withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR)
            .create());

    expectedSecurityException(() -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Document1", "id"));

    expectedSecurityException(() -> database.getSchema().dropType("Document1"));
    expectedSecurityException(() -> database.getSchema().dropIndex("Idx1"));
    expectedSecurityException(() -> database.getSchema().dropBucket("Bucket1"));
  }

  private void expectedSecurityException(final CallableNoReturn callback) throws Throwable {
    try {
      callback.call();
      fail("");
    } catch (final SecurityException e) {
      // EXPECTED
    }
  }

  private void checkJohnUser(final ServerSecurityUser albert) {
    assertThat(albert).isNotNull();
    final ServerSecurityUser authJohn = SECURITY.authenticate("albert", "einstein", null);
    assertThat(authJohn).isNotNull();
    assertThat(authJohn.getName()).isEqualTo(albert.getName());

    final SecurityUserFileRepository repository = new SecurityUserFileRepository("./target/config");
    assertThat(repository.getUsers().size()).isEqualTo(2);
    assertThat(repository.getUsers().get(0).getString("name")).isEqualTo("albert");
  }

  private ServerSecurityUser setCurrentUser(final String userName, final DatabaseInternal database) {
    final ServerSecurityUser user = SECURITY.getUser(userName);
    final SecurityDatabaseUser dbUser = user.getDatabaseUser(database);
    DatabaseContext.INSTANCE.init(database).setCurrentUser(dbUser);
    assertThat(DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).getCurrentUser()).isEqualTo(dbUser);
    assertThat(dbUser.getName()).isEqualTo(userName);
    return user;
  }

  private RID createSomeRecords(final DatabaseInternal database, final boolean createEdge) {
    final AtomicReference<RID> validRID = new AtomicReference<>();
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("Vertex1").save();
      final MutableVertex v2 = database.newVertex("Vertex1").save();
      if (createEdge)
        v1.newEdge("Edge1", v2, true);
      database.newDocument("Document1").save();

      validRID.set(v1.getIdentity());
    });
    return validRID.get();
  }

  private void createSchema(final DatabaseInternal database) {
    database.getSchema().createBucket("Bucket1");
    database.getSchema().createVertexType("Vertex1");
    database.getSchema().createEdgeType("Edge1");
    database.getSchema().createDocumentType("Document1");
  }

  private void dropSchema(final DatabaseInternal database) {
    database.getSchema().dropBucket("Bucket1");
    database.getSchema().dropType("Vertex1");
    database.getSchema().dropType("Edge1");
    database.getSchema().dropType("Document1");
  }

  @BeforeAll
  public static void beforeAll() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("dD5ed08c");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY.setValue(100);

    SERVER = new ArcadeDBServer();
    SERVER.start();
    createSecurity();

    SERVER.getOrCreateDatabase(DATABASE_NAME);
  }

  @AfterAll
  public static void afterAll() {
    SERVER.stop();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);

    FileUtils.deleteRecursively(new File("./target/config"));
    TestServerHelper.deleteDatabaseFolders(1);

    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }

  private static void createSecurity() {
    SECURITY = SERVER.getSecurity();

    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("reader",//
        new JSONObject().put("types",
            new JSONObject().put("*", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));
    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("creator", new JSONObject().put("types",
        new JSONObject().put("*", new JSONObject().put("access", new JSONArray(new String[] { "createRecord" })))));
    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("updater", new JSONObject().put("types",
        new JSONObject().put("*", new JSONObject().put("access", new JSONArray(new String[] { "updateRecord" })))));
    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("deleter", new JSONObject().put("types",
        new JSONObject().put("*", new JSONObject().put("access", new JSONArray(new String[] { "deleteRecord" })))));

    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("readerOfDocuments",//
        new JSONObject().put("types",
            new JSONObject().put("Document1", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));
    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("creatorOfDocuments", new JSONObject().put("types",
        new JSONObject().put("Document1", new JSONObject().put("access", new JSONArray(new String[] { "createRecord" })))));
    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("updaterOfDocuments", new JSONObject().put("types",
        new JSONObject().put("Document1", new JSONObject().put("access", new JSONArray(new String[] { "updateRecord" })))));
    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("deleterOfDocuments", new JSONObject().put("types",
        new JSONObject().put("Document1", new JSONObject().put("access", new JSONArray(new String[] { "deleteRecord" })))));

    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("createOnlyGraph",//
        new JSONObject().put("types", new JSONObject()//
            .put("Vertex1", new JSONObject().put("access", new JSONArray(new String[] { "createRecord", "updateRecord" })))//
            .put("Edge1", new JSONObject().put("access", new JSONArray(new String[] { "createRecord" })))//
        ));

    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("readerOfDocumentsCapped",//
        new JSONObject().put("resultSetLimit", 10)//
            .put("types",
                new JSONObject().put("Document1", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));

    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("readerOfDocumentsShortTimeout",//
        new JSONObject().put("readTimeout", 1).put("types",
            new JSONObject().put("Document1", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));
    SECURITY.saveGroups();
  }
}
