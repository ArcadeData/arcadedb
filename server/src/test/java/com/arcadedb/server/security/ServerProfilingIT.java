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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class ServerProfilingIT {
  private static       ArcadeDBServer SERVER;
  private static       ServerSecurity SECURITY;
  private final static String         DATABASE_NAME      = "ServerProfilingIT";
  private final static String         CONFIG_DIRECTORY   = "./target/config";
  // The types reloadWhileUsingDatabase creates while the groups file is reloaded under it, hundreds of them
  private final static String         RELOAD_TYPE_PREFIX = "ReloadTest";
  private final static Duration       RELOAD_TIMEOUT     = Duration.ofSeconds(30);

  /**
   * Leaves the database, the users and the groups as the next test expects them, whether this test passed or not. Every test
   * creates the same schema ({@code Bucket1}, {@code Vertex1}, {@code Edge1}, {@code Document1}) and the same user, and used to
   * drop them on its last lines only: one failure left them behind, and every later test of the class then failed on
   * "Cannot create type 'Vertex1' because already exists", which hid the one failure that mattered.
   */
  @AfterEach
  void cleanUp() throws IOException {
    final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);
    setCurrentUser("root", database);

    final Schema schema = database.getSchema();
    for (final String typeName : new String[] { "Edge1", "Vertex1", "Document1" })
      if (schema.existsType(typeName))
        schema.dropType(typeName);
    final List<String> reloadTypes = schema.getTypes().stream().map(DocumentType::getName)
        .filter(name -> name.startsWith(RELOAD_TYPE_PREFIX)).toList();
    for (final String typeName : reloadTypes)
      schema.dropType(typeName);
    if (schema.existsBucket("Bucket1"))
      schema.dropBucket("Bucket1");

    if (SECURITY.existsUser("albert"))
      SECURITY.dropUser("albert");

    final JSONObject wildcardGroups = SECURITY.getDatabaseGroupsConfiguration(SecurityManager.ANY);
    if (wildcardGroups != null && wildcardGroups.has("reloaded")) {
      editGroupsFile(groups -> groups.remove("reloaded"));
      awaitWildcardGroups("the clean-up", groups -> !groups.has("reloaded"));
    }
  }

  @Test
  void userDefaultAccessCannotAccessDatabase() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein")));

    try {
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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

      expectedSecurityException(() ->
        executeRemoteCommand("INSERT INTO Vertex1 set name = 'invalid'", "albert", "einstein"));
      expectedSecurityException(() ->
        executeRemoteCommand("INSERT INTO Document1 set name = 'invalid'", "albert", "einstein"));
      expectedSecurityException(() ->
        executeRemoteCommand("SELECT FROM Document1", "albert", "einstein"));
      expectedSecurityException(() ->
        executeRemoteCommand("SELECT FROM " + validRID, "albert", "einstein"));

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
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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
  void multipleGroupsAnyType() {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein")).put("databases",
        new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "creator", "reader", "updater", "deleter" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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
  void multipleGroupsSpecificType() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein")).put("databases",
        new JSONObject().put(DATABASE_NAME, new JSONArray(
            new String[] { "creatorOfDocuments", "readerOfDocuments", "updaterOfDocuments", "deleterOfDocuments" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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
      expectedSecurityException(() -> v1.newEdge("Edge1", v2).save());

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
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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
      v1.newEdge("Edge1", v2);

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
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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
  void resultSetLimit() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "readerOfDocumentsCapped" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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

      // THE CAP IS ALBERT'S, NOT THE STATEMENT'S: ROOT RUNS THE SAME TEXT, OUT OF THE SAME STATEMENT CACHE, AND READS ALL 15
      setCurrentUser("root", database);
      count = 0;
      for (final ResultSet iter = database.query("sql", "select from Document1"); iter.hasNext(); ) {
        iter.next();
        ++count;
      }
      assertThat(count).isEqualTo(15);

      // SWITCH TO ROOT TO DROP THE SCHEMA
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void readTimeout() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "readerOfDocumentsShortTimeout" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);

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

      // THE READ MUST OUTLIVE THE GROUP'S 1 MS QUOTA, AND 10,000 RECORDS DO NOT GUARANTEE IT: ON A WARM JIT THEY ITERATE IN
      // ABOUT 1 MS, AND THE DEADLINE IS COMPARED ON A MILLISECOND CLOCK, SO THE LOOP FINISHED BEFORE IT AND NOTHING WAS RAISED.
      // THE FIRST RECORD IS READ (THE GROUP GRANTS readRecord), THEN THE READ IS HELD PAST THE QUOTA BEFORE IT GOES ON
      assertThatThrownBy(() -> {
        final Iterator<Record> iter = database.iterateType("Document1", true);
        assertThat(iter.next()).isNotNull();
        outliveReadTimeout();
        while (iter.hasNext())
          iter.next();
      }).isInstanceOf(TimeoutException.class);

      assertThatThrownBy(() -> {
        try (final ResultSet iter = database.query("sql", "select from Document1")) {
          assertThat(iter.next()).isNotNull();
          outliveReadTimeout();
          while (iter.hasNext())
            iter.next();
        }
      }).isInstanceOf(TimeoutException.class);

      // THE 1 MS QUOTA IS ALBERT'S, NOT THE STATEMENT'S: ROOT RUNS THE SAME TEXT, OUT OF THE SAME STATEMENT CACHE, AS SLOWLY, AND
      // IS NOT TIMED OUT. IT WAS, AND SO WAS THE NEXT TEST TO RUN IT (resultSetLimit, "TIMEOUT clause of 1ms")
      setCurrentUser("root", database);
      try (final ResultSet iter = database.query("sql", "select from Document1")) {
        assertThat(iter.next()).isNotNull();
        outliveReadTimeout();
        int count = 1;
        while (iter.hasNext()) {
          iter.next();
          ++count;
        }
        assertThat(count).isEqualTo(10_000);
      }

      // SWITCH TO ROOT TO DROP THE SCHEMA
      setCurrentUser("root", database);
      dropSchema(database);

    } finally {
      SECURITY.dropUser("albert");
    }
  }

  @Test
  void groupsReload() throws Throwable {
    try {
      editGroupsFile(groups -> groups.put("reloaded", true));
      awaitWildcardGroups("the edit", groups -> groups.getBoolean("reloaded", false));
    } finally {
      // RESTORE THE FILE AND WAIT FOR THE RELOAD, SO NO LATER TEST RUNS AGAINST A TICK THAT HAS NOT HAPPENED YET
      editGroupsFile(groups -> groups.remove("reloaded"));
      awaitWildcardGroups("the restore", groups -> !groups.has("reloaded"));
      createSecurity();
    }
  }

  @Test
  void reloadWhileUsingDatabase() throws Throwable {
    SECURITY.createUser(new JSONObject().put("name", "albert").put("password", SECURITY.encodePassword("einstein"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "reader", "updater" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabaseForOperator(DATABASE_NAME);
      checkJohnUser(setCurrentUser("albert", database));
      // SWITCH TO ROOT TO CREATE SOME TYPES FOR FURTHER TESTS
      setCurrentUser("root", database);

      createSchema(database);

      final int CYCLES = 30;
      final CountDownLatch semaphore = new CountDownLatch(CYCLES);
      // AN ASSERTION THAT FAILS ON THE UPDATER THREAD ONLY ENDS THAT THREAD: IT IS HANDED BACK TO THE TEST THREAD BELOW, WHICH
      // USED TO SEE NOTHING BUT THE CYCLE THE THREAD DIED ON ("expected 29, but was 13")
      final AtomicReference<Throwable> updaterFailure = new AtomicReference<>();

      final Thread cfgUpdaterThread = new Thread(() -> {
        try {
          for (int i = 0; i < CYCLES; i++) {
            final int cycle = i;
            editGroupsFile(groups -> groups.put("reloaded", cycle));
            awaitWildcardGroups("the edit of cycle " + cycle, groups -> groups.getInt("reloaded", -1) == cycle);
            semaphore.countDown();
          }
        } catch (final Throwable e) {
          updaterFailure.set(e);
        }
      });

      cfgUpdaterThread.start();

      int i = 0;
      while (semaphore.getCount() > 0 && cfgUpdaterThread.isAlive())
        database.getSchema().createVertexType(RELOAD_TYPE_PREFIX + (i++));

      cfgUpdaterThread.join();
      if (updaterFailure.get() != null)
        throw updaterFailure.get();

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
    assertThatThrownBy(() -> callback.call()).isInstanceOf(SecurityException.class);
  }

  private void checkJohnUser(final ServerSecurityUser albert) {
    assertThat(albert).isNotNull();
    final ServerSecurityUser authJohn = SECURITY.authenticate("albert", "einstein", null);
    assertThat(authJohn).isNotNull();
    assertThat(authJohn.getName()).isEqualTo(albert.getName());

    final SecurityUserFileRepository repository = new SecurityUserFileRepository(CONFIG_DIRECTORY);
    // Assert WHICH principals were persisted, not which line each landed on: the order of
    // server-users.jsonl is whatever order ServerSecurity happens to serialize its user store in, and
    // that is not part of any contract. It used to come out of a ConcurrentHashMap, whose hash order put
    // "albert" first by luck; persisting the mutation before publishing it now appends the new user, so
    // "root" comes first. Nothing read the order either way - only this assertion did, and it turned an
    // internal implementation detail into 11 red tests.
    final List<String> persistedUsers = repository.getUsers().stream().map(user -> user.getString("name")).toList();
    assertThat(persistedUsers).containsExactlyInAnyOrder("root", "albert");
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
        v1.newEdge("Edge1", v2);
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
  static void beforeAll() {
    FileUtils.deleteRecursively(new File(CONFIG_DIRECTORY));
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
  static void afterAll() {
    SERVER.stop();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);

    FileUtils.deleteRecursively(new File(CONFIG_DIRECTORY));
    TestServerHelper.deleteDatabaseFolders(1);

    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }

  /**
   * Defines the groups the tests use, in the wildcard database entry. Through {@link ServerSecurity#saveGroup}, which persists
   * each group and publishes it in one step under the group store's lock, and NOT by mutating the document
   * {@code getDatabaseGroupsConfiguration()} returns and saving it afterwards: that document is the live one, and the 100 ms
   * reload tick this class configures replaces it with a fresh parse of the file. A tick landing between the mutation and the
   * save (the first tick after start reloads the default document the start just wrote) took every group put before it away.
   */
  private static void createSecurity() {
    SECURITY = SERVER.getSecurity();

    saveTestGroup("reader", new JSONObject().put("types", accessOn("*", "readRecord")));
    saveTestGroup("creator", new JSONObject().put("types", accessOn("*", "createRecord")));
    saveTestGroup("updater", new JSONObject().put("types", accessOn("*", "updateRecord")));
    saveTestGroup("deleter", new JSONObject().put("types", accessOn("*", "deleteRecord")));

    saveTestGroup("readerOfDocuments", new JSONObject().put("types", accessOn("Document1", "readRecord")));
    saveTestGroup("creatorOfDocuments", new JSONObject().put("types", accessOn("Document1", "createRecord")));
    saveTestGroup("updaterOfDocuments", new JSONObject().put("types", accessOn("Document1", "updateRecord")));
    saveTestGroup("deleterOfDocuments", new JSONObject().put("types", accessOn("Document1", "deleteRecord")));

    saveTestGroup("createOnlyGraph", new JSONObject().put("types",
        accessOn("Vertex1", "createRecord", "updateRecord").put("Edge1",
            new JSONObject().put("access", new JSONArray(new String[] { "createRecord" })))));

    saveTestGroup("readerOfDocumentsCapped",
        new JSONObject().put("resultSetLimit", 10).put("types", accessOn("Document1", "readRecord")));

    saveTestGroup("readerOfDocumentsShortTimeout",
        new JSONObject().put("readTimeout", 1).put("types", accessOn("Document1", "readRecord")));
  }

  private static void saveTestGroup(final String name, final JSONObject group) {
    SECURITY.saveGroup(SecurityManager.ANY, name, group);
  }

  private static JSONObject accessOn(final String typeName, final String... access) {
    return new JSONObject().put(typeName, new JSONObject().put("access", new JSONArray(access)));
  }

  /**
   * Edits the wildcard groups of {@link SecurityGroupFileRepository#FILE_NAME} the way an operator would, outside the server.
   * Atomically (temp file and rename), so the reload tick never reads a half-written file: a torn read is preserved as
   * {@code server-groups-error.json} and logged at SEVERE, and the rest of the write can land in the same millisecond the tick
   * stamped, which the next tick does not see as a change.
   */
  private static void editGroupsFile(final Consumer<JSONObject> edit) throws IOException {
    final File file = new File(CONFIG_DIRECTORY, SecurityGroupFileRepository.FILE_NAME);
    assertThat(file.exists()).isTrue();

    final JSONObject json = new JSONObject(FileUtils.readFileAsString(file));
    edit.accept(json.getJSONObject("databases").getJSONObject(SecurityManager.ANY).getJSONObject("groups"));
    FileUtils.atomicWriteFile(file, json.toString(2));
  }

  /**
   * Waits for the reload tick to publish an edit of the file. The watcher promises the NEXT tick, not a latency: it is a
   * fixed-delay {@code java.util.Timer} whose tick also re-derives the cached permissions of every open database, so the
   * delay is a period plus whatever the previous tick, the scheduler and the collector took. The old 150 ms sleep left 50 ms
   * for all of that and failed on a loaded runner. The bound only separates "reloaded later" from "never reloaded", so it is
   * generous.
   */
  private static void awaitWildcardGroups(final String what, final Predicate<JSONObject> reloaded) {
    await("the reload of " + what + " of '" + SecurityGroupFileRepository.FILE_NAME + "'").atMost(RELOAD_TIMEOUT)
        .pollInterval(Duration.ofMillis(10)).until(() -> reloaded.test(SECURITY.getDatabaseGroupsConfiguration(SecurityManager.ANY)));
  }

  /** Holds the current read past the 1 ms {@code readTimeout} of {@code readerOfDocumentsShortTimeout}. */
  private static void outliveReadTimeout() throws InterruptedException {
    Thread.sleep(20);
  }
}
