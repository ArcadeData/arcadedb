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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.database.Database;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A BOLT connection may only reach the databases its user is granted, and inside one only the types the user can
 * read. The BOLT listener answers database selection, {@code SHOW DATABASES} and the schema procedures itself
 * rather than through the query engine, so each of them has to apply the same per-user narrowing the HTTP, gRPC,
 * PostgreSQL, Redis and MongoDB listeners apply: a user holding a grant on one database used to be able to list every
 * database on the server and read the types, property keys, indexes and constraints of any of them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltDatabaseAuthorizationIT extends BaseBoltServerTest {
  private static final String OTHER_DATABASE = "BoltDatabaseAuthorizationOther";
  private static final String LIMITED_USER   = "boltLimitedUser";
  private static final String TYPE_READER    = "boltTypeReader";
  private static final String PASSWORD       = "boltLimitedPassword";
  private static final String READER_GROUP   = "boltAllowedTypeReader";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    final ArcadeDBServer server = getServer(0);
    if (server != null && server.isStarted()) {
      server.getConfiguration().setValue(GlobalConfiguration.BOLT_DEFAULT_DATABASE, null);
      final ServerSecurity security = server.getSecurity();
      for (final String user : new String[] { LIMITED_USER, TYPE_READER })
        if (security.getUser(user) != null)
          security.dropUser(user);
      security.deleteGroup(getDatabaseName(), READER_GROUP);
      if (server.existsDatabase(OTHER_DATABASE)) {
        // ServerDatabase refuses drop() because the handle is shared: drop the embedded instance, then unregister it
        server.getDatabase(OTHER_DATABASE).getEmbedded().drop();
        server.removeDatabase(OTHER_DATABASE);
      }
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void showDatabasesListsOnlyTheGrantedDatabases() {
    createOtherDatabase();
    createLimitedUser();

    try (final Driver driver = driver(LIMITED_USER, PASSWORD);
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final List<String> names = session.run("SHOW DATABASES").list(r -> r.get("name").asString());
      assertThat(names).contains(getDatabaseName(), "system").doesNotContain(OTHER_DATABASE);

      // The WHERE tail must not become an existence oracle for a database the user cannot see
      assertThat(session.run("SHOW DATABASES WHERE name = $dbName", Map.of("dbName", OTHER_DATABASE)).list()).isEmpty();
      assertThat(session.run("CALL dbms.listDatabases()").list(r -> r.get("name").asString()))
          .doesNotContain(OTHER_DATABASE);
    }

    try (final Driver driver = rootDriver();
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.run("SHOW DATABASES").list(r -> r.get("name").asString()))
          .contains(getDatabaseName(), OTHER_DATABASE, "system");
    }
  }

  @Test
  void runNamingAnUngrantedDatabaseIsForbidden() {
    createOtherDatabase();
    createLimitedUser();

    for (final String query : new String[] { "CALL db.labels()", "CALL db.relationshipTypes()", "CALL db.propertyKeys()",
        "SHOW INDEXES", "SHOW CONSTRAINTS", "SHOW DATABASES", "CALL db.schema.visualization()", "MATCH (n) RETURN n" }) {
      try (final Driver driver = driver(LIMITED_USER, PASSWORD);
          final Session session = driver.session(SessionConfig.forDatabase(OTHER_DATABASE))) {
        assertThatThrownBy(() -> session.run(query).list())
            .as(query)
            .isInstanceOfSatisfying(Neo4jException.class,
                e -> assertThat(e.code()).isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR));
      }
    }

    // Root is granted every database, so the same selection works for it
    try (final Driver driver = rootDriver();
        final Session session = driver.session(SessionConfig.forDatabase(OTHER_DATABASE))) {
      assertThat(session.run("CALL db.labels()").list(r -> r.get("label").asString())).contains("OtherSecretVertex");
    }
  }

  @Test
  void beginNamingAnUngrantedDatabaseIsForbidden() throws Exception {
    createOtherDatabase();
    createLimitedUser();

    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.sendNoFields(BoltMessage.LOGOFF);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.logon(LIMITED_USER, PASSWORD);

      bolt.sendBegin(OTHER_DATABASE);
      final BoltWireConnection.Summary refused = bolt.readSummary();
      assertThat(refused.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
    }
  }

  /**
   * A user without a wildcard grant is refused a name it is not granted whether or not the database exists, so the
   * answer cannot be used to probe which databases the server hosts. Root still learns that the name is wrong.
   */
  @Test
  void anUnknownDatabaseIsForbiddenRatherThanNotFoundForALimitedUser() {
    createLimitedUser();

    try (final Driver driver = driver(LIMITED_USER, PASSWORD);
        final Session session = driver.session(SessionConfig.forDatabase("neverCreatedDatabaseXyz"))) {
      assertThatThrownBy(() -> session.run("RETURN 1").list())
          .isInstanceOfSatisfying(Neo4jException.class,
              e -> assertThat(e.code()).isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR));
    }

    try (final Driver driver = rootDriver();
        final Session session = driver.session(SessionConfig.forDatabase("neverCreatedDatabaseXyz"))) {
      assertThatThrownBy(() -> session.run("RETURN 1").list())
          .isInstanceOfSatisfying(Neo4jException.class,
              e -> assertThat(e.code()).isEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR));
    }
  }

  /**
   * LOGOFF then LOGON as another user keeps the connection, and with it the database the previous user had open. The
   * new user must be checked against that database too, not only against one it names.
   */
  @Test
  void reauthenticatingAsAnotherUserDoesNotInheritTheOpenDatabase() throws Exception {
    createOtherDatabase();
    createLimitedUser();
    getServer(0).getConfiguration().setValue(GlobalConfiguration.BOLT_DEFAULT_DATABASE, OTHER_DATABASE);

    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), null)) {
      // Root opens the default database, which the limited user is not granted
      bolt.run("CALL db.labels()");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().records()).contains(List.of("OtherSecretVertex"));

      bolt.sendNoFields(BoltMessage.LOGOFF);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.logon(LIMITED_USER, PASSWORD);

      bolt.run("CALL db.labels()");
      final BoltWireConnection.Summary refused = bolt.readSummary();
      assertThat(refused.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
    }
  }

  /**
   * With no database named and no default configured, the connection falls back to a database the user is granted
   * rather than to whichever one the server happens to list first.
   */
  @Test
  void anUnnamedDatabaseFallsBackToAGrantedOne() {
    createOtherDatabase();
    createLimitedUser();

    try (final Driver driver = driver(LIMITED_USER, PASSWORD); final Session session = driver.session()) {
      final List<Record> current = session.run("SHOW DATABASES WHERE default = true").list();
      assertThat(current).hasSize(1);
      assertThat(current.getFirst().get("name").asString()).isEqualTo(getDatabaseName());
    }
  }

  /**
   * Inside a granted database, the schema listings hide the types the user cannot read - as {@code schema:types} and
   * {@code schema:indexes} do in SQL - instead of naming every type, property key, index and constraint.
   */
  @Test
  void schemaListingsHideTypesTheUserCannotRead() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final Schema schema = database.getSchema();
    schema.getOrCreateVertexType("AllowedVertex").getOrCreateProperty("allowedKey", Type.STRING);
    final VertexType secret = schema.getOrCreateVertexType("RestrictedVertex");
    secret.getOrCreateProperty("restrictedKey", Type.STRING).setMandatory(true);
    secret.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "restrictedKey");
    schema.getOrCreateEdgeType("RestrictedEdge");

    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), READER_GROUP, new JSONObject().put("types",
        new JSONObject().put("AllowedVertex", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));
    security.createUser(new JSONObject().put("name", TYPE_READER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { READER_GROUP }))));

    try (final Driver driver = driver(TYPE_READER, PASSWORD);
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.run("CALL db.labels()").list(r -> r.get("label").asString()))
          .contains("AllowedVertex").doesNotContain("RestrictedVertex");
      assertThat(session.run("CALL db.relationshipTypes()").list(r -> r.get("relationshipType").asString()))
          .doesNotContain("RestrictedEdge");
      assertThat(session.run("CALL db.propertyKeys()").list(r -> r.get("propertyKey").asString()))
          .contains("allowedKey").doesNotContain("restrictedKey");
      assertThat(session.run("CALL db.schema.visualization()").list(r -> r.get("name").asString()))
          .contains("AllowedVertex").doesNotContain("RestrictedVertex", "RestrictedEdge");
      assertThat(session.run("SHOW INDEXES").list(r -> r.get("labelsOrTypes").asList().toString()))
          .noneMatch(labels -> labels.contains("RestrictedVertex"));
      assertThat(session.run("SHOW CONSTRAINTS").list(r -> r.get("labelsOrTypes").asList().toString()))
          .noneMatch(labels -> labels.contains("RestrictedVertex"));

      // The combined form Neo4j Desktop sends: one row per procedure, each holding the whole list
      final List<Record> combined = session.run("CALL db.labels() YIELD label RETURN COLLECT(label) AS result "
          + "UNION ALL CALL db.relationshipTypes() YIELD relationshipType RETURN COLLECT(relationshipType) AS result "
          + "UNION ALL CALL db.propertyKeys() YIELD propertyKey RETURN COLLECT(propertyKey) AS result").list();
      assertThat(combined.toString()).contains("AllowedVertex")
          .doesNotContain("RestrictedVertex", "RestrictedEdge", "restrictedKey");
    }

    try (final Driver driver = rootDriver();
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.run("CALL db.labels()").list(r -> r.get("label").asString()))
          .contains("AllowedVertex", "RestrictedVertex");
      assertThat(session.run("CALL db.propertyKeys()").list(r -> r.get("propertyKey").asString()))
          .contains("restrictedKey");
      assertThat(session.run("SHOW INDEXES").list(r -> r.get("labelsOrTypes").asList().toString()))
          .anyMatch(labels -> labels.contains("RestrictedVertex"));
      assertThat(session.run("SHOW CONSTRAINTS").list(r -> r.get("labelsOrTypes").asList().toString()))
          .anyMatch(labels -> labels.contains("RestrictedVertex"));
    }
  }

  private void createOtherDatabase() {
    final Database other = getServer(0).createDatabase(OTHER_DATABASE, ComponentFile.MODE.READ_WRITE);
    other.getSchema().getOrCreateVertexType("OtherSecretVertex").getOrCreateProperty("otherSecretKey", Type.STRING);
  }

  private void createLimitedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", LIMITED_USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
  }

  private Driver driver(final String user, final String password) {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic(user, password),
        Config.builder().withoutEncryption().build());
  }

  private Driver rootDriver() {
    return driver("root", DEFAULT_PASSWORD_FOR_TESTS);
  }
}
