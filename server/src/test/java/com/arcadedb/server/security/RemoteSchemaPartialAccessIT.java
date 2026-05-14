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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.network.HostUtil;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #4238: a non-root user with readRecord granted on some types but not on
 * others must still be able to open a {@link RemoteDatabase} connection and run commands against
 * the types they are allowed to read. Before the fix, the lazy schema reload performed by the
 * remote driver issued {@code select from schema:types}, which counted records on every type and
 * aborted with {@link SecurityException} the first time it hit a type the user could not read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteSchemaPartialAccessIT {

  private static       ArcadeDBServer SERVER;
  private static       ServerSecurity SECURITY;
  private static final String         DATABASE_NAME = "RemoteSchemaPartialAccessIT";
  private static final String         USER          = "albert";
  private static final String         PWD           = "einstein";

  @Test
  void remoteCommandSucceedsWhenUserLacksReadRecordOnSomeTypes() {
    SECURITY.createUser(new JSONObject().put("name", USER).put("password", SECURITY.encodePassword(PWD))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "readerOfDocuments" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);
      database.getSchema().getOrCreateVertexType("AllowedDocument");
      database.getSchema().getOrCreateVertexType("RestrictedVertex");
      database.getSchema().getOrCreateEdgeType("RestrictedEdge");

      database.transaction(() -> {
        database.newVertex("AllowedDocument").set("name", "doc-a").save();
        database.newVertex("AllowedDocument").set("name", "doc-b").save();
        database.newVertex("RestrictedVertex").set("name", "blocked").save();
      });

      final String[] address = HostUtil.parseHostAddress(SERVER.getHttpServer().getListeningAddress(),
          HostUtil.CLIENT_DEFAULT_PORT);

      final RemoteDatabase remote = new RemoteDatabase(address[0], Integer.parseInt(address[1]), DATABASE_NAME, USER, PWD);

      // Without the fix the first command throws SecurityException because RemoteSchema.reload()
      // walks every type and counts records, tripping on RestrictedVertex/RestrictedEdge.
      final ResultSet rs = remote.command("sql", "SELECT FROM AllowedDocument");
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      assertThat(rows).isEqualTo(2);

      // The remote schema cache must only expose the types the user is allowed to read.
      assertThat(remote.getSchema().getType("AllowedDocument")).isNotNull();
      assertThat(remote.getSchema().existsType("RestrictedVertex")).isFalse();
      assertThat(remote.getSchema().existsType("RestrictedEdge")).isFalse();

      // Run a second command to make sure subsequent calls also succeed (schemaLoaded was never
      // toggled to true before the fix, so every later call repeated the failure).
      try (final ResultSet rs2 = remote.query("sql", "SELECT count(*) AS c FROM AllowedDocument")) {
        assertThat(rs2.hasNext()).isTrue();
        final Result r = rs2.next();
        assertThat(((Number) r.getProperty("c")).longValue()).isEqualTo(2L);
      }

      // Sanity: root continues to see every type.
      final RemoteDatabase rootRemote = new RemoteDatabase(address[0], Integer.parseInt(address[1]), DATABASE_NAME, "root",
          "dD5ed08c");
      assertThat(rootRemote.getSchema().existsType("AllowedDocument")).isTrue();
      assertThat(rootRemote.getSchema().existsType("RestrictedVertex")).isTrue();
      assertThat(rootRemote.getSchema().existsType("RestrictedEdge")).isTrue();
    } finally {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);
      if (database.getSchema().existsType("RestrictedEdge"))
        database.getSchema().dropType("RestrictedEdge");
      if (database.getSchema().existsType("RestrictedVertex"))
        database.getSchema().dropType("RestrictedVertex");
      if (database.getSchema().existsType("AllowedDocument"))
        database.getSchema().dropType("AllowedDocument");
      SECURITY.dropUser(USER);
    }
  }

  @BeforeAll
  static void beforeAll() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("dD5ed08c");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");

    SERVER = new ArcadeDBServer();
    SERVER.start();

    SECURITY = SERVER.getSecurity();
    // Group granting readRecord only on AllowedDocument. Any other type defaults to no access.
    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("readerOfDocuments",
        new JSONObject().put("types", new JSONObject().put("AllowedDocument",
            new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));
    SECURITY.saveGroups();

    SERVER.getOrCreateDatabase(DATABASE_NAME);
  }

  @AfterAll
  static void afterAll() {
    if (SERVER != null && SERVER.isStarted()) {
      // Drop the test database so the AfterAll cleanup leaves no orphan files behind.
      if (SERVER.existsDatabase(DATABASE_NAME))
        ((DatabaseInternal) SERVER.getDatabase(DATABASE_NAME)).getEmbedded().drop();
      SERVER.stop();
    }
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);

    FileUtils.deleteRecursively(new File("./target/config"));
    TestServerHelper.deleteDatabaseFolders(1);
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
