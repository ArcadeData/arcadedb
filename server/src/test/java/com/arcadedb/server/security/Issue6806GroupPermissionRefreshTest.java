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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit regression test for issue #6806. {@code ServerSecurityDatabaseUser} keeps two independent permission
 * maps refreshed by two different methods, and the server's only refresh path called just one of them
 * ({@code updateFileAccess}), leaving the database-level grants read by {@code requestAccessOnDatabase} -
 * {@code updateSchema}, {@code updateSecurity}, {@code updateDatabaseSettings} - plus the
 * {@code resultSetLimit}/{@code readTimeout} quotas frozen at the values they had when the user first
 * touched the database. A revoked grant kept working until restart.
 * <p>
 * Drives {@link ServerSecurityUser#refreshDatabaseConfiguration} directly, which is what
 * {@code ServerSecurity.updateSchema} now calls for every user;
 * {@code Issue6806GroupPermissionRefreshIT} covers the same fix end to end over HTTP.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6806GroupPermissionRefreshTest {

  private static final String DATABASE = "issue6806db";
  private static final String GROUP    = "editor";

  @Test
  void revokedDatabaseGrantIsVisibleOnTheCachedDatabaseUser() {
    final DatabaseInternal database = mockDatabase();
    final ServerSecurityUser alice = newUser(database, groups(new JSONArray().put("updateSchema"), -1L));

    final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);
    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

    alice.refreshDatabaseConfiguration(database, groups(new JSONArray(), -1L));

    // The SAME cached instance must now deny: getDatabaseUser() returns it forever (databaseCache is only
    // cleared by refreshDatabaseNames(), which nothing in src/main calls), so a stale map is permanent.
    assertThat(alice.getDatabaseUser(database)).isSameAs(dbUser);
    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();
  }

  @Test
  void refreshingOnlyTheFileAccessMapLeavesTheDatabaseGrantStale() {
    // Pins the mechanism of the bug, so a future refactor cannot quietly go back to it: updateFileAccess()
    // rebuilds the per-type/per-bucket maps and NOTHING else, so calling it alone - which is what
    // ServerSecurity.updateSchema() used to do - leaves databaseAccessMap exactly as it was.
    final DatabaseInternal database = mockDatabase();
    final ServerSecurityUser alice = newUser(database, groups(new JSONArray().put("updateSchema"), -1L));

    final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);
    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();

    dbUser.updateFileAccess(database, groups(new JSONArray(), -1L));
    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("half a refresh leaves the revoked grant in effect - the defect of #6806").isTrue();

    dbUser.updateDatabaseConfiguration(groups(new JSONArray(), -1L));
    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();
  }

  @Test
  void newlyGrantedDatabaseGrantIsVisibleOnTheCachedDatabaseUser() {
    // The gap was symmetric: a grant added after the user first touched the database was ignored too.
    final DatabaseInternal database = mockDatabase();
    final ServerSecurityUser alice = newUser(database, groups(new JSONArray(), -1L));

    final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);
    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SECURITY)).isFalse();

    alice.refreshDatabaseConfiguration(database, groups(new JSONArray().put("updateSecurity"), -1L));

    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SECURITY)).isTrue();
  }

  @Test
  void groupQuotasAreRefreshedToo() {
    final DatabaseInternal database = mockDatabase();
    final ServerSecurityUser alice = newUser(database, groups(new JSONArray(), -1L));

    final ServerSecurityDatabaseUser dbUser = alice.getDatabaseUser(database);
    assertThat(dbUser.getResultSetLimit()).isEqualTo(-1L);

    alice.refreshDatabaseConfiguration(database, groups(new JSONArray(), 7L));

    assertThat(dbUser.getResultSetLimit()).isEqualTo(7L);
  }

  @Test
  void aSyntheticPrincipalIsNeverWidenedByTheGroupFile() {
    // An API-token principal carries its own group definition. The refresh must keep using it, exactly as
    // registerDatabaseUser() does, so the groups of server-groups.json can never grant it more than the
    // token's own permissions.
    final DatabaseInternal database = mockDatabase();
    final ServerSecurityUser token = newUser(database, groups(new JSONArray(), -1L))
        .withSyntheticGroupConfig(groups(new JSONArray(), -1L));

    final ServerSecurityDatabaseUser dbUser = token.getDatabaseUser(database);
    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();

    token.refreshDatabaseConfiguration(database, groups(new JSONArray().put("updateSchema"), -1L));

    assertThat(dbUser.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();
  }

  @Test
  void refreshingAUserThatNeverTouchedTheDatabaseIsANoOp() {
    final DatabaseInternal database = mockDatabase();
    // The server's live configuration (what registerDatabaseUser will read) grants nothing.
    final ServerSecurityUser alice = newUser(database, groups(new JSONArray(), -1L));

    // Nothing is cached for this pair, so the refresh must do nothing at all rather than materialise an
    // entry - ServerSecurity.updateSchema() sweeps EVERY server user on every schema change, and building a
    // ServerSecurityDatabaseUser for each of them would be both wasteful and a way to inject a permission
    // set that never came from the live configuration.
    alice.refreshDatabaseConfiguration(database, groups(new JSONArray().put("updateSchema"), -1L));

    // First access builds the entry from the live configuration, which still grants nothing.
    assertThat(alice.getDatabaseUser(database).requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();
  }

  private static JSONObject groups(final JSONArray databaseAccess, final long resultSetLimit) {
    return new JSONObject().put(GROUP, new JSONObject()
        .put("access", databaseAccess)
        .put("resultSetLimit", resultSetLimit)
        .put("readTimeout", -1L)
        .put("types", new JSONObject().put("*", new JSONObject().put("access",
            new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord")))));
  }

  private static ServerSecurityUser newUser(final DatabaseInternal database, final JSONObject groupConfiguration) {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.getDatabaseGroupsConfiguration(DATABASE)).thenReturn(groupConfiguration);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getSecurity()).thenReturn(security);

    final JSONObject userConfiguration = new JSONObject()
        .put("name", "alice")
        .put("password", "irrelevant")
        .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP)));

    return new ServerSecurityUser(server, userConfiguration);
  }

  private static DatabaseInternal mockDatabase() {
    final FileManager fileManager = mock(FileManager.class);
    when(fileManager.getFiles()).thenReturn(List.of());

    final Schema schema = mock(Schema.class);
    when(schema.getTypes()).thenReturn(List.of());

    final DatabaseInternal database = mock(DatabaseInternal.class);
    when(database.getName()).thenReturn(DATABASE);
    when(database.getFileManager()).thenReturn(fileManager);
    when(database.getSchema()).thenReturn(schema);
    return database;
  }
}
