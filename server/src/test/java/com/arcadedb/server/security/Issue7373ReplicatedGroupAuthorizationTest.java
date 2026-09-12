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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7373, the half a JSON assertion cannot reach: what a replicated group document does to an
 * <b>authorization decision</b> on a peer, rather than to a file.
 * <p>
 * The two halves are deliberately separate in the fix, and this test is what makes that separation visible.
 * {@link ServerSecurity#applyReplicatedGroups} installs the document; it does not walk the databases ON the
 * calling thread, because that thread is the Raft state-machine apply thread and {@link ServerSecurity#updateSchema}
 * opens and walks every database. The cached {@code ServerSecurityDatabaseUser} of an already-connected principal
 * therefore keeps answering from the PREVIOUS document until a refresh runs, and the refresh is somebody else's
 * call: {@code ServerControlPlane} makes it inline on the node that served the request, and on a peer issue #7510
 * hands it to a dedicated worker (with the group file's watcher left behind it as the safety net).
 * <p>
 * So both assertions below matter, and the first one is not a bug being pinned as a feature - it is the statement
 * that the apply itself stays non-blocking, and it will fail if someone calls {@code updateSchema} directly from
 * it. The mocked server here reports no open database, so #7510's asynchronous refresh has nothing to walk and
 * the assertion is not a race against it.
 */
class Issue7373ReplicatedGroupAuthorizationTest {

  private static final String CONFIG_PATH = "target/test-security-7373-authz";
  private static final String DATABASE    = "graph";
  private static final String GROUP       = "editors";

  private ServerSecurity security;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    // A mocked server rather than null: ServerSecurityUser resolves its permissions through
    // server.getSecurity().getDatabaseGroupsConfiguration(...), so the principal has to be able to see the very
    // store the replicated document is applied to. getHA() answers null, so this node is not in a cluster and
    // createUser() takes the local path - which is what a peer applying someone else's entry looks like.
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    security = new ServerSecurity(server, new ContextConfiguration(), CONFIG_PATH);
    when(server.getSecurity()).thenReturn(security);
  }

  @AfterEach
  void tearDown() {
    if (security != null)
      security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  @Test
  void aReplicatedRevocationDeniesTheCachedPrincipalOnceTheRefreshRuns() {
    final DatabaseInternal database = mockDatabase();

    // The peer starts with the grant in force, and a principal that has already resolved its permissions.
    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
    final ServerSecurityUser alice = security.createUser(new JSONObject()
        .put("name", "alice")
        .put("password", security.encodePassword("alice-password"))
        .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP))));

    final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(database);
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the grant is in force before the revocation").isTrue();

    // The revocation arrives as a replicated entry, exactly as the Raft state machine delivers it.
    security.applyReplicatedGroups(documentGranting(new JSONArray()));

    assertThat(security.getDatabaseGroupsConfiguration(DATABASE).getJSONObject(GROUP).getJSONArray("access"))
        .as("the document itself is revoked immediately on the peer").isEmpty();

    // ...but the apply itself does not walk the databases, so the principal that was already connected still
    // answers from its cache. This is what would fail if someone called updateSchema() directly from the
    // state-machine apply thread; #7510's off-thread refresh finds no open database on this mocked server.
    assertThat(alice.getDatabaseUser(database))
        .as("getDatabaseUser caches per database, and nothing in the apply clears it").isSameAs(cached);
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("the cached principal has not been refreshed yet").isTrue();

    // The refresh is the second half: ServerControlPlane runs it inline on the serving node, and on a peer the
    // worker added by #7510 does. Once it has run, the replicated revocation is enforced against the live
    // principal.
    security.updateSchema(database);

    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA))
        .as("after the refresh the replicated revocation denies the principal that was already connected")
        .isFalse();
  }

  /** The grant direction, so the test cannot pass by denying everything. */
  @Test
  void aReplicatedGrantAllowsThePrincipalOnceTheRefreshRuns() {
    final DatabaseInternal database = mockDatabase();

    security.applyReplicatedGroups(documentGranting(new JSONArray()));
    final ServerSecurityUser alice = security.createUser(new JSONObject()
        .put("name", "alice")
        .put("password", security.encodePassword("alice-password"))
        .put("databases", new JSONObject().put(DATABASE, new JSONArray().put(GROUP))));

    final ServerSecurityDatabaseUser cached = alice.getDatabaseUser(database);
    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isFalse();

    security.applyReplicatedGroups(documentGranting(new JSONArray().put("updateSchema")));
    security.updateSchema(database);

    assertThat(cached.requestAccessOnDatabase(DATABASE_ACCESS.UPDATE_SCHEMA)).isTrue();
  }

  /** A whole group document, in the shape a {@code SECURITY_GROUPS_ENTRY} carries. */
  private static String documentGranting(final JSONArray databaseAccess) {
    final JSONObject group = new JSONObject()
        .put("access", databaseAccess)
        .put("resultSetLimit", -1L)
        .put("readTimeout", -1L)
        .put("types", new JSONObject().put("*", new JSONObject().put("access",
            new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))));

    return new JSONObject()
        .put("version", ServerSecurity.LATEST_VERSION)
        .put("databases", new JSONObject().put(DATABASE,
            new JSONObject().put("groups", new JSONObject().put(GROUP, group))))
        .toString();
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
