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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * How the server and its HA plugin meet: the leader a refusal names, a server that requested HA with no plugin present,
 * and the HA wrap applied to a database registered by a caller.
 */
class ServerHAPluginIntegrationTest {
  /*
   * Issue #10066: a refusal that names the leader must not name the node that is refusing. On a cluster whose peers share a host
   * and declare no HTTP port (issue #6191) a follower resolves every peer's HTTP address, the leader's included, to its OWN, so the
   * address {@link HAServerPlugin#getLeaderAddress()} answers sends the client back to the node that just refused it.
   * {@link HAServerPlugin#getLeaderForRefusal()} is the one place that decides what a refusal names instead.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10066 a refusal naming the leader must not name the refusing node
  @Test
  void aLeaderAddressThatIsNotThisNodesOwnIsNamedAsItIs() {
    assertThat(new Cluster("db0:2480", false, "ArcadeDB_0").getLeaderForRefusal()).isEqualTo("db0:2480");
  }

  // Regression: #10066 a refusal naming the leader must not name the refusing node
  @Test
  void aLeaderAddressThatIsThisNodesOwnIsReplacedByTheLeadersServerName() {
    assertThat(new Cluster("db1:2480", true, "ArcadeDB_0").getLeaderForRefusal()).isEqualTo("ArcadeDB_0");
  }

  // Regression: #10066 a refusal naming the leader must not name the refusing node
  @Test
  void noLeaderNamesNobody() {
    assertThat(new Cluster(null, false, null).getLeaderForRefusal()).isNull();
    assertThat(new Cluster("", false, "ArcadeDB_0").getLeaderForRefusal()).isNull();
  }

  // Regression: #10066 a refusal naming the leader must not name the refusing node
  @Test
  void anOwnAddressWithNoKnownLeaderNameNamesNobodyRatherThanTheRefusingNode() {
    assertThat(new Cluster("db1:2480", true, null).getLeaderForRefusal()).isNull();
  }

  /** A cluster view reduced to the three answers {@link HAServerPlugin#getLeaderForRefusal()} is made of. */
  private record Cluster(String leaderAddress, boolean addressIsOwn, String leaderName) implements HAServerPlugin {
    @Override
    public String getLeaderAddress() {
      return leaderAddress;
    }

    @Override
    public boolean isOwnHttpAddress(final String address) {
      return addressIsOwn;
    }

    @Override
    public String getLeaderName() {
      return leaderName;
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public void startService() {
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public Map<String, Object> getStats() {
      return Collections.emptyMap();
    }

    @Override
    public int getConfiguredServers() {
      return 3;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }
  }

  // Regression: #8270 HA requested without the plugin opens databases refusing writes
  /**
   * Issue #8270: a server started with high availability requested opens its databases refusing writes, because the HA
   * plugin that wraps them for replication starts only after the network listeners do. This module has no HA plugin on
   * its classpath, so startup has to conclude that none is coming and let the node run standalone - as its warning
   * says - with every database writable: the one opened at startup, and one created afterwards.
   */
  @Nested
  class HARequestedWithoutPlugin extends StaticBaseServerTest {
    private static final String DATABASE = "issue8270";

    private ArcadeDBServer server;

    @AfterEach
    @Override
    public void endTest() {
      if (server != null && server.isStarted())
        server.stop();
      server = null;
      super.endTest();
    }

    @Test
    void aStandaloneFallbackLeavesEveryDatabaseWritable() {
      try (final DatabaseFactory factory = new DatabaseFactory("./target/databases0/" + DATABASE);
          final Database database = factory.create()) {
        database.getSchema().createDocumentType("Doc");
      }

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_NAME, "issue8270");
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases0");
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
      configuration.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2424");

      server = new ArcadeDBServer(configuration);
      server.start();

      assertThat(server.isHARequested()).isTrue();
      assertThat(server.getHA()).as("no HA plugin in this module").isNull();

      final ServerDatabase loaded = server.getDatabaseForOperator(DATABASE);
      assertThat(((LocalDatabase) loaded.getWrappedDatabaseInstance()).getWriteRefusal())
          .as("the database opened while the HA plugin was expected accepts writes once startup gave up on it").isNull();
      loaded.transaction(() -> loaded.newDocument("Doc").set("n", 1).save());
      assertThat(loaded.countType("Doc", true)).isEqualTo(1L);

      final ServerDatabase created = server.createDatabase(DATABASE + "_created", ComponentFile.MODE.READ_WRITE);
      assertThat(((LocalDatabase) created.getWrappedDatabaseInstance()).getWriteRefusal()).isNull();
      created.transaction(() -> created.getSchema().createDocumentType("Doc"));
      created.transaction(() -> created.newDocument("Doc").set("n", 1).save());
      assertThat(created.countType("Doc", true)).isEqualTo(1L);
    }
  }

  // Regression: #8281 registerDatabase wraps a caller-supplied database for replication
  /**
   * Issue #8281: {@link ArcadeDBServer#registerDatabase} put a caller-supplied database in the registry exactly as given.
   * Every database the server opens or creates goes through one step (issue #8270) that wraps it for replication when
   * the HA wrapper is installed and refuses writes while that wrapper is still expected; a plain {@link LocalDatabase}
   * registered this way skipped it, and served every protocol committing locally without replication. It now goes
   * through the same step. The replication end to end is {@code HaStaleHandleIT} in ha-raft.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RegisterDatabaseHAWrap {
    private static final String DB = "issue8281";

    @TempDir
    private Path serverDir;

    private ArcadeDBServer      server;
    private final List<LocalDatabase> opened = new ArrayList<>();

    @BeforeEach
    void setUp() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
      server = new ArcadeDBServer(config);
    }

    @AfterEach
    void tearDown() {
      for (final LocalDatabase db : opened)
        if (db.isOpen())
          db.drop();
    }

    @Test
    void aRegisteredDatabaseIsWrappedWhenTheHAWrapperIsInstalled() {
      final LocalDatabase local = newLocal(DB);
      final List<LocalDatabase> wrappedInstances = new ArrayList<>();
      server.setDatabaseWrapper(recordingWrapper(wrappedInstances));

      final ServerDatabase registered = server.registerDatabase(DB, local);

      assertThat(wrappedInstances).as("the registered instance went through the HA wrapper").containsExactly(local);
      assertThat(registered.getWrappedDatabaseInstance()).isNotSameAs(local);
      assertThat(registered.getWrappedDatabaseInstance().getEmbedded()).isSameAs(local);
    }

    @Test
    void aRegisteredDatabaseRefusesWritesWhileTheHAWrapperIsStillExpected() throws Exception {
      final LocalDatabase local = newLocal(DB);
      setAwaitingHAWrapper(true);

      final ServerDatabase registered = server.registerDatabase(DB, local);

      assertThat(registered.getWrappedDatabaseInstance()).isSameAs(local);
      assertThat(local.getWriteRefusal())
          .as("a write here before the HA plugin wraps it would commit on this node only")
          .isNotNull();

      // The HA plugin arrives: the registered database is wrapped like every other one, and writes are accepted again.
      final List<LocalDatabase> wrappedInstances = new ArrayList<>();
      server.setDatabaseWrapper(recordingWrapper(wrappedInstances));
      server.rewrapDatabases();

      assertThat(wrappedInstances).containsExactly(local);
      assertThat(local.getWriteRefusal()).isNull();
      assertThat(server.getDatabaseForOperator(DB).getWrappedDatabaseInstance().getEmbedded()).isSameAs(local);
    }

    @Test
    void aRefusedRegistrationLeavesTheCallersInstanceUntouched() throws Exception {
      server.registerDatabase(DB, newLocal(DB));
      setAwaitingHAWrapper(true);
      final LocalDatabase second = newLocal(DB + "_other");

      assertThatThrownBy(() -> server.registerDatabase(DB, second)).isInstanceOf(IllegalArgumentException.class);
      assertThat(second.getWriteRefusal()).as("the name was taken, so nothing was registered and nothing refused").isNull();
    }

    @Test
    void withoutHighAvailabilityTheInstanceIsRegisteredAsGiven() {
      final LocalDatabase local = newLocal(DB);

      final ServerDatabase registered = server.registerDatabase(DB, local);

      assertThat(registered.getWrappedDatabaseInstance()).isSameAs(local);
      assertThat(local.getWriteRefusal()).isNull();
    }

    private LocalDatabase newLocal(final String name) {
      final LocalDatabase local = (LocalDatabase) new DatabaseFactory(serverDir.resolve(name).toString()).create();
      opened.add(local);
      return local;
    }

    /** Stands in for the Raft wrapper: a distinct instance whose embedded database is the one it was given. */
    private static Function<LocalDatabase, DatabaseInternal> recordingWrapper(final List<LocalDatabase> wrappedInstances) {
      return local -> {
        wrappedInstances.add(local);
        final DatabaseInternal wrapper = mock(DatabaseInternal.class);
        when(wrapper.getEmbedded()).thenReturn(local);
        when(wrapper.isOpen()).thenReturn(true);
        return wrapper;
      };
    }

    /** The state start() puts a server in while it waits for the HA plugin; this test does not start the server. */
    private void setAwaitingHAWrapper(final boolean awaiting) throws Exception {
      final Field field = ArcadeDBServer.class.getDeclaredField("awaitingHAWrapper");
      field.setAccessible(true);
      field.setBoolean(server, awaiting);
    }
  }
}
