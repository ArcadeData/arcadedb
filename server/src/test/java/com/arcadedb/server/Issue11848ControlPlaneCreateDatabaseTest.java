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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseOperationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11848: the cluster-wide create of {@link ServerControlPlane} used to create the database on this node and only then
 * replicate it, so a replication that never committed left a database on this node that no other node had. With high
 * availability active the create now belongs to {@link HAServerPlugin#createDatabaseInCluster}, whose apply creates the
 * database on every node, and the control plane creates nothing itself.
 * <p>
 * Like {@code Issue7401ServerControlPlaneConnectClusterTest} this boots a real single-node {@link ArcadeDBServer}, which has
 * no HA implementation of its own because the {@code server} module does not depend on {@code arcadedb-ha-raft}, and
 * supplies the HA behaviour through a hand-written plugin. The Raft side of the contract (an entry that never commits leaves
 * no database on any node) is {@code Issue11848CreateDatabaseUncommittedIT} in that module.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11848ControlPlaneCreateDatabaseTest extends StaticBaseServerTest {

  private static final String NAME = "created11848";

  private ArcadeDBServer     server;
  private ServerControlPlane controlPlane;

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

    server = new ArcadeDBServer(config);
    server.start();
    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
    assertThat(server.getHA()).isNull();

    controlPlane = new ServerControlPlane(server);
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      if (server != null && server.isStarted()) {
        // The stub only exists to answer the create: dropping through it would be asked to replicate a drop it knows nothing of
        server.setHA(null);
        if (server.existsDatabase(NAME))
          controlPlane.dropDatabase(NAME);
        server.stop();
      }
    } finally {
      super.endTest();
    }
  }

  /**
   * The control plane asks the HA implementation first and creates nothing before it answers: a database it had created by
   * then is the copy that nothing removes when the entry never commits.
   */
  @Test
  void theControlPlaneCreatesNothingBeforeTheHaImplementationDid() {
    final CreatingHAPlugin ha = new CreatingHAPlugin();
    server.setHA(ha);

    final ServerDatabase created = controlPlane.createDatabase(NAME);

    assertThat(ha.asked).containsExactly(NAME);
    assertThat(ha.existedWhenAsked).as("the database existed when the HA implementation was asked").isFalse();
    assertThat(created.getName()).isEqualTo(NAME);
    assertThat(server.existsDatabase(NAME)).isTrue();
  }

  /**
   * The apply of the entry can fail on this node (it quarantines the database) or a drop can follow it before the caller looks.
   * Either way the create is not reported as done: the caller would get a handle to nothing.
   */
  @Test
  void aCreateWhoseApplyRegisteredNothingOnThisServerIsReportedAsAFailure() {
    final HAServerPlugin ha = new InertHAPlugin() {
      @Override
      public boolean createDatabaseInCluster(final String databaseName) {
        asked.add(databaseName);
        return true;
      }
    };
    server.setHA(ha);

    assertThatThrownBy(() -> controlPlane.createDatabase(NAME)).isInstanceOf(DatabaseOperationException.class)
        .hasMessageContaining("'" + NAME + "'").hasMessageContaining("has not registered it");

    assertNoDatabase();
  }

  /** A name the HA implementation refuses reaches the caller as it is and leaves nothing on this server. */
  @Test
  void aRefusalOfTheHaImplementationReachesTheCallerAndLeavesNothingBehind() {
    final HAServerPlugin ha = new InertHAPlugin() {
      @Override
      public boolean createDatabaseInCluster(final String databaseName) {
        throw new DatabaseAlreadyExistsException("Database '" + databaseName + "' already exists");
      }
    };
    server.setHA(ha);

    assertThatThrownBy(() -> controlPlane.createDatabase(NAME)).isInstanceOf(DatabaseAlreadyExistsException.class)
        .hasMessageContaining(NAME);

    assertNoDatabase();
  }

  /**
   * An HA implementation that replicates no database creation keeps the interface default, so the create stays local, as it
   * is without any HA implementation.
   */
  @Test
  void anHaImplementationThatReplicatesNoCreationLeavesTheLocalCreate() {
    final InertHAPlugin ha = new InertHAPlugin();
    server.setHA(ha);

    final ServerDatabase created = controlPlane.createDatabase(NAME);

    assertThat(created.getName()).isEqualTo(NAME);
    assertThat(server.existsDatabase(NAME)).isTrue();
    assertThat(ha.asked).as("the default is answered without recording anything").isEmpty();
    assertThat(ha.createDatabaseInCluster(NAME)).as("the interface default").isFalse();
  }

  @Test
  void withoutHaTheDatabaseIsCreatedLocally() {
    final ServerDatabase created = controlPlane.createDatabase(NAME);

    assertThat(created.getName()).isEqualTo(NAME);
    assertThat(server.existsDatabase(NAME)).isTrue();
  }

  /** The check does not create: the name is free afterwards, which is what lets the apply create it. */
  @Test
  void aFreeNameIsAcceptedAndNothingIsCreated() {
    assertThatCode(() -> server.checkDatabaseCanBeCreated(NAME)).doesNotThrowAnyException();

    assertNoDatabase();
  }

  /** The refusal of the check is the refusal of the create it stands for, so a caller is told the same either way. */
  @Test
  void aRegisteredNameIsRefusedAsACreateWouldRefuseIt() {
    server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);

    assertThatThrownBy(() -> server.checkDatabaseCanBeCreated(NAME)).isInstanceOf(DatabaseAlreadyExistsException.class)
        .hasMessage("Database '" + NAME + "' already exists");
    assertThatThrownBy(() -> server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE)).isInstanceOf(
        DatabaseAlreadyExistsException.class).hasMessage("Database '" + NAME + "' already exists");
  }

  /** A database on disk the registry does not list (closed for idleness, not loaded yet) is as taken as a registered one. */
  @Test
  void aNameWhoseFilesAreOnDiskButNotRegisteredIsRefused() {
    final String path = databaseDirectory() + File.separator + NAME;
    final Database onDisk = new DatabaseFactory(path).create();
    try {
      assertThat(server.existsDatabase(NAME)).isFalse();

      assertThatThrownBy(() -> server.checkDatabaseCanBeCreated(NAME)).isInstanceOf(DatabaseAlreadyExistsException.class)
          .hasMessage("Database '" + NAME + "' already exists");
      assertThatThrownBy(() -> server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE)).isInstanceOf(
          DatabaseAlreadyExistsException.class);
    } finally {
      onDisk.drop();
    }
  }

  /** The name is validated before any path is built from it. */
  @Test
  void aNameThatLeavesTheDatabaseDirectoryIsRefused() {
    assertThatThrownBy(() -> server.checkDatabaseCanBeCreated("../" + NAME)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.checkDatabaseCanBeCreated(" ")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.checkDatabaseCanBeCreated(null)).isInstanceOf(IllegalArgumentException.class);
  }

  private void assertNoDatabase() {
    assertThat(server.existsDatabase(NAME)).as("'%s' is registered", NAME).isFalse();
    assertThat(server.getAvailableDatabaseNames()).as("'%s' is listed", NAME).doesNotContain(NAME);
    assertThat(new File(databaseDirectory(), NAME)).as("the directory of '%s'", NAME).doesNotExist();
  }

  private String databaseDirectory() {
    return server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
  }

  /** Answers the create the way the Raft plugin does: the apply of the entry creates the database, here. */
  private class CreatingHAPlugin extends InertHAPlugin {
    private boolean existedWhenAsked = true;

    @Override
    public boolean createDatabaseInCluster(final String databaseName) {
      asked.add(databaseName);
      existedWhenAsked = server.existsDatabase(databaseName);
      server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
      return true;
    }
  }

  /** An {@link HAServerPlugin} with nothing to do, that records the names it was asked to create. */
  private static class InertHAPlugin implements HAServerPlugin {
    final List<String> asked = new ArrayList<>();

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public void startService() {
    }

    @Override
    public boolean isLeader() {
      return true;
    }

    @Override
    public String getLeaderName() {
      return null;
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
      return 1;
    }

    @Override
    public String getLeaderAddress() {
      return null;
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
}
