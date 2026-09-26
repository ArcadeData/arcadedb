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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8281: {@link ArcadeDBServer#registerDatabase} put a caller-supplied database in the registry exactly as given.
 * Every database the server opens or creates goes through one step (issue #8270) that wraps it for replication when
 * the HA wrapper is installed and refuses writes while that wrapper is still expected; a plain {@link LocalDatabase}
 * registered this way skipped it, and served every protocol committing locally without replication. It now goes
 * through the same step. The replication end to end is {@code Issue8281RegisteredDatabaseReplicatesIT} in ha-raft.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8281RegisterDatabaseHAWrapTest {
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
    assertThat(server.getDatabase(DB).getWrappedDatabaseInstance().getEmbedded()).isSameAs(local);
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
