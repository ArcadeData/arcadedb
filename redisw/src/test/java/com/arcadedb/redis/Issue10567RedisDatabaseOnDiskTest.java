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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10567: {@code <database>.<key>} names a database of the server whether or not it is open. With
 * {@code arcadedb.server.loadAtStartup=false} every database stays closed until its first request, and the prefix of such a
 * database was taken for part of a plain key: the value went to the connection's own bucket, the client was answered {@code OK},
 * and the database never saw it. {@code SELECT} answered "not found" for a database on disk. A directory a snapshot install left
 * pending is a database too, and stays refused; a directory that holds no database is not one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10567RedisDatabaseOnDiskTest extends BaseRedisServerTest {
  private static final String ON_DISK = "redisondisk10567";

  @Test
  void aWriteToADatabaseOnDiskOpensItAndLandsInIt() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);
    try (final Jedis jedis = connect()) {
      assertThat(jedis.set(ON_DISK + ".k", "v")).isEqualTo("OK");
      assertThat(server.existsDatabase(ON_DISK)).as("the command opened the database").isTrue();
      assertThat(server.getDatabase(ON_DISK).getGlobalVariableStore().get("k")).as("the value is the database's").isEqualTo("v");
      assertThat(jedis.get(ON_DISK + ".k")).isEqualTo("v");
    } finally {
      dropIfPresent(server);
    }
  }

  @Test
  void selectOpensADatabaseOnDisk() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);
    try (final Jedis jedis = connect()) {
      assertThat(jedis.sendCommand(Protocol.Command.SELECT, ON_DISK)).isEqualTo("OK".getBytes());
      assertThat(jedis.set("k", "selected")).isEqualTo("OK");
      assertThat(server.getDatabase(ON_DISK).getGlobalVariableStore().get("k")).isEqualTo("selected");
    } finally {
      dropIfPresent(server);
    }
  }

  @Test
  void aDatabaseLeftPendingBySnapshotInstallIsRefusedAndNotKeptAsAPlainKey() throws IOException {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);
    final File marker = new File(databaseDirectory(server), ArcadeDBServer.SNAPSHOT_PENDING_FILE);
    assertThat(marker.createNewFile()).isTrue();
    try (final Jedis jedis = connect()) {
      assertThatThrownBy(() -> jedis.set(ON_DISK + ".k", "v")).isInstanceOf(JedisDataException.class).hasMessageStartingWith("LOADING ");
      assertThat(server.existsDatabase(ON_DISK)).as("a pending marker is a refusal, never an open").isFalse();
    } finally {
      marker.delete();
    }
    try (final Jedis jedis = connect()) {
      assertThat(jedis.get(ON_DISK + ".k")).as("the refused write was kept nowhere").isNull();
    } finally {
      dropIfPresent(server);
    }
  }

  @Test
  void aDirectoryThatHoldsNoDatabaseIsNoDatabase() {
    final File notADatabase = new File(databaseDirectory(getServer(0)).getParentFile(), "redisnotadb10567");
    assertThat(notADatabase.mkdirs()).isTrue();
    try (final Jedis jedis = connect()) {
      assertThat(jedis.set("redisnotadb10567.k", "v")).isEqualTo("OK");
      assertThat(getServer(0).existsDatabase("redisnotadb10567")).isFalse();
      assertThat(new File(notADatabase, "schema.json")).as("no database was created inside it").doesNotExist();
      assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT, "redisnotadb10567")).isInstanceOf(JedisDataException.class)
          .hasMessageContaining("not found");
    } finally {
      FileUtils.deleteRecursively(notADatabase);
    }
  }

  @Test
  void aDroppedDatabaseIsNoDatabaseAnyMore() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);
    try (final Jedis jedis = connect()) {
      assertThat(jedis.set(ON_DISK + ".k", "v")).isEqualTo("OK");
      server.dropLocalDatabase(server.getDatabase(ON_DISK).getEmbedded(), ON_DISK);
      assertThat(jedis.set(ON_DISK + ".k", "plain")).isEqualTo("OK");
      assertThat(server.existsDatabase(ON_DISK)).as("a dropped database is not opened again").isFalse();
      assertThat(jedis.get(ON_DISK + ".k")).isEqualTo("plain");
    } finally {
      dropIfPresent(server);
    }
  }

  /** Leaves {@link #ON_DISK} on disk, closed and deregistered: every database, with loadAtStartup off, before its first request. */
  private static void onDiskNotOpen(final ArcadeDBServer server) {
    final ServerDatabase created = server.createDatabase(ON_DISK, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(ON_DISK);
    assertThat(server.existsDatabase(ON_DISK)).isFalse();
    assertThat(server.isDatabaseHeld(ON_DISK)).isFalse();
  }

  private static File databaseDirectory(final ArcadeDBServer server) {
    return new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), ON_DISK);
  }

  private static void dropIfPresent(final ArcadeDBServer server) {
    if (server.existsDatabase(ON_DISK))
      server.dropLocalDatabase(server.getDatabase(ON_DISK).getEmbedded(), ON_DISK);
    FileUtils.deleteRecursively(databaseDirectory(server));
  }

  private Jedis connect() {
    final Jedis jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    return jedis;
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }
}
