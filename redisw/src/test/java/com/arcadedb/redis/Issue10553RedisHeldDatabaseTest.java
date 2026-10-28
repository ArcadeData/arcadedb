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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10553, through a real connection: a Redis client that names a database an {@code overwrite} restore holds, or that a
 * {@code close database} closed, is answered {@code LOADING} - the database exists and cannot serve now - and not the generic
 * {@code ERR} of a server fault. Once nothing holds the name the same command goes through.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10553RedisHeldDatabaseTest extends BaseRedisServerTest {
  private static final String HELD = "redisheld10553";

  @Test
  void aCommandOnADatabaseARestoreHoldsIsLoading() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.reserveDatabaseNameForRestore(HELD);
    try (final Jedis jedis = connect()) {
      assertThatThrownBy(() -> jedis.get(HELD + ".k")).isInstanceOf(JedisDataException.class).hasMessageStartingWith("LOADING ");
      assertThat(server.existsDatabase(HELD)).as("the command opened nothing").isFalse();
    } finally {
      server.releaseDatabaseNameReservedForRestore(HELD);
    }

    try (final Jedis jedis = connect()) {
      assertThat(jedis.set(HELD + ".k", "v")).as("a released reservation holds nothing").isEqualTo("OK");
      assertThat(jedis.get(HELD + ".k")).isEqualTo("v");
    } finally {
      dropIfPresent(server);
    }
  }

  @Test
  void aCommandOnADatabaseACloseCommandClosedIsLoading() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.markClosedByCommand(HELD);
    try (final Jedis jedis = connect()) {
      assertThatThrownBy(() -> jedis.get(HELD + ".k")).isInstanceOf(JedisDataException.class).hasMessageStartingWith("LOADING ");
    } finally {
      server.unmarkClosedByCommand(HELD);
      dropIfPresent(server);
    }
  }

  /**
   * A write to {@code <database>.<key>} while a restore holds the database is refused, and does not land in the connection's own
   * bucket: the prefix was taken for part of a plain key because the held database is not registered, so the client was answered
   * {@code OK} and the value was missing from the database once the restore ended.
   */
  @Test
  void aWriteToAHeldDatabaseIsRefusedAndNotKeptAsAPlainKey() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.reserveDatabaseNameForRestore(HELD);
    try (final Jedis jedis = connect()) {
      assertThatThrownBy(() -> jedis.set(HELD + ".k", "v")).isInstanceOf(JedisDataException.class)
          .hasMessageStartingWith("LOADING ");
      assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT, HELD)).isInstanceOf(JedisDataException.class)
          .hasMessageStartingWith("LOADING ");
    } finally {
      server.releaseDatabaseNameReservedForRestore(HELD);
    }

    try (final Jedis jedis = connect()) {
      assertThat(jedis.get(HELD + ".k")).as("the refused write was kept nowhere").isNull();
    } finally {
      dropIfPresent(server);
    }
  }

  /** A held database named by {@code arcadedb.redis.defaultDatabase} is the connection's database all the same: an unprefixed write is refused. */
  @Test
  void aWriteToAHeldConfiguredDefaultDatabaseIsRefusedAndNotKeptAsAPlainKey() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);
    final String previous = server.getConfiguration().getValueAsString(GlobalConfiguration.REDIS_DEFAULT_DATABASE);
    server.getConfiguration().setValue(GlobalConfiguration.REDIS_DEFAULT_DATABASE, HELD);

    server.reserveDatabaseNameForRestore(HELD);
    try (final Jedis jedis = connect()) {
      assertThatThrownBy(() -> jedis.set("k", "v")).isInstanceOf(JedisDataException.class).hasMessageStartingWith("LOADING ");
    } finally {
      server.releaseDatabaseNameReservedForRestore(HELD);
    }

    // Released and opened again (a database on disk that nothing opened yet is the separate case of issue #10567)
    server.getDatabaseForOperator(HELD);
    try (final Jedis jedis = connect()) {
      assertThat(jedis.set("k", "v")).as("released, the same configured default takes the write").isEqualTo("OK");
      assertThat(jedis.get(HELD + ".k")).isEqualTo("v");
    } finally {
      server.getConfiguration().setValue(GlobalConfiguration.REDIS_DEFAULT_DATABASE, previous != null ? previous : "");
      dropIfPresent(server);
    }
  }

  @Test
  void aDottedKeyWhosePrefixIsNoDatabaseStaysAnOrdinaryKey() {
    try (final Jedis jedis = connect()) {
      assertThat(jedis.set("redisnothing10553.k", "v")).isEqualTo("OK");
      assertThat(jedis.get("redisnothing10553.k")).isEqualTo("v");
      assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SELECT, "redisnothing10553")).isInstanceOf(JedisDataException.class)
          .hasMessageContaining("not found");
    }
  }

  /** Leaves {@link #HELD} on disk, closed and deregistered: what an {@code overwrite} restore finds on its target. */
  private static void onDiskNotOpen(final ArcadeDBServer server) {
    final ServerDatabase created = server.createDatabase(HELD, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(HELD);
    assertThat(server.existsDatabase(HELD)).isFalse();
  }

  private static void dropIfPresent(final ArcadeDBServer server) {
    if (server.existsDatabase(HELD))
      server.dropLocalDatabase(server.getDatabaseForOperator(HELD).getEmbedded(), HELD);
    FileUtils.deleteRecursively(
        new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), HELD));
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
