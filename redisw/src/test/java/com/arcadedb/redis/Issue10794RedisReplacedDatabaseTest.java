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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10794, through one live connection: the server replaces the instance behind a database name - a {@code close database}
 * followed by {@code open database}, a {@code drop database} followed by {@code create database}, a {@code restore backup} with
 * {@code overwrite} - while a Redis client keeps the connection that {@code SELECT}ed it, and so the database context its thread
 * bound to the instance that is gone. Every command after the swap is served by the new instance, through the selection and
 * through a {@code <database>.} prefix alike. A command in the window between the two halves is refused with what the name is at
 * that moment: {@code LOADING} for a database the server holds closed, and for one that no longer exists the {@code ERR} the
 * {@code SELECT} of such a name answers, in the same words (issue #10795), never the engine's sentence naming its directory.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10794RedisReplacedDatabaseTest extends BaseRedisServerTest {
  private static final String NAME = "redisswap10794";

  private ServerControlPlane controlPlane;
  private Jedis              jedis;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @BeforeEach
  void connect() {
    controlPlane = new ServerControlPlane(getServer(0));
    dropIfPresent();
    create();

    jedis = new Jedis("localhost", getServerRedisPort());
    jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
    assertThat(jedis.sendCommand(Protocol.Command.SELECT, NAME)).isNotNull();
    insert(0, 5);
    assertThat(jedis.set("k", "before")).isEqualTo("OK");
  }

  @AfterEach
  void disconnect() {
    try {
      if (jedis != null)
        jedis.close();
    } finally {
      dropIfPresent();
    }
  }

  @Test
  void aDatabaseClosedAndOpenedAgainIsServedAgainOnTheSameConnection() {
    controlPlane.closeDatabase(NAME);

    assertThatThrownBy(() -> jedis.get("k")).isInstanceOf(JedisDataException.class).hasMessageStartingWith("LOADING ");
    assertThatThrownBy(() -> present(0)).isInstanceOf(JedisDataException.class).hasMessageStartingWith("LOADING ");

    controlPlane.openDatabase(NAME);

    assertThat(presentOf(0, 5)).as("the reopened instance, not the closed one").isEqualTo(5);
    insert(5, 2);
    assertThat(presentOf(0, 7)).isEqualTo(7);
    // The selection survives, on the reopened instance
    assertThat(jedis.set("k", "after")).isEqualTo("OK");
    assertThat(jedis.get("k")).isEqualTo("after");
  }

  @Test
  void aDatabaseDroppedAndCreatedAgainIsServedAgainOnTheSameConnection() {
    for (final int rows : new int[] { 7, 9 }) {
      controlPlane.dropDatabase(NAME);

      assertThatThrownBy(() -> jedis.get("k")).isInstanceOf(JedisDataException.class)
          .hasMessage("ERR Database '" + NAME + "' not found");

      create();

      assertThat(jedis.get("k")).as("the selection now names the database created again, which holds nothing").isNull();
      insert(100, rows);
      assertThat(presentOf(0, 5)).as("nothing of the dropped database").isZero();
      assertThat(presentOf(100, rows)).isEqualTo(rows);
    }
  }

  @Test
  void aDatabaseARestoreReplacedIsServedAgainOnTheSameConnection() {
    // THE FILE NAME, RESOLVED UNDER THE SERVER'S BACKUP DIRECTORY
    final String backupFile = new File(controlPlane.triggerBackup(NAME).getString("backupFile")).getName();
    insert(5, 2);
    assertThat(presentOf(0, 7)).isEqualTo(7);

    try {
      controlPlane.restoreBackup(NAME, backupFile, NAME, true, ServerControlPlane.ProgressListener.NOOP);

      assertThat(presentOf(0, 5)).as("the restored copy").isEqualTo(5);
      assertThat(presentOf(5, 2)).as("taken before the last two inserts").isZero();
      insert(5, 1);
      assertThat(presentOf(0, 7)).isEqualTo(6);
      assertThat(jedis.set("k", "after")).isEqualTo("OK");
      assertThat(jedis.get("k")).isEqualTo("after");
    } finally {
      FileUtils.deleteRecursively(controlPlane.resolveBackupFile(NAME, backupFile).getParent().toFile());
    }
  }

  private void create() {
    controlPlane.createDatabase(NAME);
    getServer(0).getDatabaseForOperator(NAME).command("sqlscript", "CREATE DOCUMENT TYPE S; CREATE PROPERTY S.i LONG; CREATE INDEX ON S (i) UNIQUE;");
  }

  private void insert(final int from, final int rows) {
    for (int i = from; i < from + rows; i++)
      assertThat(jedis.hset(NAME, "S", "{'i':" + i + "}")).isEqualTo(1L);
  }

  private boolean present(final int i) {
    return jedis.hexists(NAME + ".S[i]", String.valueOf(i));
  }

  private int presentOf(final int from, final int rows) {
    int present = 0;
    for (int i = from; i < from + rows; i++)
      if (present(i))
        ++present;
    return present;
  }

  private void dropIfPresent() {
    final ArcadeDBServer server = getServer(0);
    if (server.existsDatabase(NAME))
      server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
    server.unmarkClosedByCommand(NAME);
    FileUtils.deleteRecursively(
        new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  @Override
  protected void populateDatabase() {
  }
}
