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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.utility.ServerPathUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4849: temporal values nested inside a projected collection must be returned as the
 * configured Java temporal type on the remote client, consistently with scalar projections.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteCollectionTemporalIT {
  @Test
  void temporalElementsInProjectedCollection() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = ServerPathUtils.setRootPath(serverConfiguration);
    serverConfiguration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, rootPath + "/databases");
    serverConfiguration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);

    // The remote client reads the temporal Java implementation from the JVM-global configuration.
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);

    final ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
    arcadeDBServer.start();

    try {
      final Database database = arcadeDBServer.getOrCreateDatabase("remotecolltemporal");
      database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");

      database.command("sql", "CREATE VERTEX TYPE Event IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Event.startedAt DATETIME");
      database.command("sql", "CREATE VERTEX TYPE EventGroup IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE EventGroup_events IF NOT EXISTS");
      database.command("sql", "INSERT INTO EventGroup SET id = 'g1'");
      database.command("sql", "INSERT INTO Event SET id = 'e1', startedAt = '2026-01-02 03:04:05'");
      database.command("sql",
          "CREATE EDGE EventGroup_events FROM (SELECT FROM EventGroup WHERE id = 'g1') TO (SELECT FROM Event WHERE id = 'e1')");

      final int httpPort = arcadeDBServer.getHttpServer().getPort();
      final RemoteDatabase remote = new RemoteDatabase("localhost", httpPort, "remotecolltemporal", "root",
          DEFAULT_PASSWORD_FOR_TESTS);

      // Control: scalar DATETIME projection returns the configured LocalDateTime.
      try (final ResultSet rs = remote.query("sql", "SELECT startedAt FROM Event WHERE id = 'e1'")) {
        final Object startedAt = rs.next().getProperty("startedAt");
        assertThat(startedAt).isInstanceOf(LocalDateTime.class);
        assertThat(startedAt).isEqualTo(LocalDateTime.of(2026, 1, 2, 3, 4, 5));
      }

      // Issue #4849: collection projection must return LocalDateTime elements, not raw Long epoch-millis.
      try (final ResultSet rs = remote.query("sql",
          "SELECT out('EventGroup_events').startedAt AS dates FROM EventGroup WHERE id = 'g1'")) {
        final List<?> dates = rs.next().getProperty("dates");
        assertThat(dates).isNotEmpty();
        assertThat(dates.getFirst()).isInstanceOf(LocalDateTime.class);
        assertThat(dates.getFirst()).isEqualTo(LocalDateTime.of(2026, 1, 2, 3, 4, 5));
      }
    } finally {
      arcadeDBServer.stop();
    }
  }

  @Test
  void temporalValuesInProjectedMap() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = ServerPathUtils.setRootPath(serverConfiguration);
    serverConfiguration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, rootPath + "/databases");
    serverConfiguration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);

    // The remote client reads the temporal Java implementation from the JVM-global configuration.
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);

    final ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
    arcadeDBServer.start();

    try {
      final Database database = arcadeDBServer.getOrCreateDatabase("remotecolltemporal");
      database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");

      database.command("sql", "CREATE DOCUMENT TYPE Rec");
      // Untyped MAP property (no declared ofType): the element type must be inferred at runtime.
      database.command("sql", "CREATE PROPERTY Rec.moments MAP");

      final LocalDateTime first = LocalDateTime.of(2026, 1, 2, 3, 4, 5);
      final LocalDateTime second = LocalDateTime.of(2026, 6, 7, 8, 9, 10);
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Rec");
        final Map<String, Object> moments = Map.of(
            "a", first,
            "b", second);
        doc.set("moments", moments);
        doc.save();
      });

      final int httpPort = arcadeDBServer.getHttpServer().getPort();
      final RemoteDatabase remote = new RemoteDatabase("localhost", httpPort, "remotecolltemporal", "root",
          DEFAULT_PASSWORD_FOR_TESTS);

      // Issue #4849: map projection must return LocalDateTime values, not raw Long epoch-millis.
      try (final ResultSet rs = remote.query("sql", "SELECT moments AS moments FROM Rec")) {
        final Map<String, ?> moments = rs.next().getProperty("moments");
        assertThat(moments).isNotEmpty();
        assertThat(moments.get("a")).isInstanceOf(LocalDateTime.class);
        assertThat(moments.get("a")).isEqualTo(first);
        assertThat(moments.get("b")).isInstanceOf(LocalDateTime.class);
        assertThat(moments.get("b")).isEqualTo(second);
      }
    } finally {
      arcadeDBServer.stop();
    }
  }

  @BeforeEach
  void beginTest() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = ServerPathUtils.setRootPath(serverConfiguration);
    final DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/remotecolltemporal");
    if (databaseFactory.exists())
      databaseFactory.open().drop();
  }

  @AfterEach
  void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
