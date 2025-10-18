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
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.temporal.*;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests dates by using server and/or remote connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteDateIT {
  @Test
  public void testDateTimeMicros1() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    // Set the database directory explicitly
    final String databaseDir = rootPath + "/databases";
    serverConfiguration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databaseDir);

    // Set server configuration BEFORE creating the server
    serverConfiguration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);

    // Start the server first so security is properly initialized
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
    arcadeDBServer.start();

    // Create and configure the database through the server
    Database database = arcadeDBServer.getOrCreateDatabase("remotedate");
    try {
      // Configure datetime settings
      database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");
      database.command("sql", "alter database `arcadedb.dateTimeFormat` \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS\"");

      // Create schema
      database.transaction(() -> {
        if (!database.getSchema().existsType("Order")) {
          DocumentType dtOrders = database.getSchema().createDocumentType("Order");
          dtOrders.createProperty("vstart", Type.DATETIME_MICROS);
        }
      });

      LocalDateTime vstart = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);

      database.begin();
      try (ResultSet resultSet = database.command("sql",  "INSERT INTO Order SET vstart = ?", vstart)) {
        assertThat(resultSet.next().toJSON().getLong("vstart")).isEqualTo(DateUtils.dateTimeToTimestamp(vstart, ChronoUnit.MICROS));
      }
      try (ResultSet resultSet = database.query("sql", "select from Order")) {
        Result result = resultSet.next();
        assertThat(result.toElement().get("vstart")).isEqualTo(vstart);
      }

      database.commit();

      // Now the remote connection will work because the database has proper security setup
      final RemoteDatabase remote = new RemoteDatabase("localhost", 2480, "remotedate", "root", DEFAULT_PASSWORD_FOR_TESTS);

      try (ResultSet resultSet = remote.query("sql", "select from Order")) {
        Result result = resultSet.next();
        assertThat(result.toElement().get("vstart")).isEqualTo(vstart.toString());
      }

    } finally {
      arcadeDBServer.stop();
    }
  }

  @BeforeEach
  public void beginTest() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);
    DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/remotedate");
    if (databaseFactory.exists())
      databaseFactory.open().drop();
  }

  @AfterEach
  public void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
