/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/remotedate");

    try (Database db = databaseFactory.create()) {
      db.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");
      db.command("sql", "alter database `arcadedb.dateTimeFormat` \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS\"");
      db.transaction(() -> {
        DocumentType dtOrders = db.getSchema().createDocumentType("Order");
        dtOrders.createProperty("vstart", Type.DATETIME_MICROS);
      });
    }

    serverConfiguration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
    arcadeDBServer.start();

    Database database = arcadeDBServer.getDatabase("remotedate");
    try {
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
