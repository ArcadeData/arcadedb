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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.temporal.*;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

/**
 * Tests dates by using server and/or remote connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteDateIT {
  @Test
  public void testDateTimeMicros1() {
    dropDatabase();

    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/test")) {
      if (!databaseFactory.exists()) {
        try (Database db = databaseFactory.create()) {
          db.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");
          db.command("sql", "alter database `arcadedb.dateTimeFormat` \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS\"");
          db.transaction(() -> {
            DocumentType dtOrders = db.getSchema().createDocumentType("Order");
            dtOrders.createProperty("vstart", Type.DATETIME_MICROS);
          });
        }
      }
    }

    serverConfiguration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(serverConfiguration);
    arcadeDBServer.start();

    Database database = arcadeDBServer.getDatabase("test");
    try {
      String sqlString;
      LocalDateTime vstart = LocalDateTime.now();

      database.begin();
      sqlString = "INSERT INTO Order SET vstart = ?";
      try (ResultSet resultSet = database.command("sql", sqlString, vstart)) {
        Assertions.assertEquals(DateUtils.dateTimeToTimestamp(vstart, ChronoUnit.NANOS), new JSONObject(resultSet.next().toJSON()).getLong("vstart"));
      }
      sqlString = "select from Order";
      System.out.println(sqlString);
      Result result = null;
      try (ResultSet resultSet = database.query("sql", sqlString)) {
        result = resultSet.next();
        Assertions.assertEquals(vstart, result.toElement().get("vstart"));
      }
      Assertions.assertNotNull(result);

      database.commit();

      final RemoteDatabase remote = new RemoteDatabase("localhost", 2480, "test", "root", DEFAULT_PASSWORD_FOR_TESTS);

      result = null;
      try (ResultSet resultSet = remote.query("sql", sqlString)) {
        result = resultSet.next();
        Assertions.assertEquals(vstart.toString(), result.toElement().get("vstart"));
      }
      Assertions.assertNotNull(result);

    } finally {
      arcadeDBServer.stop();

      dropDatabase();
    }
  }

  private static void dropDatabase() {
    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/test")) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }
  }
}
