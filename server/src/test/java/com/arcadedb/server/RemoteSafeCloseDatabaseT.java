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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.*;

/**
 * Tests dates by using server and/or remote connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteSafeCloseDatabaseT {

  @Test
  public void testCreateDatabaseThenStartAndStopServer() {
    final String DATABASE_NAME = "test";
    final int PARALLEL_LEVEL = 6;
    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists()) {
        databaseFactory.open().drop();
      }
      try (Database db = databaseFactory.create()) {
        DocumentType dtBatches = db.getSchema().buildDocumentType().withName("Batch").withTotalBuckets(1).create();
        dtBatches.createProperty("id", Type.STRING);
        dtBatches.createProperty("processor", Type.STRING);
        dtBatches.createProperty("start", Type.DATETIME);
        dtBatches.createProperty("stop", Type.DATETIME);
        dtBatches.createProperty("firstOrderId", Type.INTEGER);
        dtBatches.createProperty("lastOrderId", Type.INTEGER);
        dtBatches.createProperty("nCompleted", Type.INTEGER);
        dtBatches.createProperty("nError", Type.INTEGER);
        dtBatches.createProperty("fopStart", Type.DATETIME);
        dtBatches.createProperty("lopStop", Type.DATETIME);
        DocumentType dtErrors = db.getSchema().buildDocumentType().withName("ErrorInfo").withTotalBuckets(1).create();
        dtErrors.createProperty("batch", Type.LINK);
        dtErrors.createProperty("trigger", Type.STRING);
        dtErrors.createProperty("pTask", Type.STRING);
        dtErrors.createProperty("message", Type.STRING);
      }
    }
    String dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS";
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue(dateTimePattern);
    GlobalConfiguration.SERVER_METRICS.setValue(false);
    GlobalConfiguration.HA_ENABLED.setValue(false);
    GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(PARALLEL_LEVEL);
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("CRYO_CSRS");
    ContextConfiguration configuration = new ContextConfiguration();
    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();
    System.out.println();
    arcadeDBServer.stop();
  }

  @AfterEach
  public void endTests() {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
