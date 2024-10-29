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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.*;
import java.time.format.*;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteQueriesIT {

  private final static String         DATABASE_NAME = "remotequeries";
  private              ArcadeDBServer arcadeDBServer;

  @Test
  public void testEdgeDirection() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME);
    databaseFactory.create().close();

    serverConfiguration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    arcadeDBServer = new ArcadeDBServer(serverConfiguration);
    arcadeDBServer.start();

    Database database = arcadeDBServer.getDatabase(DATABASE_NAME);

    //Create FromVtx Type
    database.command("sql", "CREATE VERTEX TYPE FromVtx IF NOT EXISTS");

    //Create ToVtx Type
    database.command("sql", "CREATE VERTEX TYPE ToVtx IF NOT EXISTS");

    //Create ConEdge Type
    database.command("sql", "CREATE EDGE TYPE ConEdge IF NOT EXISTS");

    /*
     * Create the vertices and edges
     * (Fromvtx) ---[ConEdge]---> (ToVtx)
     */
    Vertex fromVtx = database.command("sql", "CREATE VERTEX FromVtx").next().getVertex().get();
    Vertex toVtx = database.command("sql", "CREATE VERTEX ToVtx").next().getVertex().get();
    Edge conEdg = fromVtx.newEdge("ConEdge", toVtx, true);

    assertThat(fromVtx.countEdges(Vertex.DIRECTION.OUT, "ConEdge")).isEqualTo(1);
    assertThat(fromVtx.countEdges(Vertex.DIRECTION.IN, "ConEdge")).isEqualTo(0);
    assertThat(fromVtx.getEdges(Vertex.DIRECTION.OUT, "ConEdge").iterator().hasNext()).isTrue();
    assertThat(fromVtx.getEdges(Vertex.DIRECTION.IN, "ConEdge").iterator().hasNext()).isFalse();
  }

  @Test
  public void testWhereEqualsAfterUpdate() {
    // create database
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    GlobalConfiguration.TX_RETRIES.setValue(0);
    final TypeIndex[] typeIndex = new TypeIndex[1];

    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/" + DATABASE_NAME);

    try (Database db = databaseFactory.create()) {
      db.transaction(() -> {
        DocumentType dtOrders = db.getSchema().createDocumentType("Order");
        dtOrders.createProperty("id", Type.STRING);
        dtOrders.createProperty("processor", Type.STRING);
        dtOrders.createProperty("status", Type.STRING);
        typeIndex[0] = dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
      });
    }

    serverConfiguration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    arcadeDBServer = new ArcadeDBServer(serverConfiguration);
    arcadeDBServer.start();

    Database database = arcadeDBServer.getDatabase(DATABASE_NAME);

    // insert 2 records
    String processor = "SIR1LRM-7.1";
    String status = "PENDING";
    for (int i = 0; i < 2; i++) {
      int id = i + 1;
      database.transaction(() -> {
        String sqlString = "INSERT INTO Order SET id = ?, status = ?, processor = ?";
        try (ResultSet resultSet1 = database.command("sql", sqlString, id, status, processor)) {
          assertThat(resultSet1.next().<String>getProperty("id")).isEqualTo("" + id);
        }
      });
    }
    // update first record
    database.transaction(() -> {
      Object[] parameters2 = { "ERROR", 1 };
      String sqlString = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
      try (ResultSet resultSet1 = database.command("sql", sqlString, parameters2)) {
          assertThat(resultSet1.next().<String>getProperty("id")).isEqualTo("1");
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    });

    //database.command("sql", "rebuild index `" + typeIndex[0].getName() + "`");

    // select records with status = 'PENDING'
    database.transaction(() -> {
      Object[] parameters2 = { "PENDING" };
      String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
      try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
        assertThat(resultSet1.next().<String>getProperty("status")).isEqualTo("PENDING");
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    });
    // drop index
    database.getSchema().dropIndex(typeIndex[0].getName());

    // repeat select records with status = 'PENDING'
    database.transaction(() -> {
      Object[] parameters2 = { "PENDING" };
      String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
      try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
        assertThat(resultSet1.next().<String>getProperty("status")).isEqualTo("PENDING");
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    });
  }

  @Test
  public void testLocalDateTimeOrderBy() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);

    try (DatabaseFactory databaseFactory = new DatabaseFactory(rootPath + "/databases/testLocalDateTimeOrderBy")) {
      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtProduct = db.getSchema().createDocumentType("Product");
          dtProduct.createProperty("name", Type.STRING);
          dtProduct.createProperty("type", Type.STRING);
          dtProduct.createProperty("start", Type.DATETIME_MICROS);
          dtProduct.createProperty("stop", Type.DATETIME_MICROS);
          dtProduct.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
          dtProduct.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "type", "start", "stop");
        });
      }
    }
    ContextConfiguration configuration = new ContextConfiguration();
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(java.time.LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    assertThat(configuration.getValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION) == java.time.LocalDateTime.class).isTrue();

    arcadeDBServer = new ArcadeDBServer(configuration);
    arcadeDBServer.start();
    Database database = arcadeDBServer.getDatabase("testLocalDateTimeOrderBy");
    String name, type;
    LocalDateTime start, stop;
    DateTimeFormatter FILENAME_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
    Result result;
    String sqlString = "INSERT INTO Product SET name = ?, type = ?, start = ?, stop = ?";

    name = "CS_OPER_AUX_ORBDOR_20220318T215523_20220320T002323_F001.EEF";
    type = "AUX_ORBDOR";
    start = LocalDateTime.parse("20220318T215523", FILENAME_TIME_FORMAT);
    stop = LocalDateTime.parse("20220320T002323", FILENAME_TIME_FORMAT);
    Object[] parameters1 = { name, type, start, stop };
    try (ResultSet resultSet = database.command("sql", sqlString, parameters1)) {
      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(start).as("start value retrieved does not match start value inserted").isEqualTo(result.getProperty("start"));
    } catch (Exception e) {
      e.printStackTrace();
    }
    sqlString = "SELECT name, start, stop FROM Product WHERE type = ? AND start <= ? AND stop >= ? ORDER BY start DESC";
    type = "AUX_ORBDOR";
    start = LocalDateTime.parse("2022-03-19T00:26:24.404379", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
    stop = LocalDateTime.parse("2022-03-19T00:28:26.525650", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
    Object[] parameters2 = { type, start, stop };
    try (ResultSet resultSet = database.query("sql", sqlString, parameters2)) {
      assertThat(resultSet.hasNext()).isTrue();
      while (resultSet.hasNext()) {
        result = resultSet.next();
        //Assertions.assertThat(result.getProperty("start").equals(start)).as("start value retrieved does not match start value inserted").isTrue();
      }
    }
  }

  @BeforeEach
  public void beginTests() {
    try (DatabaseFactory databaseFactory = new DatabaseFactory("./databases/" + DATABASE_NAME)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }

    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final String rootPath = IntegrationUtils.setRootPath(serverConfiguration);
    FileUtils.deleteRecursively(new File(rootPath + "/databases"));

    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  public void afterEach() {
    if (arcadeDBServer != null)
      arcadeDBServer.stop();
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }
}
