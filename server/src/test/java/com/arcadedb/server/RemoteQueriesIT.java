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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteQueriesIT {

  @Test
  public void testWhereEqualsAfterUpdate() {
    // create database
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    GlobalConfiguration.TX_RETRIES.setValue(0);
    final TypeIndex[] typeIndex = new TypeIndex[1];
    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/test")) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtOrders = db.getSchema().createDocumentType("Order");
          dtOrders.createProperty("id", Type.STRING);
          dtOrders.createProperty("processor", Type.STRING);
          dtOrders.createProperty("status", Type.STRING);
          typeIndex[0] = dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
        });
      }
    }

    ArcadeDBServer arcadeDBServer = new ArcadeDBServer(new ContextConfiguration());
    arcadeDBServer.start();
    Database database = arcadeDBServer.getDatabase("test");
    System.out.println();
    // insert 2 records
    String processor = "SIR1LRM-7.1";
    String status = "PENDING";
    for (int i = 0; i < 2; i++) {
      int id = i + 1;
      database.transaction(() -> {
        String sqlString = "INSERT INTO Order SET id = ?, status = ?, processor = ?";
        System.out.print(sqlString);
        System.out.println("; parameters: " + id + ", " + processor + ", " + status);
        try (ResultSet resultSet1 = database.command("sql", sqlString, id, status, processor)) {
          System.out.println("result = " + resultSet1.next().toJSON());
        }
      });
    }
    // update first record
    database.transaction(() -> {
      Object[] parameters2 = { "ERROR", 1 };
      String sqlString = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
      System.out.print(sqlString);
      System.out.println(", parameters: " + Arrays.toString(parameters2));
      try (ResultSet resultSet1 = database.command("sql", sqlString, parameters2)) {
        System.out.println("result = " + resultSet1.next().toJSON());
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
      System.out.print(sqlString);
      System.out.println(", parameters: " + Arrays.toString(parameters2));
      try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
        Assertions.assertEquals("PENDING", resultSet1.next().getProperty("status"));
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    });
    // drop index
    database.getSchema().dropIndex(typeIndex[0].getName());

    System.out.println("index dropped");
    // repeat select records with status = 'PENDING'
    database.transaction(() -> {
      Object[] parameters2 = { "PENDING" };
      String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
      System.out.print(sqlString);
      System.out.println(", parameters: " + Arrays.toString(parameters2));
      try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
        Assertions.assertEquals("PENDING", resultSet1.next().getProperty("status"));
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    });
    arcadeDBServer.stop();
  }
}
