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

package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.format.*;

import static org.assertj.core.api.Assertions.*;

/**
 * From Issue https://github.com/ArcadeData/arcadedb/issues/839
 */
public class OrderByTest {

  @BeforeEach
  void setUp() {
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.resetAll();
  }

  @Test
  public void testLocalDateTimeOrderBy() {
    DatabaseFactory databaseFactory = new DatabaseFactory("databases/OrderByTest");

    if (databaseFactory.exists())
      databaseFactory.open().drop();

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


    final Database database = databaseFactory.open();

    database.transaction(() -> {
      String name;
      String type;
      LocalDateTime start;
      LocalDateTime stop;
      DateTimeFormatter FILENAME_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
      System.out.println();
      Result result;
      String sqlString = "INSERT INTO Product SET name = ?, type = ?, start = ?, stop = ?";

      try {
        name = "CS_OPER_AUX_ORBDOR_20220318T215523_20220320T002323_F001.EEF";
        type = "AUX_ORBDOR";
        start = LocalDateTime.parse("20220318T215523", FILENAME_TIME_FORMAT);
        stop = LocalDateTime.parse("20220320T002323", FILENAME_TIME_FORMAT);
        Object[] parameters1 = { name, type, start, stop };
        try (ResultSet resultSet = database.command("sql", sqlString, parameters1)) {
          assertThat(resultSet.hasNext()).isTrue();
          result = resultSet.next();
        } catch (Exception e) {
          System.out.println(e.getMessage());
          e.printStackTrace();
        }

        sqlString = "SELECT name, start, stop FROM Product WHERE type = ? AND start <= ? AND stop >= ? ORDER BY start DESC";
        type = "AUX_ORBDOR";
        start = LocalDateTime.parse("2022-03-19T00:26:24.404379", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
        stop = LocalDateTime.parse("2022-03-19T00:28:26.525650", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
        Object[] parameters3 = { type, start, stop };

        LocalDateTime lastStart = null;

        try (ResultSet resultSet = database.query("sql", sqlString, parameters3)) {

          assertThat(resultSet.hasNext()).isTrue();

          while (resultSet.hasNext()) {
            result = resultSet.next();

            start = result.getProperty("start");

            if (lastStart != null) {
              assertThat(start.compareTo(lastStart)).isLessThanOrEqualTo(0)
                  .withFailMessage("" + start + " is greater than " + lastStart);
            }

            lastStart = start;
          }
        }

        lastStart = null;

        sqlString = "SELECT name, start, stop FROM Product WHERE type = ? AND start <= ? AND stop >= ? ORDER BY start ASC";
        try (ResultSet resultSet = database.query("sql", sqlString, parameters3)) {

          assertThat(resultSet.hasNext()).isTrue();

          while (resultSet.hasNext()) {
            result = resultSet.next();

            start = result.getProperty("start");

            if (lastStart != null) {

              assertThat(start.compareTo(lastStart)).isGreaterThanOrEqualTo(0)
                  .withFailMessage("" + start + " is smaller than " + lastStart);
            }

            lastStart = start;
          }
        }
      } catch (Exception e) {
        System.out.println("Error");
        e.printStackTrace();
      }
    });
  }
}
