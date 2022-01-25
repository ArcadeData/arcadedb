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
package com.arcadedb;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultipleDatabasesTest extends TestHelper {
  @Test
  public void testMovingRecordsAcrossDatabases() {
    final DatabaseInternal database2 = (DatabaseInternal) new DatabaseFactory(getDatabasePath() + "2").create();
    final DatabaseInternal database3 = (DatabaseInternal) new DatabaseFactory(getDatabasePath() + "3").create();

    // SCHEMA FIRST
    database.getSchema().createVertexType("V1");
    database.getSchema().createDocumentType("Embedded");
    database2.getSchema().createVertexType("V1");
    database2.getSchema().createDocumentType("Embedded");
    database3.getSchema().createVertexType("V1");
    database3.getSchema().createDocumentType("Embedded");

    // CREATE VERTEX AND DOCUMENT AS EMBEDDED
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V1").set("db", 1).save();
      try {
        v.newEmbeddedDocument("V1", "embedded").set("db", 1).set("embedded", true).save();
        Assertions.fail();
      } catch (IllegalArgumentException e) {
        // EXPECTED
      }
      v.newEmbeddedDocument("Embedded", "embedded").set("db", 1).set("embedded", true).save();
    });

    database2.transaction(() -> {
      final MutableVertex v = database2.newVertex("V1").set("db", 2).save();
      v.newEmbeddedDocument("Embedded", "embedded").set("db", 2).set("embedded", true).save();
    });

    database3.transaction(() -> {
      final MutableVertex v = database3.newVertex("V1").set("db", 3).save();
      v.newEmbeddedDocument("Embedded", "embedded").set("db", 3).set("embedded", true).save();
    });

    // SET THE EMBEDDED DOCUMENTS OF DATABASE 1 AND 2 IN 3
    database3.transaction(() -> {
      MutableVertex v3 = database3.iterateType("V1", true).next().asVertex().modify();
      v3.set("embedded1", database.iterateType("V1", true).next().asVertex().getEmbedded("embedded"));

      v3.set("list2", Arrays.asList(database2.iterateType("V1", true).next().asVertex().getEmbedded("embedded")));
      v3.save();

      final Map<String, EmbeddedDocument> map = new HashMap<>();
      map.put("copied2", database2.iterateType("V1", true).next().asVertex().getEmbedded("embedded"));
      v3.set("map2", map);
      v3.save();
    });

    // CHECK PRESENCE OF RECORDS IN EACH DATABASE
    Assertions.assertEquals(1, database.iterateType("V1", true).next().asVertex().get("db"));
    Assertions.assertEquals(1, database.iterateType("V1", true).next().asVertex().getEmbedded("embedded").get("db"));
    Assertions.assertEquals(2, database2.iterateType("V1", true).next().asVertex().get("db"));
    Assertions.assertEquals(2, database2.iterateType("V1", true).next().asVertex().getEmbedded("embedded").get("db"));
    Assertions.assertEquals(3, database3.iterateType("V1", true).next().asVertex().get("db"));
    Assertions.assertEquals(3, database3.iterateType("V1", true).next().asVertex().getEmbedded("embedded").get("db"));

    // CHECK COPIED RECORDS TOO
    Assertions.assertEquals(1, database3.iterateType("V1", true).next().asVertex().getEmbedded("embedded1").get("db"));
    Assertions.assertEquals(2, ((List<EmbeddedDocument>) database3.iterateType("V1", true).next().asVertex().get("list2")).get(0).get("db"));
    Assertions.assertEquals(2, ((Map<String, EmbeddedDocument>) database3.iterateType("V1", true).next().asVertex().get("map2")).get("copied2").get("db"));

    database.close();
    database2.close();
    database3.close();

    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
  }

  @Test
  public void testErrorMultipleDatabaseInstancesSamePath() {
    try {
      new DatabaseFactory(getDatabasePath()).open();
      Assertions.fail();
    } catch (DatabaseOperationException e) {
      // EXPECTED
    }
  }

  @AfterEach
  @BeforeEach
  public void beforeTest() {
    FileUtils.deleteRecursively(new File(getDatabasePath() + "2"));
    FileUtils.deleteRecursively(new File(getDatabasePath() + "3"));
  }
}
