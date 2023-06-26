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
package com.arcadedb.integration.importer;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class Neo4jImporterIT {
  private final static String DATABASE_PATH = "target/databases/neo4j";

  @Test
  public void testImportNeo4jDirectOK() throws IOException {
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final URL inputFile = Neo4jImporterIT.class.getClassLoader().getResource("neo4j-export-mini.jsonl");

      final Neo4jImporter importer = new Neo4jImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o -decimalType double").split(" "));
      importer.run();

      Assertions.assertFalse(importer.isError());

      Assertions.assertTrue(databaseDirectory.exists());

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          final DocumentType personType = database.getSchema().getType("User");
          Assertions.assertNotNull(personType);
          Assertions.assertEquals(3, database.countType("User", true));

          final IndexCursor cursor = database.lookupByKey("User", "id", "0");
          Assertions.assertTrue(cursor.hasNext());
          final Vertex v = cursor.next().asVertex();
          Assertions.assertEquals("Adam", v.get("name"));
          Assertions.assertEquals("2015-07-04T19:32:24", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(v.getLong("born")));

          final Map<String, Object> place = (Map<String, Object>) v.get("place");
          Assertions.assertEquals(33.46789, place.get("latitude"));
          Assertions.assertNull(place.get("height"));

          Assertions.assertEquals(Arrays.asList("Sam", "Anna", "Grace"), v.get("kids"));

          final DocumentType friendType = database.getSchema().getType("KNOWS");
          Assertions.assertNotNull(friendType);
          Assertions.assertEquals(1, database.countType("KNOWS", true));

          final Iterator<Edge> relationships = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          Assertions.assertTrue(relationships.hasNext());
          final Edge e = relationships.next();

          Assertions.assertEquals(1993, e.get("since"));
          Assertions.assertEquals("P5M1DT12H", e.get("bffSince"));
        }
      }
      TestHelper.checkActiveDatabases();
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

  @Test
  public void testImportNoFile() throws IOException {
    final URL inputFile = Neo4jImporterIT.class.getClassLoader().getResource("neo4j-export-mini.jsonl");
    final Neo4jImporter importer = new Neo4jImporter(("-i " + inputFile.getFile() + "2 -d " + DATABASE_PATH + " -o").split(" "));
    try {
      importer.run();
      Assertions.fail("Expected File Not Found Exception");
    } catch (final IllegalArgumentException e) {
    }
    Assertions.assertTrue(importer.isError());
  }

  @Test
  public void testConcurrentCompact() throws IOException, InterruptedException {
    final File databaseDirectory = new File(DATABASE_PATH);

    final int TOTAL = 100_000;
    final int INDEX_PAGE_SIZE = 64 * 1024; // 64K
    final int COMPACTION_RAM_MB = 1; // 1MB

    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(COMPACTION_RAM_MB);
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

    try {
      final StringBuilder content = new StringBuilder();
      for (int i = 0; i < TOTAL; i += 2) {
        content.append("{\"type\":\"node\",\"id\":\"").append(i).append(
            "\",\"labels\":[\"User\"],\"properties\":{\"born\":\"2015-07-04T19:32:24\",\"name\":\"Adam\",\"place\":{\"crs\":\"wgs-84\",\"latitude\":33.46789,\"longitude\":13.1,\"height\":null},\"age\":42,\"male\":true,\"kids\":[\"Sam\",\"Anna\",\"Grace\"]}}\n");
        content.append("{\"type\":\"node\",\"id\":\"").append(i + 1).append("\",\"labels\":[\"User\"],\"properties\":{\"name\":\"Jim\",\"age\":42}}\n");
        content.append(
                "{\"id\":\"0\",\"type\":\"relationship\",\"label\":\"KNOWS\",\"properties\":{\"since\":1993,\"bffSince\":\"P5M1DT12H\"},\"start\":{\"id\":\"")
            .append(i).append("\",\"labels\":[\"User\"]},\"end\":{\"id\":\"").append(i + 1).append("\",\"labels\":[\"User\"]}}\n");
      }

      final ByteArrayInputStream is = new ByteArrayInputStream(content.toString().getBytes());

      final Neo4jImporter importer = new Neo4jImporter(is, (" -d " + DATABASE_PATH + " -o -decimalType double").split(" "));
      importer.bucketsPerType = 1;
      importer.indexPageSize = INDEX_PAGE_SIZE;

      final AtomicReference<Thread> t = new AtomicReference<>();

      final AtomicInteger parsed = new AtomicInteger();
      importer.parsingCallback = iArgument -> {
        parsed.incrementAndGet();

        if (parsed.get() == 70000) {
          LogManager.instance().log(this, Level.SEVERE, "STARTED REQUEST FOR COMPACTION...");

          t.set(new Thread(() -> {
            for (Index idx : importer.database.getSchema().getIndexes()) {
              try {
                ((IndexInternal) idx).scheduleCompaction();
                ((IndexInternal) idx).compact();
              } catch (Exception e) {
                LogManager.instance().log(this, Level.SEVERE, "Error during compaction", e);
              }
            }
          }));
          t.get().start();

//          try {
//            Thread.sleep(10);
//          } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//          }
        }

        return null;
      };

      importer.run();

      t.get().join();

      Assertions.assertFalse(importer.isError());

      Assertions.assertTrue(databaseDirectory.exists());

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          final DocumentType personType = database.getSchema().getType("User");
          Assertions.assertNotNull(personType);
          Assertions.assertEquals(TOTAL, database.countType("User", true));

          final IndexCursor cursor = database.lookupByKey("User", "id", "0");
          Assertions.assertTrue(cursor.hasNext());
          final Vertex v = cursor.next().asVertex();
          Assertions.assertEquals("Adam", v.get("name"));
          Assertions.assertEquals("2015-07-04T19:32:24", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(v.getLong("born")));

          final Map<String, Object> place = (Map<String, Object>) v.get("place");
          Assertions.assertEquals(33.46789, place.get("latitude"));
          Assertions.assertNull(place.get("height"));

          Assertions.assertEquals(Arrays.asList("Sam", "Anna", "Grace"), v.get("kids"));

          final DocumentType friendType = database.getSchema().getType("KNOWS");
          Assertions.assertNotNull(friendType);
          Assertions.assertEquals(TOTAL / 2, database.countType("KNOWS", true));

          final Iterator<Edge> relationships = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          Assertions.assertTrue(relationships.hasNext());
          final Edge e = relationships.next();

          Assertions.assertEquals(1993, e.get("since"));
          Assertions.assertEquals("P5M1DT12H", e.get("bffSince"));
        }
      }
      TestHelper.checkActiveDatabases();
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.reset();
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();

      FileUtils.deleteRecursively(databaseDirectory);
    }
  }
}
