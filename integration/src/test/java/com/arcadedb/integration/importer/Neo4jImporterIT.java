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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class Neo4jImporterIT {
  private final static String DATABASE_PATH = "target/databases/neo4j";

  @Test
  public void testImportOK() throws IOException {
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final URL inputFile = Neo4jImporterIT.class.getClassLoader().getResource("neo4j-export-mini.jsonl");

      final Neo4jImporter importer = new Neo4jImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o -decimalType double").split(" "));
      importer.run();

      Assertions.assertFalse(importer.isError());

      Assertions.assertTrue(databaseDirectory.exists());

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (Database database = factory.open()) {
          DocumentType personType = database.getSchema().getType("User");
          Assertions.assertNotNull(personType);
          Assertions.assertEquals(3, database.countType("User", true));

          IndexCursor cursor = database.lookupByKey("User", "id", "0");
          Assertions.assertTrue(cursor.hasNext());
          Vertex v = cursor.next().asVertex();
          Assertions.assertEquals("Adam", v.get("name"));
          Assertions.assertEquals("2015-07-04T19:32:24", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(v.getLong("born")));

          Map<String, Object> place = (Map<String, Object>) v.get("place");
          Assertions.assertEquals(33.46789, place.get("latitude"));
          Assertions.assertNull(place.get("height"));

          Assertions.assertEquals(Arrays.asList("Sam", "Anna", "Grace"), v.get("kids"));

          DocumentType friendType = database.getSchema().getType("KNOWS");
          Assertions.assertNotNull(friendType);
          Assertions.assertEquals(1, database.countType("KNOWS", true));

          Iterator<Edge> relationships = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          Assertions.assertTrue(relationships.hasNext());
          Edge e = relationships.next();

          Assertions.assertEquals(1993, e.get("since"));
          Assertions.assertEquals("P5M1DT12H", e.get("bffSince"));
        }
      }
      Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
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
    } catch (IllegalArgumentException e) {
    }
    Assertions.assertTrue(importer.isError());
  }
}
