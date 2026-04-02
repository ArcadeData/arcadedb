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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Neo4jImporterIT {
  private final static String DATABASE_PATH = "target/databases/neo4j-imported";

  @Test
  void importNeo4jDirectOK() throws Exception {
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final URL inputFile = Neo4jImporterIT.class.getClassLoader().getResource("neo4j-export-mini.jsonl");

      final Neo4jImporter importer = new Neo4jImporter(
          ("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o -decimalType double").split(" "));
      importer.run();

      assertThat(importer.isError()).isFalse();

      assertThat(databaseDirectory.exists()).isTrue();

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          final DocumentType personType = database.getSchema().getType("User");
          assertThat(personType).isNotNull();
          assertThat(database.countType("User", true)).isEqualTo(3);

          final IndexCursor cursor = database.lookupByKey("User", "id", "0");
          assertThat(cursor.hasNext()).isTrue();
          final Vertex v = cursor.next().asVertex();
          assertThat(v.get("name")).isEqualTo("Adam");
          assertThat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(v.getLong("born"))).isEqualTo("2015-07-04T19:32:24");

          final Map<String, Object> place = (Map<String, Object>) v.get("place");
          assertThat(place.get("latitude")).isEqualTo(33.46789);
          assertThat(place.get("height")).isNull();

          assertThat(v.get("kids")).isEqualTo(Arrays.asList("Sam", "Anna", "Grace"));

          final DocumentType friendType = database.getSchema().getType("KNOWS");
          assertThat(friendType).isNotNull();
          assertThat(database.countType("KNOWS", true)).isEqualTo(1);

          final Iterator<Edge> relationships = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          assertThat(relationships.hasNext()).isTrue();
          final Edge e = relationships.next();

          assertThat(e.get("since")).isEqualTo(1993);
          assertThat(e.get("bffSince")).isEqualTo("P5M1DT12H");
        }
      }
      TestHelper.checkActiveDatabases();
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

  @Test
  void importNoFile() throws Exception {
    final URL inputFile = Neo4jImporterIT.class.getClassLoader().getResource("neo4j-export-mini.jsonl");
    final Neo4jImporter importer = new Neo4jImporter(("-i " + inputFile.getFile() + "2 -d " + DATABASE_PATH + " -o").split(" "));
    assertThatThrownBy(() -> importer.run()).isInstanceOf(IllegalArgumentException.class);
    assertThat(importer.isError()).isTrue();
  }

  @Test
  void concurrentCompact() throws Exception {
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
        content.append("{\"type\":\"node\",\"id\":\"").append(i + 1)
            .append("\",\"labels\":[\"User\"],\"properties\":{\"name\":\"Jim\",\"age\":42}}\n");
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

      assertThat(importer.isError()).isFalse();

      assertThat(databaseDirectory.exists()).isTrue();

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          final DocumentType personType = database.getSchema().getType("User");
          assertThat(personType).isNotNull();
          assertThat(database.countType("User", true)).isEqualTo(TOTAL);

          final IndexCursor cursor = database.lookupByKey("User", "id", "0");
          assertThat(cursor.hasNext()).isTrue();
          final Vertex v = cursor.next().asVertex();
          assertThat(v.get("name")).isEqualTo("Adam");
          assertThat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(v.getLong("born"))).isEqualTo("2015-07-04T19:32:24");

          final Map<String, Object> place = (Map<String, Object>) v.get("place");
          assertThat(place.get("latitude")).isEqualTo(33.46789);
          assertThat(place.get("height")).isNull();

          assertThat(v.get("kids")).isEqualTo(Arrays.asList("Sam", "Anna", "Grace"));

          final DocumentType friendType = database.getSchema().getType("KNOWS");
          assertThat(friendType).isNotNull();
          assertThat(database.countType("KNOWS", true)).isEqualTo(TOTAL / 2);

          final Iterator<Edge> relationships = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          assertThat(relationships.hasNext()).isTrue();
          final Edge e = relationships.next();

          assertThat(e.get("since")).isEqualTo(1993);
          assertThat(e.get("bffSince")).isEqualTo("P5M1DT12H");
        }
      }
      TestHelper.checkActiveDatabases();
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.reset();
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();

      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

  @Test
  void importNeo4jMixedIds() throws Exception {
    // Starts with numeric IDs, then switches to string IDs mid-import to test migration
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final StringBuilder content = new StringBuilder();
      // Numeric IDs first
      content.append("{\"type\":\"node\",\"id\":\"0\",\"labels\":[\"Person\"],\"properties\":{\"name\":\"Alice\"}}\n");
      content.append("{\"type\":\"node\",\"id\":\"1\",\"labels\":[\"Person\"],\"properties\":{\"name\":\"Bob\"}}\n");
      // Non-numeric ID triggers migration to string mode
      content.append("{\"type\":\"node\",\"id\":\"node-abc\",\"labels\":[\"Person\"],\"properties\":{\"name\":\"Charlie\"}}\n");
      content.append("{\"type\":\"node\",\"id\":\"node-def\",\"labels\":[\"Person\"],\"properties\":{\"name\":\"Diana\"}}\n");
      // Edges: numeric-to-numeric, numeric-to-string, string-to-string
      content.append("{\"id\":\"r0\",\"type\":\"relationship\",\"label\":\"KNOWS\",\"properties\":{\"since\":2020},");
      content.append("\"start\":{\"id\":\"0\",\"labels\":[\"Person\"]},\"end\":{\"id\":\"1\",\"labels\":[\"Person\"]}}\n");
      content.append("{\"id\":\"r1\",\"type\":\"relationship\",\"label\":\"KNOWS\",\"properties\":{\"since\":2021},");
      content.append("\"start\":{\"id\":\"1\",\"labels\":[\"Person\"]},\"end\":{\"id\":\"node-abc\",\"labels\":[\"Person\"]}}\n");
      content.append("{\"id\":\"r2\",\"type\":\"relationship\",\"label\":\"KNOWS\",\"properties\":{\"since\":2022},");
      content.append("\"start\":{\"id\":\"node-abc\",\"labels\":[\"Person\"]},\"end\":{\"id\":\"node-def\",\"labels\":[\"Person\"]}}\n");

      final ByteArrayInputStream is = new ByteArrayInputStream(content.toString().getBytes());
      final Neo4jImporter importer = new Neo4jImporter(is, (" -d " + DATABASE_PATH + " -o").split(" "));
      importer.run();

      assertThat(importer.isError()).isFalse();

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          assertThat(database.countType("Person", true)).isEqualTo(4);
          assertThat(database.countType("KNOWS", true)).isEqualTo(3);

          // Verify all vertices are accessible
          assertThat(database.lookupByKey("Person", "id", "0").next().asVertex().get("name")).isEqualTo("Alice");
          assertThat(database.lookupByKey("Person", "id", "1").next().asVertex().get("name")).isEqualTo("Bob");
          assertThat(database.lookupByKey("Person", "id", "node-abc").next().asVertex().get("name")).isEqualTo("Charlie");
          assertThat(database.lookupByKey("Person", "id", "node-def").next().asVertex().get("name")).isEqualTo("Diana");

          // Verify edges are traversable across the migration boundary
          final Vertex alice = database.lookupByKey("Person", "id", "0").next().asVertex();
          final Iterator<Edge> aliceEdges = alice.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          assertThat(aliceEdges.hasNext()).isTrue();
          assertThat(aliceEdges.next().get("since")).isEqualTo(2020);

          final Vertex bob = database.lookupByKey("Person", "id", "1").next().asVertex();
          final Iterator<Edge> bobEdges = bob.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          assertThat(bobEdges.hasNext()).isTrue();
          assertThat(bobEdges.next().get("since")).isEqualTo(2021);

          final Vertex charlie = database.lookupByKey("Person", "id", "node-abc").next().asVertex();
          final Iterator<Edge> charlieEdges = charlie.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          assertThat(charlieEdges.hasNext()).isTrue();
          assertThat(charlieEdges.next().get("since")).isEqualTo(2022);
        }
      }
      TestHelper.checkActiveDatabases();
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

  @Test
  void importNeo4jAllStringIds() throws Exception {
    // All non-numeric IDs - goes directly to string mode on the first vertex
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final StringBuilder content = new StringBuilder();
      content.append("{\"type\":\"node\",\"id\":\"user-alice\",\"labels\":[\"Person\"],\"properties\":{\"name\":\"Alice\"}}\n");
      content.append("{\"type\":\"node\",\"id\":\"user-bob\",\"labels\":[\"Person\"],\"properties\":{\"name\":\"Bob\"}}\n");
      content.append("{\"type\":\"node\",\"id\":\"company-acme\",\"labels\":[\"Company\"],\"properties\":{\"name\":\"Acme\"}}\n");
      content.append("{\"id\":\"rel-1\",\"type\":\"relationship\",\"label\":\"KNOWS\",\"properties\":{\"since\":2019},");
      content.append("\"start\":{\"id\":\"user-alice\",\"labels\":[\"Person\"]},\"end\":{\"id\":\"user-bob\",\"labels\":[\"Person\"]}}\n");
      content.append("{\"id\":\"rel-2\",\"type\":\"relationship\",\"label\":\"WORKS_AT\",\"properties\":{},");
      content.append("\"start\":{\"id\":\"user-alice\",\"labels\":[\"Person\"]},\"end\":{\"id\":\"company-acme\",\"labels\":[\"Company\"]}}\n");

      final ByteArrayInputStream is = new ByteArrayInputStream(content.toString().getBytes());
      final Neo4jImporter importer = new Neo4jImporter(is, (" -d " + DATABASE_PATH + " -o").split(" "));
      importer.run();

      assertThat(importer.isError()).isFalse();

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          assertThat(database.countType("Person", true)).isEqualTo(2);
          assertThat(database.countType("Company", true)).isEqualTo(1);
          assertThat(database.countType("KNOWS", true)).isEqualTo(1);
          assertThat(database.countType("WORKS_AT", true)).isEqualTo(1);

          final Vertex alice = database.lookupByKey("Person", "id", "user-alice").next().asVertex();
          assertThat(alice.get("name")).isEqualTo("Alice");

          final Iterator<Edge> knowsEdges = alice.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          assertThat(knowsEdges.hasNext()).isTrue();
          assertThat(knowsEdges.next().get("since")).isEqualTo(2019);

          final Iterator<Edge> worksAtEdges = alice.getEdges(Vertex.DIRECTION.OUT, "WORKS_AT").iterator();
          assertThat(worksAtEdges.hasNext()).isTrue();
        }
      }
      TestHelper.checkActiveDatabases();
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

  @Test
  void importNeo4jMultiTypes() throws Exception {
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final URL inputFile = Neo4jImporterIT.class.getClassLoader().getResource("neo4j-export-multitypes.jsonl");

      final Neo4jImporter importer = new Neo4jImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH).split(" "));
      importer.run();

      assertThat(importer.isError()).isFalse();

      assertThat(databaseDirectory.exists()).isTrue();

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          final DocumentType placeType = database.getSchema().getType("Place");
          assertThat(placeType).isNotNull();
          assertThat(database.countType("Place", true)).isEqualTo(1);

          final DocumentType cityType = database.getSchema().getType("City");
          assertThat(cityType).isNotNull();
          assertThat(database.countType("City", true)).isEqualTo(1);

          IndexCursor cursor = database.lookupByKey("City~Place", "id", "0");
          assertThat(cursor.hasNext()).isTrue();
          Vertex v = cursor.next().asVertex();
          assertThat(v.get("name")).isEqualTo("Test");
        }
      }
      TestHelper.checkActiveDatabases();
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

}
