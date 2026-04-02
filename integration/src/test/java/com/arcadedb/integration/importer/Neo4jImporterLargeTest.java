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
import com.arcadedb.integration.TestHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Neo4j importer with a medium-large synthetic dataset to verify memory efficiency
 * and correctness of the GraphBatch-based import.
 *
 * @author Luca Garulli
 */
@Tag("slow")
class Neo4jImporterLargeTest {

  private static final String DATABASE_PATH = "target/databases/neo4j-large-test";
  private static final String EXPORT_FILE   = "target/neo4j-large-export.jsonl";

  // 500K vertices, 1M edges - generates ~300MB JSONL
  private static final int PERSON_COUNT     = 200_000;
  private static final int COMPANY_COUNT    = 50_000;
  private static final int KNOWS_COUNT      = 400_000;
  private static final int WORKS_AT_COUNT   = 200_000;

  @Test
  void importLargeSyntheticDataset() throws Exception {
    final File databaseDirectory = new File(DATABASE_PATH);
    final File exportFile = new File(EXPORT_FILE);

    try {
      // Generate the JSONL file
      generateNeo4jExport(exportFile);
      final long fileSizeMB = exportFile.length() / (1024 * 1024);
      System.out.printf("Generated export file: %,d MB%n", fileSizeMB);

      final Neo4jImporter importer = new Neo4jImporter(
          ("-i " + exportFile.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
      importer.run();

      assertThat(importer.isError()).isFalse();
      assertThat(databaseDirectory.exists()).isTrue();

      // Verify data
      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          // Verify vertex counts
          assertThat(database.countType("Person", false)).isEqualTo(PERSON_COUNT);
          assertThat(database.countType("Company", false)).isEqualTo(COMPANY_COUNT);

          // Verify edge counts
          assertThat(database.countType("KNOWS", true)).isEqualTo(KNOWS_COUNT);
          assertThat(database.countType("WORKS_AT", true)).isEqualTo(WORKS_AT_COUNT);

          // Spot-check a vertex and its edges
          final Vertex v = database.lookupByKey("Person", "id", "0").next().asVertex();
          assertThat(v.get("name")).isEqualTo("Person_0");
          assertThat(v.get("age")).isEqualTo(20);

          // Check that edges are traversable
          final Iterator<Edge> outEdges = v.getEdges(Vertex.DIRECTION.OUT, "KNOWS").iterator();
          assertThat(outEdges.hasNext()).isTrue();
          final Edge e = outEdges.next();
          assertThat(e.get("since")).isNotNull();
        }
      }

      TestHelper.checkActiveDatabases();
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
      exportFile.delete();
    }
  }

  /**
   * Generates a Neo4j APOC JSON export with the specified number of vertices and edges.
   * Uses numeric IDs (like real Neo4j APOC exports): persons 0..PERSON_COUNT-1,
   * companies PERSON_COUNT..PERSON_COUNT+COMPANY_COUNT-1.
   * Format matches: CALL apoc.export.json.all("file.json", {useTypes:true})
   */
  private static void generateNeo4jExport(final File file) throws IOException {
    file.getParentFile().mkdirs();

    try (final BufferedWriter w = new BufferedWriter(new FileWriter(file), 1024 * 1024)) {
      // Person vertices (IDs: 0 .. PERSON_COUNT-1)
      for (int i = 0; i < PERSON_COUNT; i++) {
        w.write("{\"type\":\"node\",\"id\":\"");
        w.write(Integer.toString(i));
        w.write("\",\"labels\":[\"Person\"],\"properties\":{\"name\":\"Person_");
        w.write(Integer.toString(i));
        w.write("\",\"age\":");
        w.write(Integer.toString(20 + (i % 60)));
        w.write(",\"email\":\"person");
        w.write(Integer.toString(i));
        w.write("@example.com\",\"bio\":\"");
        // Add some bulk text to make the file realistically sized
        w.write("This is the biography of person " + i + ". They have many interests and hobbies.");
        w.write("\"}}");
        w.newLine();
      }

      // Company vertices (IDs: PERSON_COUNT .. PERSON_COUNT+COMPANY_COUNT-1)
      for (int i = 0; i < COMPANY_COUNT; i++) {
        final int companyId = PERSON_COUNT + i;
        w.write("{\"type\":\"node\",\"id\":\"");
        w.write(Integer.toString(companyId));
        w.write("\",\"labels\":[\"Company\"],\"properties\":{\"name\":\"Company_");
        w.write(Integer.toString(i));
        w.write("\",\"founded\":");
        w.write(Integer.toString(1950 + (i % 75)));
        w.write(",\"employees\":");
        w.write(Integer.toString(10 + (i % 10000)));
        w.write("}}");
        w.newLine();
      }

      // KNOWS edges (Person -> Person)
      for (int i = 0; i < KNOWS_COUNT; i++) {
        final int from = i % PERSON_COUNT;
        final int to = (i * 7 + 13) % PERSON_COUNT;
        w.write("{\"id\":\"");
        w.write(Integer.toString(i));
        w.write("\",\"type\":\"relationship\",\"label\":\"KNOWS\",\"properties\":{\"since\":");
        w.write(Integer.toString(2000 + (i % 25)));
        w.write("},\"start\":{\"id\":\"");
        w.write(Integer.toString(from));
        w.write("\",\"labels\":[\"Person\"]},\"end\":{\"id\":\"");
        w.write(Integer.toString(to));
        w.write("\",\"labels\":[\"Person\"]}}");
        w.newLine();
      }

      // WORKS_AT edges (Person -> Company)
      for (int i = 0; i < WORKS_AT_COUNT; i++) {
        final int from = i % PERSON_COUNT;
        final int companyId = PERSON_COUNT + (i % COMPANY_COUNT);
        w.write("{\"id\":\"");
        w.write(Integer.toString(KNOWS_COUNT + i));
        w.write("\",\"type\":\"relationship\",\"label\":\"WORKS_AT\",\"properties\":{\"role\":\"Role_");
        w.write(Integer.toString(i % 100));
        w.write("\"},\"start\":{\"id\":\"");
        w.write(Integer.toString(from));
        w.write("\",\"labels\":[\"Person\"]},\"end\":{\"id\":\"");
        w.write(Integer.toString(companyId));
        w.write("\",\"labels\":[\"Company\"]}}");
        w.newLine();
      }
    }
  }
}
