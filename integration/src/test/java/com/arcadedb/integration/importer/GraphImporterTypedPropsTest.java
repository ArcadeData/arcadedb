/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link GraphImporter} with DOUBLE, LONG, and DATETIME property types,
 * both via the programmatic API and via JSON configuration.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterTypedPropsTest {

  private static final String DB_PATH = "target/databases/graph-importer-typed-props-test";
  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null)
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void importDoubleLongDatetimeViaApi() throws Exception {
    final String resourceDir = new File("src/test/resources").getAbsolutePath();

    database.transaction(() -> database.getSchema().createVertexType("Place"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Place", new CsvRowSource(resourceDir + "/importer-typed-props.csv"), v -> {
          v.id("Id");
          v.property("name", "Name");
          v.doubleProperty("lat", "Latitude");
          v.doubleProperty("lng", "Longitude");
          v.longProperty("score", "Score");
          v.datetimeProperty("timestamp", "Timestamp");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Place WHERE name = 'Alpha'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();

        // Double properties
        assertThat(v.getDouble("lat")).isEqualTo(37.7749);
        assertThat(v.getDouble("lng")).isEqualTo(-122.4194);

        // Long property
        assertThat(v.getLong("score")).isEqualTo(98765432100L);

        // Datetime property
        final LocalDateTime ts = v.getLocalDateTime("timestamp");
        assertThat(ts).isEqualTo(LocalDateTime.of(2023, 1, 15, 8, 30, 0));
      }
    });

    // Verify all 3 records imported with correct types
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Place WHERE name = 'Gamma'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        assertThat(v.getDouble("lat")).isEqualTo(51.5074);
        assertThat(v.getLong("score")).isEqualTo(98765432300L);
        assertThat(v.getLocalDateTime("timestamp")).isEqualTo(LocalDateTime.of(2023, 12, 1, 23, 59, 59));
      }
    });
  }

  @Test
  void importDoubleLongDatetimeViaJson() throws Exception {
    final String resourceDir = new File("src/test/resources").getAbsolutePath();

    final String json = """
        {
          "vertices": [
            {
              "type": "Place",
              "file": "importer-typed-props.csv",
              "id": "Id",
              "properties": {
                "name": "Name",
                "lat": "double:Latitude",
                "lng": "double:Longitude",
                "score": "long:Score",
                "timestamp": "datetime:Timestamp"
              }
            }
          ]
        }
        """;

    final JSONObject config = new JSONObject(json);
    GraphImporter.createSchemaFromConfig(database, config);

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, resourceDir)) {
      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Place WHERE name = 'Beta'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();

        assertThat(v.getDouble("lat")).isEqualTo(40.7128);
        assertThat(v.getDouble("lng")).isEqualTo(-74.0060);
        assertThat(v.getLong("score")).isEqualTo(98765432200L);
        assertThat(v.getLocalDateTime("timestamp")).isEqualTo(LocalDateTime.of(2023, 6, 20, 14, 45, 30));
      }
    });
  }
}
