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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class VectorGremlinIT {
  @Test
  void importDocuments() {
    final String databasePath = "target/databases/test-glove";

    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "import database file://src/test/resources/importer-glove.txt "//
          + "with distanceFunction = cosine, m = 16, ef = 128, efConstruction = 128, " //
          + "vertexType = Word, edgeType = Proximity, vectorProperty = vector, idProperty = name" //
      );

      assertThat(db.countType("Word", true)).isEqualTo(10);

      final float[] vector = {
          0.41344f, -0.40603f, 0.33597f, -0.36816f, -0.83081f, 0.017751f, -0.25325f, 0.70141f, 0.80453f, 1.8683f, 0.090854f,
          -1.0312f, -0.48193f,
          0.37293f, -0.30705f, 0.61617f, -0.040523f, -0.10005f, -0.67102f, -0.075665f, -1.2193f, 0.19714f, -0.85279f, -0.30502f,
          -0.50188f,
          -0.74649f, -0.25207f, -0.28968f, 0.14505f, -0.054608f, -0.44494f, 0.52489f, 1.2581f, 0.33759f, 1.8549f, -0.42854f,
          -0.11269f,
          -0.49523f, 0.29345f, 0.1934f, -2.104f, -0.16325f, 0.18264f, 0.15553f, 0.23637f, 0.0076311f, -1.8425f, -0.75107f, 0.44849f,
          0.2597f, 0.12551f, -0.31736f, -0.53202f, 0.40523f, 0.29191f, 0.12471f, -0.19319f, 0.18022f, 1.4314f, -0.14817f, -0.35196f,
          0.21997f, 0.12066f, -0.14021f, -0.22512f, -0.6414f, -0.20043f, -1.4553f, -0.15688f, 0.47271f, 0.33006f, -0.12094f,
          0.035584f, 0.50094f, 0.39446f, 0.23234f, 0.58614f, 0.027875f, -0.26279f, -0.44492f, 1.1477f, -0.40976f, -0.031541f,
          0.57674f, -0.32274f, -0.54373f, 0.041997f, 0.44756f, 0.070903f, -0.2283f, -0.22984f, 0.29805f, 0.39809f, 0.65451f,
          -0.59439f, 0.75782f, 1.1569f, 0.60621f, -0.75041f, -0.84309f
      };

      ResultSet resultSet = db.query("sql", "select vectorNeighbors('Word[vector]', ?,?) as neighbors", vector, 10);
      assertThat(resultSet.hasNext()).isTrue();
      final List<RID> approximateResults = new ArrayList<>();
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<Map<String, Object>> neighbors = row.getProperty("neighbors");

        for (Map<String, Object> neighbor : neighbors) {
          final Document record = (Document) neighbor.get("record");
          approximateResults.add(record.getIdentity());
        }
      }

      assertThat(approximateResults).hasSize(10);

      resultSet = db.query("gremlin",
          "g.call('arcadedb#vectorNeighbors', params)",
          "params", Map.of("indexName", "Word[vector]", "vector", vector, "limit", 10));
      assertThat(resultSet.hasNext()).isTrue();
      final List<RID> approximateResultsFromGremlin = new ArrayList<>();
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<Map<String, Object>> neighbors = row.getProperty("result");

        for (Map<String, Object> neighbor : neighbors) {
          final ArcadeVertex record = (ArcadeVertex) neighbor.get("record");
          approximateResultsFromGremlin.add(record.getIdentity());
        }
      }

      assertThat(approximateResultsFromGremlin).hasSize(10);

      assertThat(approximateResultsFromGremlin).isEqualTo(approximateResults);

    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }
}
