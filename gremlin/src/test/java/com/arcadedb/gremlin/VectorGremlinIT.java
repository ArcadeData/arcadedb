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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorGremlinIT {
  @Test
  public void importDocuments() {
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

      final float[] vector = new float[100];

      ResultSet resultSet = db.query("sql", "select vectorNeighbors('Word[name,vector]', ?,?) as neighbors", vector, 10);
      assertThat(resultSet.hasNext()).isTrue();
      final List<RID> approximateResults = new ArrayList<>();
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<Map<String, Object>> neighbors = row.getProperty("neighbors");

        for (Map<String, Object> neighbor : neighbors) {
          ImmutableVertex vertex = (ImmutableVertex) neighbor.get("vertex");
          approximateResults.add(vertex.getIdentity());

        }
      }

      assertThat(approximateResults).hasSize(10);

      resultSet = db.query("gremlin",
          "g.call('arcadedb#vectorNeighbors', [ 'indexName': 'Word[name,vector]', 'vector': vector, 'limit': 10 ] )", "vector",
          vector);
      assertThat(resultSet.hasNext()).isTrue();
      final List<RID> approximateResultsFromGremlin = new ArrayList<>();
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<Map<String, Object>> neighbors = row.getProperty("result");

        for (Map<String, Object> neighbor : neighbors) {
          ArcadeVertex vertex = (ArcadeVertex) neighbor.get("vertex");
          approximateResultsFromGremlin.add(vertex.getIdentity());
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
