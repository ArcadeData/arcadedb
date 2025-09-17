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
import com.arcadedb.database.Identifiable;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class GloVeImporterIT {
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
          + "with similarityFunction = COSINE, maxConnections = 16, beamWidth = 128, " //
          + "vertexType = Word, edgeType = Proximity, vectorProperty = vector, idProperty = name" //
      );

      assertThat(db.countType("Word", true)).isEqualTo(10);

      // Week 3: Test SQL vectorNeighbors function integration

      // Get a vector from an existing word to use as search key
      ResultSet vectorQuery = db.query("sql", "select vector from Word limit 1");
      assertThat(vectorQuery.hasNext()).isTrue();
      final float[] searchVector = vectorQuery.next().getProperty("vector");
      assertThat(searchVector).isNotNull();
      assertThat(searchVector.length).isEqualTo(100);

      // Test the vectorNeighbors SQL function
      ResultSet neighborsResult = db.query("sql", "select vectorNeighbors('Word[name,vector]', ?, 5) as neighbors", searchVector);
      assertThat(neighborsResult.hasNext()).isTrue();
      final Result result = neighborsResult.next();
      final List<Map<String, Object>> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      System.out.println("SQL vectorNeighbors returned " + neighbors.size() + " results");

      // Week 3 validation: SQL integration working correctly

    } finally {
      db.drop();
      TestHelper.checkActiveDatabases();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }
}
