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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

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
          + "with distanceFunction = cosine, m = 16, ef = 128, efConstruction = 128, " //
          + "vertexType = Word, edgeType = Proximity, vectorProperty = vector, idProperty = name" //
      );

      Assertions.assertEquals(10, db.countType("Word", true));

      final float[] key = new float[100];

      ResultSet resultSet = db.query("sql", "select vectorNeighbors('Word[name,vector]', ?,?) as neighbors", key, 10);
      Assertions.assertTrue(resultSet.hasNext());
      final List<Pair<Identifiable, Float>> approximateResults = new ArrayList<>();
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<Map<String, Object>> neighbors = row.getProperty("neighbors");

        for (Map<String, Object> neighbor : neighbors)
          approximateResults.add(new Pair<>((Identifiable) neighbor.get("vertex"), ((Number) neighbor.get("distance")).floatValue()));
      }

    } finally {
      db.drop();
      TestHelper.checkActiveDatabases();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }
}
