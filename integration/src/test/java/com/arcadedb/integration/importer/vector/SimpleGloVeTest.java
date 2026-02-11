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
package com.arcadedb.integration.importer.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Example application that downloads the english fast-text word vectors, inserts them into an hnsw index and lets
 * you query them.
 */
public class SimpleGloVeTest {
  private final static int     PARALLEL_LEVEL = 8;
  private static final String  FILE_NAME      = "/Users/frank/Downloads/glove.twitter.27B/glove.twitter.27B.100d.txt";
  private              boolean USE_SQL        = true;

  public static void main(String[] args) {
    new SimpleGloVeTest();
  }

  public SimpleGloVeTest() {

    final DatabaseFactory factory = new DatabaseFactory("databases/glovedb");
    final Database database;

    try {

      database = importDataSet(factory);

      LogManager.instance().log(this, Level.SEVERE, "Found  index with %d entries",
          database.getSchema().getIndexByName("Word[vector]").countEntries());

      querySQL(database);
    } finally {
      factory.close();
    }
    System.exit(0);
  }

  private void querySQL(Database database) {
    final Random random = ThreadLocalRandom.current();
    int k = 128;

    final List<Identifiable> words = new ArrayList<>();
    for (Iterator<Record> it = database.iterateType("Word", true); it.hasNext(); )
      words.add(it.next().getIdentity());

    final int randomWord = random.nextInt(words.size());
    Identifiable input = words.get(randomWord);
    float[] vector = (float[]) input.asDocument().get("vector");

    final long startWord = System.currentTimeMillis();

    final List<Pair<RID, Float>> approximateResults;
    // Convert float[] to Float[] to avoid ClassCastException when passing as query parameter
    Float[] boxedVector = new Float[vector.length];
    for (int i = 0; i < vector.length; i++) {
      boxedVector[i] = vector[i];
    }

    final ResultSet resultSet = database.query("sql", "select vectorNeighbors('Word[vector]', ?,?) as neighbors",
        boxedVector,
        k);
    approximateResults = new ArrayList<>();

    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      final List<Map<String, Object>> neighbors = row.getProperty("neighbors");
      for (Map<String, Object> neighbor : neighbors) {
        ImmutableVertex vertex = (ImmutableVertex) neighbor.get("vertex");
        approximateResults.add(
            new Pair<>(vertex.getIdentity(), (Float) neighbor.get("distance")));
      }

    }
    final Map<String, Float> results = new LinkedHashMap<>();
    for (Pair<RID, Float> result : approximateResults)
      results.put(result.getFirst().asVertex().getString("name"), result.getSecond());

    LogManager.instance()
        .log(this, Level.SEVERE, "Found %d similar words for '%s' in : %s", results.size(), input, results);

  }

  private Database importDataSet(DatabaseFactory factory) {
    long start = System.currentTimeMillis();
    final Database database;
    // TODO: REMOVE THIS
//    if (factory.exists())
//      factory.open().drop();

    if (factory.exists()) {
      database = factory.open();
      LogManager.instance().log(this, Level.SEVERE, "Found existent database with %d words", database.countType("Word", false));
    } else {
      database = factory.create();
      LogManager.instance().log(this, Level.SEVERE, "Creating new database");

      final File file = new File(FILE_NAME);
      if (!file.exists()) {
        LogManager.instance().log(this, Level.SEVERE, "File %s not found\n", FILE_NAME);
        System.exit(1);
      }

      database.command("sql", "import database file://" + file.getAbsolutePath() + " "//
          + "with distanceFunction = 'cosine', m = 16, ef = 128, efConstruction = 128, " //
          + "vertexType = 'Word', vectorProperty = 'vector', vectorType = Float, idProperty = 'name'" //
      );

      long end = System.currentTimeMillis();
      long duration = end - start;

      LogManager.instance().log(this, Level.SEVERE, "Creating index took %d millis which is %d minutes.%n", duration,
          MILLISECONDS.toMinutes(duration));

    }
    return database;
  }
}
