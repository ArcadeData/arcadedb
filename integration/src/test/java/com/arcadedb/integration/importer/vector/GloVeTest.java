/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.integration.importer.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.index.vector.HnswVectorIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Example application that downloads the english fast-text word vectors, inserts them into an hnsw index and lets
 * you query them.
 */
public class GloVeTest {
  private final static int     PARALLEL_LEVEL = 8;
  private static final String  FILE_NAME      = "/Users/luca/Downloads/glove.twitter.27B.100d.txt";
  private              boolean USE_SQL        = false;

  public static void main(String[] args) {
    new GloVeTest();
  }

  public GloVeTest() {
    final long start = System.currentTimeMillis();

    final Database database;

    final DatabaseFactory factory = new DatabaseFactory("glovedb");

    // TODO: REMOVE THIS
//    if (factory.exists())
//      factory.open().drop();

    if (factory.exists()) {
      database = factory.open();
      //LogManager.instance().log(this, Level.SEVERE, "Found existent database with %d words", database.countType("Word", false));

    } else {
      database = factory.create();
      LogManager.instance().log(this, Level.SEVERE, "Creating new database");
      final VertexType vType = database.getSchema().createVertexType("Word");
      vType.getOrCreateProperty("name", Type.STRING);

      final File file = new File(FILE_NAME);
      if (!file.exists()) {
        LogManager.instance().log(this, Level.SEVERE, "File %s not found\n", FILE_NAME);
        System.exit(1);
      }

      database.command("sql", "import database file://" + file.getAbsolutePath() + " "//
          + "with distanceFunction = 'cosine', m = 16, ef = 128, efConstruction = 128, " //
          + "vertexType = 'Word', edgeType = 'Proximity', vectorProperty = 'vector', idProperty = 'name'" //
      );

      long end = System.currentTimeMillis();
      long duration = end - start;

      LogManager.instance().log(this, Level.SEVERE, "Creating index with took %d millis which is %d minutes.%n", duration, MILLISECONDS.toMinutes(duration));

      database.close();
      System.exit(1);
    }

    final HnswVectorIndex persistentIndex = (HnswVectorIndex) database.getSchema().getIndexByName("Word[name,vector]");

    try {

      int k = 128;

      final Random random = new Random();

      final ExecutorService executor = new ThreadPoolExecutor(PARALLEL_LEVEL, PARALLEL_LEVEL, 5, TimeUnit.SECONDS, new SynchronousQueue(),
          new ThreadPoolExecutor.CallerRunsPolicy());

      final AtomicLong totalSearchTime = new AtomicLong();
      final AtomicInteger totalHits = new AtomicInteger();
      final long begin = System.currentTimeMillis();
      final AtomicLong lastStats = new AtomicLong();

      final List<String> words = new ArrayList<>();
      for (Iterator<Record> it = database.iterateType("Word", true); it.hasNext(); )
        words.add(it.next().asVertex().getString("name"));

      for (int cycle = 0; ; ++cycle) {
        final int currentCycle = cycle;

        executor.submit(() -> {
          try {
            final int randomWord = random.nextInt(words.size());
            String input = words.get(randomWord);

            final long startWord = System.currentTimeMillis();

            final List<Pair<Identifiable, Float>> approximateResults;
            if (USE_SQL) {
              final ResultSet resultSet = database.query("sql", "select vectorNeighbors('Word[name,vector]', ?,?) as neighbors", input, k);
              if (resultSet.hasNext()) {
                approximateResults = new ArrayList<>();
                while (resultSet.hasNext()) {
                  final Result row = resultSet.next();
                  final List<Map<String, Object>> neighbors = row.getProperty("neighbors");

                  for (Map<String, Object> neighbor : neighbors)
                    approximateResults.add(new Pair<>((Identifiable) neighbor.get("vertex"), ((Number) neighbor.get("distance")).floatValue()));
                }

              } else {
                LogManager.instance().log(this, Level.SEVERE, "Not Found %s", input);
                return;
              }
            } else {
              database.begin();
              approximateResults = persistentIndex.findNeighbors(input, k);
              database.rollback();
            }

            final long now = System.currentTimeMillis();
            final long delta = now - startWord;

            totalHits.incrementAndGet();
            totalSearchTime.addAndGet(delta);

            final Map<String, Float> results = new LinkedHashMap<>();
            for (Pair<Identifiable, Float> result : approximateResults)
              results.put(result.getFirst().asVertex().getString("name"), result.getSecond());

//            LogManager.instance()
//                .log(this, Level.SEVERE, "%d Found %d similar words for '%s' in %dms: %s", currentCycle, results.size(), input, delta, results);

            final float throughput = totalHits.get() / ((now - begin) / 1000F);

            if (now - lastStats.get() >= 1000) {
              LogManager.instance()
                  .log(this, Level.SEVERE, "STATS: %d searched words, avg %dms per single word, total throughput %.2f words/sec", totalSearchTime.get(),
                      totalSearchTime.get() / totalHits.get(), throughput);
              lastStats.set(now);
            }

          } catch (Exception e) {
            e.printStackTrace();
          }
        });

      }
    } finally {
      database.close();
    }
  }
}
