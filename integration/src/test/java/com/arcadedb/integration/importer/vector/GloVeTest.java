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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.assertj.core.api.Assertions;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Example application that downloads the english fast-text word vectors, inserts them into an hnsw index and lets
 * you query them.
 */
public class GloVeTest {
  private final static int     PARALLEL_LEVEL = 8;
  private static final String  FILE_NAME      = "/Users/frank/Downloads/glove.twitter.27B/glove.twitter.27B.100d.txt";
  private              boolean USE_SQL        = false;

  public static void main(String[] args) {
    new GloVeTest();
  }

  public GloVeTest() {
    final long start = System.currentTimeMillis();

    final Database database;

    final DatabaseFactory factory = new DatabaseFactory("databases/glovedb");

    // TODO: REMOVE THIS
//    if (factory.exists())
//      factory.open().drop();

    if (factory.exists()) {
      database = factory.open();
      //LogManager.instance().log(this, Level.SEVERE, "Found existent database with %d words", database.countType("Word", false));

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
          + "vertexType = 'Word', edgeType = 'Proximity', vectorProperty = 'vector', vectorType = Float, idProperty = 'name'" //
      );

      long end = System.currentTimeMillis();
      long duration = end - start;

      LogManager.instance().log(this, Level.SEVERE, "Creating index took %d millis which is %d minutes.%n", duration,
          MILLISECONDS.toMinutes(duration));

      LogManager.instance().log(this, Level.SEVERE, "Building vector index...");

      // Get the LSMVectorIndex from the bucket
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Word[vector]");
      final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

      // Build with callbacks
      lsmIndex.build(
          100000, // batch size
          (doc, total) -> {
            if (total % 10000 == 0) {
              LogManager.instance().log(this, Level.SEVERE, "Indexed " + total + " documents...");
            }
          },
          (phase, processedNodes, totalNodes, vecAccesses) -> {
            switch (phase) {
            case "validating":
              LogManager.instance().log(this, Level.SEVERE, "Validating vectors: %d / %d", processedNodes, totalNodes);
              break;
            case "building":
              LogManager.instance().log(this, Level.SEVERE, "Building graph: %d unique nodes accessed, %d total vector accesses",
                  processedNodes, vecAccesses);
              break;
            case "persisting":
              LogManager.instance().log(this, Level.SEVERE, "Persisting graph: %d / %d nodes", processedNodes, totalNodes);
              break;
            default:
              LogManager.instance().log(this, Level.SEVERE, "Unknown phase: %d / %d nodes", processedNodes, totalNodes);
              break;
            }
          }
      );

      database.close();
      System.exit(1);
    }

    final TypeIndex persistentIndex = (TypeIndex) database.getSchema().getIndexByName("Word[vector]");

    try {

      int k = 128;

      final Random random = new Random();

      final ExecutorService executor = new ThreadPoolExecutor(PARALLEL_LEVEL, PARALLEL_LEVEL, 5, TimeUnit.SECONDS,
          new SynchronousQueue(),
          new ThreadPoolExecutor.CallerRunsPolicy());

      final AtomicLong totalSearchTime = new AtomicLong();
      final AtomicInteger totalHits = new AtomicInteger();
      final long begin = System.currentTimeMillis();
      final AtomicLong lastStats = new AtomicLong();

      LogManager.instance().log(this, Level.SEVERE, "Loaded words embeddings from database...");

      final List<String> words = new ArrayList<>();
      final List<float[]> embeddings = new ArrayList<>();
      for (Iterator<Record> it = database.iterateType("Word", true); it.hasNext(); ) {
        final Vertex v = it.next().asVertex();
        words.add(v.getString("name"));
        embeddings.add((float[]) v.get("vector"));
      }

      LogManager.instance().log(this, Level.SEVERE, "Loaded %d embeddings", embeddings.size());

      for (int cycle = 0; ; ++cycle) {
        final int currentCycle = cycle;

        executor.submit(() -> {
          try {
            //final int randomWordIndex = random.nextInt(embeddings.size());
            final int randomWordIndex = currentCycle >= words.size() ? 0 : currentCycle;

            final String wordToSearch = words.get(randomWordIndex);
            final float[] embeddingsToSearch = embeddings.get(randomWordIndex);

            final long startWord = System.currentTimeMillis();

            final List<Pair<RID, Float>> approximateResults;
            if (USE_SQL) {
              final ResultSet resultSet = database.query("sql", "select vectorNeighbors('Word[vector]', ?,?) as neighbors",
                  embeddingsToSearch,
                  k);
              if (resultSet.hasNext()) {
                approximateResults = new ArrayList<>();
                while (resultSet.hasNext()) {
                  final Result row = resultSet.next();
                  final List<Map<String, Object>> neighbors = row.getProperty("neighbors");

                  for (Map<String, Object> neighbor : neighbors)
                    approximateResults.add(
                        new Pair<>((RID) neighbor.get("vertex"), ((Number) neighbor.get("distance")).floatValue()));
                }

              } else {
                LogManager.instance().log(this, Level.SEVERE, "Not Found %s", embeddingsToSearch);
                return;
              }
            } else {
              // Vector searches are read-only, no transaction needed
              approximateResults = ((LSMVectorIndex) persistentIndex.getIndexesOnBuckets()[0]).findNeighborsFromVector(
                  embeddingsToSearch, k);
              Assertions.assertThat(approximateResults.size()).isNotEqualTo(0);
            }

            final long now = System.currentTimeMillis();
            final long delta = now - startWord;

            totalHits.incrementAndGet();
            totalSearchTime.addAndGet(delta);

            final Map<String, Float> results = new LinkedHashMap<>();
            for (Pair<RID, Float> result : approximateResults)
              results.put(result.getFirst().asVertex().getString("name"), result.getSecond());

//            LogManager.instance()
//                .log(this, Level.SEVERE, "%d Found %d similar words for '%s' in %dms: %s", currentCycle, results.size(), input, delta, results);

            final float throughput = totalHits.get() / ((now - begin) / 1000F);

            if (now - lastStats.get() >= 1000) {
              LogManager.instance()
                  .log(this, Level.SEVERE,
                      "STATS: %d searched words, (last %s -> found %d), avg %dms per single word, total throughput %.2f words/sec",
                      totalSearchTime.get(),
                      wordToSearch, approximateResults.size(),
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
