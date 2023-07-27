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
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.vector.HnswVectorIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Example application that downloads the english fast-text word vectors, inserts them into an hnsw index and lets
 * you query them.
 */
public class FastTextDatabase {

  private static final String WORDS_FILE_URL = "https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.vec.gz";

  private static final Path TMP_PATH       = Paths.get(System.getProperty("java.io.tmpdir"));
  private final static int  PARALLEL_LEVEL = 8;

  public static void main(String[] args) throws Exception {
    new FastTextDatabase();
  }

  public FastTextDatabase() throws IOException, InterruptedException, ReflectiveOperationException {
    final long start = System.currentTimeMillis();

    final Database database;

    final DatabaseFactory factory = new DatabaseFactory("textdb");

    // TODO: REMOVE THIS
//    if (factory.exists())
//      factory.open().drop();

    if (factory.exists()) {
      database = factory.open();
      //LogManager.instance().log(this, Level.SEVERE, "Found existent database with %d words", database.countType("Word", false));

    } else {
      database = factory.create();
      LogManager.instance().log(this, Level.SEVERE, "Creating new database");

      final Path file = TMP_PATH.resolve("cc.en.300.vec.gz");
      if (!Files.exists(file)) {
        downloadFile(WORDS_FILE_URL, file);
      } else {
        LogManager.instance().log(this, Level.SEVERE, "Input file already downloaded. Using %s\n", file);
      }

      database.command("sql", "import database file://" + file.toAbsolutePath() + " "//
          + "with distanceFunction = cosine, m = 16, ef = 128, efConstruction = 128," //
          + "vertexType = Word, edgeType = Proximity, vectorProperty = vector, vectorType = Float, idProperty = name" //
      );

      LogManager.instance().log(this, Level.SEVERE, "Creating index took %d millis which is %d minutes.%n", System.currentTimeMillis() - start,
          MILLISECONDS.toMinutes(System.currentTimeMillis() - start));
    }

    final HnswVectorIndex persistentIndex = (HnswVectorIndex) database.getSchema().getIndexByName("Word[name,vector]");

    try {
      int k = 10;

      final Random random = new Random();

      final List<Bucket> buckets = database.getSchema().getType("Word").getBuckets(false);

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

            database.begin();

            List<Pair<Vertex, Float>> approximateResults = persistentIndex.findNeighborsFromVector(input, k);

            final long delta = System.currentTimeMillis() - startWord;

            totalHits.incrementAndGet();
            totalSearchTime.addAndGet(delta);

            final Map<String, Float> results = new LinkedHashMap<>();
            for (Pair<Vertex, Float> result : approximateResults)
              results.put(result.getFirst().getString("name"), result.getSecond());

            //LogManager.instance().log(this, Level.SEVERE, "%d Found similar words for '%s' in %dms: %s", currentCycle, input, delta, results);

            final long now = System.currentTimeMillis();
            final float throughput = totalHits.get() / ((now - begin) / 1000F);

            if (now - lastStats.get() >= 1000) {
              LogManager.instance()
                  .log(this, Level.SEVERE, "STATS: %d searched words, avg %dms per single word, total throughput %.2f words/sec", totalSearchTime.get(),
                      totalSearchTime.get() / totalHits.get(), throughput);
              lastStats.set(now);
            }

            database.rollback();

          } catch (Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Not Found");
          }
        });

      }
    } finally {
      database.close();
    }
  }

  private void downloadFile(String url, Path path) throws IOException {
    LogManager.instance().log(this, Level.SEVERE, "Downloading %s to %s. This may take a while", url, path);
    try (InputStream in = new URL(url).openStream()) {
      Files.copy(in, path);
    }
    LogManager.instance().log(this, Level.SEVERE, "Downloaded %s", FileUtils.getSizeAsString(path.toFile().length()));
  }
}
