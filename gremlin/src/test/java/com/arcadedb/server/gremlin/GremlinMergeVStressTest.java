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
package com.arcadedb.server.gremlin;

import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.test.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Stress test for concurrent mergeV operations with thread bucket strategy.
 * Tests the fix for issue #1640 and #3135 - concurrent modifications on multi-page records.
 *
 * @author Luca Garulli
 */
class GremlinMergeVStressTest extends AbstractGremlinServerIT {

  @Test
  void highConcurrencyMergeVWithThreadBucketStrategy() throws Exception {
    final int nOfThreads = 12;
    final int batchSize = 200;
    final int nOfProperties = 8;
    final int iterations = 2;

    // Create vertex type with buckets matching thread count
    try (RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      database.transaction(() -> {
        database.command("sql", "DROP TYPE Imported IF EXISTS");
        database.command("sql", "CREATE VERTEX TYPE Imported IF NOT EXISTS BUCKETS " + nOfThreads);
        database.command("sql", "ALTER TYPE Imported BucketSelectionStrategy `thread`");
      });
    }

    // Prepare Gremlin query using mergeV
    final String query = """
        g.inject(rows).unfold()
          .mergeV(select('pk'))
            .option(Merge.onMatch, select('properties'))
            .option(Merge.onCreate, select('properties'));
        """;

    // Create Gremlin client
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));

    final Cluster cluster = Cluster.build().enableSsl(false).addContactPoint("localhost").port(8182)
        .credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).serializer(serializer).create();

    final Client client = cluster.connect();

    try {
      final AtomicInteger totalErrors = new AtomicInteger(0);
      final AtomicLong totalInserted = new AtomicLong(0);

      // Callable for concurrent execution
      class Importer implements Callable<String> {
        private final int threadId;
        private final int iteration;

        public Importer(int threadId, int iteration) {
          this.threadId = threadId;
          this.iteration = iteration;
        }

        @Override
        public String call() {
          final int maxRetries = 5;
          int attempt = 0;
          Exception lastException = null;

          while (attempt < maxRetries) {
            try {
              List<Map<String, Object>> queryInputParams = createQueryInputParams(batchSize, threadId, iteration, nOfProperties);
              Map<String, Object> params = Map.of("rows", queryInputParams);

//              if (attempt > 0) {
//                System.out.println(Thread.currentThread().getName() + " (id=" + Thread.currentThread().threadId() +
//                    ") Iteration " + iteration + " RETRY #" + attempt + " importing " + batchSize + " entries");
//              } else {
//                System.out.println(Thread.currentThread().getName() + " (id=" + Thread.currentThread().threadId() +
//                    ") Iteration " + iteration + " importing " + batchSize + " entries");
//              }

              int nOfResults = client.submit(query, params).all().join().size();
              totalInserted.addAndGet(nOfResults);

              return Thread.currentThread().getName() + " iteration " + iteration + " -> " + nOfResults +
                  " results (attempts: " + (attempt + 1) + ")";

            } catch (Exception e) {
              lastException = e;
              String errorMsg = e.getMessage();

              // Check if it's a concurrent modification exception that should be retried
              if (errorMsg != null && (errorMsg.contains("Concurrent modification") ||
                  errorMsg.contains("ConcurrentModificationException"))) {
                attempt++;
                System.err.println(Thread.currentThread().getName() + " iteration " + iteration +
                    " - Concurrent modification detected, retry " + attempt + "/" + maxRetries);

                // Brief pause before retry
                try {
                  Thread.sleep(10 * attempt); // Exponential backoff
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  break;
                }
                continue;
              }

              // Not a concurrent modification error, fail immediately
              totalErrors.incrementAndGet();
              System.err.println("Non-retriable error in thread " + Thread.currentThread().getName() +
                  " iteration " + iteration + ": " + errorMsg);
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }

          // Max retries exceeded
          totalErrors.incrementAndGet();
          System.err.println("Max retries exceeded in thread " + Thread.currentThread().getName() +
              " iteration " + iteration);
          if (lastException != null) {
            lastException.printStackTrace();
          }
          throw new RuntimeException("Max retries exceeded", lastException);
        }

        private List<Map<String, Object>> createQueryInputParams(int batchSize, int threadId, int iteration, int nOfProperties) {
          List<Map<String, Object>> queryInputParams = new ArrayList<>();
          long baseTime = System.currentTimeMillis();

          for (int i = 0; i < batchSize; i++) {
            Map<String, Object> row = new HashMap<>();
            Map<Object, Object> pk = new HashMap<>();
            Map<Object, Object> prop = new HashMap<>();

            row.put("pk", pk);
            row.put("properties", prop);

            pk.put(T.label, "Imported");
            // Use unique IDs based on thread, iteration, and batch position
            pk.put("id", baseTime + (threadId * 1000000L) + (iteration * 100000L) + i);

            for (int j = 0; j < nOfProperties; j++) {
              prop.put("p" + j, "value_" + j + "_" + threadId + "_" + iteration);
            }

            queryInputParams.add(row);
          }
          return queryInputParams;
        }
      }

      // Run multiple iterations with high concurrency
      for (int iteration = 0; iteration < iterations; iteration++) {
//        System.out.println("\n=== Starting iteration " + iteration + " with " + nOfThreads + " threads ===");

        ExecutorService executorService = Executors.newFixedThreadPool(nOfThreads);
        ExecutorCompletionService<String> completionService = new ExecutorCompletionService<>(executorService);

        for (int i = 0; i < nOfThreads; i++) {
          completionService.submit(new Importer(i, iteration));
        }

        int receivedResults = 0;
        while (receivedResults < nOfThreads) {
          try {
            Future<String> future = completionService.take();
            String result = future.get();
//            System.out.println("Results from " + result);
            receivedResults++;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            System.err.println("Execution exception: " + e.getMessage());
            e.printStackTrace();
            receivedResults++;
          }
        }

        executorService.shutdown();
        if (!executorService.awaitTermination(2, TimeUnit.MINUTES)) {
          executorService.shutdownNow();
        }
      }

      // Verify no errors occurred
      if (totalErrors.get() > 0) {
        fail("Test failed with " + totalErrors.get() + " concurrent modification or other errors");
      }

      // Verify all vertices were created
      try (RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

        Number count = (Number) database.query("sql", "SELECT count(*) as count FROM Imported")
            .next().getProperty("count");

        long expectedCount = (long) nOfThreads * batchSize * iterations;
//        System.out.println("\nTotal vertices created: " + count + ", expected: " + expectedCount);
//        System.out.println("Total inserted reported by threads: " + totalInserted.get());

        assertThat(count.longValue()).isEqualTo(expectedCount);
        assertThat(totalInserted.get()).isEqualTo(expectedCount);
      }

    } finally {
      cluster.close();
    }
  }
}
