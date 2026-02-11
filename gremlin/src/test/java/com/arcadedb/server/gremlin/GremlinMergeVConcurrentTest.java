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

import com.arcadedb.GlobalConfiguration;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Test for issue #1640: Concurrent modification exception while importing same type vertices
 * using multiple threads and buckets with thread strategy.
 *
 * @author Luca Garulli
 */
class GremlinMergeVConcurrentTest extends AbstractGremlinServerIT {

  @Test
  void testConcurrentMergeVWithThreadBucketStrategy() throws Exception {
    final int nOfThreads = 8;
    final int batchSize = 100;
    final int nOfProperties = 8;

    // Create vertex type with multiple buckets and thread selection strategy
    try (RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      database.transaction(() -> {
        // Drop and recreate type
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
      // Callable for concurrent execution
      class Importer implements Callable<String> {
        private final int threadId;
        private final AtomicInteger errorCount;

        public Importer(int threadId, AtomicInteger errorCount) {
          this.threadId = threadId;
          this.errorCount = errorCount;
        }

        @Override
        public String call() {
          try {
            List<Map<String, Object>> queryInputParams = createQueryInputParams(batchSize, threadId, nOfProperties);
            Map<String, Object> params = Map.of("rows", queryInputParams);

            System.out.println(Thread.currentThread().getName() + " (id=" + Thread.currentThread().getId() +
                ") Importing " + batchSize + " entries");

            int nOfResults = client.submit(query, params).all().join().size();

            return Thread.currentThread().getName() + " -> " + nOfResults + " results";
          } catch (Exception e) {
            errorCount.incrementAndGet();
            System.err.println("Error in thread " + Thread.currentThread().getName() + ": " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }

        private List<Map<String, Object>> createQueryInputParams(int batchSize, int threadId, int nOfProperties) {
          List<Map<String, Object>> queryInputParams = new ArrayList<>();
          long baseTime = System.currentTimeMillis();

          for (int i = 0; i < batchSize; i++) {
            Map<String, Object> row = new HashMap<>();
            Map<Object, Object> pk = new HashMap<>();
            Map<Object, Object> prop = new HashMap<>();

            row.put("pk", pk);
            row.put("properties", prop);

            pk.put(T.label, "Imported");
            // Use unique IDs based on thread and batch position
            pk.put("id", baseTime + (threadId * 100000L) + i);

            for (int j = 0; j < nOfProperties; j++) {
              prop.put("p" + j, j);
            }

            queryInputParams.add(row);
          }
          return queryInputParams;
        }
      }

      // Execute concurrent imports
      ExecutorService executorService = Executors.newFixedThreadPool(nOfThreads);
      ExecutorCompletionService<String> completionService = new ExecutorCompletionService<>(executorService);
      AtomicInteger errorCount = new AtomicInteger(0);

      for (int i = 0; i < nOfThreads; i++) {
        completionService.submit(new Importer(i, errorCount));
      }

      int receivedResults = 0;
      while (receivedResults < nOfThreads) {
        try {
          Future<String> future = completionService.take();
          String result = future.get();
          System.out.println("Results from " + result);
          receivedResults++;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          System.err.println("Execution exception: " + e.getMessage());
          e.printStackTrace();
          // Continue to collect other results
          receivedResults++;
        }
      }

      executorService.shutdown();
      if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
        executorService.shutdownNow();
      }

      // Verify no errors occurred
      if (errorCount.get() > 0) {
        fail("Test failed with " + errorCount.get() + " concurrent modification or other errors");
      }

      // Verify all vertices were created
      try (RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

        Number count = (Number) database.query("sql", "SELECT count(*) as count FROM Imported")
            .next().getProperty("count");

        System.out.println("Total vertices created: " + count);
        assertThat(count.longValue()).isEqualTo(nOfThreads * batchSize);
      }

    } finally {
      cluster.close();
    }
  }
}
