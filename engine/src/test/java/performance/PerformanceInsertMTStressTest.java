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
package performance;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DefaultDataEncryption;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.engine.PageManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.security.*;
import java.security.spec.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class PerformanceInsertMTStressTest {

  public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException {
    GlobalConfiguration.MAX_PAGE_RAM.setValue(150);
    RunConfig runConfig = new RunConfig();
    DatabaseConfig dbConfig = new DatabaseConfig();

    configureSystemProperties();
    Database db = initializeDatabase(dbConfig);

    try {
      setupDatabaseSchema(db, runConfig.getThreadCount());

      long executionTime = executeDataInsertion(db, runConfig);
      System.out.println("\nInserting took " + executionTime + " ms");

      queryAndPrintResults(db, "Resource");
    } finally {
      db.close();
    }
  }

  private static void configureSystemProperties() {
    System.setProperty("arcadedb.server.mode", "production");
    //System.setProperty("arcadedb.profile", "high-performance");
  }

  private static Database initializeDatabase(DatabaseConfig config)
      throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException {
    final SecretKey key = DefaultDataEncryption.getSecretKeyFromPasswordUsingDefaults(config.getEncryptionKey(),
        config.getEncryptionSalt());
    final ContextConfiguration contextConfig = new ContextConfiguration();
    contextConfig.setValue(GlobalConfiguration.DUMP_METRICS_EVERY, config.getMetricsDumpInterval());
    contextConfig.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, config.getDatabaseDirectory());
    contextConfig.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, config.getRootPassword());
    contextConfig.setValue(GlobalConfiguration.SERVER_NAME, config.getServerName());

    final DatabaseFactory factory = new DatabaseFactory(config.getName());
    final Database db = factory.create();
    //db.setDataEncryption(DefaultDataEncryption.useDefaults(key));
    return db;
  }

  private static void setupDatabaseSchema(Database db, int threadCount) {
    db.begin();
    String resourceTypeName = "Resource";
    com.arcadedb.schema.DocumentType resourceType = db.getSchema().createDocumentType(resourceTypeName);

    IntStream.rangeClosed(1, threadCount).forEach(i -> {
      resourceType.addBucket(db.getSchema().createBucket(resourceTypeName + "_" + i));
    });

    resourceType.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
    resourceType.createProperty("identifier", Type.STRING);
    resourceType.createProperty("value", Type.LONG);
    resourceType.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, resourceTypeName, "value");
    db.commit();
  }

  private static long executeDataInsertion(Database db, RunConfig config) {
    System.out.println();
    List<Thread> threads = new ArrayList<>();
    Map<Integer, Integer> retryCounters = new HashMap<>(); // Track retries per batch

    long startTime = System.nanoTime();
    long startTimeInMs = System.currentTimeMillis();
    long startTimeLastLapInMs = System.currentTimeMillis();

    for (int batch = 0; batch < config.getThreadBatches(); batch++) {

      for (int thread = 0; thread < config.getThreadCount(); thread++) {
        int finalBatch = batch;
        Thread currentThread = new Thread(() -> {
          int retryCount = 0;
          boolean success = false;
          Random random = new Random();

          while (retryCount < config.getRetries() && !success) {
            try {
              db.begin();
              for (int i = 0; i < config.getRecordsPerThread(); i++) {
                db.newDocument("Resource").set("identifier", Arrays.asList("a", "b").get(random.nextInt(2)))
                    .set("value", random.nextLong(0, 14 * 24 * 60 * 60 * 1000L)).save();
              }
              db.commit();
              success = true;
            } catch (Exception ex) {
              retryCount++;
              retryCounters.merge(finalBatch, 1, Integer::sum);

              System.out.println("+WARNING: Batch " + finalBatch + " has reached " + retryCount + " retries");

              if (retryCount >= config.getWarningThreshold()) {
                System.out.println("WARNING: Batch " + finalBatch + " has reached " + retryCount + " retries");
                ex.printStackTrace();
              }

              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
            }
          }

          DatabaseContext.INSTANCE.removeContext(db.getDatabasePath());
        });
        threads.add(currentThread);
        currentThread.start();
      }

      for (Thread t : threads) {
        try {
          t.join();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      threads.clear();
      if (batch % 100 == 0 || (batch >= 92200)) {
        System.out.println(
            batch + "/" + config.getThreadBatches() + " batches completed in " + (System.currentTimeMillis() - startTimeLastLapInMs)
                + "ms (total " + ((System.currentTimeMillis() - startTimeInMs) / 1000) + "s) cacheRAM=" +
                FileUtils.getSizeAsString(PageManager.INSTANCE.getStats().readCacheRAM) + "/" +
                FileUtils.getSizeAsString(PageManager.INSTANCE.getStats().maxRAM));
        startTimeLastLapInMs = System.currentTimeMillis();
      }
    }

    long endTime = System.nanoTime();
    long totalRetries = retryCounters.values().stream().mapToLong(Integer::longValue).sum();

    if (totalRetries > 0) {
      System.out.println("\nRetry statistics:");
      System.out.println("Total retries: " + totalRetries);
      System.out.println("Batches with retries: " + retryCounters.size());
      int maxRetries = retryCounters.values().stream().mapToInt(v -> v).max().orElse(0);
      System.out.println("Maximum retries for a batch: " + maxRetries);
    }

    return TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
  }

  private static void queryAndPrintResults(Database db, String resourceTypeName) {
    long count = db.query("sql", "SELECT COUNT(*) as count FROM " + resourceTypeName + ";").next().getProperty("count");
    System.out.println("There are now " + count + " entries in the database.");

    long startTimeQuery = System.nanoTime();
    ResultSet resultQuery = db.query("sql",
        "SELECT identifier, SUM(value) as sum FROM " + resourceTypeName + " GROUP BY identifier;");
    while (resultQuery.hasNext()) {
      Result next = resultQuery.next();
      System.out.println(next.getPropertyNames());
      System.out.println(next.getProperty("identifier") + ": " + next.getProperty("sum"));
    }
    long endTimeQuery = System.nanoTime();
    System.out.println("Query took " + TimeUnit.NANOSECONDS.toMillis(endTimeQuery - startTimeQuery) + " ms");

    long startTimeManualAgg = System.nanoTime();
    Map<String, Long> data = new HashMap<>();
    ResultSet resultManual = db.query("sql", "SELECT * FROM " + resourceTypeName + ";");
    while (resultManual.hasNext()) {
      Result doc = resultManual.next();
      String id = doc.getProperty("identifier");
      Long currentValue = data.getOrDefault(id, 0L);
      data.put(id, currentValue + (Long) doc.getProperty("value"));
    }
    System.out.println("Manual aggregation result: " + data);
    long endTimeManualAgg = System.nanoTime();
    System.out.println("Manual aggregation took " + TimeUnit.NANOSECONDS.toMillis(endTimeManualAgg - startTimeManualAgg) + " ms");
  }
}

class RunConfig {
  private final int retries;
  private final int threadCount;
  private final int recordsPerThread;
  private final int threadBatches;
  private final int warningThreshold;

  public RunConfig() {
    this(100, 4, 25, 100_000, 10);
  }

  public RunConfig(int retries, int threadCount, int recordsPerThread, int threadBatches, int warningThreshold) {
    this.retries = retries;
    this.threadCount = threadCount;
    this.recordsPerThread = recordsPerThread;
    this.threadBatches = threadBatches;
    this.warningThreshold = warningThreshold;
  }

  public int getRetries() {
    return retries;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public int getRecordsPerThread() {
    return recordsPerThread;
  }

  public int getThreadBatches() {
    return threadBatches;
  }

  public int getWarningThreshold() {
    return warningThreshold;
  }
}

class DatabaseConfig {
  private final String name;
  private final String encryptionKey;
  private final String encryptionSalt;
  private final String databaseDirectory;
  private final String rootPassword;
  private final String serverName;
  private final int    metricsDumpInterval;

  public DatabaseConfig() {
    this("averages-" + Clock.systemDefaultZone().millis(), "hunter2", "abcdefgh", "db/", "super-s3cr3t", "ViaDB", 5);
  }

  public DatabaseConfig(String name, String encryptionKey, String encryptionSalt, String databaseDirectory, String rootPassword,
      String serverName, int metricsDumpInterval) {
    this.name = name;
    this.encryptionKey = encryptionKey;
    this.encryptionSalt = encryptionSalt;
    this.databaseDirectory = databaseDirectory;
    this.rootPassword = rootPassword;
    this.serverName = serverName;
    this.metricsDumpInterval = metricsDumpInterval;
  }

  public String getName() {
    return name;
  }

  public String getEncryptionKey() {
    return encryptionKey;
  }

  public String getEncryptionSalt() {
    return encryptionSalt;
  }

  public String getDatabaseDirectory() {
    return databaseDirectory;
  }

  public String getRootPassword() {
    return rootPassword;
  }

  public String getServerName() {
    return serverName;
  }

  public int getMetricsDumpInterval() {
    return metricsDumpInterval;
  }
}
