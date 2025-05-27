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
package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.select.SelectCompiled;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalDatabaseBenchmark {
  private static final int TOTAL    = 10_000_000;
  private static final int BATCH_TX = 200;

  private static final int                 PRINT_EVERY_MS       = 1_000;
  private static final int                 BUCKETS              = 7;
  private static final int                 CONCURRENT_THREADS   = BUCKETS;
  private static final int                 TX_RETRY             = CONCURRENT_THREADS * 100;
  private static final String              DATABASE_NAME        = "benchmark";
  private final        Map<String, Object> globalStats          = new HashMap<>();
  private              AtomicLong          globalCounter        = new AtomicLong();
  private              AtomicLong          lastCounter          = new AtomicLong();
  private              AtomicInteger       concurrentExceptions = new AtomicInteger();
  private              AtomicInteger       errors               = new AtomicInteger();
  private              Database            database;

  public static void main(String[] args) {
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);

    final LocalDatabaseBenchmark perf = new LocalDatabaseBenchmark();
    perf.beginTest();
    try {
      perf.run();
    } finally {
      perf.endTest();
    }
  }

  private void endTest() {
    database.close();
    GlobalConfiguration.resetAll();
  }

  private void beginTest() {
    GlobalConfiguration.BACKUP_ENABLED.setValue(false);
    if (new DatabaseFactory(DATABASE_NAME).exists())
      new DatabaseFactory(DATABASE_NAME).open().drop();
    database = new DatabaseFactory(DATABASE_NAME).create();
  }

  public void run() {
    database.command("sql", "create vertex type User buckets " + BUCKETS);
    database.command("sql", "create property User.id long");
//    database.command("sql", "create index on User (id) notunique");
    database.command("sql", "alter type User BucketSelectionStrategy `thread`");

    //System.out.println("BEGIN SERVER " + getServer(0).getDatabase(DATABASE_NAME).getStats());

    final Timer timer = spawnStatThread();
    final long beginTime = System.currentTimeMillis();

    try {
      // SPAWN ALL THE THREADS
      final Thread[] threads = new Thread[CONCURRENT_THREADS];
      for (int i = 0; i < CONCURRENT_THREADS; i++) {
        int finalI = i;
        threads[i] = new Thread(() -> executeInThread(finalI));
        threads[i].start();
      }

      // WAIT FOR ALL THE THREADS
      for (int i = 0; i < CONCURRENT_THREADS; i++)
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          // IGNORE IT
        }
    } finally {
      timer.cancel();
      printStats(0L);
      System.out.println("END CLIENT " + globalStats);
      //System.out.println("END SERVER " + getServer(0).getDatabase(DATABASE_NAME).getStats());
    }

    final double deltaInSecs = (System.currentTimeMillis() - beginTime) / 1000;

    System.out.printf("INSERTION completed in %.1f seconds (%.0f req/sec)\n", deltaInSecs, globalCounter.get() / deltaInSecs);

    long totalRecordsOnClusters = 0L;
    for (int i = 0; i < BUCKETS; i++)
      totalRecordsOnClusters += database.countBucket("User_" + i);

    //printAllRecordsInDatabase(database);

    List<Long> allIds = checkRecordSequence(database);

    assertThat(allIds.size()).isEqualTo(TOTAL * CONCURRENT_THREADS);

    assertThat(totalRecordsOnClusters).isEqualTo(TOTAL * CONCURRENT_THREADS);

    assertThat(database.countType("User", true)).isEqualTo(TOTAL * CONCURRENT_THREADS);

//    queryNative();
//    querySQL();
//    queryNative();

    database.close();
  }

  private void queryNative() {
    final long begin = System.currentTimeMillis();
    final SelectCompiled cached = database.select().fromType("User").where()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id").and()//
        .property("id").eq().parameter("id")//
        .compile();
    for (int i = 0; i < TOTAL * CONCURRENT_THREADS; i++) {
      assertThat(cached.parameter("id", i).vertices().toList().size()).isEqualTo(1);
    }
    System.out.println("NATIVE " + (System.currentTimeMillis() - begin) + "ms");
  }

  private void querySQL() {
    long begin = System.currentTimeMillis();
    for (int i = 0; i < TOTAL * CONCURRENT_THREADS; i++) {
      assertThat(database.query("sql",
          "select from User where id = ? and id = ? and id = ? and id = ? and id = ? and id = ? and id = ? and id = ? and id = ? and id = ?",
          i, i, i, i, i, i, i, i, i, i).toVertices().size()).isEqualTo(1);
    }
    System.out.println("SQL " + (System.currentTimeMillis() - begin) + "ms");
  }

  private List<Long> checkRecordSequence(final Database database) {
    final List<Long> allIds = new ArrayList<>();
    database.iterateType("User", true).forEachRemaining((a) -> allIds.add(a.getRecord().asVertex().getLong("id")));
    allIds.sort(Long::compareTo);

    long last = -1;
    for (int i = 0; i < allIds.size(); i++) {
      if (allIds.get(i) != last + 1)
        System.out.println("MISSING ID " + i);
      last = allIds.get(i);
    }
    return allIds;
  }

  private void printAllRecordsInDatabase(Database database) {
    database.iterateType("User", true).forEachRemaining((a) -> System.out.println("Record " + a.getRecord().asVertex().toJSON()));
  }

  private Timer spawnStatThread() {
    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      long beginTime = System.currentTimeMillis();

      @Override
      public void run() {
        beginTime = printStats(beginTime);
      }
    }, PRINT_EVERY_MS, PRINT_EVERY_MS);
    return timer;
  }

  private void executeInThread(final int threadId) {
    try {

      for (AtomicInteger threadCounter = new AtomicInteger(); threadCounter.get() < TOTAL; ) {
        database.transaction(() -> {
          for (int txCounter = 0; txCounter < BATCH_TX; txCounter++) {
            try {
              final int id = threadId * TOTAL + (threadCounter.get() + txCounter);
              final MutableVertex user = database.newVertex("User").set("id", id);
              user.save();

              //LogManager.instance().log(this, Level.WARNING, "Saving user id %d as %s (threadId=%d)", id, user.getIdentity(), Thread.currentThread().getId());

            } catch (Throwable t) {
              incrementError(t);
            }
          }
        }, false, TX_RETRY, null, (e) -> {
          if (e instanceof ConcurrentModificationException)
            concurrentExceptions.incrementAndGet();
          else {
            incrementError(e);
          }
        });

        threadCounter.addAndGet(BATCH_TX);
        globalCounter.addAndGet(BATCH_TX);
      }
    } catch (Throwable t) {
      incrementError(t);
    } finally {
      mergeStats(database.getStats());
    }
  }

  private void incrementError(final Throwable t) {
    t.printStackTrace();
    errors.incrementAndGet();
  }

  private long printStats(long beginTime) {
    final long now = System.currentTimeMillis();

    if (beginTime > 0) {
      final long delta = now - beginTime;
      beginTime = System.currentTimeMillis();
      System.out.println(
          ((globalCounter.get() - lastCounter.get()) * PRINT_EVERY_MS / (float) delta) + " req/sec (counter=" + globalCounter.get()
              + "/" + (CONCURRENT_THREADS * TOTAL) + ", conflicts=" + concurrentExceptions.get() + ", errors=" + errors.get()
              + ")");
    } else {
      System.out.println("COMPLETED (counter=" + globalCounter.get() + "/" + (CONCURRENT_THREADS * TOTAL) + ", conflicts="
          + concurrentExceptions.get() + ", errors=" + errors.get() + ")");
    }
    lastCounter.set(globalCounter.get());

    return beginTime;
  }

  private synchronized void mergeStats(final Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Long current = (Long) globalStats.getOrDefault(entry.getKey(), 0L);
      globalStats.put(entry.getKey(), current + (Long) entry.getValue());
    }
  }
}
