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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.server.BaseGraphServerTest;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteDatabaseBenchmark extends BaseGraphServerTest {
  private static final int TOTAL              = 10_000;
  private static final int BATCH_TX           = 1;
  private static final int PRINT_EVERY_MS     = 1_000;
  private static final int BUCKETS            = 2;
  private static final int CONCURRENT_THREADS = BUCKETS;
  private static final int TX_RETRY           = CONCURRENT_THREADS * 2;

  private static final String              DATABASE_NAME = "benchmark";
  private final        Map<String, Object> globalStats   = new HashMap<>();

  private AtomicLong    globalCounter        = new AtomicLong();
  private AtomicLong    lastCounter          = new AtomicLong();
  private AtomicInteger concurrentExceptions = new AtomicInteger();
  private AtomicInteger errors               = new AtomicInteger();

  public static void main(String[] args) {
    final RemoteDatabaseBenchmark perf = new RemoteDatabaseBenchmark();
    perf.beginTest();
    try {
      perf.run();
    } finally {
      perf.endTest();
    }
  }

  public void run() {
    new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).create(DATABASE_NAME);

    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "create vertex type User buckets " + BUCKETS);

    System.out.println("BEGIN SERVER " + getServer(0).getDatabase(DATABASE_NAME).getStats());

    final Timer timer = spawnStatThread();

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
      printStats(System.currentTimeMillis());
      System.out.println("END CLIENT " + globalStats);
      System.out.println("END SERVER " + getServer(0).getDatabase(DATABASE_NAME).getStats());
    }

    long totalRecordsOnClusters = 0L;
    for (int i = 0; i < BUCKETS; i++)
      totalRecordsOnClusters += database.countBucket("User_" + i);

    final List<Long> allIds = new ArrayList<>();
    getServer(0).getDatabase(DATABASE_NAME).iterateType("User", true)
        .forEachRemaining((a) -> allIds.add(a.getRecord().asVertex().getLong("id")));
    allIds.sort(Long::compareTo);

    long last = -1;
    for (int i = 0; i < allIds.size(); i++) {
      if (allIds.get(i) != last + 1)
        System.out.println("MISSING ID " + i);
      last = allIds.get(i);
    }

    assertThat(allIds.size()).isEqualTo(TOTAL * CONCURRENT_THREADS);

    assertThat(totalRecordsOnClusters).isEqualTo(TOTAL * CONCURRENT_THREADS);

    assertThat(database.countType("User", true)).isEqualTo(TOTAL * CONCURRENT_THREADS);

    database.close();
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
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {

      for (AtomicInteger threadCounter = new AtomicInteger(); threadCounter.get() < TOTAL; ) {
        database.transaction(() -> {
          for (int txCounter = 0; txCounter < BATCH_TX; txCounter++) {
            try {
              final MutableVertex user = database.newVertex("User").set("id", threadId * TOTAL + (threadCounter.get() + txCounter));
              user.save();
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
      database.close();
    }
  }

  private void incrementError(final Throwable t) {
    t.printStackTrace();
    errors.incrementAndGet();
  }

  private long printStats(long beginTime) {
    final long now = System.currentTimeMillis();
    final long delta = now - beginTime;

    beginTime = System.currentTimeMillis();
    System.out.println(
        ((globalCounter.get() - lastCounter.get()) * PRINT_EVERY_MS / (float) delta) + " req/sec (counter=" + globalCounter.get()
            + "/" + TOTAL + ", conflicts=" + concurrentExceptions.get() + ", errors=" + errors.get() + ")");
    lastCounter.set(globalCounter.get());

    return beginTime;
  }

  private synchronized void mergeStats(final Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Long current = (Long) globalStats.getOrDefault(entry.getKey(), 0L);
      globalStats.put(entry.getKey(), current + (Long) entry.getValue());
    }
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }
}
