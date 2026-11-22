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
package com.arcadedb.database;

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Threads that each call {@code new DatabaseFactory(path).open()} on one path: one instance wins, the others are refused with
 * "Found active instance ... already in use". The refusal must be clean. {@code open()} only serialised on its own factory, which
 * is a new object per caller, so a loser that passed the active-instance check built a whole second {@link LocalDatabase} over the
 * winner's files. A {@code LocalDatabase} is equal to every other one of the same path, so the loser's open and its failure
 * clean-up dropped the winner's cached and queued pages, rolled back its transactions and left its record counters off by the
 * winner's last commits: {@code count(*)} read 99, 98 or 101 over 100 rows, commits failed with NullPointerException and
 * "Transaction not begun", and the next open found the WAL gone. About one open in 200 shows it, hence the number of opens.
 */
@Tag("slow")
class ConcurrentOpenOfSamePathTest {
  private static final String PATH  = "target/databases/ConcurrentOpenOfSamePathTest";
  private static final int    ROWS  = 100;
  private static final int    OPENS = 1200;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(PATH));
    try (final Database db = new DatabaseFactory(PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          db.newDocument("T").set("i", i).save();
      });
    }
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  @Timeout(300)
  void aRefusedConcurrentOpenLeavesTheWinnerAlone() throws Exception {
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger counterDrift = new AtomicInteger();
    final AtomicInteger failedCommits = new AtomicInteger();
    final List<String> failures = new ArrayList<>();

    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(240);
    final Callable<Void> loop = () -> {
      int mine = 0;
      while (mine < OPENS) {
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException("stopped after " + mine + " of " + OPENS + " opens");
        if (System.nanoTime() - deadline > 0)
          throw new AssertionError("only " + mine + " of " + OPENS + " opens in the time given");
        final Database db;
        try {
          db = new DatabaseFactory(PATH).open();
        } catch (final DatabaseOperationException e) {
          // refused: another thread holds the database, or won the race to open it. Anything else ends the test, the refusal by
          // the file lock included: that is a loser that built an instance over the winner's files, which is the defect (#12791)
          if (!isRefusalOfAConcurrentOpen(e))
            throw e;
          continue;
        }
        mine++;
        opened.incrementAndGet();
        try {
          final long counted = count(db, "SELECT count(*) AS c FROM T");
          final long scanned = db.countType("T", false);
          long iterated = 0;
          try (final ResultSet rs = db.query("sql", "SELECT i FROM T")) {
            while (rs.hasNext()) {
              rs.next();
              iterated++;
            }
          }
          if (counted != ROWS || scanned != ROWS || iterated != ROWS) {
            counterDrift.incrementAndGet();
            synchronized (failures) {
              if (failures.size() < 5)
                failures.add("count(*)=" + counted + " countType=" + scanned + " iterated=" + iterated);
            }
          }
          try {
            db.transaction(() -> db.newDocument("T").set("i", -1).save());
            db.transaction(() -> {
              try (final ResultSet rs = db.query("sql", "SELECT FROM T WHERE i = -1")) {
                while (rs.hasNext())
                  rs.next().getElement().get().delete();
              }
            });
          } catch (final RuntimeException e) {
            failedCommits.incrementAndGet();
            synchronized (failures) {
              if (failures.size() < 5)
                failures.add("transaction failed: " + e);
            }
          }
        } finally {
          db.close();
        }
      }
      return null;
    };

    // FUTURES, NOT BARE THREADS: AN EXCEPTION OF A WORKER (A QUERY, A CLOSE, AN UNEXPECTED REFUSAL) REACHES THE TEST THREAD
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<Void> a = executor.submit(loop);
      final Future<Void> b = executor.submit(loop);
      a.get();
      b.get();
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }

    assertThat(opened.get()).isEqualTo(2 * OPENS);
    assertThat(failures).as("what the winner saw while the loser's open was refused").isEmpty();
    assertThat(counterDrift.get()).isZero();
    assertThat(failedCommits.get()).isZero();

    try (final Database db = new DatabaseFactory(PATH).open()) {
      assertThat(db.countType("T", false)).isEqualTo(ROWS);
    }
  }

  private static boolean isRefusalOfAConcurrentOpen(final DatabaseOperationException e) {
    final String message = e.getMessage();
    return message != null && message.startsWith("Found active instance of database");
  }

  private static long count(final Database db, final String sql) {
    try (final ResultSet rs = db.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
