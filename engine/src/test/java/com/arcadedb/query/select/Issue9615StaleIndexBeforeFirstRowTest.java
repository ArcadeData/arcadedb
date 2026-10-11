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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9615: a SQL range or ORDER BY {@code SELECT} that had returned no row yet still failed to the caller while another
 * thread dropped the index it read. The retry of #9331 plans the statement again when its index went away before the first
 * row, but the "File with id n was not found" of a sub-index file deleted by the drop was turned into the retry's
 * {@code IndexException} only once the whole index was invalid, which the drop does last, and the steps read the uniqueness
 * of the index on every entry, after the first row was computed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9615StaleIndexBeforeFirstRowTest extends TestHelper {
  private static final int  ROWS    = 5_000;
  private static final int  ROUNDS  = 40;
  private static final long RANGE   = 50;

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void rangeAndOrderBySelectsNeverFailBeforeTheirFirstRowWhileTheIndexIsDropped() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Person BUCKETS 8");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.transaction(() -> {
      for (long i = 0; i < ROWS; i++)
        database.newDocument("Person").set("id", i).save();
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong answered = new AtomicLong();
    final AtomicLong shortAnswers = new AtomicLong();
    final Map<String, AtomicLong> failuresBeforeFirstRow = new ConcurrentHashMap<>();
    final List<Thread> readers = new ArrayList<>();
    for (int t = 0; t < 8; t++) {
      final Random random = new Random(t);
      final int shape = t < 4 ? 0 : t < 6 ? 1 : 2;
      final Thread thread = new Thread(() -> {
        while (!stop.get()) {
          final long from = random.nextInt(ROWS - (int) RANGE);
          final long expected = shape == 0 ? RANGE : 1;
          int rows = 0;
          try (final ResultSet rs = switch (shape) {
            case 0 -> database.query("sql", "SELECT FROM Person WHERE id >= ? AND id < ?", from, from + RANGE);
            case 1 -> database.query("sql", "SELECT FROM Person WHERE id = ?", from);
            default -> database.query("sql", "SELECT FROM Person WHERE id = ? ORDER BY id", from);
          }) {
            while (rs.hasNext()) {
              rs.next();
              rows++;
            }
            if (rows != expected)
              shortAnswers.incrementAndGet();
            answered.incrementAndGet();
          } catch (final Throwable e) {
            // An index dropped after rows were handed over surfaces as an exception by design (see SelectStatement): only a
            // statement that has returned nothing can be, and must be, planned again
            if (rows == 0)
              failuresBeforeFirstRow.computeIfAbsent("shape " + shape + " " + describe(e), k -> new AtomicLong()).incrementAndGet();
          }
        }
      });
      readers.add(thread);
      thread.start();
    }

    try {
      for (int i = 0; i < ROUNDS; i++) {
        database.command("sql", "CREATE INDEX ON Person (id) NOTUNIQUE");
        database.command("sql", "DROP INDEX `Person[id]`");
      }
    } finally {
      stop.set(true);
      for (final Thread thread : readers)
        thread.join();
    }

    assertThat(answered.get()).isGreaterThan(0);
    assertThat(failuresBeforeFirstRow).as("statements answered: " + answered.get() + ", short: " + shortAnswers.get()).isEmpty();
  }

  private static String describe(final Throwable e) {
    final StringWriter trace = new StringWriter();
    e.printStackTrace(new PrintWriter(trace));
    final String[] lines = trace.toString().split("\n");
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < Math.min(lines.length, 14); i++)
      b.append(lines[i]).append('\n');
    return b.toString();
  }
}
