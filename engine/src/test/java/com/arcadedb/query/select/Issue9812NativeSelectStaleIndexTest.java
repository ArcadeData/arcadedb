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
import com.arcadedb.database.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9812: the native {@code database.select()} falls back to a scan when the index it reads is dropped under it, but its
 * catch asked {@code isValid()}, which {@code TypeIndex.drop()} clears last: during the whole window in which the sub-index
 * files are already gone the "File with id n was not found" reached the caller. The equality lookup is read whole before the
 * first row, so it must also never answer short when a drop took sub-indexes away under it without an error (#9651).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9812NativeSelectStaleIndexTest extends TestHelper {
  private static final int  ROWS   = 5_000;
  private static final int  ROUNDS = 40;
  private static final long RANGE  = 50;

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void nativeSelectsNeverFailBeforeTheirFirstRowWhileTheIndexIsDropped() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Person BUCKETS 8");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.transaction(() -> {
      for (long i = 0; i < ROWS; i++)
        database.newDocument("Person").set("id", i).save();
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong answered = new AtomicLong();
    final AtomicLong shortPointAnswers = new AtomicLong();
    final Map<String, AtomicLong> failuresBeforeFirstRow = new ConcurrentHashMap<>();
    final List<Thread> readers = new ArrayList<>();
    for (int t = 0; t < 6; t++) {
      final Random random = new Random(t);
      final boolean range = t < 3;
      final Thread thread = new Thread(() -> {
        while (!stop.get()) {
          final long from = random.nextInt(ROWS - (int) RANGE);
          int rows = 0;
          try {
            final Iterator<? extends Document> it = range ?
                database.select().fromType("Person").where().property("id").ge().value(from).and().property("id").lt()
                    .value(from + RANGE).documents() :
                database.select().fromType("Person").where().property("id").eq().value(from).documents();
            while (it.hasNext()) {
              it.next();
              rows++;
            }
            if (!range && rows != 1)
              shortPointAnswers.incrementAndGet();
            answered.incrementAndGet();
          } catch (final Throwable e) {
            // An index dropped after rows were handed over cannot be read again from the start: only a select that has
            // returned nothing can, and must, answer without it
            if (rows == 0)
              failuresBeforeFirstRow.computeIfAbsent((range ? "range " : "point ") + e, k -> new AtomicLong()).incrementAndGet();
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
    assertThat(failuresBeforeFirstRow).as("selects answered: " + answered.get()).isEmpty();
    assertThat(shortPointAnswers.get()).as("point lookups that answered no row for an id every record set holds").isZero();
  }
}
