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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writers that each own a disjoint set of records but share the pages of a small bucket: inserts, updates that grow or shrink a
 * record (up to several pages) and deletes, committed or rolled back, while readers scan the whole type. A transaction that
 * lost the race for a page is retried by its writer, so whatever the interleaving, at the end the type must hold exactly the
 * union of what each writer committed (no lost update, no resurrected delete, no foreign record) and no reader may ever have
 * seen a torn record: every record carries the seed its text is generated from.
 */
@Tag("slow")
class SharedPageWritersContractTest extends TestHelper {

  private record Rec(int seed, int length) {
  }

  @ParameterizedTest(name = "page size {0}, {1} writers, longest record {2} pages")
  @CsvSource({ "12298, 4, 0", "12298, 6, 2", "65536, 4, 1" })
  void theTypeHoldsTheUnionOfWhatEveryWriterCommitted(final int pageSize, final int writers, final int longestRecordInPages) throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, pageSize);
    final DocumentType type = database.getSchema().buildDocumentType().withName("B").withTotalBuckets(1).create();
    type.createProperty("owner", Type.INTEGER);
    type.createProperty("seed", Type.INTEGER);
    type.createProperty("length", Type.INTEGER);
    type.createProperty("data", Type.STRING);

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Map<String, Rec>> committed = new ArrayList<>();
    final List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      final int owner = w;
      final Map<String, Rec> mine = new LinkedHashMap<>();
      committed.add(mine);
      threads.add(new Thread(() -> write(owner, mine, pageSize, longestRecordInPages, stop, failure), "writer-" + owner));
    }
    for (int r = 0; r < 2; r++)
      threads.add(new Thread(() -> read(stop, failure), "reader-" + r));

    threads.forEach(Thread::start);
    Thread.sleep(2_000);
    stop.set(true);
    for (final Thread thread : threads)
      thread.join(60_000);
    assertThat(failure.get()).as("a worker failed").isNull();

    final Map<String, Rec> expected = new HashMap<>();
    committed.forEach(expected::putAll);
    final Map<String, Rec> actual = new HashMap<>();
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM B")) {
      while (resultSet.hasNext()) {
        final Result result = resultSet.next();
        final int seed = result.<Number>getProperty("seed").intValue();
        final int length = result.<Number>getProperty("length").intValue();
        assertThat(result.<String>getProperty("data")).as("record %s", result.getIdentity().get()).isEqualTo(text(seed, length));
        actual.put(result.getIdentity().get().toString(), new Rec(seed, length));
      }
    }
    assertThat(actual).isEqualTo(expected);

    reopenDatabase();
    assertThat(database.countType("B", true)).isEqualTo(expected.size());
  }

  private void write(final int owner, final Map<String, Rec> mine, final int pageSize, final int longestRecordInPages, final AtomicBoolean stop,
      final AtomicReference<Throwable> failure) {
    final Random random = new Random(owner * 7919L + 1);
    int tag = owner * 1_000_000;
    while (!stop.get() && failure.get() == null) {
      final Map<String, Rec> work = new LinkedHashMap<>(mine);
      try {
        database.begin();
        final int operations = 1 + random.nextInt(6);
        for (int i = 0; i < operations; i++) {
          final int op = random.nextInt(10);
          if (op < 4 || work.isEmpty()) {
            final int length = length(random, pageSize, longestRecordInPages);
            final int seed = tag++;
            final MutableDocument document = database.newDocument("B").set("owner", owner).set("seed", seed).set("length", length)
                .set("data", text(seed, length));
            document.save();
            work.put(document.getIdentity().toString(), new Rec(seed, length));
          } else if (op < 8) {
            final String rid = pick(work, random);
            final int length = length(random, pageSize, longestRecordInPages);
            final int seed = tag++;
            final MutableDocument document = database.lookupByRID(database.newRID(rid), true).asDocument().modify();
            document.set("seed", seed).set("length", length).set("data", text(seed, length));
            document.save();
            work.put(rid, new Rec(seed, length));
          } else {
            final String rid = pick(work, random);
            database.lookupByRID(database.newRID(rid), true).asDocument().delete();
            work.remove(rid);
          }
        }
        if (random.nextInt(8) == 0)
          database.rollback();
        else {
          database.commit();
          mine.clear();
          mine.putAll(work);
        }
      } catch (final NeedRetryException e) {
        // lost the race for a page: the writer starts over from what it committed
        if (database.isTransactionActive())
          database.rollback();
      } catch (final Throwable e) {
        failure.compareAndSet(null, e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }
  }

  private void read(final AtomicBoolean stop, final AtomicReference<Throwable> failure) {
    while (!stop.get() && failure.get() == null) {
      try (final ResultSet resultSet = database.query("sql", "SELECT FROM B")) {
        while (resultSet.hasNext()) {
          final Result result = resultSet.next();
          final int seed = result.<Number>getProperty("seed").intValue();
          final int length = result.<Number>getProperty("length").intValue();
          assertThat(result.<String>getProperty("data")).as("torn record %s", result.getIdentity().get()).isEqualTo(text(seed, length));
        }
      } catch (final NeedRetryException e) {
        // a scan is allowed to be asked to start again
      } catch (final Throwable e) {
        failure.compareAndSet(null, e);
      }
    }
  }

  private static int length(final Random random, final int pageSize, final int longestRecordInPages) {
    final int r = random.nextInt(100);
    if (r < 60 || longestRecordInPages == 0)
      return random.nextInt(150);
    if (r < 85)
      return random.nextInt(2_000);
    return random.nextInt(pageSize * longestRecordInPages);
  }

  private static String pick(final Map<String, Rec> records, final Random random) {
    final int target = random.nextInt(records.size());
    int i = 0;
    for (final String rid : records.keySet())
      if (i++ == target)
        return rid;
    throw new IllegalStateException();
  }

  private static String text(final int seed, final int length) {
    final StringBuilder builder = new StringBuilder(length);
    int x = seed * 31 + 7;
    for (int i = 0; i < length; i++) {
      x = x * 1103515245 + 12345;
      builder.append((char) ('a' + ((x >>> 16) & 15)));
    }
    return builder.toString();
  }
}
