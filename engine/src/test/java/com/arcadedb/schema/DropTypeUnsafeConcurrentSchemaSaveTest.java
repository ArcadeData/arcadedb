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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A DROP TYPE ... UNSAFE runs under the database write lock, but a schema save made by another thread without that lock (the
 * swap at the end of an index compaction, a vector index persisting its build state) refreshes the bucket-to-type maps. While a
 * type is being dropped that refresh always rebuilds them from the type graph, so it can snapshot the graph before the drop
 * detaches a bucket from the type and publish it after the detach reset the "stale" flag: the dropBucket() that follows the
 * detach then reads an owner the type no longer has and refuses with "is assigned to type" (issue #13696).
 */
@Tag("slow")
class DropTypeUnsafeConcurrentSchemaSaveTest extends TestHelper {

  private static final long RUN_MS        = 30_000L;
  private static final int  PADDING_TYPES = 100;
  private static final int  BUCKETS       = 16;
  /**
   * The saver's pause between two saves. The schema monitor is not fair: a saver that takes it again the moment it lets it
   * go starves the DDL thread, which takes it at the entry and the save of every frame, down to 2 or 3 cycles in 30 s, and
   * a run that starved is not a run that raced. The real savers (a compaction swap, a vector index persisting its build
   * state) do not loop either.
   */
  private static final long SAVER_PAUSE_NS = 20_000L;
  /**
   * Fewer DDL cycles than this means the loop was starved rather than raced: on main the race hit within 1 to 5 cycles, and the
   * fixed code runs about 1600 in 30 s on a laptop, so a green run below this proves nothing and a slow CI runner still clears it.
   */
  private static final long MIN_CYCLES    = 100L;

  private final AtomicLong                     saverAssertions = new AtomicLong();
  private final AtomicLong                     saves           = new AtomicLong();
  private final ConcurrentLinkedQueue<Throwable> saverFailures = new ConcurrentLinkedQueue<>();

  @Test
  void dropTypeUnsafeSurvivesConcurrentSchemaSaves() throws Exception {
    // more types make each rebuild of the bucket maps longer, which widens the window the race needs
    for (int i = 0; i < PADDING_TYPES; i++)
      database.getSchema().createDocumentType("Padding" + i);

    final AtomicBoolean stop = new AtomicBoolean();
    final Thread saver = new Thread(() -> {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      while (!stop.get())
        try {
          schema.saveConfiguration();
          saves.incrementAndGet();
          LockSupport.parkNanos(SAVER_PAUSE_NS);
        } catch (final AssertionError e) {
          // under -ea the refresh asserts the published maps agree with the type graph: a save that raced the drop on the
          // DDL thread used to trip it. Counted, so the thread outlives it and the test reports it, rather than swallowed
          saverAssertions.incrementAndGet();
        } catch (final Throwable e) {
          // anything else ends the adversary, and a test whose adversary died would pass with nothing racing it
          saverFailures.add(e);
          return;
        }
    }, "schema-saver");

    runDdlLoopAgainst(saver, stop, "SaveScratch");
  }

  private void runDdlLoopAgainst(final Thread concurrent, final AtomicBoolean stop, final String prefix) throws Exception {
    final ConcurrentLinkedQueue<Throwable> refusals = new ConcurrentLinkedQueue<>();
    final AtomicLong cycles = new AtomicLong();

    concurrent.start();
    try {
      final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RUN_MS);
      for (long n = 0; System.nanoTime() < deadline && refusals.isEmpty() && saverFailures.isEmpty(); n++) {
        final String type = prefix + n;
        database.command("sql", "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS BUCKETS " + BUCKETS).close();
        database.command("sql", "CREATE PROPERTY " + type + ".v IF NOT EXISTS LONG").close();
        database.command("sql", "CREATE INDEX IF NOT EXISTS ON " + type + " (v) NOTUNIQUE").close();
        try {
          database.command("sql", "DROP TYPE " + type + " IF EXISTS UNSAFE").close();
        } catch (final SchemaException e) {
          refusals.add(e);
        }
        cycles.incrementAndGet();
      }
    } finally {
      stop.set(true);
      concurrent.join(30_000L);
    }

    final StringWriter firstStack = new StringWriter();
    if (!refusals.isEmpty())
      refusals.peek().printStackTrace(new PrintWriter(firstStack));
    assertThat(refusals).as("DROP TYPE ... UNSAFE refusals over %d cycles (%d racing-save assertions on the other thread)%n%s",
        cycles.get(), saverAssertions.get(), firstStack).isEmpty();
    // a save made outside the write lock no longer reads the bucket maps a frame of another thread is changing
    assertThat(saverAssertions.get()).as("bucket map assertions tripped on the saving thread over %d cycles", cycles.get())
        .isZero();

    // the adversary was there for the whole run: it saved, it did not die of anything else, and it ended when asked
    final StringWriter saverStack = new StringWriter();
    if (!saverFailures.isEmpty())
      saverFailures.peek().printStackTrace(new PrintWriter(saverStack));
    assertThat(saverFailures).as("unexpected failures of the saving thread%n%s", saverStack).isEmpty();
    assertThat(saves.get()).as("schema saves made by the other thread over %d cycles", cycles.get()).isPositive();
    assertThat(concurrent.isAlive()).as("the saving thread ended after the loop").isFalse();
    assertThat(cycles.get()).as("DDL cycles run against %d concurrent saves", saves.get()).isGreaterThanOrEqualTo(MIN_CYCLES);
  }
}
