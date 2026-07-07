package com.arcadedb.query.select;/*
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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.async.DatabaseAsyncBrowseIterator;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.utility.MultiIterator;
import com.conversantmedia.util.concurrent.MultithreadConcurrentQueue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Query iterator returned from queries. Extends the base Java iterator with convenient methods.
 *
 * <h4>Implementation details</h4>
 * <p>
 * The iterator keeps track for the returned records in case multiple indexes have being used. In fact, in case multiple
 * indexes are used, it's much simpler to just return index cursor tha could overlap. In this case, the property
 * `filterOutRecords` keeps track of the returning RIDs to avoid returning duplicates.</p>
 *
 * <p>
 * This iterator creates one task per bucket and executes the fetching in parallel on the database async workers. The
 * fetched records are handed to the consumer through a fixed-size concurrent queue. A producer facing a full queue waits
 * with a bounded park instead of busy-spinning (#5065): it gives up as soon as the iterator is closed, the worker is
 * interrupted (executor shutdown), enough records were produced to satisfy the limit, or the consumer made no progress
 * for the whole stall bound (the select timeout when set, otherwise {@link #DEFAULT_STALL_TIMEOUT_MS}). A producer that
 * gives up ends its task through the regular completion path, so the async executor contracts (completed() notification,
 * batch commit) are preserved.</p>
 *
 * <p>
 * The consumer mirrors the cooperative backoff: on an empty queue it spins briefly for low hand-off latency, then parks
 * between polls. The select timeout is enforced on every fetch, including when records are immediately available, matching
 * the serial path semantics.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectParallelIterator<T extends Document> extends SelectIterator<T> {
  // HOW LONG A PRODUCER WAITS ON A FULL QUEUE WITH ZERO CONSUMER PROGRESS BEFORE CONCLUDING THE CONSUMER IS GONE
  // (#5065). ALIGNED WITH THE ASYNC EXECUTOR ZERO-PROGRESS BACKSTOP INTRODUCED WITH #5062 (60 SECONDS BY DEFAULT).
  // A CONSUMER THAT POLLS NOTHING FOR THE WHOLE BOUND WHILE THE QUEUE IS FULL IS INDISTINGUISHABLE FROM A DEAD ONE
  private static final long DEFAULT_STALL_TIMEOUT_MS   = 60_000;
  // PAUSE BETWEEN OFFER ATTEMPTS ON A FULL QUEUE: 0.1 MS KEEPS THE HAND-OFF LATENCY NEGLIGIBLE WITHOUT BURNING CPU
  private static final long OFFER_PARK_NANOS           = 100_000;
  // EMPTY-QUEUE POLLS BEFORE THE CONSUMER SWITCHES FROM SPINNING TO PARKING (SEE fetchNext())
  private static final int  CONSUMER_SPINS_BEFORE_PARK = 64;

  private final    CountDownLatch                       semaphore;
  private final    MultithreadConcurrentQueue<Document> queue;
  // COUNTS THE RECORDS ENQUEUED BY THE PRODUCERS (POST WHERE/DEDUPLICATION), USED TO STOP THEM AT THE LIMIT. THE
  // INHERITED `returned` FIELD COUNTS THE RECORDS HANDED TO THE CONSUMER, MATCHING THE SERIAL SEMANTICS (SEE hasNext())
  private final    AtomicLong                           produced = new AtomicLong();
  private volatile boolean                              closed   = false;
  private volatile boolean                              producerStalled;

  protected SelectParallelIterator(final SelectExecutor executor, final Iterator<? extends Identifiable> iterator,
      final boolean enforceUniqueReturn) {
    super(executor, iterator, enforceUniqueReturn);

    if (iterator instanceof MultiIterator<? extends Identifiable> it) {
      queue = new MultithreadConcurrentQueue<>(4_096);

      final List<Object> sources = it.getSources();

      final DatabaseInternal database = executor.select.database;
      final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
      final int backPressurePercentage = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_BACK_PRESSURE);

      final int limit = executor.select.limit;
      final long stallTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(
          executor.select.timeoutInMs > 0 ? executor.select.timeoutInMs : DEFAULT_STALL_TIMEOUT_MS);

      semaphore = new CountDownLatch(sources.size());

      for (int i = 0; i < sources.size(); i++) {
        final Iterator<? extends Identifiable> source = (Iterator<? extends Identifiable>) sources.get(i);

        async.scheduleTask(-1, new DatabaseAsyncBrowseIterator(semaphore, record -> {
          if (closed)
            // THE CONSUMER CLOSED THE ITERATOR OR GAVE UP: STOP BROWSING. RETURNING FALSE ENDS THE TASK THROUGH THE
            // REGULAR COMPLETION PATH (completed() + SEMAPHORE COUNTDOWN), WITHOUT THROWING THROUGH THE ASYNC WORKER
            return false;

          if (filterOutRecords != null && filterOutRecords.contains(record.getIdentity()))
            // ALREADY RETURNED, AVOID DUPLICATES IN THE RESULT SET
            return true;

          if (executor.select.rootTreeElement == null || executor.evaluateWhere(record)) {
            if (limit > -1 && produced.incrementAndGet() > limit)
              // ENOUGH RECORDS PRODUCED TO SATISFY THE LIMIT: THE CONSUMER CANNOT TAKE MORE, STOP THIS PRODUCER
              // (BEFORE REGISTERING THE RECORD AMONG THE RETURNED ONES, SINCE IT IS NEVER HANDED OVER)
              return false;

            if (filterOutRecords != null)
              filterOutRecords.add(record.getIdentity());

            // BOUNDED OFFER (#5065): THE FORMER UNBOUNDED while(true) BUSY-SPIN PINNED AN ASYNC WORKER AT 100% CPU
            // FOREVER (AND HUNG Database.close() BEHIND ITS UNBOUNDED waitCompletion()) WHEN THE CONSUMER STOPPED
            // DRAINING BECAUSE OF AN EXCEPTION, AN EARLY CLOSE OR A SATISFIED LIMIT
            boolean waiting = false;
            long waitStart = 0L;
            while (!queue.offer(record)) {
              if (closed || Thread.currentThread().isInterrupted())
                return false;

              final long now = System.nanoTime();
              if (!waiting) {
                waiting = true;
                waitStart = now;
              } else if (now - waitStart >= stallTimeoutNanos) {
                // THE CONSUMER MADE NO PROGRESS FOR THE WHOLE BOUND: GIVE UP INSTEAD OF SPINNING FOREVER. A CONSUMER
                // STILL ALIVE IS TOLD THE RESULT SET WOULD BE INCOMPLETE (SEE fetchNext())
                producerStalled = true;
                closed = true;
                return false;
              }

              LockSupport.parkNanos(OFFER_PARK_NANOS);
            }
          }
          return true;
        }, source), true, backPressurePercentage);
      }

    } else {
      queue = null;
      semaphore = null;
    }
  }

  @Override
  protected T fetchNext() {
    if (!(iterator instanceof MultiIterator))
      // NO CHANCE TO GO IN PARALLEL, FALL BACK TO THE SERIAL IMPLEMENTATION
      return super.fetchNext();

    if (queue == null)
      // INVOKED BY THE BASE CLASS CONSTRUCTOR (SKIP CONSUMPTION) BEFORE THE PARALLEL MACHINERY IS INITIALIZED AND THE
      // PRODUCERS ARE SCHEDULED: CONSUME SERIALLY FROM THE UNDERLYING ITERATOR
      return super.fetchNext();

    final MultiIterator<? extends Identifiable> it = (MultiIterator<? extends Identifiable>) iterator;

    int emptyPolls = 0;
    while (true) {
      if (producerStalled)
        return onProducerStalled();

      if (closed)
        return null;

      // ENFORCE THE SELECT TIMEOUT ON EVERY FETCH, INCLUDING WHEN A RECORD IS IMMEDIATELY AVAILABLE: THE SERIAL PATH
      // ALWAYS CHECKED IT AT THE TOP OF fetchNext(), SO A SLOW CONSUMER OF AN ALWAYS-READY QUEUE MUST STILL TIME OUT.
      // THE CHECK IS A SHORT-CIRCUITED COMPARISON WHEN NO TIMEOUT IS SET, NEGLIGIBLE PER RECORD OTHERWISE
      try {
        if (it.checkForTimeout()) {
          // TIMEOUT WITHOUT EXCEPTION REQUESTED: STOP THE PRODUCERS AND RETURN WHAT WAS FETCHED SO FAR
          closed = true;
          return null;
        }
      } catch (final TimeoutException e) {
        // STOP THE PRODUCERS BEFORE SURFACING THE TIMEOUT TO THE CALLER
        closed = true;
        throw e;
      }

      final T record = (T) queue.poll();
      if (record != null) {
        ++returned;
        return record;
      }

      if (semaphore.getCount() == 0) {
        // ALL THE PRODUCERS ARE DONE. EVERY OFFER HAPPENS BEFORE THE PRODUCER'S COUNTDOWN, SO ONE LAST POLL DRAINS A
        // RECORD PUBLISHED BETWEEN THE POLL ABOVE AND THE LAST COUNTDOWN
        final T last = (T) queue.poll();
        if (last != null) {
          ++returned;
          return last;
        }
        return producerStalled ? onProducerStalled() : null;
      }

      // COOPERATIVE BACKOFF ON THE EMPTY QUEUE, MIRRORING THE PRODUCER SIDE: SPIN BRIEFLY TO KEEP THE HAND-OFF LATENCY
      // LOW ON A TRANSIENT GAP, THEN PARK BETWEEN POLLS SO A SLOW PRODUCER (E.G. A HEAVY WHERE SCANNING MANY
      // NON-MATCHING ROWS) DOES NOT PIN THE CALLER THREAD AT 100% CPU
      if (++emptyPolls <= CONSUMER_SPINS_BEFORE_PARK)
        Thread.onSpinWait();
      else
        LockSupport.parkNanos(OFFER_PARK_NANOS);
    }
  }

  /**
   * Stops the background producers. Each producer ends its async task through the regular completion path, releasing
   * the worker within one park cycle. Idempotent and safe to call from any thread.
   */
  @Override
  public void close() {
    closed = true;
  }

  private T onProducerStalled() {
    // A PRODUCER GAVE UP AFTER WAITING THE WHOLE STALL BOUND ON THE FULL QUEUE, SO THE RESULT SET WOULD BE INCOMPLETE.
    // WITH AN EXPLICIT NON-THROWING TIMEOUT THE TRUNCATION IS WHAT THE CALLER ASKED FOR, OTHERWISE FAIL LOUD
    if (executor.select.timeoutInMs > 0 && !executor.select.exceptionOnTimeout)
      return null;
    throw new TimeoutException(
        "Timeout on parallel select: the producers waited more than " + (executor.select.timeoutInMs > 0 ?
            executor.select.timeoutInMs :
            DEFAULT_STALL_TIMEOUT_MS) + " ms for the consumer to fetch records, the result set would be incomplete");
  }
}
