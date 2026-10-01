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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.HeapLimitExceededException;

import java.util.Collection;

/**
 * The limits on the heap one operation of a query holds in its buffer: the rows a sort or a join buffers, the keys a
 * DISTINCT remembers, the groups an aggregation keeps, the values a collect() gathers.
 * <ul>
 *   <li>{@link GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP} caps the elements the operation holds: past
 *   it the query fails with a {@link CommandExecutionException} naming the setting, instead of the server running out
 *   of memory (issue #8585). The setting is read once, when the operation starts, so the per-element check is a
 *   comparison.</li>
 *   <li>the estimated bytes of those elements are charged to the query's {@link QueryHeapTracker}, which reserves them
 *   from the {@link QueryHeapBudget} every query in the JVM shares (issue #8591). The operation gives them back with
 *   {@link #release()} when its buffer goes - on close, when it is exhausted, or when it fails.</li>
 * </ul>
 * The size of an element is estimated by {@link HeapEstimator} for the first {@link #EXACT_SAMPLES} elements and for one
 * in {@link #SAMPLE_EVERY} after them, the others being charged the average of those: the elements of one operation
 * share a shape, and a sort of millions of rows then pays for a fraction of the estimates. Charges reach the tracker
 * in batches of {@link #FORWARD_BYTES}, so the tracker's monitor is taken once per several kilobytes, not per row.
 * <p>
 * An operation nested in another one - the list a collect() gathers inside a GROUP BY - is a {@link #child(String)}:
 * it counts its own elements, and charges its bytes to its parent, which releases them with its own.
 * <p>
 * Not thread-safe: one operation is run by one thread. The workers of a parallel scan each use their own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class OperationHeapLimit {
  /** Elements whose size is always estimated, before the sampling starts. */
  static final int EXACT_SAMPLES = 64;
  /** Past {@link #EXACT_SAMPLES}, one element in this many is estimated and the others charged the average. */
  static final int SAMPLE_EVERY  = 16;
  /** Charges accumulate on the operation up to this many bytes before they reach the tracker. */
  static final int FORWARD_BYTES = 16 * 1024;

  private final long               maxElements;
  private final String             elementsName;
  private final String             operation;
  private final QueryHeapTracker   tracker;
  private final OperationHeapLimit parent;
  private       long               charged;
  private       long               pending;
  private       long               estimatedElements;
  private       long               estimatedBytes;
  private       int                sampleCountdown;

  private OperationHeapLimit(final long maxElements, final String elementsName, final String operation,
      final QueryHeapTracker tracker, final OperationHeapLimit parent) {
    this.maxElements = maxElements;
    this.elementsName = elementsName;
    this.operation = operation;
    this.tracker = tracker;
    this.parent = parent;
  }

  /**
   * @param context   the command context, whose database may override the element cap (null reads the global one) and
   *                  whose query's tracker is charged (none without a context, or while the budget is disabled)
   * @param operation what holds the elements, as the error messages name it (e.g. "ORDER BY", "Cartesian product")
   */
  public static OperationHeapLimit of(final CommandContext context, final String operation) {
    return of(context, "elements", operation);
  }

  /**
   * @param elementsName what the operation holds, as the error message of the element cap names it (e.g. "groups")
   */
  public static OperationHeapLimit of(final CommandContext context, final String elementsName, final String operation) {
    final Database database = context == null ? null : context.getDatabase();
    final long maxElements = database == null ?
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong() :
        database.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    final QueryHeapTracker tracker = context != null && QueryHeapBudget.isEnabled() ? context.getQueryHeapTracker() : null;
    return new OperationHeapLimit(maxElements, elementsName, operation, tracker, null);
  }

  /**
   * An operation nested in this one, under the same element cap, whose bytes are charged to this one and released with
   * it: the list a collect() gathers for one group of a GROUP BY.
   */
  public OperationHeapLimit child(final String operation) {
    return new OperationHeapLimit(maxElements, "elements", operation, tracker, this);
  }

  /**
   * Fails the query when the operation holds more elements than allowed.
   *
   * @param elements the number of elements the operation holds, the one being added included
   */
  public void check(final long elements) {
    if (isExceededBy(elements))
      throw new HeapLimitExceededException(
          "Limit of allowed " + elementsName + " for in-heap " + operation + " in a single query exceeded (" + maxElements
              + "). You can set " + GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey() + " to increase this limit");
  }

  /**
   * Fails the query when the operation holds more elements than allowed, after {@code release} lets go of what it holds:
   * the query fails, but the heap it took is given back at once rather than when the plan is collected.
   */
  public void check(final long elements, final Runnable release) {
    if (isExceededBy(elements)) {
      release.run();
      check(elements);
    }
  }

  /** Whether holding {@code elements} is past the element cap. */
  public boolean isExceededBy(final long elements) {
    return maxElements > 0 && elements > maxElements;
  }

  /**
   * Accounts one more element held by the operation against both limits.
   *
   * @param elements the number of elements the operation holds, {@code element} included
   * @param element  the element, whose estimated size is charged
   */
  public void add(final long elements, final Object element) {
    check(elements);
    if (tracker != null)
      charge(estimate(element));
  }

  /**
   * Like {@link #add(long, Object)}, for an element held with {@code overheadBytes} of structure around it that the
   * element itself does not show: the entry of the hash map that holds a group, for instance.
   */
  public void add(final long elements, final Object element, final int overheadBytes) {
    check(elements);
    chargeElement(element, overheadBytes);
  }

  /**
   * Charges the estimated size of one more element held, with {@code overheadBytes} of structure around it, for an
   * operation that enforces its element cap on its own.
   */
  public void chargeElement(final Object element, final int overheadBytes) {
    if (tracker != null)
      charge(estimate(element) + overheadBytes);
  }

  /**
   * Charges {@code bytes} more held by the operation, for which the query may be refused (see {@link QueryHeapTracker}).
   * A refused charge is not counted, as the tracker does not count it either: what the operation holds charged stays
   * what it held before, so an owner that gives back its own share (a join buffer sharing its operation with a hash
   * table) never gives back more or less than it was charged.
   */
  public void charge(final long bytes) {
    if (tracker == null || bytes <= 0)
      return;
    if (parent != null) {
      parent.charge(bytes);
      charged += bytes;
      return;
    }
    pending += bytes;
    if (pending >= FORWARD_BYTES) {
      try {
        tracker.charge(pending, operation);
      } catch (final RuntimeException e) {
        pending -= bytes;
        throw e;
      }
      pending = 0L;
    }
    charged += bytes;
  }

  /**
   * Adjusts the charge, in one step, to what {@code elements} - everything the operation holds now - are estimated to
   * take: a buffer that dropped some of its elements (a top-N sort keeping its best rows) gives back what the dropped
   * ones took, whatever their size. Every element is estimated, not sampled: the ones kept are the exception, not the
   * average of what went through.
   */
  public void rechargeAll(final Collection<?> elements) {
    if (tracker == null)
      return;
    long bytes = 0L;
    for (final Object element : elements)
      bytes += HeapEstimator.estimate(element) + HeapEstimator.REFERENCE_BYTES;
    if (bytes < charged)
      release(charged - bytes);
    else
      charge(bytes - charged);
  }

  /** Adjusts the charge for {@code added} taking the place of {@code removed}: a full top-K heap keeping a better row. */
  public void replace(final Object removed, final Object added) {
    if (tracker == null)
      return;
    final long delta = HeapEstimator.estimate(added) - HeapEstimator.estimate(removed);
    if (delta > 0)
      charge(delta);
    else
      release(-delta);
  }

  /** Gives back {@code bytes} of what the operation charged: its buffer shrank. */
  public void release(final long bytes) {
    final long released = Math.min(bytes, charged);
    if (released <= 0)
      return;
    charged -= released;
    if (parent != null) {
      parent.release(released);
      return;
    }
    final long fromPending = Math.min(released, pending);
    pending -= fromPending;
    if (released > fromPending)
      tracker.release(released - fromPending);
  }

  /**
   * Takes over what {@code other} charged - its buffer is now held by this operation - without the tracker seeing it:
   * the query holds the same bytes before and after, so no other query can take them in between. Only between the top
   * operations of one query, as the workers of a parallel scan and the step merging their partials are.
   *
   * @return false, taking nothing over, when the two do not charge the same query: the budget was switched on or off
   * between their creations
   */
  public boolean transferFrom(final OperationHeapLimit other) {
    if (tracker == null || other.tracker != tracker || parent != null || other.parent != null)
      return false;
    charged += other.charged;
    pending += other.pending;
    other.charged = 0L;
    other.pending = 0L;
    return true;
  }

  /** Gives back everything the operation charged: its buffer is gone. Idempotent. */
  public void release() {
    release(charged);
  }

  /** The estimated bytes the operation holds charged. */
  public long getChargedBytes() {
    return charged;
  }

  /** Whether the bytes are charged at all: not without a context, nor while the budget is disabled. */
  public boolean isCharging() {
    return tracker != null;
  }

  /** The maximum number of elements, or a non-positive number when there is no limit. */
  public long getMaxElements() {
    return maxElements;
  }

  private long estimate(final Object element) {
    if (estimatedElements < EXACT_SAMPLES || ++sampleCountdown == SAMPLE_EVERY) {
      sampleCountdown = 0;
      final long bytes = HeapEstimator.estimate(element) + HeapEstimator.REFERENCE_BYTES;
      estimatedBytes += bytes;
      ++estimatedElements;
      return bytes;
    }
    return estimatedBytes / estimatedElements;
  }
}
