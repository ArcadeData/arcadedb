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

import com.arcadedb.database.RID;
import com.arcadedb.schema.MaterializedViewGroupedPlan.Aggregate;
import com.arcadedb.schema.MaterializedViewGroupedPlan.Contribution;
import com.arcadedb.serializer.BinaryComparator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The aggregate state of a grouped materialized view maintained by delta (issue #11937), and the deltas applied to it.
 * <p>
 * A {@link Groups} map holds, per group, everything the row of the group is computed from: the member count (whether the row
 * exists, and {@code count(*)}), and per aggregate the count of non-null arguments, their exact {@code long} total, the total of
 * their absolute values and how many were {@code Long}s (the class of {@code sum}), and for {@code min}/{@code max} the extreme
 * and the one class the group's values have. Kept in memory by the view, built by the rebuild pass (a scan of the source) and
 * moved forward by each commit's delta; never persisted, so a reopen rebuilds it.
 * <p>
 * The same map type is the delta of a commit, where the counts are signed and {@code min}/{@code max} keep the extreme of the values
 * added and the extreme-side value of the ones removed. Every combination is commutative, so the deltas of concurrent commits
 * merge in any order. What cannot be applied exactly is answered, never guessed: a removed value that may have been the current
 * {@code min}/{@code max} asks for a rebuild, and a value or a total outside what the query computes order-independently (see
 * {@link MaterializedViewGroupedPlan}) for the full pass.
 */
final class MaterializedViewGroupedState {
  /** The share of the heap one view's state may take: a thirty-second, so a few views leave the rest to the application. */
  private static final long HEAP_SHARE_DIVISOR   = 32;
  /** What a group costs besides its aggregates: the group, its key list and map entry, its raw keys and its backing record's RID. */
  private static final long BYTES_PER_GROUP      = 192;
  /** What each aggregate of a group costs: the {@link Aggregation} and the boxed values it holds. */
  private static final long BYTES_PER_AGGREGATE  = 96;
  /** Whatever the heap, a state of this many groups is always allowed. */
  private static final long MIN_GROUPS           = 10_000;
  /** Test-only: the cap on the groups of a state, when above 0. */
  static volatile int maxGroupsOverride;

  private MaterializedViewGroupedState() {
  }

  /**
   * The most groups a state may hold: the state lives in memory for as long as the database is open, and the number of groups is the
   * cardinality of the GROUP BY keys, which the source decides (a user id, a device id). Past it the view is not maintained by delta
   * but by the defining query, as when its values leave the exact domain, which holds nothing. Derived from the heap, so that no
   * setting has to be tuned: a thirty-second of it per view, at the estimated cost of a group of the plan.
   */
  static int maxGroups(final MaterializedViewGroupedPlan plan) {
    final int override = maxGroupsOverride;
    if (override > 0)
      return override;
    final long perGroup = BYTES_PER_GROUP + BYTES_PER_AGGREGATE * plan.aggregates.length;
    return (int) Math.min(Integer.MAX_VALUE, Math.max(MIN_GROUPS, Runtime.getRuntime().maxMemory() / HEAP_SHARE_DIVISOR / perGroup));
  }

  /** What applying a delta needs instead, when it cannot be applied. */
  enum Outcome {
    APPLIED,
    /** A group lost its current {@code min}/{@code max}, or the state and the delta disagree: rebuild from the source. */
    REBUILD,
    /** A value or a total that the query computes in an order-dependent way: only the defining query itself is exact. */
    INEXACT
  }

  static final class Aggregation {
    long     nonNull;
    long     sum;
    long     absoluteSum;
    long     longs;
    /** {@code min}/{@code max}: the current extreme in a state, the extreme of the added values in a delta. */
    Object   extreme;
    /** {@code min}/{@code max} of a delta: the smallest removed value for {@code min}, the largest for {@code max}. */
    Object   removed;
    Class<?> valueClass;

    Aggregation copy() {
      final Aggregation copy = new Aggregation();
      copy.nonNull = nonNull;
      copy.sum = sum;
      copy.absoluteSum = absoluteSum;
      copy.longs = longs;
      copy.extreme = extreme;
      copy.removed = removed;
      copy.valueClass = valueClass;
      return copy;
    }

    boolean isEmpty() {
      return nonNull == 0 && sum == 0 && absoluteSum == 0 && longs == 0 && extreme == null && removed == null;
    }
  }

  static final class Group {
    /** The values of the key the row shows: one class per position for the whole group, so any member's are the row's. */
    Object[]            rawKeys;
    /** The backing record of the group's row, in a state; null in a delta and for a group whose row is not written yet. */
    RID                 row;
    long                members;
    final Aggregation[] aggregations;

    Group(final int aggregates) {
      aggregations = new Aggregation[aggregates];
      for (int i = 0; i < aggregates; i++)
        aggregations[i] = new Aggregation();
    }

    Group copy() {
      final Group copy = new Group(aggregations.length);
      copy.rawKeys = rawKeys;
      copy.row = row;
      copy.members = members;
      for (int i = 0; i < aggregations.length; i++)
        copy.aggregations[i] = aggregations[i].copy();
      return copy;
    }

    boolean isEmpty() {
      if (members != 0)
        return false;
      for (final Aggregation aggregation : aggregations)
        if (!aggregation.isEmpty())
          return false;
      return true;
    }
  }

  /**
   * Groups by their normalized key ({@link Contribution#key}). Used both as the state of a view and as a delta. {@code inexact} is
   * set, and stays, once a contribution or a total left the exact domain.
   */
  static final class Groups {
    final MaterializedViewGroupedPlan plan;
    final Map<List<Object>, Group>    groups = new HashMap<>();
    boolean                           inexact;

    Groups(final MaterializedViewGroupedPlan plan) {
      this.plan = plan;
    }

    int size() {
      return groups.size();
    }

    /** Adds ({@code sign} 1) or removes ({@code sign} -1) one record's contribution. A null contribution is no record. */
    void add(final Contribution contribution, final int sign) {
      if (contribution == null || inexact)
        return;
      if (contribution == Contribution.INEXACT) {
        inexact = true;
        return;
      }
      final Group group = groups.computeIfAbsent(contribution.key, k -> new Group(plan.aggregates.length));
      if (group.rawKeys == null)
        group.rawKeys = contribution.rawKeys;
      else if (!sameClasses(group.rawKeys, contribution.rawKeys)) {
        inexact = true;
        return;
      }
      try {
        group.members = Math.addExact(group.members, sign);
        for (int i = 0; i < plan.aggregates.length; i++) {
          final Aggregate aggregate = plan.aggregates[i];
          final Object value = contribution.arguments[i];
          if (aggregate == Aggregate.COUNT_STAR || value == null)
            continue;
          final Aggregation aggregation = group.aggregations[i];
          aggregation.nonNull = Math.addExact(aggregation.nonNull, sign);
          switch (aggregate) {
          case SUM, AVG -> {
            final long number = ((Number) value).longValue();
            aggregation.sum = Math.addExact(aggregation.sum, sign > 0 ? number : Math.negateExact(number));
            final long absolute = Math.absExact(number);
            aggregation.absoluteSum = Math.addExact(aggregation.absoluteSum, sign > 0 ? absolute : -absolute);
            if (value instanceof Long)
              aggregation.longs += sign;
          }
          case MIN, MAX -> {
            if (aggregation.valueClass == null)
              aggregation.valueClass = value.getClass();
            else if (aggregation.valueClass != value.getClass()) {
              inexact = true;
              return;
            }
            if (sign > 0)
              aggregation.extreme = better(aggregate, aggregation.extreme, value);
            else
              aggregation.removed = better(aggregate, aggregation.removed, value);
          }
          default -> {
          }
          }
        }
      } catch (final ArithmeticException e) {
        inexact = true;
      }
    }

    /** Folds another delta into this one: the result is the delta of both, whatever the order they committed in. */
    void merge(final Groups other) {
      if (inexact)
        return;
      if (other.inexact) {
        inexact = true;
        return;
      }
      try {
        for (final Map.Entry<List<Object>, Group> entry : other.groups.entrySet()) {
          final Group that = entry.getValue();
          final Group group = groups.get(entry.getKey());
          if (group == null) {
            groups.put(entry.getKey(), that.copy());
            continue;
          }
          if (!sameClasses(group.rawKeys, that.rawKeys)) {
            inexact = true;
            return;
          }
          group.members = Math.addExact(group.members, that.members);
          for (int i = 0; i < plan.aggregates.length; i++) {
            final Aggregate aggregate = plan.aggregates[i];
            final Aggregation mine = group.aggregations[i];
            final Aggregation theirs = that.aggregations[i];
            mine.nonNull = Math.addExact(mine.nonNull, theirs.nonNull);
            mine.sum = Math.addExact(mine.sum, theirs.sum);
            mine.absoluteSum = Math.addExact(mine.absoluteSum, theirs.absoluteSum);
            mine.longs = Math.addExact(mine.longs, theirs.longs);
            if (aggregate == Aggregate.MIN || aggregate == Aggregate.MAX) {
              if (mine.valueClass != null && theirs.valueClass != null && mine.valueClass != theirs.valueClass) {
                inexact = true;
                return;
              }
              if (mine.valueClass == null)
                mine.valueClass = theirs.valueClass;
              mine.extreme = better(aggregate, mine.extreme, theirs.extreme);
              mine.removed = better(aggregate, mine.removed, theirs.removed);
            }
          }
        }
      } catch (final ArithmeticException e) {
        inexact = true;
      }
    }

    /**
     * Ends the build of a state from a scan and its corrections: the corrections removed values, and a group whose removed value may
     * have been its {@code min}/{@code max} cannot tell its extreme. Groups left with no member go. Also checks the domain.
     */
    Outcome seal() {
      if (inexact)
        return Outcome.INEXACT;
      for (final var iterator = groups.values().iterator(); iterator.hasNext(); ) {
        final Group group = iterator.next();
        if (group.members < 0)
          return Outcome.REBUILD;
        for (int i = 0; i < plan.aggregates.length; i++) {
          final Aggregation aggregation = group.aggregations[i];
          if (aggregation.nonNull < 0 || aggregation.longs < 0 || aggregation.absoluteSum < 0)
            return Outcome.REBUILD;
          final Aggregate aggregate = plan.aggregates[i];
          if (aggregate == Aggregate.MIN || aggregate == Aggregate.MAX) {
            if (aggregation.removed != null && aggregation.nonNull > 0 && !strictlyWorse(aggregate, aggregation.removed,
                aggregation.extreme))
              return Outcome.REBUILD;
            aggregation.removed = null;
            if (aggregation.nonNull == 0) {
              aggregation.extreme = null;
              aggregation.valueClass = null;
            }
          }
          if (!inDomain(aggregate, aggregation))
            return Outcome.INEXACT;
        }
        if (group.members == 0) {
          for (final Aggregation aggregation : group.aggregations)
            if (aggregation.nonNull != 0)
              return Outcome.REBUILD;
          iterator.remove();
        }
      }
      return Outcome.APPLIED;
    }

    /**
     * Applies {@code delta} to this state, writing into {@code changed} the new value of every group it touches (null for a group
     * that loses its last member). This state is not modified: the caller installs the changes once the rows are written.
     */
    Outcome apply(final Groups delta, final Map<List<Object>, Group> changed) {
      if (delta.inexact)
        return Outcome.INEXACT;
      try {
        for (final Map.Entry<List<Object>, Group> entry : delta.groups.entrySet()) {
          final Group change = entry.getValue();
          final Group current = groups.get(entry.getKey());
          if (current == null && change.isEmpty())
            continue;
          final Group next;
          if (current == null) {
            next = new Group(plan.aggregates.length);
            next.rawKeys = change.rawKeys;
          } else {
            if (!sameClasses(current.rawKeys, change.rawKeys))
              return Outcome.INEXACT;
            next = current.copy();
          }
          next.members = Math.addExact(next.members, change.members);
          if (next.members < 0)
            return Outcome.REBUILD;
          for (int i = 0; i < plan.aggregates.length; i++) {
            final Aggregate aggregate = plan.aggregates[i];
            final Aggregation state = next.aggregations[i];
            final Aggregation step = change.aggregations[i];
            state.nonNull = Math.addExact(state.nonNull, step.nonNull);
            state.sum = Math.addExact(state.sum, step.sum);
            state.absoluteSum = Math.addExact(state.absoluteSum, step.absoluteSum);
            state.longs = Math.addExact(state.longs, step.longs);
            if (state.nonNull < 0 || state.longs < 0 || state.absoluteSum < 0)
              return Outcome.REBUILD;
            if (aggregate == Aggregate.MIN || aggregate == Aggregate.MAX) {
              if (state.valueClass != null && step.valueClass != null && state.valueClass != step.valueClass)
                return Outcome.INEXACT;
              final Object candidate = better(aggregate, state.extreme, step.extreme);
              if (state.nonNull == 0) {
                state.extreme = null;
                state.valueClass = null;
              } else {
                // a removed value at or beyond the extreme may have been the extreme, and the state holds no runner-up
                if (step.removed != null && !strictlyWorse(aggregate, step.removed, candidate))
                  return Outcome.REBUILD;
                state.extreme = candidate;
                if (state.valueClass == null)
                  state.valueClass = step.valueClass;
              }
            }
            if (!inDomain(aggregate, state))
              return Outcome.INEXACT;
          }
          if (next.members == 0) {
            for (final Aggregation aggregation : next.aggregations)
              if (aggregation.nonNull != 0)
                return Outcome.REBUILD;
            if (current != null)
              changed.put(entry.getKey(), null);
            continue;
          }
          changed.put(entry.getKey(), next);
        }
      } catch (final ArithmeticException e) {
        return Outcome.INEXACT;
      }
      return Outcome.APPLIED;
    }

    /** The value of column {@code column} of {@code group}'s row, as the defining query computes it. */
    Object columnValue(final Group group, final int column) {
      final int source = plan.columnSources[column];
      if (source >= 0)
        return group.rawKeys[source];
      final int index = -1 - source;
      final Aggregation aggregation = group.aggregations[index];
      return switch (plan.aggregates[index]) {
        case COUNT_STAR -> group.members;
        case COUNT -> aggregation.nonNull;
        // NumericSum: an Integer total while every value is an Integer and the total never left the int range (the domain check
        // keeps the sum of the absolute values in it), a Long total once a Long is added
        case SUM -> aggregation.nonNull == 0 ? null : aggregation.longs > 0 ? (Object) aggregation.sum : (Object) (int) aggregation.sum;
        // SQLFunctionAverage: the double total over the count, for a total that is no BigDecimal
        case AVG -> aggregation.nonNull == 0 ? null : (double) aggregation.sum / aggregation.nonNull;
        case MIN, MAX -> aggregation.nonNull == 0 ? null : aggregation.extreme;
      };
    }
  }

  /**
   * Whether the query computes this aggregate's value for the group independently of the order it reads the values in: an
   * {@code Integer} total that cannot leave the {@code int} range on the way (where {@code NumericSum} would widen it to
   * {@code Long} for good), and any total that cannot leave the {@code long} range (where it would turn into a {@code BigDecimal}).
   * The absolute sum is checked exactly by {@code Math.addExact} on the way.
   */
  private static boolean inDomain(final Aggregate aggregate, final Aggregation aggregation) {
    if (aggregate == Aggregate.SUM && aggregation.longs == 0)
      return aggregation.absoluteSum <= Integer.MAX_VALUE;
    return true;
  }

  private static boolean sameClasses(final Object[] a, final Object[] b) {
    for (int i = 0; i < a.length; i++) {
      final Object x = a[i];
      final Object y = b[i];
      if (x != null && y != null && x.getClass() != y.getClass())
        return false;
    }
    return true;
  }

  /** The extreme of the two for the aggregate: the smaller for {@code min}, the larger for {@code max}; null is no value. */
  private static Object better(final Aggregate aggregate, final Object a, final Object b) {
    if (a == null)
      return b;
    if (b == null)
      return a;
    final int comparison = BinaryComparator.compareTo(a, b);
    return aggregate == Aggregate.MIN ? (comparison <= 0 ? a : b) : (comparison >= 0 ? a : b);
  }

  /** Whether {@code value} is strictly on the losing side of {@code extreme}: larger for {@code min}, smaller for {@code max}. */
  private static boolean strictlyWorse(final Aggregate aggregate, final Object value, final Object extreme) {
    if (extreme == null)
      return false;
    final int comparison = BinaryComparator.compareTo(value, extreme);
    return aggregate == Aggregate.MIN ? comparison > 0 : comparison < 0;
  }

  /**
   * The commits a rebuild pass could not tell whether its scan saw (issue #11937): those whose commit point fell after the rebuild
   * began, and those that had reached their commit point but not delivered their delta when it began. For each record they changed
   * the watch keeps what the scan counted for it and its state after the last of those commits, so the rebuild ends with
   * {@code -counted + after}, which is exact whichever version of the record the scan read.
   * <p>
   * The scan skips a record the watch already holds, and a commit point registering a record the scan has already passed knows the
   * scan counted it as it was before that commit. Both happen under this object's lock, so the two cannot interleave for one
   * record.
   */
  static final class Watch {
    private final Map<RID, Entry> entries         = new LinkedHashMap<>();
    private final Set<Integer>    finishedBuckets = new HashSet<>();
    private       int             cursorBucket    = -1;
    private       long            cursorPosition  = -1;
    /** A commit the rebuild cannot correct for (no per-record changes, or a commit that failed past its point) was absorbed. */
    private       boolean         dirty;

    static final class Entry {
      Contribution counted;
      Contribution after;
    }

    /** Called by the scan before it counts {@code rid}: false when the watch holds it, which the scan then skips. */
    synchronized boolean scanning(final RID rid) {
      if (entries.containsKey(rid))
        return false;
      if (rid.getBucketId() != cursorBucket) {
        if (cursorBucket >= 0)
          finishedBuckets.add(cursorBucket);
        cursorBucket = rid.getBucketId();
      }
      cursorPosition = rid.getPosition();
      return true;
    }

    /**
     * Registers what one commit changed, in commit order.
     *
     * @param beforeScan the commit was in flight when the rebuild began: the scan has read nothing yet
     */
    synchronized void absorb(final Map<RID, MaterializedViewChangeListener.RecordChange> changes, final boolean beforeScan) {
      if (changes == null) {
        dirty = true;
        return;
      }
      for (final Map.Entry<RID, MaterializedViewChangeListener.RecordChange> change : changes.entrySet()) {
        final RID rid = change.getKey();
        Entry entry = entries.get(rid);
        if (entry == null) {
          entry = new Entry();
          entry.counted = !beforeScan && passed(rid) ? change.getValue().before : null;
          entries.put(rid, entry);
        }
        entry.after = change.getValue().after;
      }
    }

    synchronized void markDirty() {
      dirty = true;
    }

    synchronized boolean isDirty() {
      return dirty;
    }

    synchronized Map<RID, Entry> entries() {
      return entries;
    }

    private boolean passed(final RID rid) {
      return finishedBuckets.contains(rid.getBucketId()) || (rid.getBucketId() == cursorBucket
          && rid.getPosition() <= cursorPosition);
    }
  }
}
