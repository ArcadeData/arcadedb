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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.GeOperator;
import com.arcadedb.query.sql.parser.GtOperator;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.NeOperator;
import com.arcadedb.query.sql.parser.NeqOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Level;

/**
 * The {@code @rid} conjuncts of a WHERE block, resolved for one execution so that an indexed function can decide them on
 * its candidates' RIDs before it loads a single record (issue #9909). {@code WHERE @rid > :cursor AND SEARCH_INDEX(...)}
 * used to load every match and only then compare its RID with the cursor.
 * <p>
 * Each condition is resolved once: a bound that is a RID becomes a primitive range or a set, which also tells which
 * buckets can hold a match at all ({@link #acceptsBucket(int)}); any other value (a RID's string spelling, a row of a
 * sub-query held in a variable, a null) is decided by evaluating the condition itself on the RID, which is what the
 * filter this replaces did with the record - an {@code @rid} reads the identity and loads nothing. Either way the
 * answer is the condition's own, so the planner can drop the conjuncts from the filter after the fetch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RIDFilter {
  private static final String RID_ATTRIBUTE = "@rid";

  private final List<BooleanExpression> conditions;
  private final CommandContext          context;
  private       RID                     lower;
  private       boolean                 lowerInclusive;
  private       RID                     upper;
  private       boolean                 upperInclusive;
  private       Set<RID>                allowed;
  /** The buckets of {@link #allowed}, sorted, so that a bucket is decided without walking the RID set. */
  private       int[]                   allowedBuckets;
  private       Set<RID>                excluded;
  private       BooleanExpression[]     evaluated;
  private       boolean                 acceptsNothing;

  private RIDFilter(final List<BooleanExpression> conditions, final CommandContext context) {
    this.conditions = conditions;
    this.context = context;
  }

  /**
   * Resolves {@code conditions} - each one an {@code @rid} comparison or {@code @rid [NOT] IN} that
   * {@link #isPushable(BooleanExpression)} accepted at planning time - against this execution's parameters and variables.
   */
  public static RIDFilter resolve(final List<BooleanExpression> conditions, final CommandContext context) {
    final RIDFilter filter = new RIDFilter(conditions, context);
    final List<BooleanExpression> evaluated = new ArrayList<>(0);
    for (final BooleanExpression condition : conditions)
      if (!filter.resolveNatively(condition))
        evaluated.add(condition);
    filter.evaluated = evaluated.toArray(new BooleanExpression[0]);
    filter.acceptsNothing = filter.computeAcceptsNothing();
    if (filter.allowed != null) {
      final int[] buckets = new int[filter.allowed.size()];
      int i = 0;
      for (final RID rid : filter.allowed)
        buckets[i++] = rid.getBucketId();
      Arrays.sort(buckets);
      filter.allowedBuckets = buckets;
    }
    return filter;
  }

  /**
   * Whether {@code condition} has the shape this filter decides: {@code @rid} compared with {@code =}, {@code <>},
   * {@code <}, {@code <=}, {@code >} or {@code >=}, or tested with {@code [NOT] IN}, against a right-hand side that is not
   * a sub-query. Whether that right-hand side can be computed before the fetch is the caller's to decide, since that
   * depends on the variables of the statement.
   */
  public static boolean isPushable(final BooleanExpression condition) {
    if (condition instanceof final BinaryCondition binary)
      return isRid(binary.getLeft()) && binary.getRight() != null && isRangeOperator(binary.getOperator());
    if (condition instanceof final InCondition in)
      return isRid(in.getLeft()) && in.getRightStatement() == null && (in.getRightParam() != null
          || in.getRightMathExpression() != null);
    return false;
  }

  /** Whether no RID at all passes, so the search behind the filter does not have to run. */
  public boolean acceptsNothing() {
    return acceptsNothing;
  }

  /**
   * Whether a record of {@code bucketId} can pass: false when the range or the RID set lies entirely in other buckets. The
   * range half relies on {@link RID#compareTo} being bucket-major (bucket id first, then position): the range then spans
   * whole buckets between its bounds' own, which is what lets a bucket outside them be skipped without reading it.
   */
  public boolean acceptsBucket(final int bucketId) {
    if (acceptsNothing)
      return false;
    if (lower != null && bucketId < lower.getBucketId())
      return false;
    if (upper != null && bucketId > upper.getBucketId())
      return false;
    return allowedBuckets == null || Arrays.binarySearch(allowedBuckets, bucketId) >= 0;
  }

  public boolean accepts(final RID rid) {
    if (acceptsNothing)
      return false;
    if (lower != null) {
      final int c = rid.compareTo(lower);
      if (c < 0 || (c == 0 && !lowerInclusive))
        return false;
    }
    if (upper != null) {
      final int c = rid.compareTo(upper);
      if (c > 0 || (c == 0 && !upperInclusive))
        return false;
    }
    if (allowed != null && !allowed.contains(rid))
      return false;
    if (excluded != null && excluded.contains(rid))
      return false;
    // A condition left to evaluation reads the RID's identity on its left and a value resolved without a record on its right:
    // it loads nothing, and a value it cannot compare answers false or null, which rejects the RID as the filter it replaces did
    for (final BooleanExpression condition : evaluated)
      if (!Boolean.TRUE.equals(condition.evaluate((Identifiable) rid, context)))
        return false;
    return true;
  }

  /**
   * The records of {@code records} the filter accepts. For an indexed function that does not apply the filter itself:
   * its records are loaded already, so this keeps the answer right without saving the loads. The iterator closes the one
   * it wraps.
   */
  public Iterable<Record> filter(final Iterable<Record> records) {
    if (acceptsNothing)
      return List.of();
    return () -> new FilteringIterator(records.iterator(), this);
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    for (final BooleanExpression condition : conditions) {
      if (!builder.isEmpty())
        builder.append(" AND ");
      builder.append(condition);
    }
    return builder.toString();
  }

  /** Folds {@code condition} into the range or the RID sets when its bound is a RID; false to evaluate it per RID. */
  private boolean resolveNatively(final BooleanExpression condition) {
    if (condition instanceof final BinaryCondition binary) {
      final BinaryCompareOperator operator = binary.getOperator();
      final RID bound = boundOf(binary.getRight().execute((Result) null, context), operator);
      if (bound == null)
        return false;
      if (operator instanceof GtOperator || operator instanceof GeOperator)
        restrictLower(bound, operator instanceof GeOperator);
      else if (operator instanceof LtOperator || operator instanceof LeOperator)
        restrictUpper(bound, operator instanceof LeOperator);
      else if (operator instanceof EqualsCompareOperator)
        restrictAllowed(Set.of(bound));
      else
        exclude(Set.of(bound));
      return true;
    }

    final InCondition in = (InCondition) condition;
    final Set<RID> rids = ridsOf(in.evaluateRight((Identifiable) null, context));
    if (rids == null)
      return false;
    if (in.not)
      exclude(rids);
    else
      restrictAllowed(rids);
    return true;
  }

  /**
   * The RID a comparison is decided against, or null to evaluate the condition per RID. A RID's string spelling is one for
   * an ordering: the operators compare it as {@link RID#compareTo} does, parsing it into a RID, so it is parsed once here
   * instead, which also lets it prune buckets. For equality the string stays to the condition's own semantics.
   */
  private static RID boundOf(final Object value, final BinaryCompareOperator operator) {
    if (value instanceof final RID rid)
      return rid;
    if (value instanceof final String string && RID.is(string) && (operator instanceof GtOperator || operator instanceof GeOperator
        || operator instanceof LtOperator || operator instanceof LeOperator))
      return new RID(string);
    return null;
  }

  /**
   * The RIDs of an IN right-hand side made of RIDs only, or null when any element is something else: the whole condition is
   * then evaluated per RID, with its own semantics for every element. Dropping the elements that are not RIDs instead would
   * change the answer of a NOT IN, which they can make true or unknown.
   */
  private static Set<RID> ridsOf(final Object value) {
    if (value instanceof final RID rid)
      return Set.of(rid);
    final Iterator<?> iterator;
    if (value instanceof final Iterable<?> iterable)
      iterator = iterable.iterator();
    else if (value instanceof final Object[] array)
      iterator = Arrays.asList(array).iterator();
    else
      return null;
    final Set<RID> rids = new HashSet<>();
    while (iterator.hasNext())
      if (iterator.next() instanceof final RID rid)
        rids.add(rid);
      else
        return null;
    return rids;
  }

  private void restrictLower(final RID bound, final boolean inclusive) {
    final int c = lower == null ? 1 : bound.compareTo(lower);
    if (c > 0 || (c == 0 && !inclusive)) {
      lower = bound;
      lowerInclusive = inclusive;
    }
  }

  private void restrictUpper(final RID bound, final boolean inclusive) {
    final int c = upper == null ? -1 : bound.compareTo(upper);
    if (c < 0 || (c == 0 && !inclusive)) {
      upper = bound;
      upperInclusive = inclusive;
    }
  }

  private void restrictAllowed(final Set<RID> rids) {
    if (allowed == null)
      allowed = new HashSet<>(rids);
    else
      allowed.retainAll(rids);
  }

  private void exclude(final Set<RID> rids) {
    if (excluded == null)
      excluded = new HashSet<>(rids);
    else
      excluded.addAll(rids);
  }

  private boolean computeAcceptsNothing() {
    if (allowed != null && allowed.isEmpty())
      return true;
    if (lower != null && upper != null) {
      final int c = lower.compareTo(upper);
      return c > 0 || (c == 0 && !(lowerInclusive && upperInclusive));
    }
    return false;
  }

  private static boolean isRid(final Object expression) {
    return expression != null && RID_ATTRIBUTE.equalsIgnoreCase(expression.toString());
  }

  private static boolean isRangeOperator(final BinaryCompareOperator operator) {
    return operator instanceof GtOperator || operator instanceof GeOperator || operator instanceof LtOperator
        || operator instanceof LeOperator || operator instanceof EqualsCompareOperator || operator instanceof NeOperator
        || operator instanceof NeqOperator;
  }

  private static final class FilteringIterator implements Iterator<Record>, AutoCloseable {
    private final Iterator<Record> source;
    private final RIDFilter        filter;
    private       Record           next;

    private FilteringIterator(final Iterator<Record> source, final RIDFilter filter) {
      this.source = source;
      this.filter = filter;
    }

    @Override
    public boolean hasNext() {
      while (next == null && source.hasNext()) {
        final Record record = source.next();
        if (record != null && filter.accepts(record.getIdentity()))
          next = record;
      }
      return next != null;
    }

    @Override
    public Record next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final Record record = next;
      next = null;
      return record;
    }

    @Override
    public void close() {
      if (source instanceof final AutoCloseable closeable)
        try {
          closeable.close();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.FINE, "Error on closing the indexed function result iterator", e);
        }
    }
  }
}
