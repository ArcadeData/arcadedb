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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarProjection;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GeOperator;
import com.arcadedb.query.sql.parser.GtOperator;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Decides, per matched record, whether it passes the FILTER of one facet (disjunctive faceting, spec 7.5): the collect step sets bit
 * {@code i} of a record's fail mask when evaluator {@code i} refuses it. Chosen at planning by {@link #choose} (D8); every kind answers
 * exactly what the FILTER as a WHERE answers on the row, in the caller's transaction, and falls back to that evaluation wherever its
 * cheaper source cannot tell:
 * <ul>
 *   <li>{@link Index}: {@code p = v} or {@code p IN [...]} with an {@code LSM_TREE} index on {@code p}: the matching RIDs are looked
 *       up once per execution, a row is a bit test;</li>
 *   <li>{@link Projection}: comparisons ({@code = IN < <= > >= BETWEEN}) of {@code p} with literals, ANDed, with a COLUMNAR column on
 *       {@code p}: the condition is evaluated on the column's value instead of the record's;</li>
 *   <li>{@link Record}: anything else, evaluated on the row.</li>
 * </ul>
 * One instance serves one execution of a plan; {@link #copy()} gives the next one its own.
 */
public abstract sealed class FacetFilterEvaluator implements AutoCloseable permits FacetFilterEvaluator.Index,
    FacetFilterEvaluator.Projection, FacetFilterEvaluator.Record {
  protected final WhereClause filter;

  private FacetFilterEvaluator(final WhereClause filter) {
    this.filter = filter;
  }

  /**
   * The cheapest exact evaluator of the filter for rows of {@code targetType} (D8).
   *
   * @param targetType the type the query selects from, null when it is not one (a subquery): the filter is then evaluated on the rows
   */
  public static FacetFilterEvaluator choose(final WhereClause filter, final DocumentType targetType, final DatabaseInternal db) {
    if (targetType == null)
      return new Record(filter);
    final List<BooleanExpression> atoms = comparisons(filter);
    final String property = atoms == null ? null : property(atoms);
    if (property == null)
      return new Record(filter);
    final Property declared = targetType.getPolymorphicPropertyIfExists(property);
    if (declared == null)
      return new Record(filter);
    final Type type = declared.getType();

    if (atoms.size() == 1 && isLookup(atoms.getFirst()) && Index.isKeyType(type) && Index.keyIndex(targetType, property) != null)
      return new Index(filter, targetType.getName(), property, type);
    if (Projection.isProjectable(type) && targetType.getPolymorphicColumnarIndex(property) != null)
      return new Projection(filter, property, type);
    return new Record(filter);
  }

  /** Prepares one execution, before the first record. */
  public abstract void open(CommandContext ctx);

  /**
   * @param row the row of the record, as the plan produced it; null to have the record loaded by its RID
   */
  public abstract boolean passes(RID rid, Result row, CommandContext ctx);

  /** How it decides, as EXPLAIN prints it: {@code INDEX}, {@code PROJECTION} or {@code RECORD}. */
  public abstract String kind();

  /** A fresh evaluator of the same filter, for another execution of a cached plan. */
  public abstract FacetFilterEvaluator copy();

  /** Releases what {@link #open} acquired; idempotent, and safe without a prior {@link #open}. */
  @Override
  public abstract void close();

  /** The filter as a WHERE on the row, the record loaded by its RID when there is no row (false when it no longer exists). */
  protected final boolean matchesRow(final RID rid, final Result row, final CommandContext ctx) {
    if (row != null)
      return filter.matchesFilters(row, ctx);
    try {
      return filter.matchesFilters(new ResultInternal(ctx.getDatabase().lookupByRID(rid, true)), ctx);
    } catch (final RecordNotFoundException e) {
      return false;
    }
  }

  /**
   * The conditions of the filter when it is one AND of comparisons of a bare property with literals (input parameters included), null
   * otherwise (an OR, a NOT, a function, a subquery, a literal on the left).
   */
  private static List<BooleanExpression> comparisons(final WhereClause filter) {
    final List<AndBlock> ors = filter.flatten();
    if (ors.size() != 1)
      return null;
    final List<BooleanExpression> atoms = ors.getFirst().getSubBlocks();
    if (atoms.isEmpty())
      return null;
    for (final BooleanExpression atom : atoms) {
      final boolean comparison = switch (atom) {
        case BinaryCondition binary -> binary.left.isBaseIdentifier() && binary.right.isLiteral(true) && (
            binary.operator instanceof EqualsCompareOperator || binary.operator instanceof LtOperator
                || binary.operator instanceof LeOperator || binary.operator instanceof GtOperator || binary.operator instanceof GeOperator);
        case InCondition in -> in.left.isBaseIdentifier() && !in.not && in.rightStatement == null && in.right == null && (
            in.rightParam != null || (in.rightMathExpression != null && in.rightMathExpression.isLiteral(true)));
        case BetweenCondition between -> between.getFirst().isBaseIdentifier() && between.getSecond().isLiteral(true)
            && between.getThird().isLiteral(true);
        default -> false;
      };
      if (!comparison)
        return null;
    }
    return atoms;
  }

  /** The one property every comparison is on, null when there are several. */
  private static String property(final List<BooleanExpression> atoms) {
    String property = null;
    for (final BooleanExpression atom : atoms) {
      final Expression left = switch (atom) {
        case BinaryCondition binary -> binary.left;
        case InCondition in -> in.left;
        default -> ((BetweenCondition) atom).getFirst();
      };
      final String name = left.getDefaultAlias().getStringValue();
      if (property == null)
        property = name;
      else if (!property.equals(name))
        return null;
    }
    return property;
  }

  private static boolean isLookup(final BooleanExpression atom) {
    return atom instanceof InCondition || (atom instanceof BinaryCondition binary && binary.operator instanceof EqualsCompareOperator);
  }

  /**
   * {@code p = v} or {@code p IN [...]} over an {@code LSM_TREE} index on {@code p} alone: the RIDs of the keys are looked up once per
   * execution into a {@link RidHashSet}, in the caller's transaction (the index answers with its changes). A row of a bucket the index
   * does not cover, and every row when the index is gone, not ready, folds case ({@code COLLATE ci}), or a key is not exactly of the
   * property's kind (a number for a STRING, a decimal for a LONG, a null), is evaluated on the row instead.
   * <p>
   * The set is charged to {@link FacetMemoryBudget} in 1 MiB steps while the cursor fills it (waiting up to
   * {@code COLUMNAR_QUERY_BUDGET_WAIT} per step). A refused step drops the partial set and releases its reservations: every row is then
   * evaluated on the row (exact, slower), and the facets stay available. The reservations are released by {@link #close()}.
   * {@link #fallback()} tells why the rows answer, for PROFILE.
   */
  public static final class Index extends FacetFilterEvaluator {
    /** The RID set is charged to the budget in steps of this size, while the cursor fills it, as {@code MatchSet.Builder} does. */
    private static final long STEP_BYTES = 1L << 20;

    private final String                              typeName;
    private final String                              property;
    private final Type                                propertyType;
    private final List<FacetMemoryBudget.Reservation> reservations = new ArrayList<>();
    private       RidHashSet                          matches;
    private       boolean[]                           coveredBuckets;
    /** Why this execution answers every row from the row instead of the RID set, null when the set answers. */
    private       String                              fallback;

    private Index(final WhereClause filter, final String typeName, final String property, final Type propertyType) {
      super(filter);
      this.typeName = typeName;
      this.property = property;
      this.propertyType = propertyType;
    }

    static boolean isKeyType(final Type type) {
      return switch (type) {
        case STRING, BOOLEAN, BYTE, SHORT, INTEGER, LONG -> true;
        default -> false;
      };
    }

    /** The plannable {@code LSM_TREE} index on exactly the property, of the type or a super type, null when there is none. */
    static TypeIndex keyIndex(final DocumentType type, final String property) {
      final TypeIndex index = type.getPolymorphicIndexByProperties(List.of(property));
      return index != null && index.getType() == Schema.INDEX_TYPE.LSM_TREE ? index : null;
    }

    @Override
    public void open(final CommandContext ctx) {
      close();
      fallback = null;
      final DatabaseInternal db = ctx.getDatabase();
      final ColumnarStats stats = ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager().getStats();
      openIndex(ctx, db, stats);
      if (fallback != null)
        stats.queryFilterFallback();
    }

    private void openIndex(final CommandContext ctx, final DatabaseInternal db, final ColumnarStats stats) {
      final TypeIndex index = db.getSchema().existsType(typeName) ? keyIndex(db.getSchema().getType(typeName), property) : null;
      if (index == null || !index.isReadyForQueries()) {
        fallback = "index not ready";
        return;
      }
      if (foldsCase(index)) {
        fallback = "collation";
        return;
      }
      final List<Object> keys = keys(ctx);
      if (keys == null) {
        fallback = "key type";
        return;
      }

      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      final long budgetWaitMs = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
      final Runnable countWait = () -> stats.queryBudgetWaits(1);
      final RidHashSet found = new RidHashSet();
      long reserved = 0;
      for (final Object key : keys) {
        final IndexCursor cursor = index.get(new Object[] { key });
        while (cursor.hasNext()) {
          final Identifiable next = cursor.next();
          if (next == null || !found.add(next.getIdentity()))
            continue;
          // charged as the set grows, at the set's cost per RID (its free slots included), step by step
          while ((long) found.size() * RidHashSet.BYTES_PER_ENTRY > reserved) {
            final FacetMemoryBudget.Reservation reservation = budget.reserve(STEP_BYTES, budgetWaitMs, countWait);
            if (reservation == null) {
              // refused: the rows answer (exact, slower), the facets stay available
              releaseReservations();
              fallback = "memory budget";
              return;
            }
            reservations.add(reservation);
            reserved += STEP_BYTES;
          }
        }
      }
      final List<IndexInternal> subIndexes = index.getSubIndexes();
      int maxBucket = -1;
      for (final IndexInternal sub : subIndexes)
        maxBucket = Math.max(maxBucket, sub.getAssociatedBucketId());
      final boolean[] covered = new boolean[maxBucket + 1];
      for (final IndexInternal sub : subIndexes)
        if (sub.getAssociatedBucketId() >= 0)
          covered[sub.getAssociatedBucketId()] = true;
      coveredBuckets = covered;
      matches = found;
    }

    private static boolean foldsCase(final TypeIndex index) {
      final IndexMetadata metadata = index.getMetadata();
      if (metadata == null || metadata.collations == null)
        return false;
      for (final String collation : metadata.collations)
        if (collation != null && !IndexMetadata.COLLATION_DEFAULT.equalsIgnoreCase(collation))
          return true;
      return false;
    }

    /**
     * The keys of the filter for this execution, null when one is not exactly of the property's kind. An integral key outside the range
     * of the property's type is left out: no stored value can equal it, as the row's own comparison finds.
     */
    private List<Object> keys(final CommandContext ctx) {
      final BooleanExpression atom = comparisons(filter).getFirst();
      final List<Object> keys = new ArrayList<>();
      if (atom instanceof BinaryCondition binary)
        keys.add(binary.right.execute((Result) null, ctx));
      else {
        final Object right = ((InCondition) atom).evaluateRight((Result) null, ctx);
        if (MultiValue.isMultiValue(right))
          for (final Object item : MultiValue.getMultiValueIterable(right))
            keys.add(item);
        else
          keys.add(right);
      }
      final List<Object> inRange = new ArrayList<>(keys.size());
      for (final Object key : keys) {
        if (!isExactKey(key))
          return null;
        if (!(key instanceof Number number) || fitsProperty(number.longValue()))
          inRange.add(key);
      }
      return inRange;
    }

    private boolean isExactKey(final Object key) {
      return switch (propertyType) {
        case STRING -> key instanceof String;
        case BOOLEAN -> key instanceof Boolean;
        default -> key instanceof Long || key instanceof Integer || key instanceof Short || key instanceof Byte;
      };
    }

    private boolean fitsProperty(final long value) {
      return switch (propertyType) {
        case BYTE -> value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE;
        case SHORT -> value >= Short.MIN_VALUE && value <= Short.MAX_VALUE;
        case INTEGER -> value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
        default -> true;
      };
    }

    /** Why this execution answers from the rows ({@code memory budget}, {@code index not ready}, ...), null when the index does. */
    public String fallback() {
      return fallback;
    }

    private void releaseReservations() {
      for (final FacetMemoryBudget.Reservation reservation : reservations)
        reservation.close();
      reservations.clear();
    }

    @Override
    public boolean passes(final RID rid, final Result row, final CommandContext ctx) {
      final int bucketId = rid.getBucketId();
      if (matches == null || bucketId < 0 || bucketId >= coveredBuckets.length || !coveredBuckets[bucketId])
        return matchesRow(rid, row, ctx);
      return matches.contains(rid);
    }

    @Override
    public String kind() {
      return "INDEX";
    }

    @Override
    public FacetFilterEvaluator copy() {
      return new Index(filter.copy(), typeName, property, propertyType);
    }

    @Override
    public void close() {
      matches = null;
      coveredBuckets = null;
      // fallback is kept: PROFILE may print the plan after the step released everything
      releaseReservations();
    }
  }

  /**
   * Comparisons of one property with literals, with a COLUMNAR column on it: the condition is evaluated on the column's value, decoded
   * into the object the record holds (a dictionary value, a number of the property's type, a date of the database's implementation),
   * so the WHERE's own comparison and conversions decide; a dictionary value is decided once per ordinal. The snapshot of a bucket is
   * acquired by the first row of the bucket and held until {@link #close()}. The record (the row) answers instead when the column
   * cannot: a pending position, one no segment covers, no value in the column (missing, or a value it could not hold), a zero double
   * (the column does not keep the sign), a bucket with no usable projection or one that does not publish the column, and a bucket the
   * caller's open transaction changed.
   * <p>
   * Not charged to {@link FacetMemoryBudget}: the snapshots pin projection state the projection holds anyway, and the per-ordinal
   * verdict memo is one byte per dictionary value up to the highest ordinal a row reached in the bucket, a small fraction of the
   * dictionary it decodes (which already lives in memory).
   */
  public static final class Projection extends FacetFilterEvaluator {
    private static final BucketColumn FROM_RECORDS = new BucketColumn(null, null, null);

    private final String               property;
    private final Type                 propertyType;
    private       DatabaseInternal     database;
    private       ColumnarIndexManager manager;
    private       ResultInternal       scratch;
    private       BucketColumn[]       buckets = new BucketColumn[0];
    private       long                 columnAnswers;
    private       long                 recordAnswers;

    private Projection(final WhereClause filter, final String property, final Type propertyType) {
      super(filter);
      this.property = property;
      this.propertyType = propertyType;
    }

    static boolean isProjectable(final Type type) {
      return switch (type) {
        case STRING, BOOLEAN, BYTE, SHORT, INTEGER, LONG, DOUBLE, DATE, DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> true;
        default -> false;
      };
    }

    /** The column of one bucket's snapshot; {@link #FROM_RECORDS} for a bucket the records answer. */
    private static final class BucketColumn {
      private final ColumnarSnapshot      snapshot;
      private final ColumnarSegmentView[] views;
      private final ColumnReader[]        readers;
      private final PositionBitmap        pending;
      private final GlobalDictionary      dictionary;
      /** Per ordinal: 0 not decided yet, 1 passes, 2 fails. */
      private       byte[]                verdicts = new byte[0];

      private BucketColumn(final ColumnarSnapshot snapshot, final String property, final List<ColumnarSegmentView> segments) {
        this.snapshot = snapshot;
        if (snapshot == null) {
          views = null;
          readers = null;
          pending = null;
          dictionary = null;
          return;
        }
        views = segments.toArray(new ColumnarSegmentView[0]);
        readers = new ColumnReader[views.length];
        for (int i = 0; i < views.length; i++)
          readers[i] = views[i].column(property);
        pending = snapshot.pendingPositions();
        dictionary = snapshot.dictionary(property);
      }
    }

    @Override
    public void open(final CommandContext ctx) {
      close();
      database = ctx.getDatabase();
      manager = ((LocalDatabase) database.getEmbedded()).getColumnarIndexManager();
      scratch = new ResultInternal(database);
      columnAnswers = 0;
      recordAnswers = 0;
    }

    @Override
    public boolean passes(final RID rid, final Result row, final CommandContext ctx) {
      final long position = rid.getPosition();
      final BucketColumn column = position < 0 ? FROM_RECORDS : bucket(rid.getBucketId());
      if (column.snapshot != null && !column.pending.get(position))
        for (int i = 0; i < column.views.length; i++) {
          if (!column.views[i].live().get(position))
            continue;
          final ColumnReader reader = column.readers[i];
          if (reader != null && reader.isPresent(position)) {
            final int verdict = columnVerdict(column, reader, position, ctx);
            if (verdict >= 0) {
              columnAnswers++;
              return verdict == 1;
            }
          }
          break;
        }
      recordAnswers++;
      return matchesRow(rid, row, ctx);
    }

    /** 1 passes, 0 fails, -1 when the column's value cannot stand for the record's. */
    private int columnVerdict(final BucketColumn column, final ColumnReader reader, final long position, final CommandContext ctx) {
      final Object value;
      switch (reader.kind()) {
      case ORDINAL -> {
        final int ordinal = reader.ordinal(position);
        if (ordinal >= column.verdicts.length)
          column.verdicts = Arrays.copyOf(column.verdicts, Math.max(ordinal + 1, column.verdicts.length * 2));
        if (column.verdicts[ordinal] == 0)
          column.verdicts[ordinal] = (byte) (matchesValue(column.dictionary.valueOf(ordinal), ctx) ? 1 : 2);
        return column.verdicts[ordinal] == 1 ? 1 : 0;
      }
      case LONG -> {
        final long stored = reader.longValue(position);
        value = switch (propertyType) {
          case BYTE -> (byte) stored;
          case SHORT -> (short) stored;
          case INTEGER -> (int) stored;
          case LONG -> stored;
          default -> database.getSerializer().deserializeTemporal(database, stored, propertyType.getBinaryType());
        };
      }
      case DOUBLE -> {
        final double stored = reader.doubleValue(position);
        if (stored == 0d)
          return -1;
        value = stored;
      }
      default -> {
        return -1;
      }
      }
      return matchesValue(value, ctx) ? 1 : 0;
    }

    private boolean matchesValue(final Object value, final CommandContext ctx) {
      scratch.setProperty(property, value);
      return filter.matchesFilters(scratch, ctx);
    }

    private BucketColumn bucket(final int bucketId) {
      if (bucketId < buckets.length && buckets[bucketId] != null)
        return buckets[bucketId];
      if (bucketId >= buckets.length)
        buckets = Arrays.copyOf(buckets, Math.max(bucketId + 1, buckets.length * 2));
      return buckets[bucketId] = acquire(bucketId);
    }

    private BucketColumn acquire(final int bucketId) {
      if (database.isTransactionActive()) {
        final IntHashSet single = new IntHashSet(1);
        single.add(bucketId);
        if (database.getTransaction().hasChangesInFiles(single))
          // the column holds committed values only; the rows are read as the transaction sees them
          return FROM_RECORDS;
      }
      final ColumnarProjection projection = manager.projection(bucketId);
      if (projection == null)
        return FROM_RECORDS;
      final ColumnarSnapshot snapshot;
      try {
        snapshot = projection.acquire();
      } catch (final RuntimeException e) {
        return FROM_RECORDS;
      }
      try {
        if (snapshot.isUsable() && snapshot.publishes(property))
          return new BucketColumn(snapshot, property, snapshot.segments());
      } catch (final RuntimeException | Error e) {
        snapshot.close();
        throw e;
      }
      snapshot.close();
      return FROM_RECORDS;
    }

    /** The rows this execution answered from the column so far. */
    public long columnAnswers() {
      return columnAnswers;
    }

    /** The rows this execution answered from the record so far. */
    public long recordAnswers() {
      return recordAnswers;
    }

    @Override
    public String kind() {
      return "PROJECTION";
    }

    @Override
    public FacetFilterEvaluator copy() {
      return new Projection(filter.copy(), property, propertyType);
    }

    @Override
    public void close() {
      final BucketColumn[] held = buckets;
      buckets = new BucketColumn[0];
      RuntimeException failure = null;
      for (final BucketColumn column : held)
        if (column != null && column.snapshot != null)
          try {
            column.snapshot.close();
          } catch (final RuntimeException e) {
            if (failure == null)
              failure = e;
            else
              failure.addSuppressed(e);
          }
      if (failure != null)
        throw failure;
    }
  }

  /** Any other filter: evaluated on the row, as the WHERE would. */
  public static final class Record extends FacetFilterEvaluator {
    private Record(final WhereClause filter) {
      super(filter);
    }

    @Override
    public void open(final CommandContext ctx) {
      // nothing to prepare
    }

    @Override
    public boolean passes(final RID rid, final Result row, final CommandContext ctx) {
      return matchesRow(rid, row, ctx);
    }

    @Override
    public String kind() {
      return "RECORD";
    }

    @Override
    public FacetFilterEvaluator copy() {
      return new Record(filter.copy());
    }

    @Override
    public void close() {
      // nothing held
    }
  }
}
