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

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.ContinuousAggregateRefresher;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class SaveElementStep extends AbstractExecutionStep {
  private final Identifier bucket;
  private final boolean    createAlways;
  private final boolean    skipDuplicateKey;

  public SaveElementStep(final CommandContext context, final Identifier bucket, final boolean createAlways) {
    this(context, bucket, createAlways, false);
  }

  /**
   * @param skipDuplicateKey when true (issue #4918's {@code INSERT ... ON DUPLICATE KEY SKIP}), a record that
   *                         would violate a unique index already carried by a previously committed record OR
   *                         an earlier record in this same batch is never persisted at all, and is reported as
   *                         a skipped row instead of aborting the whole statement. The conflict is detected by
   *                         probing the unique indexes directly (see {@link #findDuplicateKeyConflict}) rather
   *                         than by catching the save failure: {@code Index.put()} only throws synchronously
   *                         for a conflict against another key staged earlier in the SAME open transaction -
   *                         a conflict against already-committed data is detected at commit time, by which
   *                         point the record's bucket write already happened and cannot be undone record-by-
   *                         record without a full compensating delete.
   *                         <p>
   *                         Known limitation: the probe only sees keys staged by THIS transaction plus already
   *                         -committed data, so two concurrent transactions each inserting the same new key can
   *                         both pass the probe: one of them still hits an uncaught {@code DuplicatedKeyException}
   *                         at its own commit, aborting that whole batch rather than skipping just that row. This
   *                         mirrors ArcadeDB's commit-time conflict model for every other write, not a gap
   *                         specific to this clause.
   */
  public SaveElementStep(final CommandContext context, final Identifier bucket, final boolean createAlways,
      final boolean skipDuplicateKey) {
    super(context);
    this.bucket = bucket;
    this.createAlways = createAlways;
    this.skipDuplicateKey = skipDuplicateKey;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    return new ResultSet() {
      // Every record pulled through one INSERT statement targets the same type, so the unique-index list is
      // resolved once per distinct type name seen rather than on every single row of a CONTENT [...] batch.
      private String          cachedTypeName;
      private List<TypeIndex> cachedUniqueIndexes;

      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        final Result result = upstream.next();
        if (result != null && result.isElement()) {
          final Document doc = result.getElement().orElse(null);

          if (doc == null)
            throw new IllegalArgumentException("Cannot save a null document");

          // Check if this is a TimeSeries type — route to TimeSeriesEngine
          final var docType = context.getDatabase().getSchema().getType(doc.getTypeName());
          if (docType instanceof LocalTimeSeriesType tsType) {
            // requireEngine() fails loudly - naming the type and, when known, why - instead of silently falling
            // through to the generic document save below, which a TimeSeries type with no record bucket of its
            // own cannot serve correctly either (issue #6356).
            // Gated accessor: appending a sample is a record creation on a type that owns no bucket, so the
            // per-type check is the only thing that can enforce a "createRecord" denial on it.
            saveToTimeSeries(tsType, tsType.requireEngine(SecurityDatabaseUser.ACCESS.CREATE_RECORD), doc, context);
            scheduleContinuousAggregateRefresh(context, tsType);
            return result;
          }

          final MutableDocument modifiableDoc = doc.modify();

          // On UPDATE (createAlways=false) skip the save - and the resulting MVCC version bump - when an
          // existing record was not actually modified (e.g. UPDATE ... SET x = <same value>). Otherwise a
          // clean record gets enrolled in the transaction and concurrent updaters collide with
          // ConcurrentModificationException. INSERT/CREATE paths (createAlways=true) always persist.
          if (!createAlways && modifiableDoc.getIdentity() != null && !modifiableDoc.isDirty())
            return result;

          if (skipDuplicateKey) {
            if (!docType.getName().equals(cachedTypeName)) {
              cachedTypeName = docType.getName();
              cachedUniqueIndexes = docType.getAllIndexes(true).stream().filter(TypeIndex::isUnique).toList();
            }
            final DuplicateKeyConflict conflict = findDuplicateKeyConflict(modifiableDoc, cachedUniqueIndexes);
            if (conflict != null)
              return skippedResult(modifiableDoc, conflict);
          }

          if (bucket == null)
            modifiableDoc.save();
          else
            modifiableDoc.save(bucket.getStringValue());
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  /**
   * Probes the document type's unique indexes (issue #4918, already filtered to the unique ones and cached by
   * the caller - see {@link #syncPull}) for a key already claimed by another record - either a previously
   * committed one or an earlier record in this same batch, since {@link TypeIndex#get} reads through to this
   * transaction's own staged-but-uncommitted index entries.
   * <p>
   * A key is exempted from the check exactly when {@link LSMTreeIndexAbstract#isKeyNull} says so (every
   * component null, not just one) - unconditionally, regardless of the index's {@code NULL_STRATEGY}, matching
   * both call sites of the engine's own commit-time duplicate check ({@code TransactionIndexContext}'s
   * {@code checkUniqueIndexKeys()} and {@code addIndexKeyLock()}). "Multiple NULLs allowed in a unique index" is
   * deliberate SQL-standard behavior that holds under {@code NULL_STRATEGY.INDEX} too - that setting affects
   * whether a null key gets a physical index entry, not whether an all-null key is exempt from uniqueness. A
   * composite key with only SOME null components is not exempt: it is still a real key that must be probed.
   *
   * @return the conflicting index/RID, or {@code null} if no unique index on this type is violated
   */
  private static DuplicateKeyConflict findDuplicateKeyConflict(final MutableDocument doc, final List<TypeIndex> uniqueIndexes) {
    for (final TypeIndex index : uniqueIndexes) {
      final List<String> keyProperties = index.getPropertyNames();
      final Object[] keyValues = new Object[keyProperties.size()];
      for (int i = 0; i < keyProperties.size(); i++)
        keyValues[i] = doc.get(keyProperties.get(i));

      if (LSMTreeIndexAbstract.isKeyNull(keyValues))
        continue;

      try (final IndexCursor existing = index.get(keyValues, 1)) {
        if (existing.hasNext())
          return new DuplicateKeyConflict(index.getName(), keyValues, existing.next().getIdentity());
      }
    }
    return null;
  }

  private record DuplicateKeyConflict(String indexName, Object[] keys, RID existingRID) {
  }

  /**
   * Builds the row reported for a record skipped by {@code ON DUPLICATE KEY SKIP} (issue #4918): the attempted
   * properties are kept so a {@code RETURN} projection can still report on them, plus {@code @skipped} and the
   * conflict details so the caller can tell a skipped row from an inserted one and knows why it was skipped.
   */
  private static Result skippedResult(final MutableDocument attemptedDoc, final DuplicateKeyConflict conflict) {
    final ResultInternal result = new ResultInternal(attemptedDoc.toMap(false));
    result.setProperty("@skipped", true);
    result.setProperty("@type", attemptedDoc.getTypeName());
    result.setProperty("@duplicateIndex", conflict.indexName());
    result.setProperty("@duplicateKeys", Arrays.asList(conflict.keys()));
    result.setProperty("@existingRID", conflict.existingRID());
    return result;
  }

  private void saveToTimeSeries(final LocalTimeSeriesType tsType, final TimeSeriesEngine engine, final Document doc,
      final CommandContext context) {
    final List<ColumnDefinition> columns = tsType.getTsColumns();
    final ZoneId zoneId = context.getDatabase().getSchema().getZoneId();

    final long[] timestamps = new long[1];
    int nonTsCount = 0;
    for (final ColumnDefinition col : columns)
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        nonTsCount++;
    final Object[][] columnValues = new Object[nonTsCount][1];

    int colIdx = 0;
    for (int i = 0; i < columns.size(); i++) {
      final ColumnDefinition col = columns.get(i);
      final Object value = doc.get(col.getName());

      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP) {
        timestamps[0] = toEpochMs(value, zoneId);
      } else {
        columnValues[colIdx][0] = convertValue(value, col.getDataType());
        colIdx++;
      }
    }

    try {
      engine.appendSamples(timestamps, columnValues);
    } catch (final IOException e) {
      throw new CommandExecutionException("Error appending to TimeSeries engine", e);
    }
  }

  private void scheduleContinuousAggregateRefresh(final CommandContext context, final LocalTimeSeriesType tsType) {
    final LocalSchema schema = (LocalSchema) context.getDatabase().getSchema();
    final ContinuousAggregate[] aggregates = schema.getContinuousAggregates();
    if (aggregates.length == 0)
      return;

    final String typeName = tsType.getName();
    final TransactionContext tx = context.getDatabase().getTransaction();

    for (final ContinuousAggregate ca : aggregates) {
      if (typeName.equals(ca.getSourceTypeName())) {
        final String callbackKey = "ca-refresh:" + ca.getName();
        final ContinuousAggregateImpl caImpl = (ContinuousAggregateImpl) ca;
        tx.addAfterCommitCallbackIfAbsent(callbackKey, () -> {
          try {
            ContinuousAggregateRefresher.incrementalRefresh(context.getDatabase(), caImpl);
          } catch (final Exception e) {
            LogManager.instance().log(SaveElementStep.class, Level.WARNING,
                "Error refreshing continuous aggregate '%s' after commit: %s", e, ca.getName(), e.getMessage());
          }
        });
      }
    }
  }

  private static long toEpochMs(final Object value, final ZoneId zoneId) {
    if (value instanceof Long l)
      return l;
    if (value instanceof Date d)
      return d.getTime();
    if (value instanceof Instant i)
      return i.toEpochMilli();
    if (value instanceof Number n)
      return n.longValue();
    if (value instanceof LocalDateTime ldt)
      return ldt.atZone(zoneId).toInstant().toEpochMilli();
    if (value instanceof LocalDate ld)
      return ld.atStartOfDay(zoneId).toInstant().toEpochMilli();
    if (value instanceof String s) {
      try {
        return Instant.parse(s).toEpochMilli();
      } catch (final Exception e) {
        try {
          return LocalDate.parse(s).atStartOfDay(zoneId).toInstant().toEpochMilli();
        } catch (final Exception e2) {
          throw new CommandExecutionException("Cannot parse timestamp: '" + s + "'", e);
        }
      }
    }
    throw new CommandExecutionException("Cannot convert to timestamp: " + (value != null ? value.getClass().getName() : "null"));
  }

  private static Object convertValue(final Object value, final Type targetType) {
    if (value == null)
      return null;
    return switch (targetType) {
      case DOUBLE -> value instanceof Number n ? n.doubleValue() : Double.parseDouble(value.toString());
      case LONG -> value instanceof Number n ? n.longValue() : Long.parseLong(value.toString());
      case INTEGER -> value instanceof Number n ? n.intValue() : Integer.parseInt(value.toString());
      case FLOAT -> value instanceof Number n ? n.floatValue() : Float.parseFloat(value.toString());
      case SHORT -> value instanceof Number n ? n.shortValue() : Short.parseShort(value.toString());
      default -> value;
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SAVE RECORD");
    if (bucket != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  on bucket ").append(bucket);
    }
    return result.toString();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new SaveElementStep(context, bucket == null ? null : bucket.copy(), createAlways, skipDuplicateKey);
  }
}
