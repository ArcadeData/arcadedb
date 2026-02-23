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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.ContinuousAggregateRefresher;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class SaveElementStep extends AbstractExecutionStep {
  private final Identifier bucket;
  private final boolean    createAlways;

  public SaveElementStep(final CommandContext context, final Identifier bucket, final boolean createAlways) {
    super(context);
    this.bucket = bucket;
    this.createAlways = createAlways;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    return new ResultSet() {
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
          if (docType instanceof LocalTimeSeriesType tsType && tsType.getEngine() != null) {
            saveToTimeSeries(tsType, doc, context);
            scheduleContinuousAggregateRefresh(context, tsType);
            return result;
          }

          final MutableDocument modifiableDoc = doc.modify();

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

  private void saveToTimeSeries(final LocalTimeSeriesType tsType, final Document doc, final CommandContext context) {
    final TimeSeriesEngine engine = tsType.getEngine();
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
    if (value instanceof java.time.LocalDateTime ldt)
      return ldt.atZone(zoneId).toInstant().toEpochMilli();
    if (value instanceof java.time.LocalDate ld)
      return ld.atStartOfDay(zoneId).toInstant().toEpochMilli();
    if (value instanceof String s) {
      try {
        return Instant.parse(s).toEpochMilli();
      } catch (final Exception e) {
        try {
          return java.time.LocalDate.parse(s).atStartOfDay(zoneId).toInstant().toEpochMilli();
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
    return new SaveElementStep(context, bucket == null ? null : bucket.copy(), createAlways);
  }
}
