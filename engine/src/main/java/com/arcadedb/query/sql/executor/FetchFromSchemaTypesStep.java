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

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesShard;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.*;
import java.util.stream.*;

/**
 * Returns an Result containing metadata regarding the schema types.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class FetchFromSchemaTypesStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int cursor = 0;

  public FetchFromSchemaTypesStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (cursor == 0) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        final Schema schema = context.getDatabase().getSchema();

        final SecurityDatabaseUser currentUser = currentUser(context);

        final List<String> orderedTypes = schema.getTypes().stream().map(x -> x.getName()).sorted(String::compareToIgnoreCase)
            .collect(Collectors.toList());
        for (final String typeName : orderedTypes) {
          final DocumentType type = schema.getType(typeName);

          // Issue #4238: hide types the current user cannot read at all instead of throwing.
          // RemoteSchema.reload() lists every type on its first command and aborted the whole
          // listing the first time it hit a restricted type, locking the remote driver out
          // even from types the user was allowed to read.
          if (!canReadAnyBucket(currentUser, type))
            continue;

          final ResultInternal r = new ResultInternal(context.getDatabase());
          result.add(r);

          r.setProperty("name", type.getName());

          final boolean isTimeSeries = type instanceof LocalTimeSeriesType;
          String t = "?";

          if (isTimeSeries)
            t = LocalTimeSeriesType.KIND_CODE;
          else if (type.getType() == Document.RECORD_TYPE)
            t = "document";
          else if (type.getType() == Vertex.RECORD_TYPE)
            t = "vertex";
          else if (type.getType() == Edge.RECORD_TYPE)
            t = "edge";

          r.setProperty("type", t);

          if (isTimeSeries)
            populateTimeSeriesMetadata(r, (LocalTimeSeriesType) type);
          else
            r.setProperty("records", safeCountType(context, typeName));
          r.setProperty("buckets", type.getBuckets(false).stream().map((b) -> b.getName()).collect(Collectors.toList()));
          r.setProperty("bucketSelectionStrategy", type.getBucketSelectionStrategy().getName());
          // Expose the partition-mapping-stale flag (issue #4087). Set unconditionally via the
          // {@link DocumentType} interface so any implementation - including future replica or
          // proxy types - participates without an instanceof gate.
          r.setProperty("needsRepartition", type.isNeedsRepartition());

          // Expose the primary->external bucket mapping for types that have any EXTERNAL property. Lets tooling
          // (Studio etc.) tell the user where the externalised values for each primary bucket are stored.
          if (type instanceof LocalDocumentType ldt) {
            final Map<String, String> extMap = new HashMap<>();
            for (final Bucket b : type.getBuckets(false)) {
              final Integer extId = ldt.getExternalBucketIdFor(b.getFileId());
              if (extId != null) {
                final Bucket extBucket = context.getDatabase().getSchema().getBucketById(extId);
                if (extBucket != null)
                  extMap.put(b.getName(), extBucket.getName());
              }
            }
            if (!extMap.isEmpty())
              r.setProperty("externalBuckets", extMap);
          }

          final List<String> parents = type.getSuperTypes().stream().map(pt -> pt.getName()).collect(Collectors.toList());
          r.setProperty("parentTypes", parents);

          final List<ResultInternal> propertiesTypes = type.getPropertyNames().stream().sorted(String::compareToIgnoreCase)
              .map(name -> type.getProperty(name)).map(property -> {
                final ResultInternal propRes = new ResultInternal(context.getDatabase());
                propRes.setProperty("id", property.getId());
                propRes.setProperty("name", property.getName());
                propRes.setProperty("type", property.getType());

                if (property.getOfType() != null)
                  propRes.setProperty("ofType", property.getOfType());
                if (property.isMandatory())
                  propRes.setProperty("mandatory", property.isMandatory());
                if (property.isReadonly())
                  propRes.setProperty("readOnly", property.isReadonly());
                if (property.isNotNull())
                  propRes.setProperty("notNull", property.isNotNull());
                if (property.isHidden())
                  propRes.setProperty("hidden", property.isHidden());
                if (property.isExternal())
                  propRes.setProperty("external", property.isExternal());
                // Only emit the compression policy when explicitly set to a non-default value, mirroring the
                // toJSON convention. Studio renders the badge tooltip and a small label off this field.
                final String cmp = property.getCompression();
                if (cmp != null && !"none".equalsIgnoreCase(cmp))
                  propRes.setProperty("compression", cmp);
                if (property.getMin() != null)
                  propRes.setProperty("min", property.getMin());
                if (property.getMax() != null)
                  propRes.setProperty("max", property.getMax());
                if (property.getDefaultValue() != null)
                  propRes.setProperty("default", property.getDefaultValue());
                if (property.getRegexp() != null)
                  propRes.setProperty("regexp", property.getRegexp());

                final Map<String, Object> customs = new HashMap<>();
                for (final Object customKey : property.getCustomKeys().stream().sorted(String::compareToIgnoreCase).toArray())
                  customs.put((String) customKey, property.getCustomValue((String) customKey));
                propRes.setProperty("custom", customs);

                return propRes;
              }).collect(Collectors.toList());
          r.setProperty("properties", propertiesTypes);

          final List<ResultInternal> indexes = type.getAllIndexes(false).stream().sorted(Comparator.comparing(Index::getName))
              .map(typeIndex -> {
                final ResultInternal propRes = new ResultInternal();
                propRes.setProperty("name", typeIndex.getName());
                propRes.setProperty("typeName", typeIndex.getTypeName());
                propRes.setProperty("type", typeIndex.getType());
                propRes.setProperty("unique", typeIndex.isUnique());
                propRes.setProperty("properties", typeIndex.getPropertyNames());
                propRes.setProperty("automatic", typeIndex.isAutomatic());
                return propRes;
              }).collect(Collectors.toList());
          r.setProperty("indexes", indexes);

          final Map<String, Object> customs = new HashMap<>();
          for (final Object customKey : type.getCustomKeys().stream().sorted(String::compareToIgnoreCase).toArray())
            customs.put((String) customKey, type.getCustomValue((String) customKey));
          r.setProperty("custom", customs);

          context.setVariable("current", r);
        }
      } finally {
        if (context.isProfiling()) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < result.size();
      }

      @Override
      public Result next() {
        return result.get(cursor++);
      }

      @Override
      public void close() {
        result.clear();
      }

      @Override
      public void reset() {
        cursor = 0;
      }
    };
  }

  private static SecurityDatabaseUser currentUser(final CommandContext context) {
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    return dbContext != null ? dbContext.getCurrentUser() : null;
  }

  /**
   * Returns true when the type is visible to the current user (issue #4238). With no security
   * context (embedded usage or root) every type is visible. Otherwise, the type is visible when
   * the user has READ_RECORD on at least one of its buckets - matching the principle that a user
   * with partial access should still see the resource in catalog listings.
   */
  private static boolean canReadAnyBucket(final SecurityDatabaseUser user, final DocumentType type) {
    if (user == null)
      return true;
    final List<Bucket> buckets = type.getBuckets(false);
    if (buckets.isEmpty())
      return true;
    for (final Bucket b : buckets)
      if (user.requestAccessOnFile(b.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD))
        return true;
    return false;
  }

  /**
   * countType walks every bucket and throws SecurityException on the first one the current user
   * cannot read. Issue #4238: in the rare case where a user has READ_RECORD on some but not all
   * buckets of the type, swallow that exception and report zero rather than aborting the whole
   * schema listing.
   */
  private static long safeCountType(final CommandContext context, final String typeName) {
    try {
      return context.getDatabase().countType(typeName, false);
    } catch (final SecurityException e) {
      return 0L;
    }
  }

  private void populateTimeSeriesMetadata(final ResultInternal r, final LocalTimeSeriesType tsType) {
    r.setProperty("timestampColumn", tsType.getTimestampColumn());
    r.setProperty("shardCount", tsType.getShardCount());
    r.setProperty("retentionMs", tsType.getRetentionMs());
    r.setProperty("compactionBucketIntervalMs", tsType.getCompactionBucketIntervalMs());

    // Column definitions
    final List<ResultInternal> tsColResults = new ArrayList<>();
    for (final ColumnDefinition col : tsType.getTsColumns()) {
      final ResultInternal colR = new ResultInternal();
      colR.setProperty("name", col.getName());
      colR.setProperty("dataType", col.getDataType().name());
      colR.setProperty("role", col.getRole().name());
      tsColResults.add(colR);
    }
    r.setProperty("tsColumns", tsColResults);

    // Downsampling tiers
    final List<DownsamplingTier> tiers = tsType.getDownsamplingTiers();
    if (tiers != null && !tiers.isEmpty()) {
      final List<ResultInternal> tierResults = new ArrayList<>();
      for (final DownsamplingTier tier : tiers) {
        final ResultInternal tierR = new ResultInternal();
        tierR.setProperty("afterMs", tier.afterMs());
        tierR.setProperty("granularityMs", tier.granularityMs());
        tierResults.add(tierR);
      }
      r.setProperty("downsamplingTiers", tierResults);
    }

    // Engine runtime stats (per-shard diagnostics)
    final TimeSeriesEngine engine = tsType.getEngine();
    if (engine != null) {
      long totalSamples = 0;
      long globalMin = Long.MAX_VALUE;
      long globalMax = Long.MIN_VALUE;

      final List<ResultInternal> shardStats = new ArrayList<>();
      for (int s = 0; s < engine.getShardCount(); s++) {
        final TimeSeriesShard shard = engine.getShard(s);
        final ResultInternal shardR = new ResultInternal();
        shardR.setProperty("shard", s);

        try {
          final long sealedSamples = shard.getSealedStore().getTotalSampleCount();
          final long mutableSamples = shard.getMutableBucket().getSampleCount();
          final int sealedBlocks = shard.getSealedStore().getBlockCount();

          shardR.setProperty("sealedBlocks", sealedBlocks);
          shardR.setProperty("sealedSamples", sealedSamples);
          shardR.setProperty("mutableSamples", mutableSamples);
          shardR.setProperty("totalSamples", sealedSamples + mutableSamples);

          totalSamples += sealedSamples + mutableSamples;

          if (sealedBlocks > 0) {
            final long min = shard.getSealedStore().getGlobalMinTimestamp();
            final long max = shard.getSealedStore().getGlobalMaxTimestamp();
            shardR.setProperty("minTimestamp", min);
            shardR.setProperty("maxTimestamp", max);
            if (min < globalMin)
              globalMin = min;
            if (max > globalMax)
              globalMax = max;
          }

          if (mutableSamples > 0) {
            final long mMin = shard.getMutableBucket().getMinTimestamp();
            final long mMax = shard.getMutableBucket().getMaxTimestamp();
            shardR.setProperty("mutableMinTimestamp", mMin);
            shardR.setProperty("mutableMaxTimestamp", mMax);
            if (mMin < globalMin)
              globalMin = mMin;
            if (mMax > globalMax)
              globalMax = mMax;
          }

        } catch (final Exception e) {
          shardR.setProperty("error", e.getMessage());
        }

        shardStats.add(shardR);
      }

      r.setProperty("records", totalSamples);
      r.setProperty("shards", shardStats);

      if (globalMin != Long.MAX_VALUE)
        r.setProperty("globalMinTimestamp", globalMin);
      if (globalMax != Long.MIN_VALUE)
        r.setProperty("globalMaxTimestamp", globalMax);
    } else {
      r.setProperty("records", 0L);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA TYPES";
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

}
