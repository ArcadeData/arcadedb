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
package com.arcadedb.remote;

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * TimeSeries type used by {@link RemoteDatabase}: the server's own declaration of the type, as
 * {@code SELECT FROM schema:types} reports it. Before issue #7399 a remote client saw a TIMESERIES type as a plain
 * {@link RemoteDocumentType}, which meant the time-series half of its declaration - the timestamp column, the
 * precision, the column roles, the retention and the downsampling tiers - was reachable only by re-querying
 * {@code schema:types} by hand.
 * <p>
 * Read-only, like every remote schema object: the values are a cached snapshot, refreshed by
 * {@link RemoteSchema#reload()}.
 * <p>
 * This class is not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteTimeSeriesType extends RemoteDocumentType implements TimeSeriesType {
  // No field initializers: RemoteDocumentType's constructor calls reload(), so anything assigned here would run
  // AFTER reload() has already populated these fields and would wipe them. reload() assigns every one of them.
  private String                 timestampColumn;
  private String                 precision;
  private int                    shardCount;
  private long                   retentionMs;
  private long                   compactionBucketIntervalMs;
  private List<ColumnDefinition> tsColumns;
  private List<DownsamplingTier> downsamplingTiers;

  RemoteTimeSeriesType(final RemoteDatabase remoteDatabase, final Result record) {
    super(remoteDatabase, record);
  }

  @Override
  void reload(final Result record) {
    super.reload(record);

    timestampColumn = record.getProperty("timestampColumn");
    precision = record.getProperty("precision");
    shardCount = (int) asLong(record.getProperty("shardCount"), 0L);
    retentionMs = asLong(record.getProperty("retentionMs"), 0L);
    compactionBucketIntervalMs = asLong(record.getProperty("compactionBucketIntervalMs"), 0L);

    final List<Map<String, Object>> columns = record.getProperty("tsColumns");
    if (columns == null || columns.isEmpty())
      tsColumns = Collections.emptyList();
    else {
      final List<ColumnDefinition> parsed = new ArrayList<>(columns.size());
      for (final Map<String, Object> column : columns) {
        final Type dataType = Type.getTypeByName((String) column.get("dataType"));
        final ColumnDefinition.ColumnRole role = ColumnDefinition.ColumnRole.valueOf((String) column.get("role"));
        // A server older than issue #7399 does not report the codec. Falling back to the default for the type and
        // role is what this class did implicitly before; doing it explicitly keeps the remote view usable against
        // such a server instead of failing the whole schema reload on a missing key.
        final String compression = (String) column.get("compression");
        parsed.add(compression != null ?
            new ColumnDefinition((String) column.get("name"), dataType, role, TimeSeriesCodec.valueOf(compression)) :
            new ColumnDefinition((String) column.get("name"), dataType, role));
      }
      tsColumns = Collections.unmodifiableList(parsed);
    }

    final List<Map<String, Object>> tiers = record.getProperty("downsamplingTiers");
    if (tiers == null || tiers.isEmpty())
      downsamplingTiers = Collections.emptyList();
    else {
      final List<DownsamplingTier> parsed = new ArrayList<>(tiers.size());
      for (final Map<String, Object> tier : tiers)
        parsed.add(new DownsamplingTier(asLong(tier.get("afterMs"), 0L), asLong(tier.get("granularityMs"), 0L)));
      downsamplingTiers = Collections.unmodifiableList(parsed);
    }
  }

  /**
   * A numeric schema value as a {@code long}. It arrives as whatever the JSON decoder chose - an {@link Integer}
   * for a small count, a {@link Long} for a millisecond duration - so a plain cast to either one throws on the
   * other.
   */
  private static long asLong(final Object value, final long defaultValue) {
    return value instanceof Number number ? number.longValue() : defaultValue;
  }

  @Override
  public String getTimestampColumn() {
    return timestampColumn;
  }

  @Override
  public String getPrecision() {
    return precision;
  }

  @Override
  public int getShardCount() {
    return shardCount;
  }

  @Override
  public long getRetentionMs() {
    return retentionMs;
  }

  @Override
  public long getCompactionBucketIntervalMs() {
    return compactionBucketIntervalMs;
  }

  @Override
  public List<ColumnDefinition> getTsColumns() {
    return tsColumns;
  }

  @Override
  public List<DownsamplingTier> getDownsamplingTiers() {
    return downsamplingTiers;
  }
}
