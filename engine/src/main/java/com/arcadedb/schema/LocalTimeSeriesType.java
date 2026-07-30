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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.TimeSeriesBucket;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Schema type for TimeSeries data. Extends LocalDocumentType and
 * owns a TimeSeriesEngine for managing sharded time-series storage.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalTimeSeriesType extends LocalDocumentType {

  public static final String KIND_CODE = "t";

  private String                       timestampColumn;
  private String                       precision;
  private int                          shardCount;
  private long                         retentionMs;
  private long                         compactionBucketIntervalMs;
  private int                          sealedFormatVersion  = TimeSeriesSealedStore.CURRENT_VERSION;
  // A type created by this build writes the current mutable row format; one restored from JSON keeps
  // whatever version wrote its pages, which is what lets a pre-#5519 database open unchanged.
  private int                          mutableFormatVersion = TimeSeriesBucket.CURRENT_VERSION;
  private final List<ColumnDefinition>  tsColumns         = new ArrayList<>();
  private       List<DownsamplingTier> downsamplingTiers = new ArrayList<>();
  private volatile TimeSeriesEngine    engine;

  public LocalTimeSeriesType(final LocalSchema schema, final String name) {
    super(schema, name);
  }

  /**
   * Initializes the TimeSeriesEngine. Called after all column definitions are set.
   * Thread-safe: double-checked locking on the volatile {@code engine} field.
   */
  public synchronized void initEngine() throws IOException {
    if (engine != null)
      return;
    engine = new TimeSeriesEngine((DatabaseInternal) schema.getDatabase(), name, tsColumns, shardCount > 0 ? shardCount : 1,
        compactionBucketIntervalMs, mutableFormatVersion);
  }

  public TimeSeriesEngine getEngine() {
    return engine;
  }

  public void close() {
    if (engine != null) {
      try {
        engine.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing TimeSeriesEngine for type '%s': %s", e, name, e.getMessage());
      }
      engine = null;
    }
  }

  /**
   * Drops this timeseries type: deletes all shard data files (mutable bucket and sealed store)
   * from disk and shuts down the engine.
   */
  public void drop() {
    if (engine != null) {
      try {
        engine.drop();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error dropping TimeSeriesEngine for type '%s': %s", e, name, e.getMessage());
      }
      engine = null;
    }
  }

  public String getTimestampColumn() {
    return timestampColumn;
  }

  public void setTimestampColumn(final String timestampColumn) {
    this.timestampColumn = timestampColumn;
  }

  public String getPrecision() {
    return precision;
  }

  public void setPrecision(final String precision) {
    this.precision = precision;
  }

  public int getShardCount() {
    return shardCount;
  }

  public void setShardCount(final int shardCount) {
    this.shardCount = shardCount;
  }

  public long getRetentionMs() {
    return retentionMs;
  }

  public void setRetentionMs(final long retentionMs) {
    this.retentionMs = retentionMs;
  }

  public long getCompactionBucketIntervalMs() {
    return compactionBucketIntervalMs;
  }

  public void setCompactionBucketIntervalMs(final long compactionBucketIntervalMs) {
    this.compactionBucketIntervalMs = compactionBucketIntervalMs;
  }

  public List<ColumnDefinition> getTsColumns() {
    return tsColumns;
  }

  public void addTsColumn(final ColumnDefinition column) {
    tsColumns.add(column);
  }

  public List<DownsamplingTier> getDownsamplingTiers() {
    return downsamplingTiers;
  }

  public void setDownsamplingTiers(final List<DownsamplingTier> tiers) {
    checkForSchemaMutation();
    this.downsamplingTiers = tiers != null ? new ArrayList<>(tiers) : new ArrayList<>();
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = super.toJSON();
    // Override kind to "t"
    json.put("type", KIND_CODE);

    // TimeSeries-specific fields
    json.put("timestampColumn", timestampColumn);
    if (precision != null)
      json.put("precision", precision);
    json.put("shardCount", shardCount);
    json.put("retentionMs", retentionMs);
    if (compactionBucketIntervalMs > 0)
      json.put("compactionBucketIntervalMs", compactionBucketIntervalMs);
    json.put("sealedFormatVersion", sealedFormatVersion);
    // The version that wrote this type's pages, NOT the current constant: rewriting an existing v0
    // type as v1 would tell the next open to read inline tag bytes as dictionary ids.
    json.put("mutableFormatVersion", mutableFormatVersion);

    final JSONArray colArray = new JSONArray();
    for (final ColumnDefinition col : tsColumns) {
      final JSONObject colJson = new JSONObject();
      colJson.put("name", col.getName());
      colJson.put("dataType", col.getDataType().name());
      colJson.put("role", col.getRole().name());
      // The codec is not recorded inside a sealed block: it is resolved from the schema when the store
      // is opened, so it must be persisted or a future change to the default table would silently
      // decode existing blocks with the wrong codec (issue #5475).
      colJson.put("compression", col.getCompressionHint().name());
      colArray.put(colJson);
    }
    json.put("tsColumns", colArray);

    if (!downsamplingTiers.isEmpty()) {
      final JSONArray tierArray = new JSONArray();
      for (final DownsamplingTier tier : downsamplingTiers) {
        final JSONObject tierJson = new JSONObject();
        tierJson.put("afterMs", tier.afterMs());
        tierJson.put("granularityMs", tier.granularityMs());
        tierArray.put(tierJson);
      }
      json.put("downsamplingTiers", tierArray);
    }

    return json;
  }

  /**
   * Restores TimeSeries-specific fields from schema JSON.
   */
  public void fromJSON(final JSONObject json) {
    timestampColumn = json.getString("timestampColumn", null);
    precision = json.getString("precision", null);
    shardCount = json.getInt("shardCount", 1);
    retentionMs = json.getLong("retentionMs", 0L);
    compactionBucketIntervalMs = json.getLong("compactionBucketIntervalMs", 0L);
    sealedFormatVersion = json.getInt("sealedFormatVersion", 0);
    if (sealedFormatVersion != TimeSeriesSealedStore.CURRENT_VERSION)
      throw new IllegalStateException(
          "Unsupported sealed store format version " + sealedFormatVersion + " (expected " +
              TimeSeriesSealedStore.CURRENT_VERSION + ") for TimeSeries type '" + name + "'");
    // Older mutable formats stay readable: the version selects the row layout rather than gating the
    // open, so a type written before the tag dictionary (issue #5519) keeps its inline tag columns.
    // Only a version this build does not know about is refused.
    mutableFormatVersion = json.getInt("mutableFormatVersion", 0);
    if (mutableFormatVersion > TimeSeriesBucket.CURRENT_VERSION)
      throw new IllegalStateException(
          "Unsupported mutable bucket format version " + mutableFormatVersion + " (this build supports up to " +
              TimeSeriesBucket.CURRENT_VERSION + ") for TimeSeries type '" + name + "'");

    tsColumns.clear();
    final JSONArray colArray = json.getJSONArray("tsColumns", null);
    if (colArray != null) {
      for (int i = 0; i < colArray.length(); i++) {
        final JSONObject colJson = colArray.getJSONObject(i);
        final Type dataType = Type.getTypeByName(colJson.getString("dataType"));
        final ColumnDefinition.ColumnRole role = ColumnDefinition.ColumnRole.valueOf(colJson.getString("role"));
        // A schema written before issue #5475 has no codec recorded; its blocks were encoded with the
        // codec table of that build, so that table is what must decode them.
        final String compression = colJson.getString("compression", null);
        final TimeSeriesCodec codec = compression != null ?
            TimeSeriesCodec.valueOf(compression) :
            ColumnDefinition.legacyCodecFor(dataType, role);
        tsColumns.add(new ColumnDefinition(colJson.getString("name"), dataType, role, codec));
      }
    }

    downsamplingTiers.clear();
    final JSONArray tierArray = json.getJSONArray("downsamplingTiers", null);
    if (tierArray != null) {
      for (int i = 0; i < tierArray.length(); i++) {
        final JSONObject tierJson = tierArray.getJSONObject(i);
        downsamplingTiers.add(new DownsamplingTier(
            tierJson.getLong("afterMs"),
            tierJson.getLong("granularityMs")
        ));
      }
    }
  }
}
