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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.exception.SchemaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for creating TimeSeries types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesTypeBuilder {

  private final DatabaseInternal       database;
  private       String                 typeName;
  private       String                 timestampColumn;
  private       int                    shards      = 0; // 0 = default (async worker threads)
  private       long                   retentionMs                = 0;
  private       long                   compactionBucketIntervalMs = 0;
  private       List<DownsamplingTier> downsamplingTiers          = new ArrayList<>();
  private final List<ColumnDefinition> columns                    = new ArrayList<>();

  public TimeSeriesTypeBuilder(final DatabaseInternal database) {
    this.database = database;
  }

  public TimeSeriesTypeBuilder withName(final String name) {
    this.typeName = name;
    return this;
  }

  public TimeSeriesTypeBuilder withTimestamp(final String name) {
    this.timestampColumn = name;
    this.columns.add(new ColumnDefinition(name, Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP));
    return this;
  }

  public TimeSeriesTypeBuilder withTag(final String name, final Type type) {
    this.columns.add(new ColumnDefinition(name, type, ColumnDefinition.ColumnRole.TAG));
    return this;
  }

  public TimeSeriesTypeBuilder withField(final String name, final Type type) {
    this.columns.add(new ColumnDefinition(name, type, ColumnDefinition.ColumnRole.FIELD));
    return this;
  }

  public TimeSeriesTypeBuilder withShards(final int shards) {
    this.shards = shards;
    return this;
  }

  public TimeSeriesTypeBuilder withRetention(final long retentionMs) {
    this.retentionMs = retentionMs;
    return this;
  }

  public TimeSeriesTypeBuilder withCompactionBucketInterval(final long compactionBucketIntervalMs) {
    this.compactionBucketIntervalMs = compactionBucketIntervalMs;
    return this;
  }

  public TimeSeriesTypeBuilder withDownsamplingTiers(final List<DownsamplingTier> tiers) {
    this.downsamplingTiers = tiers != null ? new ArrayList<>(tiers) : new ArrayList<>();
    return this;
  }

  public LocalTimeSeriesType create() {
    if (typeName == null || typeName.isEmpty())
      throw new SchemaException("TimeSeries type name is required");
    if (timestampColumn == null)
      throw new SchemaException("TimeSeries type requires a TIMESTAMP column");

    final LocalSchema schema = (LocalSchema) database.getSchema();
    if (schema.existsType(typeName))
      throw new SchemaException("Type '" + typeName + "' already exists");

    final LocalTimeSeriesType type = new LocalTimeSeriesType(schema, typeName);
    type.setTimestampColumn(timestampColumn);
    type.setShardCount(shards > 0 ? shards : database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS));
    type.setRetentionMs(retentionMs);
    type.setCompactionBucketIntervalMs(compactionBucketIntervalMs);
    type.setDownsamplingTiers(downsamplingTiers);

    for (final ColumnDefinition col : columns)
      type.addTsColumn(col);

    // Register properties for each column
    for (final ColumnDefinition col : columns)
      type.createProperty(col.getName(), col.getDataType());

    try {
      database.begin();
      type.initEngine();
      database.commit();
    } catch (final Exception e) {
      if (database.isTransactionActive())
        database.rollback();
      throw new SchemaException("Failed to initialize TimeSeries engine for type '" + typeName + "'", e);
    }

    // Register the type with the schema only after successful engine initialization
    schema.registerType(type);

    // Schedule automatic retention/downsampling if policies are defined
    schema.getTimeSeriesMaintenanceScheduler().schedule(database, type);

    schema.saveConfiguration();
    return type;
  }
}
