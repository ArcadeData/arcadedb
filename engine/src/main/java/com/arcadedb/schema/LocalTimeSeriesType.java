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
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Schema type for TimeSeries data. Extends LocalDocumentType and
 * owns a TimeSeriesEngine for managing sharded time-series storage.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalTimeSeriesType extends LocalDocumentType {

  public static final String KIND_CODE = "t";

  private String                       timestampColumn;
  private int                          shardCount;
  private long                         retentionMs;
  private final List<ColumnDefinition> tsColumns = new ArrayList<>();
  private TimeSeriesEngine             engine;

  public LocalTimeSeriesType(final LocalSchema schema, final String name) {
    super(schema, name);
  }

  /**
   * Initializes the TimeSeriesEngine. Called after all column definitions are set.
   */
  public void initEngine() throws IOException {
    if (engine != null)
      return;
    engine = new TimeSeriesEngine((DatabaseInternal) schema.getDatabase(), name, tsColumns, shardCount > 0 ? shardCount : 1);
  }

  public TimeSeriesEngine getEngine() {
    return engine;
  }

  public String getTimestampColumn() {
    return timestampColumn;
  }

  public void setTimestampColumn(final String timestampColumn) {
    this.timestampColumn = timestampColumn;
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

  public List<ColumnDefinition> getTsColumns() {
    return tsColumns;
  }

  public void addTsColumn(final ColumnDefinition column) {
    tsColumns.add(column);
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = super.toJSON();
    // Override kind to "t"
    json.put("type", KIND_CODE);

    // TimeSeries-specific fields
    json.put("timestampColumn", timestampColumn);
    json.put("shardCount", shardCount);
    json.put("retentionMs", retentionMs);

    final JSONArray colArray = new JSONArray();
    for (final ColumnDefinition col : tsColumns) {
      final JSONObject colJson = new JSONObject();
      colJson.put("name", col.getName());
      colJson.put("dataType", col.getDataType().name());
      colJson.put("role", col.getRole().name());
      colArray.put(colJson);
    }
    json.put("tsColumns", colArray);

    return json;
  }

  /**
   * Restores TimeSeries-specific fields from schema JSON.
   */
  public void fromJSON(final JSONObject json) {
    timestampColumn = json.getString("timestampColumn", null);
    shardCount = json.getInt("shardCount", 1);
    retentionMs = json.getLong("retentionMs", 0L);

    tsColumns.clear();
    final JSONArray colArray = json.getJSONArray("tsColumns", null);
    if (colArray != null) {
      for (int i = 0; i < colArray.length(); i++) {
        final JSONObject colJson = colArray.getJSONObject(i);
        tsColumns.add(new ColumnDefinition(
            colJson.getString("name"),
            Type.getTypeByName(colJson.getString("dataType")),
            ColumnDefinition.ColumnRole.valueOf(colJson.getString("role"))
        ));
      }
    }
  }
}
