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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;

/**
 * A read-only snapshot of a continuous aggregate, built from one row of {@code SELECT FROM schema:continuousaggregates}
 * (issue #7688). The counterpart of {@link RemoteMaterializedView}: the accessors answer from the row, and the
 * operations that act on the aggregate name their SQL alternative instead.
 */
public class RemoteContinuousAggregate implements ContinuousAggregate {
  private final String  name;
  private final String  query;
  private final String  backingTypeName;
  private final String  sourceTypeName;
  private final long    bucketIntervalMs;
  private final String  bucketColumn;
  private final String  timestampColumn;
  private final long    watermarkTs;
  private final boolean watermarkSet;
  private final long    lastRefreshTime;
  private final String  status;
  private final long    refreshCount;
  private final long    refreshTotalTimeMs;
  private final long    refreshMinTimeMs;
  private final long    refreshMaxTimeMs;
  private final long    errorCount;
  private final long    lastRefreshDurationMs;

  public RemoteContinuousAggregate(final Result result) {
    this.name = result.getProperty("name");
    this.query = result.getProperty("query");
    this.backingTypeName = result.getProperty("backingType");
    this.sourceTypeName = result.getProperty("sourceType");
    this.bucketIntervalMs = getLong(result, "bucketIntervalMs");
    this.bucketColumn = result.getProperty("bucketColumn");
    this.timestampColumn = result.getProperty("timestampColumn");
    this.watermarkTs = getLong(result, "watermarkTs");
    final Object set = result.getProperty("watermarkSet");
    this.watermarkSet = set instanceof Boolean b && b;
    this.lastRefreshTime = getLong(result, "lastRefreshTime");
    final Object st = result.getProperty("status");
    this.status = st != null ? st.toString() : "VALID";
    this.refreshCount = getLong(result, "refreshCount");
    this.refreshTotalTimeMs = getLong(result, "refreshTotalTimeMs");
    this.refreshMinTimeMs = getLong(result, "refreshMinTimeMs");
    this.refreshMaxTimeMs = getLong(result, "refreshMaxTimeMs");
    this.errorCount = getLong(result, "errorCount");
    this.lastRefreshDurationMs = getLong(result, "lastRefreshDurationMs");
  }

  private static long getLong(final Result result, final String property) {
    final Object value = result.getProperty(property);
    return value instanceof Number n ? n.longValue() : 0L;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public DocumentType getBackingType() {
    throw new UnsupportedOperationException(
        "getBackingType() is not supported in remote database. Use SQL SELECT FROM schema:types instead.");
  }

  /** The backing type's name, which the row carries even though the type itself cannot be handed out remotely. */
  public String getBackingTypeName() {
    return backingTypeName;
  }

  @Override
  public String getSourceTypeName() {
    return sourceTypeName;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public long getWatermarkTs() {
    return watermarkTs;
  }

  @Override
  public boolean isWatermarkSet() {
    return watermarkSet;
  }

  @Override
  public long getBucketIntervalMs() {
    return bucketIntervalMs;
  }

  @Override
  public String getBucketColumn() {
    return bucketColumn;
  }

  @Override
  public String getTimestampColumn() {
    return timestampColumn;
  }

  @Override
  public long getLastRefreshTime() {
    return lastRefreshTime;
  }

  @Override
  public void refresh() {
    throw new UnsupportedOperationException(
        "refresh() is not supported in remote database. Use SQL REFRESH CONTINUOUS AGGREGATE instead.");
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException(
        "drop() is not supported in remote database. Use SQL DROP CONTINUOUS AGGREGATE instead.");
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("name", name);
    json.put("query", query);
    json.put("backingType", backingTypeName);
    json.put("sourceType", sourceTypeName);
    json.put("bucketIntervalMs", bucketIntervalMs);
    json.put("bucketColumn", bucketColumn);
    json.put("timestampColumn", timestampColumn);
    json.put("watermarkTs", watermarkTs);
    json.put("watermarkSet", watermarkSet);
    json.put("lastRefreshTime", lastRefreshTime);
    json.put("status", status);
    return json;
  }

  @Override
  public long getRefreshCount() {
    return refreshCount;
  }

  @Override
  public long getRefreshTotalTimeMs() {
    return refreshTotalTimeMs;
  }

  @Override
  public long getRefreshMinTimeMs() {
    return refreshMinTimeMs;
  }

  @Override
  public long getRefreshMaxTimeMs() {
    return refreshMaxTimeMs;
  }

  @Override
  public long getErrorCount() {
    return errorCount;
  }

  @Override
  public long getLastRefreshDurationMs() {
    return lastRefreshDurationMs;
  }
}
