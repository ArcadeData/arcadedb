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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ContinuousAggregateImpl implements ContinuousAggregate {
  private final Database database;
  private final String   name;
  private final String   query;
  private final String   backingTypeName;
  private final String   sourceTypeName;
  private final long     bucketIntervalMs;
  private final String   bucketColumn;
  private final String   timestampColumn;
  private volatile long                   watermarkTs;
  private volatile long                   lastRefreshTime;
  private volatile MaterializedViewStatus status;
  private final    AtomicBoolean          refreshInProgress = new AtomicBoolean(false);

  // Runtime metrics (not persisted)
  private final AtomicLong refreshCount          = new AtomicLong(0);
  private final AtomicLong refreshTotalTimeMs    = new AtomicLong(0);
  private final AtomicLong refreshMinTimeMs      = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong refreshMaxTimeMs      = new AtomicLong(0);
  private final AtomicLong errorCount            = new AtomicLong(0);
  private final AtomicLong lastRefreshDurationMs = new AtomicLong(0);

  public ContinuousAggregateImpl(final Database database, final String name, final String query,
      final String backingTypeName, final String sourceTypeName,
      final long bucketIntervalMs, final String bucketColumn, final String timestampColumn) {
    this.database = database;
    this.name = name;
    this.query = query;
    this.backingTypeName = backingTypeName;
    this.sourceTypeName = sourceTypeName;
    this.bucketIntervalMs = bucketIntervalMs;
    this.bucketColumn = bucketColumn;
    this.timestampColumn = timestampColumn;
    this.watermarkTs = 0;
    this.lastRefreshTime = 0;
    this.status = MaterializedViewStatus.VALID;
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
    return database.getSchema().getType(backingTypeName);
  }

  public String getBackingTypeName() {
    return backingTypeName;
  }

  @Override
  public String getSourceTypeName() {
    return sourceTypeName;
  }

  @Override
  public String getStatus() {
    return status.name();
  }

  @Override
  public long getWatermarkTs() {
    return watermarkTs;
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

  public void setStatus(final MaterializedViewStatus status) {
    this.status = status;
  }

  public void setWatermarkTs(final long watermarkTs) {
    this.watermarkTs = watermarkTs;
  }

  public void setLastRefreshTime(final long lastRefreshTime) {
    this.lastRefreshTime = lastRefreshTime;
  }

  public void updateLastRefreshTime() {
    this.lastRefreshTime = System.currentTimeMillis();
  }

  public boolean tryBeginRefresh() {
    return refreshInProgress.compareAndSet(false, true);
  }

  public void endRefresh() {
    refreshInProgress.set(false);
  }

  @Override
  public long getRefreshCount() {
    return refreshCount.get();
  }

  @Override
  public long getRefreshTotalTimeMs() {
    return refreshTotalTimeMs.get();
  }

  @Override
  public long getRefreshMinTimeMs() {
    final long v = refreshMinTimeMs.get();
    return v == Long.MAX_VALUE ? 0 : v;
  }

  @Override
  public long getRefreshMaxTimeMs() {
    return refreshMaxTimeMs.get();
  }

  @Override
  public long getErrorCount() {
    return errorCount.get();
  }

  @Override
  public long getLastRefreshDurationMs() {
    return lastRefreshDurationMs.get();
  }

  public void recordRefreshSuccess(final long durationMs) {
    refreshCount.incrementAndGet();
    refreshTotalTimeMs.addAndGet(durationMs);
    lastRefreshDurationMs.set(durationMs);
    long prev;
    do {
      prev = refreshMinTimeMs.get();
      if (durationMs >= prev)
        break;
    } while (!refreshMinTimeMs.compareAndSet(prev, durationMs));
    do {
      prev = refreshMaxTimeMs.get();
      if (durationMs <= prev)
        break;
    } while (!refreshMaxTimeMs.compareAndSet(prev, durationMs));
  }

  public void recordRefreshError() {
    errorCount.incrementAndGet();
  }

  @Override
  public void refresh() {
    ContinuousAggregateRefresher.incrementalRefresh(database, this);
  }

  @Override
  public void drop() {
    database.getSchema().dropContinuousAggregate(name);
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
    json.put("lastRefreshTime", lastRefreshTime);
    json.put("status", status.name());
    return json;
  }

  public static ContinuousAggregateImpl fromJSON(final Database database, final JSONObject json) {
    final String loadedName = json.getString("name");
    if (loadedName != null && loadedName.contains("`"))
      throw new IllegalArgumentException("Continuous aggregate name loaded from schema contains illegal backtick character: " + loadedName);

    final ContinuousAggregateImpl ca = new ContinuousAggregateImpl(
        database,
        loadedName,
        json.getString("query"),
        json.getString("backingType"),
        json.getString("sourceType"),
        json.getLong("bucketIntervalMs", 0),
        json.getString("bucketColumn"),
        json.getString("timestampColumn"));
    ca.watermarkTs = json.getLong("watermarkTs", 0);
    ca.lastRefreshTime = json.getLong("lastRefreshTime", 0);
    ca.status = MaterializedViewStatus.valueOf(json.getString("status", "VALID"));
    return ca;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final ContinuousAggregateImpl that = (ContinuousAggregateImpl) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return "ContinuousAggregate{name='" + name + "', status=" + status +
        ", watermarkTs=" + watermarkTs + ", bucketColumn='" + bucketColumn + "'}";
  }
}
