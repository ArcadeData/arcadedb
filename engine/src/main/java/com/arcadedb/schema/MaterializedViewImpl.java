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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MaterializedViewImpl implements MaterializedView {
  private final Database database;
  private final String name;
  private final String query;
  private final String backingTypeName;
  private final List<String> sourceTypeNames;
  private final MaterializedViewRefreshMode refreshMode;
  private final boolean simpleQuery;
  private final long refreshInterval;
  private volatile long lastRefreshTime;
  private volatile MaterializedViewStatus status;
  private volatile MaterializedViewChangeListener changeListener;
  private final AtomicBoolean refreshInProgress = new AtomicBoolean(false);

  // Runtime metrics (not persisted)
  private final AtomicLong refreshCount         = new AtomicLong(0);
  private final AtomicLong refreshTotalTimeMs   = new AtomicLong(0);
  private final AtomicLong refreshMinTimeMs     = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong refreshMaxTimeMs     = new AtomicLong(0);
  private final AtomicLong errorCount           = new AtomicLong(0);
  private final AtomicLong lastRefreshDurationMs = new AtomicLong(0);

  public MaterializedViewImpl(final Database database, final String name, final String query,
      final String backingTypeName, final List<String> sourceTypeNames,
      final MaterializedViewRefreshMode refreshMode, final boolean simpleQuery,
      final long refreshInterval) {
    this.database = database;
    this.name = name;
    this.query = query;
    this.backingTypeName = backingTypeName;
    this.sourceTypeNames = List.copyOf(sourceTypeNames);
    this.refreshMode = refreshMode;
    this.simpleQuery = simpleQuery;
    this.refreshInterval = refreshInterval;
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
  public List<String> getSourceTypeNames() {
    return sourceTypeNames;
  }

  @Override
  public MaterializedViewRefreshMode getRefreshMode() {
    return refreshMode;
  }

  @Override
  public long getLastRefreshTime() {
    return lastRefreshTime;
  }

  @Override
  public String getStatus() {
    return status.name();
  }

  @Override
  public boolean isSimpleQuery() {
    return simpleQuery;
  }

  public long getRefreshInterval() {
    return refreshInterval;
  }

  public void setStatus(final MaterializedViewStatus status) {
    this.status = status;
  }

  public MaterializedViewChangeListener getChangeListener() {
    return changeListener;
  }

  public void setChangeListener(final MaterializedViewChangeListener listener) {
    this.changeListener = listener;
  }

  public void updateLastRefreshTime() {
    this.lastRefreshTime = System.currentTimeMillis();
  }

  public void setLastRefreshTime(final long lastRefreshTime) {
    this.lastRefreshTime = lastRefreshTime;
  }

  /** Atomically marks refresh as in-progress. Returns {@code true} if successful, {@code false} if already running. */
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
    // Update min
    long prev;
    do {
      prev = refreshMinTimeMs.get();
      if (durationMs >= prev)
        break;
    } while (!refreshMinTimeMs.compareAndSet(prev, durationMs));
    // Update max
    do {
      prev = refreshMaxTimeMs.get();
      if (durationMs <= prev)
        break;
    } while (!refreshMaxTimeMs.compareAndSet(prev, durationMs));
  }

  public void recordRefreshError() {
    errorCount.incrementAndGet();
  }

  MaterializedViewImpl copyWithRefreshMode(final MaterializedViewRefreshMode newMode,
      final long newInterval) {
    final MaterializedViewImpl copy = new MaterializedViewImpl(
        database, name, query, backingTypeName, sourceTypeNames,
        newMode, simpleQuery, newInterval);
    copy.lastRefreshTime = this.lastRefreshTime;
    copy.status = this.status;
    return copy;
  }

  @Override
  public void refresh() {
    MaterializedViewRefresher.fullRefresh(database, this);
  }

  @Override
  public void drop() {
    database.getSchema().dropMaterializedView(name);
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("name", name);
    json.put("query", query);
    json.put("backingType", backingTypeName);
    json.put("refreshMode", refreshMode.name());
    json.put("simpleQuery", simpleQuery);
    json.put("refreshInterval", refreshInterval);
    json.put("lastRefreshTime", lastRefreshTime);
    json.put("status", status.name());
    final JSONArray srcTypes = new JSONArray();
    for (final String src : sourceTypeNames)
      srcTypes.put(src);
    json.put("sourceTypes", srcTypes);
    return json;
  }

  public static MaterializedViewImpl fromJSON(final Database database, final JSONObject json) {
    final List<String> sourceTypes = new ArrayList<>();
    final JSONArray srcArray = json.getJSONArray("sourceTypes");
    for (int i = 0; i < srcArray.length(); i++)
      sourceTypes.add(srcArray.getString(i));

    final String loadedName = json.getString("name");
    if (loadedName != null && loadedName.contains("`"))
      throw new IllegalArgumentException("Materialized view name loaded from schema contains illegal backtick character: " + loadedName);

    final MaterializedViewImpl view = new MaterializedViewImpl(
        database,
        loadedName,
        json.getString("query"),
        json.getString("backingType"),
        sourceTypes,
        MaterializedViewRefreshMode.valueOf(json.getString("refreshMode")),
        json.getBoolean("simpleQuery", false),
        json.getLong("refreshInterval", 0));
    view.lastRefreshTime = json.getLong("lastRefreshTime", 0);
    view.status = MaterializedViewStatus.valueOf(json.getString("status", "VALID"));
    return view;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final MaterializedViewImpl that = (MaterializedViewImpl) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return "MaterializedView{name='" + name + "', refreshMode=" + refreshMode +
        ", status=" + status + ", simpleQuery=" + simpleQuery + '}';
  }
}
