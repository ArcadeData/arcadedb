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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MaterializedViewImpl implements MaterializedView {
  private final Database database;
  private final String name;
  private final String query;
  private final String backingTypeName;
  /** No refresh running. */
  private static final int REFRESH_IDLE            = 0;
  /** A refresh is running and no further one has been requested. */
  private static final int REFRESH_RUNNING         = 1;
  /** A refresh is running and at least one more was requested while it ran. */
  private static final int REFRESH_RUNNING_PENDING = 2;

  private final List<String> sourceTypeNames;
  private final MaterializedViewRefreshMode refreshMode;
  private final boolean simpleQuery;
  private final long refreshInterval;
  private volatile long lastRefreshTime;
  private volatile MaterializedViewStatus status;
  private volatile MaterializedViewChangeListener changeListener;
  private final AtomicInteger refreshState = new AtomicInteger(REFRESH_IDLE);

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
    return refreshState.compareAndSet(REFRESH_IDLE, REFRESH_RUNNING);
  }

  /**
   * Releases ownership unconditionally, discarding any pending request without reporting it.
   * <p>
   * Prefer {@link #finishRefreshPassAndCheckPending()} (success) or
   * {@link #releaseRefreshAfterFailure()} (failure): both release with a CAS, so a request registered
   * concurrently is either serviced or reported. This method overwrites such a request silently,
   * which is the defect coalescing exists to prevent, so use it only where no coalescing is in play.
   */
  public void endRefresh() {
    refreshState.set(REFRESH_IDLE);
  }

  /**
   * Records that a refresh was requested while another was already running, so the running refresh
   * makes a further pass instead of the request being dropped. Returns {@code false} if no refresh is
   * running any more, in which case the caller must run the refresh itself.
   */
  public boolean markRefreshPendingIfRunning() {
    while (true) {
      final int state = refreshState.get();
      if (state == REFRESH_IDLE)
        return false;
      if (state == REFRESH_RUNNING_PENDING)
        return true;
      if (refreshState.compareAndSet(REFRESH_RUNNING, REFRESH_RUNNING_PENDING))
        return true;
    }
  }

  /**
   * Ends one refresh pass. Returns {@code true} if a request arrived while that pass was running, in
   * which case ownership is retained and the caller must make another pass; otherwise ownership is
   * released and {@code false} is returned.
   * <p>
   * Releasing ownership and testing for a pending request must be one atomic step: if they were
   * separate, a request registered in between would be seen by neither the outgoing owner nor the
   * requester (which observed the refresh as still running), and the view would stay stale forever.
   */
  public boolean finishRefreshPassAndCheckPending() {
    while (true) {
      if (refreshState.compareAndSet(REFRESH_RUNNING, REFRESH_IDLE))
        return false;
      if (refreshState.compareAndSet(REFRESH_RUNNING_PENDING, REFRESH_RUNNING))
        return true;
      if (refreshState.get() == REFRESH_IDLE)
        // Defensive: the owner is the only thread that moves RUNNING/RUNNING_PENDING back to IDLE,
        // so this is unreachable while ownership is held. Stop rather than spin if it ever is.
        return false;
    }
  }

  /**
   * Releases ownership after a pass that failed. Returns {@code true} if a request had been
   * registered during that pass and is therefore being discarded.
   * <p>
   * The release is a CAS rather than a plain write for the same reason as
   * {@link #finishRefreshPassAndCheckPending()}: a plain write would clobber a request registered
   * concurrently, losing it without the requester ever learning. The request is not retried here - a
   * pass that just failed would most likely fail again, and retrying a persistent failure would spin -
   * so the caller reports the discard and leaves the view in a non-VALID status, making the staleness
   * visible instead of silent.
   */
  public boolean releaseRefreshAfterFailure() {
    while (true) {
      if (refreshState.compareAndSet(REFRESH_RUNNING, REFRESH_IDLE))
        return false;
      if (refreshState.compareAndSet(REFRESH_RUNNING_PENDING, REFRESH_IDLE))
        return true;
      if (refreshState.get() == REFRESH_IDLE)
        return false;
    }
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
