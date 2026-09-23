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
  /**
   * The watermark and whether it has ever been set, as ONE value.
   * <p>
   * #8152: 0 used to mean BOTH "the watermark has never been set" and "the watermark is the epoch". A continuous
   * aggregate whose newest bucket legitimately IS the epoch therefore never had its incomplete-bucket rows deleted
   * before being recomputed, so that one bucket accumulated a duplicate on every refresh. The flag separates the two
   * readings; {@link #getWatermarkTs()} keeps answering 0 while unset, which is what the public API always returned.
   * <p>
   * The two are held in one immutable record behind one {@code volatile} rather than as two independent fields, so
   * a reader racing a refresh - {@code SELECT FROM schema:continuousAggregates}, say - cannot observe a new
   * timestamp beside a stale flag. They only mean anything read together (found in review).
   */
  private volatile Watermark              watermark = Watermark.UNSET;
  /**
   * Set while loading a schema written BEFORE #8152 whose watermark is the ambiguous 0 - which, since the defect
   * kept every such watermark at 0, is every aggregate that ever ran on an affected version. Its backing type holds
   * one full copy of the aggregate per refresh that has happened, and simply resuming would add one more before the
   * repaired watermark took hold. The next refresh therefore clears the backing type and rebuilds it once.
   * <p>
   * Not persisted: it exists only between reading such a schema and the first refresh that repairs it, after which
   * the schema is written with the flag this absence stood for.
   */
  private volatile boolean                needsCleanRebuild;
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
    this.watermark = Watermark.UNSET;
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
    return watermark.timestamp();
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
    this.watermark = new Watermark(watermarkTs, true);
  }

  /**
   * {@code true} once a refresh has actually read a bucket out of the defining query - see {@link #watermark}.
   */
  @Override
  public boolean isWatermarkSet() {
    return watermark.set();
  }

  /**
   * Restores the watermark to a previously observed state, flag included. Used to roll back an advance whose
   * persistence failed, which must leave the aggregate exactly as it was rather than pinned at a spurious epoch.
   */
  void restoreWatermark(final long watermarkTs, final boolean watermarkSet) {
    this.watermark = watermarkSet ? new Watermark(watermarkTs, true) : Watermark.UNSET;
  }

  /**
   * {@code true} while this aggregate still carries the duplicate rows of a pre-#8152 database - see
   * {@link #needsCleanRebuild}. Cleared by the refresh that rebuilds it.
   */
  boolean needsCleanRebuild() {
    return needsCleanRebuild;
  }

  void cleanRebuildDone() {
    this.needsCleanRebuild = false;
  }

  /**
   * Both halves of the watermark in ONE read, for a caller that acts on the pair - the refresher decides whether to
   * delete from the flag and what to delete from the timestamp, and two separate getter calls could straddle a
   * concurrent advance.
   */
  Watermark currentWatermark() {
    return watermark;
  }

  /**
   * The bucket boundary the last refresh reached, and whether a refresh has reached one at all. Immutable so that
   * the pair is published - and read - as a unit.
   */
  record Watermark(long timestamp, boolean set) {
    static final Watermark UNSET = new Watermark(0, false);
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
    final Watermark currentWatermark = watermark;
    json.put("watermarkTs", currentWatermark.timestamp());
    json.put("watermarkSet", currentWatermark.set());
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
    final long loadedWatermarkTs = json.getLong("watermarkTs", 0);
    // A schema written before #8152 carries no flag: fall back to the old reading, where a non-zero watermark is the
    // only one that counts as set. Such an aggregate is healthy - its watermark did advance, so its refreshes did
    // delete before recomputing.
    final boolean legacy = !json.has("watermarkSet");
    ca.watermark = new Watermark(loadedWatermarkTs, json.getBoolean("watermarkSet", loadedWatermarkTs != 0));
    // A LEGACY WATERMARK OF 0 IS THE DEFECT ITSELF (found by CodeRabbit): it cannot be told from "never refreshed",
    // and because the defect pinned every watermark at 0 it also means the backing type already holds one copy of
    // the aggregate per refresh that has run. Resuming normally would add one more, so the next refresh rebuilds
    // from empty instead - which repairs the existing duplication rather than merely declining to add to it.
    ca.needsCleanRebuild = legacy && loadedWatermarkTs == 0;
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
        ", watermarkTs=" + watermark.timestamp() + ", bucketColumn='" + bucketColumn + "'}";
  }
}
