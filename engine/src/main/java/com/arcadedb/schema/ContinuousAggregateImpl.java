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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.serializer.json.JSONObject;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ContinuousAggregateImpl implements ContinuousAggregate, TypeNameDependant {
  private final Database database;
  private final String   name;
  // volatile, not final: a rename of the source type rewrites them in place (#10096), see retargetSourceType()
  private volatile String query;
  private final String   backingTypeName;
  private volatile String sourceTypeName;
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
  /**
   * The time range samples of the source type were removed from since the buckets of the aggregate were last computed, or
   * {@code null} when none were (issue #10428). The refresh only recomputes the bucket open at the watermark and the newer ones, so
   * the buckets behind the watermark keep what they held when they were computed; this is what tells it which of them no longer
   * match the source. One range, the hull of every delete since: recomputing a bucket the delete did not reach gives the rows it
   * already had, so a wider range costs time and never correctness.
   * <p>
   * Part of the definition in the schema, unlike {@link #needsCleanRebuild}: a restart must not forget it, and under HA it has to
   * reach the followers in the replication unit of the delete (a follower promoted to leader would otherwise refresh without it).
   */
  private final    AtomicReference<Invalidation> invalidation = new AtomicReference<>();
  private volatile long                   lastRefreshTime;
  private volatile MaterializedViewStatus status;
  private final    AtomicBoolean          refreshInProgress = new AtomicBoolean(false);
  private final    AtomicBoolean          refreshRequested  = new AtomicBoolean(false);

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

  /**
   * The source type of this aggregate was renamed (issue #10096): the name that spells it, here and in the defining query,
   * becomes the new one. Done in place because the refresh state (watermark, status, metrics) and the schedulers hold THIS
   * instance. The names are what the schema persists, so the caller saves the schema after. The two writes are not atomic for a
   * refresh racing the rename, which fails once and succeeds on the next one: see {@link MaterializedViewImpl#retargetSourceType}.
   *
   * @param newQuery the defining query naming the type by its new name
   */
  void retargetSourceType(final String newName, final String newQuery) {
    sourceTypeName = newName;
    query = newQuery;
  }

  /** Refuses the drop of its backing type and of its source type (issue #10096). */
  @Override
  public void checkTypeDrop(final LocalSchema schema, final String typeName, final String canonicalTypeName) {
    final String source = sourceTypeName;
    TypeNameDependant.checkNamesResolve(schema, typeName, "continuous aggregate", name, dropStatement(),
        source != null ? List.of(backingTypeName, source) : List.of(backingTypeName));
    if (backingTypeName.equals(canonicalTypeName))
      throw new SchemaException("Cannot drop type '" + typeName + "' because it is the backing type for continuous aggregate '"
          + name + "'. Drop the continuous aggregate first with: " + dropStatement());
    if (source != null && canonicalTypeName.equals(TypeNameDependant.canonicalNameOf(schema, source)))
      throw new SchemaException("Cannot drop type '" + typeName + "' because it is the source type for continuous aggregate '"
          + name + "'. Drop the continuous aggregate first with: " + dropStatement());
  }

  /**
   * Refuses the rename of the backing type, which the aggregate is named after, and of the source type when its query cannot be
   * re-pointed at with certainty (issue #10096).
   */
  @Override
  public void checkTypeRename(final LocalSchema schema, final String oldName, final String newName) {
    if (backingTypeName.equals(oldName))
      throw new SchemaException("Cannot rename type '" + oldName + "' because it is the backing type for continuous aggregate '"
          + name + "': an aggregate is named after its backing type. Drop the continuous aggregate first with: " + dropStatement());
    if (oldName.equals(sourceTypeName) && schema.retargetQuery(query, oldName, newName) == null)
      throw LocalSchema.cannotRepointQuery(oldName, newName, "continuous aggregate", name, dropStatement());
  }

  @Override
  public void carryTypeRename(final LocalSchema schema, final String oldName, final String newName) {
    if (oldName.equals(sourceTypeName))
      retargetSourceType(newName, schema.retargetedQuery("continuous aggregate", name, query, oldName, newName));
  }

  private String dropStatement() {
    return "DROP CONTINUOUS AGGREGATE " + name;
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

  /**
   * Samples with a timestamp in {@code [fromTs, toTs]} were removed from the source type (issue #10428). The bounds are the ones of
   * the delete, not bucket boundaries: the refresh finds the buckets they reach in the backing type, which holds every bucket start
   * the grid of the aggregate produced, whatever origin or offset the {@code ts.timeBucket()} of its query gives it.
   */
  public record Invalidation(long fromTs, long toTs) {
    Invalidation merge(final Invalidation other) {
      return other == null ? this : new Invalidation(Math.min(fromTs, other.fromTs), Math.max(toTs, other.toTs));
    }
  }

  /**
   * Records that samples in {@code [fromTs, toTs]} of the source type were removed, so the next refresh recomputes the buckets they
   * were in. The caller persists the schema (inside the replication unit of the delete, under HA) when the answer is {@code true}.
   *
   * @return {@code false} when the range was inside the one already recorded, which is then what the schema already holds. It is
   * recorded all the same, as a new instance: a refresh that read the range before this delete must not clear it
   */
  public boolean invalidate(final long fromTs, final long toTs) {
    final Invalidation added = new Invalidation(fromTs, toTs);
    final boolean[] widened = new boolean[1];
    invalidation.updateAndGet(current -> {
      // assigned on every try of the update: the last one is the one that took effect
      widened[0] = current == null || fromTs < current.fromTs() || toTs > current.toTs();
      return added.merge(current);
    });
    return widened[0];
  }

  /** The range recorded by {@link #invalidate}, or {@code null} when the buckets of the aggregate all match the source. */
  public Invalidation getInvalidation() {
    return invalidation.get();
  }

  /**
   * Forgets {@code recomputed}, the range a refresh has just recomputed. A no-op when another delete was recorded meanwhile, even
   * one inside the range, which widens nothing: that delete may have landed after the refresh read the source, so the next refresh
   * recomputes it. This rests on the {@link AtomicReference} comparing REFERENCES, and on {@link #invalidate} storing a new
   * instance every time - never an equal record already there - so it is the identity of the record that tells a refresh whether
   * anything was recorded since it read it.
   */
  void clearInvalidation(final Invalidation recomputed) {
    invalidation.compareAndSet(recomputed, null);
  }

  /** Puts back a range whose clearing could not be persisted, with whatever was recorded since. */
  void restoreInvalidation(final Invalidation recomputed) {
    invalidation.updateAndGet(current -> recomputed.merge(current));
  }

  /** The width of one bucket, for the refresh to bound the buckets a range reaches. */
  long bucketIntervalMs() {
    return bucketIntervalMs;
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

  /** A refresh was asked for and has not begun: see {@link ContinuousAggregateRefresher#incrementalRefresh}. */
  void requestRefresh() {
    refreshRequested.set(true);
  }

  boolean isRefreshRequested() {
    return refreshRequested.get();
  }

  void clearRefreshRequest() {
    refreshRequested.set(false);
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
    final Invalidation pending = invalidation.get();
    if (pending != null) {
      json.put("invalidatedFromTs", pending.fromTs());
      json.put("invalidatedToTs", pending.toTs());
    }
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
    if (json.has("invalidatedFromTs") && json.has("invalidatedToTs"))
      ca.invalidation.set(new Invalidation(json.getLong("invalidatedFromTs"), json.getLong("invalidatedToTs")));
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
