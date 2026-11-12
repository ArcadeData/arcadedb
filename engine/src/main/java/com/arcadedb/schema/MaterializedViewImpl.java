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
import com.arcadedb.database.Database;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MaterializedViewImpl implements MaterializedView, TypeNameDependant, BackingTypeOwner {
  private final Database database;
  private final String name;
  // volatile, not final: a rename of a source type rewrites them in place (#10096), see retargetSourceType()
  private volatile String query;
  private final String backingTypeName;
  /** No refresh running. */
  private static final int REFRESH_IDLE            = 0;
  /** A refresh is running and no further one has been requested. */
  private static final int REFRESH_RUNNING         = 1;
  /** A refresh is running and at least one more was requested while it ran. */
  private static final int REFRESH_RUNNING_PENDING = 2;

  private volatile List<String> sourceTypeNames;
  private final MaterializedViewRefreshMode refreshMode;
  private final boolean simpleQuery;
  private final long refreshInterval;
  private volatile long lastRefreshTime;
  private volatile MaterializedViewStatus status;
  private volatile MaterializedViewChangeListener changeListener;
  /**
   * The rows carry a {@code _sourceRID} link to the source record they were computed from, indexed UNIQUE (issue #11475). Decided
   * once, when the link is created, and persisted: it is not recomputed from the query at load.
   */
  private volatile boolean perRecord;
  /**
   * The derived queries of a per-record view (issue #11475), each built on first use and tagged with the defining query it was
   * built from: a build that raced a change of the defining query stores a tag that no longer matches, and the next reader
   * rebuilds instead of running the stale text. Null until first use and after {@link #invalidateDerivedQuery()}.
   */
  private volatile DerivedQuery derivedQuery;
  private volatile DerivedQuery derivedRidListQuery;
  /**
   * The view an ALTER registered in place of this one (issue #11475), or null while this one is registered. An after-commit
   * callback captured against this instance by a transaction still open across the ALTER follows it, see {@link #current()}.
   */
  private volatile MaterializedViewImpl successor;
  /**
   * The source records committed since the last per-record pass drained them, or the mark that the next pass must be a full one
   * (issue #11475). Appended by the after-commit callback of every committing transaction BEFORE it tries to begin the refresh,
   * drained by the refresh owner at the start of each pass: a RID appended while a pass runs is therefore drained by the next
   * pass of the same owner, never lost between the two. Guarded by {@link #pendingDeltaLock}; held for a copy or a swap only.
   */
  private final Object           pendingDeltaLock = new Object();
  private       RecordIdentities pendingRids;
  /** The raw size past which {@link #pendingRids} is counted next: the listener's doubling guard, so an append does not re-sort. */
  private       int              pendingNextCountAt;
  private       boolean          pendingFull;
  private       boolean          pendingRecovery;
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

  /**
   * A source type of this view was renamed (issue #10096): the name that spells it, here and in the defining query, becomes the
   * new one. Done in place, not by building another view, because the incremental listener, the periodic scheduler entry and the
   * refresh state all hold THIS instance. The names are what the schema persists, so the caller saves the schema after.
   * <p>
   * The names and the query are two writes, not one: the caller holds the database write lock, so no registration or schema save
   * sees them apart, and a refresh that races the rename reads the old query against a type that was just renamed, fails once
   * and succeeds on the next refresh. One immutable holder would close that window and cost every reader an indirection for a
   * rename that happens once in the life of a database.
   *
   * @param newQuery the defining query naming the type by its new name
   */
  void retargetSourceType(final String oldName, final String newName, final String newQuery) {
    final List<String> renamed = new ArrayList<>(sourceTypeNames.size());
    for (final String sourceTypeName : sourceTypeNames)
      renamed.add(sourceTypeName.equals(oldName) ? newName : sourceTypeName);
    sourceTypeNames = List.copyOf(renamed);
    query = newQuery;
    invalidateDerivedQuery();
  }

  /** Refuses the drop of its backing type and of every source type (issue #10096). */
  @Override
  public void checkTypeDrop(final LocalSchema schema, final String typeName, final String canonicalTypeName) {
    final List<String> sources = sourceTypeNames;
    final List<String> typeNames = new ArrayList<>(sources.size() + 1);
    typeNames.add(backingTypeName);
    typeNames.addAll(sources);
    TypeNameDependant.checkNamesResolve(schema, typeName, "materialized view", name, dropStatement(), typeNames);
    if (backingTypeName.equals(canonicalTypeName))
      throw new SchemaException("Cannot drop type '" + typeName + "' because it is the backing type for materialized view '" + name
          + "'. Drop the materialized view first with: " + dropStatement());
    for (final String sourceTypeName : sources)
      if (canonicalTypeName.equals(TypeNameDependant.canonicalNameOf(schema, sourceTypeName)))
        throw new SchemaException("Cannot drop type '" + typeName + "' because it is a source type for materialized view '" + name
            + "'. Drop the materialized view first with: " + dropStatement());
  }

  /**
   * Refuses the rename of the backing type, which the view is named after, and of a source type its query cannot be re-pointed
   * at with certainty (issue #10096).
   */
  @Override
  public void checkTypeRename(final LocalSchema schema, final String oldName, final String newName) {
    if (backingTypeName.equals(oldName))
      throw new SchemaException("Cannot rename type '" + oldName + "' because it is the backing type for materialized view '" + name
          + "': a view is named after its backing type. Drop the materialized view first with: " + dropStatement());
    if (sourceTypeNames.contains(oldName) && schema.retargetQuery(query, oldName, newName) == null)
      throw LocalSchema.cannotRepointQuery(oldName, newName, "materialized view", name, dropStatement());
  }

  @Override
  public void carryTypeRename(final LocalSchema schema, final String oldName, final String newName) {
    if (sourceTypeNames.contains(oldName))
      retargetSourceType(oldName, newName, schema.retargetedQuery("materialized view", name, query, oldName, newName));
  }

  @Override
  public String dropStatement() {
    return "DROP MATERIALIZED VIEW " + name;
  }

  @Override
  public Database getDatabase() {
    return database;
  }

  @Override
  public String kind() {
    return "materialized view";
  }

  @Override
  public String refreshStatement() {
    return "REFRESH MATERIALIZED VIEW " + name;
  }

  /**
   * Appends the names this view does not list yet (issue #10205). Called before the view's listeners are registered, which walk
   * the list, so a view loaded from a schema that tracked only its first FROM identifier is maintained by every type it reads.
   */
  void addSourceTypeNames(final List<String> typeNames) {
    final Set<String> merged = new LinkedHashSet<>(sourceTypeNames);
    if (merged.addAll(typeNames))
      sourceTypeNames = List.copyOf(merged);
  }

  public boolean isPerRecord() {
    return perRecord;
  }

  void setPerRecord(final boolean perRecord) {
    this.perRecord = perRecord;
  }

  /**
   * The query a per-record pass runs: the defining query with {@code @rid AS _sourceRID} added to its projection. Built from the
   * parsed statement once and cached until the defining query changes.
   *
   * @param forRidList whether the FROM target is the list of changed records, bound to the parameter
   *                   {@link MaterializedViewPerRecordRefresher#RIDS_PARAMETER} (the delta pass), rather than the source type (the
   *                   full pass). The RID-list variant also keeps only records of the source type or a subtype
   */
  String derivedQuery(final boolean forRidList) {
    final String current = query;
    final DerivedQuery cached = forRidList ? derivedRidListQuery : derivedQuery;
    if (cached != null && cached.from() == current)
      return cached.text();
    final DerivedQuery built = new DerivedQuery(current, MaterializedViewPerRecordRefresher.buildDerivedQuery(database, current, forRidList));
    if (forRidList)
      derivedRidListQuery = built;
    else
      derivedQuery = built;
    return built.text();
  }

  /** Records that {@code replacement} is registered in place of this view. Called by the ALTER that built it, under the write lock. */
  void replacedBy(final MaterializedViewImpl replacement) {
    successor = replacement;
  }

  /**
   * The registered view this instance stands for: itself, or the latest replacement an ALTER registered in its place (issue
   * #11475). One volatile read when no ALTER happened, so the commit path of an unaltered view pays nothing more.
   */
  MaterializedViewImpl current() {
    MaterializedViewImpl view = this;
    MaterializedViewImpl next;
    while ((next = view.successor) != null)
      view = next;
    return view;
  }

  void invalidateDerivedQuery() {
    derivedQuery = null;
    derivedRidListQuery = null;
  }

  /** A derived query and the defining query it was built from, compared by identity: the defining query is replaced, never mutated. */
  private record DerivedQuery(String from, String text) {
  }

  /**
   * What {@link #drainPendingDelta()} took: the changed source records ({@code rids}, always an instance the caller may sort, empty
   * when none), or {@code full} when the next pass must rewrite the whole view, for {@code recovery} after a failed pass or otherwise
   * because a full pass was asked for.
   */
  record PendingDelta(RecordIdentities rids, boolean full, boolean recovery) {
  }


  /**
   * Adds the source records a committed transaction changed to the view's pending delta. Called after the commit and before the
   * caller tries to begin the refresh, so whichever pass drains them reads a state that includes that commit. Nothing is added
   * while a full pass is pending: that pass drains the mark before running its query, so it covers these records too. The records
   * are copied: the caller keeps {@code rids}.
   */
  void appendPendingDelta(final RecordIdentities rids) {
    appendPendingDelta(rids, false);
  }

  /**
   * As {@link #appendPendingDelta(RecordIdentities)}, but hands {@code rids} over: when nothing is pending it becomes the pending
   * delta itself instead of being copied, so the caller must never read or change it again. The after-commit callback of a
   * transaction, whose buffer dies with it, is that caller.
   */
  void adoptPendingDelta(final RecordIdentities rids) {
    appendPendingDelta(rids, true);
  }

  private void appendPendingDelta(final RecordIdentities rids, final boolean adopt) {
    if (rids.size() == 0)
      return;
    final int maxDelta = maxPerRecordDelta();
    synchronized (pendingDeltaLock) {
      if (pendingFull)
        return;
      if (pendingRids == null) {
        if (adopt)
          pendingRids = rids;
        else {
          pendingRids = new RecordIdentities(rids.size());
          pendingRids.addAll(rids);
        }
        pendingNextCountAt = maxDelta;
      } else
        pendingRids.addAll(rids);
      // The raw list repeats the records changed more than once: only the distinct ones count against the cap. They are counted
      // when the raw size passes the cap and then only once it has doubled since the last count, as the transaction buffer does,
      // so appends near the cap do not each re-sort the whole list while holding the lock. The drain makes the final count
      if (pendingRids.size() <= pendingNextCountAt)
        return;
      pendingRids.sortAndDeduplicate();
      if (pendingRids.size() > maxDelta) {
        pendingFull = true;
        pendingRids = null;
      } else
        pendingNextCountAt = Math.max(maxDelta, pendingRids.size() * 2);
    }
  }

  /** The cap on the distinct source records one pass of a per-record view re-evaluates, read from the database's configuration. */
  int maxPerRecordDelta() {
    return Math.max(1, database.getConfiguration().getValueAsInteger(GlobalConfiguration.MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA));
  }

  /**
   * Marks the next pass as a full one, dropping the pending records it covers.
   *
   * @param recovery whether a failed pass asks for it (its drained records are gone) rather than a manual, periodic or overflow
   *                 request; sticky until drained
   */
  void requestFullPass(final boolean recovery) {
    synchronized (pendingDeltaLock) {
      pendingFull = true;
      pendingRecovery |= recovery;
      pendingRids = null;
    }
  }

  /**
   * Takes the pending delta and clears it, atomically with respect to {@link #appendPendingDelta} and {@link #requestFullPass}. The
   * doubling guard of the appends can leave more distinct records pending than the cap: they are counted here, and past the cap
   * the pass is a full one, so no pass re-evaluates more than the cap one record at a time.
   */
  PendingDelta drainPendingDelta() {
    final int maxDelta = maxPerRecordDelta();
    synchronized (pendingDeltaLock) {
      if (!pendingFull && pendingRids == null)
        return new PendingDelta(new RecordIdentities(1), false, false);
      if (!pendingFull && pendingRids.size() > maxDelta) {
        pendingRids.sortAndDeduplicate();
        if (pendingRids.size() > maxDelta) {
          pendingFull = true;
          pendingRids = null;
        }
      }
      final PendingDelta drained = new PendingDelta(pendingRids != null ? pendingRids : new RecordIdentities(1), pendingFull,
          pendingRecovery);
      pendingRids = null;
      pendingFull = false;
      pendingRecovery = false;
      return drained;
    }
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
    // The rows keep their link whatever the new mode (issue #11475); the derived query is rebuilt by the copy on first use
    copy.perRecord = this.perRecord;
    // What this view still owes goes with it: a full pass owed after a failed pass, dropped here, let the copy's next delta pass
    // end VALID over the records the failure lost. Copied, not drained: a pass of this instance still running drains its own.
    // A copy that is no longer INCREMENTAL keeps it until its next full REFRESH, which drains it
    synchronized (pendingDeltaLock) {
      if (pendingFull)
        copy.requestFullPass(pendingRecovery);
      else if (pendingRids != null)
        copy.appendPendingDelta(pendingRids);
    }
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
    json.put("perRecord", perRecord);
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
    view.perRecord = json.getBoolean("perRecord", false);
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
        ", status=" + status + ", simpleQuery=" + simpleQuery + ", perRecord=" + perRecord + '}';
  }
}
