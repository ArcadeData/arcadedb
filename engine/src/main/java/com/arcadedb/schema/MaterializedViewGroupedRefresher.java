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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.MaterializedViewGroupedPlan.Contribution;
import com.arcadedb.schema.MaterializedViewGroupedState.Group;
import com.arcadedb.schema.MaterializedViewGroupedState.Groups;
import com.arcadedb.schema.MaterializedViewGroupedState.Outcome;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * The passes of a grouped materialized view maintained by delta (issue #11937).
 * <p>
 * A delta pass applies the delta of the commits drained at its start to the view's in-memory state, and writes the rows of the
 * groups it changed: a commit costs what it changed, not the size of the source. A rebuild pass scans the source and builds the
 * state and every row from scratch: on the first pass after an open, after a manual or a recovery refresh, after a change of the
 * source's schema, and whenever a delta cannot be applied exactly (a group lost its {@code min} or {@code max}, a delta past the
 * cap). When the values themselves are outside what a delta computes exactly (a floating point {@code sum}, numbers of different
 * classes in one group...), or the groups are more than a state may hold in memory (see
 * {@link MaterializedViewGroupedState#maxGroups}), the view takes the full pass of the defining query after every commit, as every
 * INCREMENTAL grouped view did before, until its next manual refresh.
 */
final class MaterializedViewGroupedRefresher {
  /** A rebuild whose scan raced commits it cannot correct for is made again, this many times at most. */
  private static final int REBUILD_ATTEMPTS = 3;
  /** Test-only: told each record the scan of a rebuild pass reaches, before it reads it. Null outside tests. */
  static volatile Consumer<RID> scanObserver;
  /** Test-only: run by a committing transaction of a grouped view between taking itself out of flight and handing over its delta. */
  static volatile Runnable deliveryObserver;

  private MaterializedViewGroupedRefresher() {
  }

  static void pass(final Database database, final MaterializedViewImpl view, final MaterializedViewImpl.PendingDelta drained) {
    final MaterializedViewGroupedPlan plan = view.groupedPlan();
    if (plan == null) {
      MaterializedViewRefresher.refreshOnce(database, view);
      return;
    }
    final List<Object> sourceSchema = sourceSchema(database, plan);
    final MaterializedViewImpl.GroupedRuntime runtime = view.groupedRuntime();
    if (!drained.full() && runtime != null && runtime.plan() == plan && runtime.sourceSchema().equals(sourceSchema)) {
      if (runtime.groups() == null) {
        // the values are outside the exact domain: the delta is dropped, and the defining query makes the view
        MaterializedViewRefresher.refreshOnce(database, view);
        return;
      }
      final Groups delta = drained.groups();
      if (delta == null)
        return;
      final Map<List<Object>, Group> changed = new HashMap<>();
      if (runtime.groups().apply(delta, changed) == Outcome.APPLIED) {
        if (!changed.isEmpty()) {
          writeChanges(database, view, runtime.groups(), changed);
          // New groups made it grow past what a state may hold: it is dropped, and the defining query keeps the view from now on
          if (runtime.groups().size() > MaterializedViewGroupedState.maxGroups(plan))
            view.setGroupedRuntime(new MaterializedViewImpl.GroupedRuntime(plan, sourceSchema, null));
        }
        return;
      }
    }
    rebuild(database, view, plan, sourceSchema);
  }

  /**
   * Builds the state from a scan of the source, corrected for the commits the scan raced (see
   * {@link MaterializedViewGroupedState.Watch}), and rewrites every row. A state built while the caller holds a transaction of its
   * own is not kept: the changes that transaction made before the view listened (a view created inside it) would be in no delta,
   * so the next pass rebuilds again.
   */
  private static void rebuild(final Database database, final MaterializedViewImpl view, final MaterializedViewGroupedPlan plan,
      final List<Object> sourceSchema) {
    final boolean keepState = !database.isTransactionActive();
    final int maxGroups = MaterializedViewGroupedState.maxGroups(plan);
    for (int attempt = 0; attempt < REBUILD_ATTEMPTS; attempt++) {
      final Outcome outcome = rebuildOnce(database, view, plan, sourceSchema, keepState, maxGroups);
      if (outcome == Outcome.APPLIED)
        return;
      if (outcome == Outcome.INEXACT)
        break;
    }

    MaterializedViewRefresher.refreshOnce(database, view);
    view.setGroupedRuntime(keepState ? new MaterializedViewImpl.GroupedRuntime(plan, sourceSchema, null) : null);
  }

  /**
   * One attempt of a rebuild.
   *
   * @return {@code APPLIED} when the state is built and the rows are written, {@code REBUILD} when the attempt has to be made again
   * (a commit it cannot correct for, or a group that lost its {@code min}/{@code max}), {@code INEXACT} when the view is not to be
   * maintained by delta: its values leave the exact domain, or its groups are more than a state may hold
   */
  private static Outcome rebuildOnce(final Database database, final MaterializedViewImpl view,
      final MaterializedViewGroupedPlan plan, final List<Object> sourceSchema, final boolean keepState, final int maxGroups) {
    final MaterializedViewGroupedState.Watch watch = view.openGroupedWatch();
    final Groups built = new Groups(plan);
    try {
      final WhereClause where = plan.newWhere();
      final CommandContext context = MaterializedViewGroupedPlan.newContext(database);
      database.transaction(() -> database.scanType(plan.sourceType, true, record -> {
        final Consumer<RID> observer = scanObserver;
        if (observer != null)
          observer.accept(record.getIdentity());
        final Contribution contribution = plan.contributionOf(record, where, context);
        if (watch.scanning(record.getIdentity()))
          built.add(contribution, 1);
        // past the groups a state may hold the scan stops: the view is kept by the defining query
        return !built.inexact && built.size() <= maxGroups;
      }), false, 1);
    } finally {
      view.closeGroupedWatch(watch);
    }

    if (built.inexact || built.size() > maxGroups)
      return Outcome.INEXACT;
    if (watch.isDirty())
      return Outcome.REBUILD;
    for (final MaterializedViewGroupedState.Watch.Entry entry : watch.entries().values()) {
      built.add(entry.counted, -1);
      built.add(entry.after, 1);
    }
    final Outcome outcome = built.seal();
    if (outcome != Outcome.APPLIED)
      return outcome;
    // the corrections can add groups too
    if (built.size() > maxGroups)
      return Outcome.INEXACT;

    writeAll(database, view, built);
    view.setGroupedRuntime(keepState ? new MaterializedViewImpl.GroupedRuntime(plan, sourceSchema, built) : null);
    return Outcome.APPLIED;
  }

  /** Writes the row of every group over the view's records, in place where it can, as the full pass of the defining query does. */
  private static void writeAll(final Database database, final MaterializedViewImpl view, final Groups built) {
    final String backingTypeName = view.getBackingTypeName();
    final List<Group> groups = new ArrayList<>(built.groups.values());
    final RID[] rows = new RID[groups.size()];
    try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(view)) {
      database.transaction(() -> {
        final RecordIdentities previous = MaterializedViewRefresher.collectIdentities(database, backingTypeName);
        int reused = 0;
        for (int i = 0; i < groups.size(); i++) {
          final MutableDocument doc = reused < previous.size() ?
              ((Document) database.lookupByRID(previous.get(reused++), true)).modify() :
              database.newDocument(backingTypeName);
          // The row comes from the source, not from the reused record: no stale-read refusal (#8610)
          doc.clearBasedOnStaleRead();
          MaterializedViewRefresher.applyRow(doc, row(database, built, groups.get(i)));
          doc.save();
          rows[i] = doc.getIdentity();
        }
        for (int i = reused; i < previous.size(); i++)
          database.lookupByRID(previous.get(i), true).delete();
      }, false);
    }
    for (int i = 0; i < groups.size(); i++)
      groups.get(i).row = rows[i];
  }

  /** Writes the rows of the groups a delta changed, then moves the state forward: only once the rows are committed. */
  private static void writeChanges(final Database database, final MaterializedViewImpl view, final Groups state,
      final Map<List<Object>, Group> changed) {
    final String backingTypeName = view.getBackingTypeName();
    try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(view)) {
      database.transaction(() -> {
        for (final Map.Entry<List<Object>, Group> entry : changed.entrySet()) {
          final Group current = state.groups.get(entry.getKey());
          final Group next = entry.getValue();
          if (next == null) {
            database.lookupByRID(current.row, true).delete();
            continue;
          }
          final MutableDocument doc;
          if (current != null) {
            doc = ((Document) database.lookupByRID(current.row, true)).modify();
            doc.clearBasedOnStaleRead();
          } else
            doc = database.newDocument(backingTypeName);
          MaterializedViewRefresher.applyRow(doc, row(database, state, next));
          doc.save();
          next.row = doc.getIdentity();
        }
      }, false);
    }
    for (final Map.Entry<List<Object>, Group> entry : changed.entrySet())
      if (entry.getValue() == null)
        state.groups.remove(entry.getKey());
      else
        state.groups.put(entry.getKey(), entry.getValue());
  }

  private static ResultInternal row(final Database database, final Groups groups, final Group group) {
    final ResultInternal row = new ResultInternal(database);
    final String[] columns = groups.plan.columnNames;
    for (int i = 0; i < columns.length; i++)
      row.setProperty(columns[i], groups.columnValue(group, i));
    return row;
  }

  /**
   * What of the source's schema a delta's exactness depends on: the types {@code SELECT FROM <source>} reads (the source and its
   * subtypes), their buckets and their properties' types. A type made a subtype of the source, or a property whose type changed,
   * brings records into the view or changes their values without one record event: the state is rebuilt.
   */
  static List<Object> sourceSchema(final Database database, final MaterializedViewGroupedPlan plan) {
    final List<Object> description = new ArrayList<>();
    if (database.getSchema().existsType(plan.sourceType))
      describe(database.getSchema().getType(plan.sourceType), description, new HashSet<>());
    return description;
  }

  private static void describe(final DocumentType type, final List<Object> description, final Set<String> described) {
    if (!described.add(type.getName()))
      return;
    description.add(type.getName());
    for (final Bucket bucket : type.getBuckets(false))
      description.add(bucket.getFileId());
    final List<String> properties = new ArrayList<>();
    for (final String name : type.getPolymorphicPropertyNames())
      properties.add(name + ':' + type.getPolymorphicPropertyIfExists(name).getType());
    properties.sort(Comparator.naturalOrder());
    description.addAll(properties);
    final List<DocumentType> subTypes = new ArrayList<>(type.getSubTypes());
    subTypes.sort(Comparator.comparing(DocumentType::getName));
    for (final DocumentType subType : subTypes)
      describe(subType, description, described);
  }
}
