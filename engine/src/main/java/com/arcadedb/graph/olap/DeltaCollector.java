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
package com.arcadedb.graph.olap;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.OrderedAfterCommitCallback;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine.EdgeWriteListener;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Detects when a transaction affects vertex/edge types covered by the GAV and handles updates.
 * <p>
 * Behavior depends on the view's {@link GraphAnalyticalView.UpdateMode}:
 * <ul>
 *   <li><b>SYNCHRONOUS</b>: Collects detailed changes (TxDelta) during the transaction,
 *       then applies them to the overlay on commit — no stale window.</li>
 *   <li><b>ASYNCHRONOUS</b>: Collects the same changes, which the view merges into the overlay off the
 *       committing thread (issue #10189) - STALE until it has.</li>
 *   <li><b>OFF</b>: Only detects relevance, marks the view as STALE on commit.</li>
 * </ul>
 */
class DeltaCollector
    implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener, EdgeWriteListener {

  private static final AtomicInteger ANONYMOUS_COUNTER = new AtomicInteger();

  // Past this many DISTINCT edges' property changes in one transaction, they stop being tracked individually.
  // See trackEdgeUpdate(). Package-private so a test can reach the boundary without writing the number down
  // itself: the contract is "a repeat update never trips it", and retuning the number must not break that.
  static final int                  MAX_TRACKED_EDGE_UPDATES = 1024;

  private final GraphAnalyticalView view;
  private final String              callbackKey;

  // SYNCHRONOUS and ASYNCHRONOUS modes collect the changes of each transaction in a TransactionChanges the TRANSACTION keeps
  // (TransactionContext.putDerived), never one per thread: a transaction whose statements run on different threads (an HTTP
  // session, one request per statement on whichever worker is free) made changes the view never heard of (issue #11794)
  private final boolean                 collectsChanges;
  // The open transactions that created or deleted an edge the view follows. A full build that opens its watch while such a
  // transaction is still open registers their sources (issue #8378): the transaction reported them before any watch existed,
  // and its commit can still land before the new scan reads the source
  private final Set<TransactionChanges> inFlightTransactions;

  DeltaCollector(final GraphAnalyticalView view) {
    this.view = view;
    this.callbackKey = "gav-delta-" + (view.getName() != null ? view.getName() : "anon-" + ANONYMOUS_COUNTER.getAndIncrement());
    this.collectsChanges = view.getUpdateMode() != GraphAnalyticalView.UpdateMode.OFF;
    this.inFlightTransactions = collectsChanges ? ConcurrentHashMap.newKeySet() : null;
  }

  @Override
  public void onAfterCreate(final Record record) {
    if (!isRelevant(record)) {
      noteUncoveredEdge(record);
      return;
    }

    if (collectsChanges) {
      // SYNCHRONOUS/ASYNCHRONOUS: collect detailed changes
      final TransactionChanges changes = changesOfTransaction();
      if (record instanceof Vertex vertex) {
        if (changes != null)
          changes.delta.addedVertices.add(new TxDelta.VertexDelta(vertex.getIdentity(), extractProperties(vertex)));
      } else if (record instanceof Edge edge) {
        watchEdgeSource(changes, edge.getOut());
        if (changes != null)
          changes.delta.addEdge(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn(), edge.getIdentity(),
              extractMaterialisedEdgeProperties(edge)));
      }
    } else {
      scheduleAsyncCallback();
    }
  }

  @Override
  public void onAfterUpdate(final Record record) {
    if (!isRelevant(record))
      return;

    if (collectsChanges) {
      // SYNCHRONOUS/ASYNCHRONOUS: collect property changes
      final TransactionChanges changes = changesOfTransaction();
      if (changes == null)
        return;
      if (record instanceof Vertex vertex)
        changes.delta.updatedProperties.put(vertex.getIdentity(), extractProperties(vertex));
      else if (record instanceof Edge edge)
        // Reported with its new values rather than as a bare "something changed" flag: an edge the overlay
        // itself holds carries its values there and can simply take the new ones, while an edge in the base
        // CSR is addressed by a column slot nothing maps back from its RID and needs the rebuild the flag used
        // to force unconditionally. DeltaOverlay.merge() is where the two are told apart, since it is what
        // knows which edges the overlay holds. See issues #4513 and #6315.
        trackEdgeUpdate(changes.delta, edge);
    } else {
      scheduleAsyncCallback();
    }
  }

  @Override
  public void onAfterDelete(final Record record) {
    if (!isRelevant(record))
      return;

    if (collectsChanges) {
      // SYNCHRONOUS/ASYNCHRONOUS: collect deletions
      final TransactionChanges changes = changesOfTransaction();
      if (record instanceof Vertex vertex) {
        if (changes != null)
          changes.delta.deletedVertices.add(vertex.getIdentity());
      } else if (record instanceof Edge edge) {
        watchEdgeSource(changes, edge.getOut());
        // An edge this transaction created is no change to the committed state once it deletes it (issue #10305). A lightweight
        // edge has no RID of its own to take back, its changes being matched by the pair of vertices they join
        if (changes != null && (edge instanceof LightEdge || !changes.delta.cancelAddedEdge(edge.getIdentity())))
          changes.delta.deletedEdges.add(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn(), changeKey(edge)));
      }
    } else {
      scheduleAsyncCallback();
    }
  }

  /**
   * The creation of a lightweight edge, which no record listener hears of: there is no record (issue #9572). Its
   * deletion needs no such hook, the record delete path reports it to {@link #onAfterDelete} like any other edge's.
   */
  @Override
  public void onLightEdgeCreated(final Edge edge) {
    if (!isRelevant(edge)) {
      noteUncoveredEdge(edge);
      return;
    }

    if (collectsChanges) {
      final TransactionChanges changes = changesOfTransaction();
      watchEdgeSource(changes, edge.getOut());
      // No properties to materialise: a lightweight edge has none
      if (changes != null)
        changes.delta.addEdge(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn(), changeKey(edge)));
    } else
      scheduleAsyncCallback();
  }

  /**
   * A bulk load wrote edges no event reported one by one (issue #9572). They are not collected: a batch writes them by
   * the million, and the view answers with a rebuild, which is what that many changes cost the overlay anyway.
   */
  @Override
  public void onEdgesWrittenInBulk(final Set<String> edgeTypeNames) {
    if (!view.isTrackingChanges())
      return;
    boolean covered = false;
    for (final String edgeTypeName : edgeTypeNames)
      if (view.coversEdgeType(edgeTypeName))
        covered = true;
      else
        view.onUncoveredEdgeWritten(edgeTypeName);
    if (covered)
      view.onEdgesWrittenInBulk();
  }

  /**
   * An edge of a type the view does not materialize was created: the view no longer holds every edge of the database,
   * if it did (issue #9915, see {@link GraphAnalyticalView#coversEveryEdge()}). Told on the creation, inside the
   * transaction: one that rolls back only costs the view the claim until its next build.
   */
  private void noteUncoveredEdge(final Record record) {
    // NOT RELEVANT AND TRACKING: AN EDGE THE VIEW DOES NOT COVER
    if (record instanceof Edge edge && view.isTrackingChanges())
      view.onUncoveredEdgeWritten(edge.getTypeName());
  }

  /** The identity a change of {@code edge} is tracked by: its own RID, or a key of the change's own for a lightweight edge. */
  private static RID changeKey(final Edge edge) {
    final RID rid = edge.getIdentity();
    return edge instanceof LightEdge ? TxDelta.lightEdgeKey(rid.getBucketId()) : rid;
  }

  private boolean isRelevant(final Record record) {
    // A view whose first build is still scanning tracks changes too: the build reconciles them (issue #8378)
    if (!view.isTrackingChanges())
      return false;
    // Both routed by the schema alone: whether the base CSR already HOLDS the type's records is a question for the query that
    // wants the view served, and routing by it dropped the vertex of a type attached after the scan (issue #10491)
    if (record instanceof Vertex vertex) {
      view.meetWrittenVertexType(vertex.getTypeName());
      return view.routesVertexType(vertex.getTypeName());
    }
    if (record instanceof Edge edge)
      return view.coversEdgeType(edge.getTypeName());
    return false;
  }

  /**
   * The changes the current transaction made so far, created and registered with it on its first change: kept by the
   * transaction (issue #11794), so every statement of it adds to the same changes whichever thread runs it, and they go with
   * the transaction however it ends. Null when no transaction is active, or the database is closing: nothing is committed then.
   */
  private TransactionChanges changesOfTransaction() {
    try {
      final DatabaseInternal dbInternal = (DatabaseInternal) view.getDatabase();
      if (!dbInternal.isTransactionActive())
        return null;
      final TransactionContext tx = dbInternal.getTransaction();
      // The delta holds what the statement did, and keeps it: a statement that fails cannot be taken back (issue #10346)
      tx.markStatementUnrestorable("it changed records a graph analytical view follows");
      TransactionChanges changes = (TransactionChanges) tx.getDerived(this);
      if (changes == null) {
        changes = new TransactionChanges();
        tx.putDerived(this, changes);
        tx.addAfterCommitCallbackIfAbsent(callbackKey, changes);
      }
      return changes;
    } catch (final DatabaseIsClosedException e) {
      LogManager.instance().log(this, Level.FINE, "Delta collection skipped (database closing): %s", e.getMessage());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Delta collection failed for GraphAnalyticalView '%s'", e, view.getName());
    }
    return null;
  }

  /**
   * What one transaction changed in the graph the view follows, kept by the transaction itself ({@link TransactionContext#putDerived})
   * and reported to the view once it committed, in the order the transactions committed (issue #10249): the transaction takes its
   * place in the view's {@link CommitOrderGate} at the commit point, while it still holds its file locks, and delivers through it.
   * A transaction that never reaches the commit point (it had nothing to write) has no place and delivers straight away.
   * <p>
   * The transaction closes it when it ends, before its after-commit callbacks run. Its edge sources stay in flight past a commit
   * that reached the commit point, until the view has the change ({@link #run} or {@link #onAbandoned}); any other end (a
   * rollback, a commit with nothing written) takes them out there.
   */
  private final class TransactionChanges implements OrderedAfterCommitCallback, AutoCloseable {
    private final    TxDelta               delta = new TxDelta();
    // Read by a build opening its watch on another thread (registerInFlightEdgeSources), hence concurrent. Null until the
    // transaction reports an edge
    private volatile Set<RID>              edgeSources;
    // Set at the commit point: from there on the transaction reports the change, or abandons it, after it closed this
    private volatile boolean               committing;
    // Written at the commit point, by the thread publishing the pages, and read by the committing thread afterwards
    private volatile CommitOrderGate.Place place;

    private void watchEdgeSource(final RID source) {
      Set<RID> sources = edgeSources;
      if (sources == null) {
        // Only one thread runs a statement of the transaction at a time
        sources = ConcurrentHashMap.newKeySet();
        edgeSources = sources;
        inFlightTransactions.add(this);
      }
      sources.add(source);
    }

    private TxDelta freeze() {
      final TxDelta frozen = new TxDelta();
      frozen.addedVertices.addAll(delta.addedVertices);
      frozen.deletedVertices.addAll(delta.deletedVertices);
      delta.copyAddedEdgesTo(frozen.addedEdges);
      frozen.deletedEdges.addAll(delta.deletedEdges);
      frozen.updatedProperties.putAll(delta.updatedProperties);
      frozen.updatedEdges.putAll(delta.updatedEdges);
      frozen.forceEdgePropertyRebuild = delta.forceEdgePropertyRebuild;
      delta.clear();
      return frozen;
    }

    @Override
    public void onCommitPoint() {
      committing = true;
      // A place already taken stays: it is the earlier one, and a second would be left unreleased at the head of the line
      if (place != null)
        return;
      // Never fails in practice (a lock-free add); if it did, the place stays null and the change is delivered unordered, as before
      place = view.getCommitOrder().takePlace();
    }

    @Override
    public void onAbandoned() {
      try {
        final CommitOrderGate.Place abandoned = place;
        if (abandoned != null) {
          place = null;
          view.getCommitOrder().abandon(abandoned, view::onCommittedDelta);
        }
      } finally {
        inFlightTransactions.remove(this);
      }
    }

    @Override
    public void run() {
      final TxDelta frozen;
      try {
        frozen = freeze();
      } catch (final Throwable t) {
        // Its place must not stay at the head of the line, or every change behind it waits for it for as long as the view lives
        onAbandoned();
        throw t;
      }
      try {
        final CommitOrderGate.Place taken = place;
        if (taken != null)
          view.getCommitOrder().deliver(taken, frozen.isEmpty() ? null : frozen, view::onCommittedDelta);
        else if (!frozen.isEmpty())
          view.onCommittedDelta(frozen);
      } finally {
        // Only now: until the view has the change, a build that opens its watch must still take this transaction's sources as
        // in flight (see registerInFlightEdgeSources), as it may wait behind the changes committed before it
        inFlightTransactions.remove(this);
      }
    }

    @Override
    public void close() {
      // The transaction ended. Past the commit point the change is still on its way to the view: run() or onAbandoned() follow
      if (!committing)
        inFlightTransactions.remove(this);
    }
  }

  private void scheduleAsyncCallback() {
    try {
      final DatabaseInternal dbInternal = (DatabaseInternal) view.getDatabase();
      if (dbInternal.isTransactionActive()) {
        dbInternal.getTransaction().markStatementUnrestorable("it changed records a graph analytical view follows");
        dbInternal.getTransaction().addAfterCommitCallbackIfAbsent(callbackKey, view::onRelevantCommitCallback);
      }
    } catch (final DatabaseIsClosedException e) {
      LogManager.instance().log(this, Level.FINE, "Change detection skipped (database closing): %s", e.getMessage());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Change detection failed for GraphAnalyticalView '%s'", e, view.getName());
    }
  }

  /**
   * Records, before the transaction commits, that it creates or deletes an edge leaving {@code source}: in the transaction's
   * in-flight sources first, then in the watch of a build in flight. Written in that order, and read in the opposite one by
   * {@link #registerInFlightEdgeSources}, so a build opening its watch concurrently misses neither.
   */
  private void watchEdgeSource(final TransactionChanges changes, final RID source) {
    if (changes != null)
      changes.watchEdgeSource(source);
    view.watchEdgeSource(source);
  }

  /**
   * Registers with a build's newly opened watch the edge sources of every transaction still open, which reported them
   * before the watch existed (issue #8378). A source left over from a rolled-back transaction is registered too, which
   * costs an observation and changes no answer.
   */
  void registerInFlightEdgeSources(final BuildWatch watch) {
    if (inFlightTransactions == null)
      return;
    for (final TransactionChanges changes : inFlightTransactions) {
      final Set<RID> sources = changes.edgeSources;
      if (sources != null)
        for (final RID source : sources)
          watch.watchSource(source);
    }
  }

  /**
   * Forgets the transactions in flight. Called when the collector is deregistered: the changes each transaction collected go
   * with the transaction.
   */
  void close() {
    if (inFlightTransactions != null)
      inFlightTransactions.clear();
  }

  /**
   * Records one edge property change, or gives up on recording them individually.
   * <p>
   * Tracking each one is what lets {@link DeltaOverlay#merge} apply an update to an edge the overlay itself
   * holds without marking the base columns out of date (issue #6315). It is worth an entry per edge only while
   * there are few of them: a transaction rewriting a million edges' weights would otherwise hold a million of
   * these, where the old boolean flag held nothing. Past the cap the delta says only "the columns are out of
   * date", which is what a bulk rewrite means anyway - the rebuild it forces is going to reread every value
   * regardless, and until it lands the view serves no edge properties at all, so no stale one can escape.
   */
  private void trackEdgeUpdate(final TxDelta delta, final Edge edge) {
    // A view that materialises no edge property columns has nothing that can go out of date when an edge's
    // properties change, and nothing to rebuild them from - the base CSR holds the topology, which an update
    // does not touch. It was rebuilt anyway, on every single edge update, because #4513's flag was raised
    // before anyone asked whether there were columns at all. Which property changed is still not asked, and
    // cannot be: the listener is handed the record, not a diff against its previous values, so a view that
    // materialises `weight` cannot tell an update of `weight` from an update of `label` and has to assume the
    // worse of the two.
    final String[] materialised = view.getEdgePropertyFilter();
    if (materialised == null || materialised.length == 0)
      return;
    if (delta.forceEdgePropertyRebuild)
      return;
    final RID rid = edge.getIdentity();
    // Asked before the cap, not after: an edge already tracked does not grow the map, so a repeat update of
    // one - the accumulator this cap counts distinct edges precisely to tolerate - must not trip it merely
    // because the transaction has touched enough OTHER edges to fill it.
    if (!delta.updatedEdges.containsKey(rid) && delta.updatedEdges.size() >= MAX_TRACKED_EDGE_UPDATES) {
      delta.forceEdgePropertyRebuild = true;
      delta.updatedEdges.clear();
      return;
    }
    delta.updatedEdges.put(rid, new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(),
        edge.getIn(), rid, extractMaterialisedEdgeProperties(edge)));
  }

  /**
   * Reads the values of the edge properties the view materialises columns for, or {@code null} when it
   * materialises none.
   * <p>
   * Captured here, at commit time, rather than looked up from the record when an algorithm asks: the overlay
   * outlives the transaction, and an added edge has no column slot to be read from - the columns were built
   * with the base CSR - so this is what lets the view answer for it exactly instead of at a default weight
   * (issue #6315). One slot per materialised name, in the filter's own order, so an edge with fifty properties
   * on a view that indexes one costs one slot - and the reader resolves the name to a position once per slice
   * rather than hashing it per edge.
   */
  private Object[] extractMaterialisedEdgeProperties(final Edge edge) {
    final String[] materialised = view.getEdgePropertyFilter();
    if (materialised == null || materialised.length == 0)
      return null;

    Object[] values = null;
    for (int i = 0; i < materialised.length; i++) {
      final Object value = edge.get(materialised[i]);
      if (value == null)
        continue;
      // Allocated only once a value is actually found, so an edge carrying none of the materialised
      // properties - and a bulk insert of them - costs nothing here.
      if (values == null)
        values = new Object[materialised.length];
      values[i] = value;
    }
    return values;
  }

  private static Map<String, Object> extractProperties(final Document doc) {
    final Set<String> names = doc.getPropertyNames();
    if (names.isEmpty())
      return Collections.emptyMap();
    final Map<String, Object> props = new HashMap<>(names.size());
    for (final String name : names)
      props.put(name, doc.get(name));
    return props;
  }
}
