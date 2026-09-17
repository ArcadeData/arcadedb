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
package com.arcadedb.server.security;

import java.util.concurrent.atomic.AtomicLong;

/**
 * What a node has actually done with the replicated group changes it received, so that "the permission change
 * reached this peer and was enforced" is something an operator can read rather than infer (issue #7529).
 * <p>
 * Issue #7510 removed the lag - a peer re-derives its cached permissions within milliseconds of applying a
 * {@code SECURITY_GROUPS_ENTRY} instead of waiting for the {@code arcadedb.server.security.reloadEvery} tick - but
 * left the other half of that report open: the hand-off logged only on its two failure paths and counted nothing,
 * so the only evidence the fast path existed was the ABSENCE of a warning. A sweep silently failing (a database
 * that keeps throwing, an executor rejecting after a restart raced a shutdown) looked exactly like the pre-#7510
 * behaviour, and nothing anywhere said so.
 * <p>
 * The counters are monotonic and the two timestamps are epoch milliseconds, which is the shape a dashboard can
 * alert on: {@code entriesApplied} rising while {@code sweepsCompleted} does not is the failure, and
 * {@code lastSweepAt} lagging {@code lastEntryAppliedAt} says how long it has been failing. A log line could say
 * neither.
 * <p>
 * <b>{@code entriesApplied} counts replicated GROUP documents, and only those.</b> A replicated API-token
 * document is not counted and schedules no sweep, which is deliberate rather than an omission: a token is an
 * authentication credential, and {@code ApiTokenConfiguration} is consulted on each authentication, so there is
 * no derived per-database state for a sweep to re-derive and nothing that could lag. These counters answer one
 * question - has this node re-derived its cached permissions from the group document it was told to enforce -
 * and a token count here would make {@code entriesApplied} vs {@code sweepsCompleted}, the comparison the whole
 * record exists for, read as a permanent gap on any cluster that mints tokens.
 * <p>
 * <b>Deliberately not derived from the executor.</b> A {@code ThreadPoolExecutor}'s own {@code completedTaskCount}
 * counts tasks that RAN, which a coalesced submission never is, and counts a task that threw as completed. What
 * matters here is what the node ended up enforcing, so each number is recorded at the point the thing it names
 * happened.
 * <p>
 * Every counter is an {@link AtomicLong} rather than a {@code LongAdder}: these are incremented once per
 * administrative change, which is the low-write/frequent-read end of the trade, and {@code AtomicLong} reads
 * without summing cells.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PermissionRefreshMetrics {
  // Declared in the order Snapshot takes them, so the two read as one list side by side.
  private final AtomicLong entriesApplied          = new AtomicLong();
  private final AtomicLong refreshesRequested      = new AtomicLong();
  private final AtomicLong refreshesCoalesced      = new AtomicLong();
  private final AtomicLong sweepsCompleted         = new AtomicLong();
  private final AtomicLong sweepsFailed            = new AtomicLong();
  private final AtomicLong databasesRefreshed      = new AtomicLong();
  private final AtomicLong databaseRefreshFailures = new AtomicLong();
  private final AtomicLong lastEntryAppliedAt      = new AtomicLong();
  private final AtomicLong lastSweepAt             = new AtomicLong();

  /**
   * An immutable reading of every counter, taken field by field.
   * <p>
   * NOT an atomic snapshot, and it does not need to be: a reader that catches a sweep in flight sees
   * {@code entriesApplied} ahead of {@code sweepsCompleted} for the few milliseconds the sweep lasts, which is
   * indistinguishable from the state this record is for. That is why {@code lastSweepAt} is what an alert should
   * key on - a gap that persists across scrapes - and not a single sample of the difference.
   *
   * @param entriesApplied          replicated group documents this node has installed since startup
   * @param refreshesRequested      hand-offs made to the refresh worker, counted whether or not it took them -
   *                                {@code ThreadPoolExecutor.execute} runs its rejection handler on the calling
   *                                thread and returns normally, so the submit itself cannot tell the two apart.
   *                                The accepted ones are {@code refreshesRequested - refreshesCoalesced}
   * @param refreshesCoalesced      hand-offs the worker refused because one was already queued, or because the
   *                                security service had stopped. Not loss on the first count - the queued task
   *                                re-reads the document when it runs - but a number that keeps climbing on an
   *                                idle cluster is a worker that is not draining
   * @param sweepsCompleted         refresh sweeps that finished, from whichever source: the replicated apply, the
   *                                {@code server-groups.json} watcher, or an inline refresh
   * @param sweepsFailed            sweeps that ended in the worker's catch-all, i.e. the node fell back to
   *                                converging on the reload tick
   * @param databasesRefreshed      databases whose cached permissions were re-derived, summed over every sweep
   * @param databaseRefreshFailures per-database refusals inside a sweep (a database dropped mid-iteration, or one
   *                                still carrying an interrupted-snapshot marker). The sweep continues past them,
   *                                so this can rise while {@code sweepsFailed} stays at 0
   * @param lastEntryAppliedAt      epoch ms of the most recent applied group document, 0 when none
   * @param lastSweepAt             epoch ms at which the most recent sweep finished, 0 when none
   */
  public record Snapshot(long entriesApplied, long refreshesRequested, long refreshesCoalesced, long sweepsCompleted,
                         long sweepsFailed, long databasesRefreshed, long databaseRefreshFailures,
                         long lastEntryAppliedAt, long lastSweepAt) {

    /**
     * The reading for a server with no security service to ask - a scrape or an info request that lands before
     * one is installed or after it is gone.
     * <p>
     * A constant rather than nine zeros written out at each reader: there are two of them (the
     * {@code arcadedb.ha.security.*} gauges and {@code ha.securityRefresh}), and a record that gains a field
     * must not be able to leave one of them reporting a stale shape. Zeros, not nulls, because "this node has
     * done nothing" and "this node cannot say" are the same answer to every question these counters are asked -
     * and an absent section is the one answer a dashboard cannot plot.
     */
    public static final Snapshot ZERO = new Snapshot(0, 0, 0, 0, 0, 0, 0, 0, 0);
  }

  /** A replicated group document has been installed and is in force on this node from now on. */
  public void entryApplied() {
    entriesApplied.incrementAndGet();
    lastEntryAppliedAt.set(System.currentTimeMillis());
  }

  /** A refresh was handed to the worker; {@link #refreshCoalesced()} says whether it took it. */
  public void refreshRequested() {
    refreshesRequested.incrementAndGet();
  }

  /** The refresh worker refused the hand-off: one is already queued, or the security service has stopped. */
  public void refreshCoalesced() {
    refreshesCoalesced.incrementAndGet();
  }

  /**
   * A sweep finished.
   *
   * @param databases the databases it re-derived
   * @param failures  the databases it could not, having continued past each
   */
  public void sweepCompleted(final long databases, final long failures) {
    sweepsCompleted.incrementAndGet();
    databasesRefreshed.addAndGet(databases);
    if (failures > 0)
      databaseRefreshFailures.addAndGet(failures);
    lastSweepAt.set(System.currentTimeMillis());
  }

  /** A sweep reached the worker's catch-all, so this node is back to converging on the reload tick. */
  public void sweepFailed() {
    sweepsFailed.incrementAndGet();
  }

  public Snapshot snapshot() {
    return new Snapshot(entriesApplied.get(), refreshesRequested.get(), refreshesCoalesced.get(),
        sweepsCompleted.get(), sweepsFailed.get(), databasesRefreshed.get(), databaseRefreshFailures.get(),
        lastEntryAppliedAt.get(), lastSweepAt.get());
  }
}
