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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.test.support.FacetSchema;
import com.arcadedb.test.support.FacetSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.function.Predicate;

/**
 * Runs with the workload quiesced, once the ledger check has converged: for each filter of {@link FacetSchema#CHECKED_FILTERS}
 * it waits until the {@code GROUP BY} counts are identical on every node and unchanged across two polls, then waits on each node
 * for the first answer whose counts are exact and compares it with that node's {@code GROUP BY}; then it waits for the first
 * settled answer (no lag, every facet read from the index) and compares again. A {@code GROUP BY} that changed between the two
 * reads is a late commit: everything is read again. One deadline covers the whole check, which stops at the first violation.
 * <ul>
 *   <li>{@code FACET_DIVERGENCE} (SAFETY): the nodes never hold the same {@code GROUP BY} counts</li>
 *   <li>{@code FACET_DRIFT} (SAFETY): an exact (or settled) answer differs from the node's {@code GROUP BY}</li>
 *   <li>{@code FACET_META} (a warning, never a violation): an answer is inexact yet reports no lag; it is logged and listed by
 *   {@link #warnings()}, and the polling goes on so wrong counts still surface as {@code FACET_DRIFT}</li>
 *   <li>{@code FACET_STALL} (AVAILABILITY): no exact answer, or no settled answer, before the deadline: the apply thread is
 *   stuck, a rebuild never ends or the memory budget keeps refusing the patch set</li>
 *   <li>{@code FACET_ERROR} (SAFETY): a 5xx on a facet or {@code GROUP BY} query. A 503 is retryable: a replica can read a
 *   record chain that a replicated apply is still writing, and the server answers that read with a retryable
 *   {@code ConcurrentModificationException}. A 503 is logged as a {@code FACET_RETRY} warning and the read is retried; only a
 *   503 that still answers at the deadline is a violation, since a chain that stays broken on a quiesced, converged cluster is a
 *   corruption. Any other 5xx is a violation at once</li>
 *   <li>{@code FACET_SCAN} (AVAILABILITY): another I/O failure that lasts until the deadline</li>
 *   <li>{@code FACET_HARNESS} (HARNESS): the server rejected a query with a 4xx, which is the harness' fault. A 401 or 403 (an
 *   authentication lockout caused by a stranger on the port) is retried like a 503 and reported only at the deadline</li>
 * </ul>
 */
public final class FacetCheck {
  private static final Logger LOGGER       = LoggerFactory.getLogger(FacetCheck.class);
  private static final int    MAX_WARNINGS = 100;

  private static final class Failed extends Exception {
    private final transient Violation violation;

    Failed(final Violation violation) {
      super(violation.describe(), null, false, false);
      this.violation = violation;
    }
  }

  private final FacetReader reader;
  private final int         nodes;
  private final Duration    timeout;
  private final Duration    pollInterval;
  private       String      lastError;
  // the 503, 401 or 403 that caused lastError, reported as FACET_ERROR or FACET_HARNESS when it outlasts the deadline; null when
  // lastError is a plain I/O failure
  private       IOException lastStatusError;
  private       boolean     rereadFailed;
  private final List<String> warnings = new ArrayList<>();

  public FacetCheck(final FacetReader reader, final int nodes, final Duration timeout, final Duration pollInterval) {
    this.reader = reader;
    this.nodes = nodes;
    this.timeout = timeout;
    this.pollInterval = pollInterval;
  }

  /**
   * @return the distinct {@code FACET_META} and {@code FACET_RETRY} warnings seen by the last run, for the run report
   */
  public List<String> warnings() {
    return warnings;
  }

  /**
   * @return no violation when the check passes, otherwise the first one found
   */
  public List<Violation> run() throws InterruptedException {
    final long deadline = System.nanoTime() + timeout.toNanos();
    warnings.clear();
    try {
      for (final String where : FacetSchema.CHECKED_FILTERS)
        while (!checkOnce(where, deadline))
          if (System.nanoTime() > deadline)
            throw rereadFailed ? unreadable(new Violation(ResultKind.AVAILABILITY, "FACET_SCAN",
                "GROUP BY could not be re-read within " + timeout + " where='" + where + "': " + lastError, new long[0])) :
                new Failed(new Violation(ResultKind.SAFETY, "FACET_DIVERGENCE",
                    "GROUP BY did not hold still within " + timeout + " where='" + where + "'", new long[0]));
      return List.of();
    } catch (final Failed e) {
      return List.of(e.violation);
    }
  }

  /**
   * @return false when a late commit changed the {@code GROUP BY} counts while the facets were read, so the filter must be
   * checked again
   */
  private boolean checkOnce(final String where, final long deadline) throws Failed, InterruptedException {
    final List<SortedMap<String, Long>> truth = awaitIdentical(where, deadline);
    for (final boolean settled : new boolean[] { false, true })
      for (int node = 0; node < nodes; node++) {
        final FacetSnapshot sample = awaitSample(node, where, settled, deadline);
        if (!stable(node, where, truth.get(node)))
          return false;
        final List<String> diff = FacetSnapshot.diff(truth.get(node), sample.counts(), InvariantChecker.MAX_KEYS);
        if (!diff.isEmpty())
          throw new Failed(new Violation(ResultKind.SAFETY, "FACET_DRIFT",
              "node " + node + " facets disagree with GROUP BY" + (settled ? " at lag 0" : "") + " where='" + where + "' ("
                  + describe(sample) + ")", new long[0], diff));
      }
    return true;
  }

  private List<SortedMap<String, Long>> awaitIdentical(final String where, final long deadline) throws Failed, InterruptedException {
    List<SortedMap<String, Long>> previous = null;
    while (true) {
      final List<SortedMap<String, Long>> current = readGroupBy(where);
      if (current != null && identical(current) && current.equals(previous))
        return current;
      previous = current;
      if (System.nanoTime() > deadline) {
        // past the deadline a single identical read is accepted: only counts that differ are a divergence
        if (current != null && identical(current))
          return current;
        if (current == null)
          throw unreadable(new Violation(ResultKind.AVAILABILITY, "FACET_SCAN",
              "GROUP BY could not be read on every node within " + timeout + " where='" + where + "': " + lastError, new long[0]));
        throw new Failed(new Violation(ResultKind.SAFETY, "FACET_DIVERGENCE",
            "nodes hold different GROUP BY counts within " + timeout + " where='" + where + "'", new long[0],
            divergence(current)));
      }
      Thread.sleep(pollInterval.toMillis());
    }
  }

  private FacetSnapshot awaitSample(final int node, final String where, final boolean settled, final long deadline)
      throws Failed, InterruptedException {
    // a settled answer is accepted by the first pass too: one that is inexact without lag is never "comparable", yet its counts
    // must still be right
    final Predicate<FacetSnapshot> wanted = settled ? FacetSnapshot::settled : s -> s.comparable() || s.settled();
    FacetSnapshot last = null;
    clearError();
    while (true) {
      try {
        last = reader.facets(node, where);
        if (!last.metaConsistent())
          warn("FACET_META node " + node + " answered an inexact result with no lag where='" + where + "' (" + describe(last) + ")");
        if (wanted.test(last))
          return last;
        clearError();
      } catch (final IOException e) {
        failedRead(e);
      }
      if (System.nanoTime() > deadline) {
        if (lastError != null)
          throw unreadable(new Violation(ResultKind.AVAILABILITY, "FACET_SCAN",
              "node " + node + " facets could not be read within " + timeout + " where='" + where + "': " + lastError,
              new long[0]));
        throw new Failed(new Violation(ResultKind.AVAILABILITY, "FACET_STALL",
            "node " + node + " never gave " + (settled ? "a settled" : "an exact") + " answer within " + timeout + " where='"
                + where + "', last (" + describe(last) + ") sources=" + last.sources(), new long[0]));
      }
      Thread.sleep(pollInterval.toMillis());
    }
  }

  /**
   * @return true when the node still holds {@code truth}; false when it changed (a late commit) or could not be read, so the
   * filter is read again under the same deadline
   */
  private boolean stable(final int node, final String where, final SortedMap<String, Long> truth) throws Failed {
    try {
      rereadFailed = false;
      return truth.equals(reader.groupBy(node, where));
    } catch (final IOException e) {
      failedRead(e);
      rereadFailed = true;
      return false;
    }
  }

  /**
   * @return the counts of every node, or null when a node could not be read ({@link #lastError} then says why)
   */
  private List<SortedMap<String, Long>> readGroupBy(final String where) throws Failed {
    final List<SortedMap<String, Long>> counts = new ArrayList<>(nodes);
    boolean readable = true;
    for (int node = 0; node < nodes; node++)
      try {
        counts.add(reader.groupBy(node, where));
      } catch (final IOException e) {
        failedRead(e);
        readable = false;
      }
    return readable ? counts : null;
  }

  private static boolean identical(final List<SortedMap<String, Long>> counts) {
    for (int i = 1; i < counts.size(); i++)
      if (!counts.get(i).equals(counts.get(0)))
        return false;
    return true;
  }

  private static List<String> divergence(final List<SortedMap<String, Long>> counts) {
    final List<String> details = new ArrayList<>();
    for (int node = 1; node < counts.size() && details.size() < InvariantChecker.MAX_KEYS; node++)
      for (final String line : FacetSnapshot.diff(counts.get(0), counts.get(node), InvariantChecker.MAX_KEYS - details.size()))
        details.add("node " + node + ": " + line);
    return details;
  }

  /**
   * Records a failed read for a retry under the same deadline, or throws when the failure is final: a 5xx other than 503, or a 4xx
   * other than 401 and 403.
   */
  private void failedRead(final IOException e) throws Failed {
    if (e instanceof HttpNodeReader.ServerErrorException server) {
      if (server.status() != 503)
        throw error(server);
      retrying(e);
    } else if (e instanceof HttpNodeReader.ClientErrorException client) {
      if (client.status() != 401 && client.status() != 403)
        throw harness(client);
      retrying(e);
    } else
      lastStatusError = null;
    lastError = e.getMessage();
  }

  private void retrying(final IOException e) {
    lastStatusError = e;
    final String warning = "FACET_RETRY " + e.getMessage();
    // every retry is logged; the list keeps the distinct ones
    if (!warn(warning))
      LOGGER.warn("{}", warning);
  }

  private void clearError() {
    lastError = null;
    lastStatusError = null;
  }

  /**
   * @return the violation for a read that kept failing until the deadline: the retried status when the last failure was one,
   * otherwise {@code availability}
   */
  private Failed unreadable(final Violation availability) {
    if (lastStatusError instanceof HttpNodeReader.ServerErrorException server)
      return error(server);
    if (lastStatusError instanceof HttpNodeReader.ClientErrorException client)
      return harness(client);
    return new Failed(availability);
  }

  private static Failed error(final HttpNodeReader.ServerErrorException e) {
    return new Failed(new Violation(ResultKind.SAFETY, "FACET_ERROR", e.getMessage(), new long[0]));
  }

  private static Failed harness(final HttpNodeReader.ClientErrorException e) {
    return new Failed(new Violation(ResultKind.HARNESS, "FACET_HARNESS", e.getMessage(), new long[0]));
  }

  /** @return true when the warning was new, so it was listed and logged */
  private boolean warn(final String warning) {
    if (warnings.size() < MAX_WARNINGS && !warnings.contains(warning)) {
      warnings.add(warning);
      LOGGER.warn("{}", warning);
      return true;
    }
    return false;
  }

  private static String describe(final FacetSnapshot sample) {
    return "exact=" + sample.exact() + " patched=" + sample.patched() + " lagMs=" + sample.lagMs();
  }
}
