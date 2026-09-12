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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ImporterContext {
  /**
   * The rows the CURRENT phase has parsed. One {@link ImporterContext} serves every phase of an import -
   * {@link Importer#load()} calls {@code loadFromSource()} for the url, documents, vertices and edges sources
   * against the same context - and {@link Importer#loadFromSource} zeroes this counter on entry to each of them
   * (issue #7342), so a format reading it back is always reading its own phase's count. It used to be each
   * format's job to remember to zero it, which two of the eleven did not, and the two that took a decision off the
   * value truncated or skipped a whole phase when they inherited a non-zero one (issues #7288, #7313).
   * <p>
   * What the import as a whole parsed is {@link #getParsedTotal()}, which is what the returned report carries.
   */
  public final AtomicLong parsed                     = new AtomicLong();
  /**
   * The rows every phase BEFORE the current one parsed, accumulated by {@link #beginPhase()} as each phase ends.
   * Added to {@link #parsed} by {@link #getParsedTotal()}, because per-phase and import-wide are two different
   * questions and one counter can only answer one of them: {@code parsedRecords} used to mean "the last phase's
   * count" for ten formats and "the last phase plus whatever preceded it" for {@code JSONImporterFormat}, so two
   * adjacent invocations of the same CLI reported the same quantity two different ways (issue #7342).
   */
  private final AtomicLong parsedInPreviousPhases    = new AtomicLong();
  public final AtomicLong parsedDocumentAndVertices  = new AtomicLong();
  public final AtomicLong createdDocuments           = new AtomicLong();
  public final AtomicLong createdVertices            = new AtomicLong();
  public final AtomicLong createdEdges               = new AtomicLong();
  public final AtomicLong createdEmbeddedDocuments   = new AtomicLong();
  /**
   * TIMESERIES samples appended by the import (issue #7032). Counted apart from the record kinds because a
   * TimeSeries type owns no record bucket.
   */
  public final AtomicLong createdTimeSeriesSamples   = new AtomicLong();
  public final AtomicLong linkedEdges                = new AtomicLong();
  public final AtomicLong updatedDocuments           = new AtomicLong();
  public final AtomicLong documentsWithLinksToUpdate = new AtomicLong();
  public final AtomicLong skippedEdges               = new AtomicLong();
  public final AtomicLong errors                     = new AtomicLong();
  public final AtomicLong warnings                   = new AtomicLong();
  /**
   * Set by {@link AbstractImporter#openDatabase()}, before anything else in the import (including schema
   * auto-creation, which can itself open and leave active a transaction of its own) touches the database's
   * transaction state: true only when the target {@link com.arcadedb.database.Database} was handed to the importer
   * already open by the caller (the {@code Importer(Database, String)} embedding constructor, or the
   * {@code IMPORT DATABASE} SQL statement, which reuses the caller's own {@code Database}) AND a transaction was
   * already active at that exact point. This is the one reliable signal that an active transaction later observed
   * by a format's {@code load()} genuinely predates - and so may hold unrelated pending work belonging to - the
   * caller, as opposed to one this importer's own schema auto-creation opened moments earlier in the same call and
   * still needs to finish committing itself; a self-opened database's ambient transaction is always empty by the
   * time a format's {@code load()} runs, so this is unconditionally false for one and taking it over is always safe.
   */
  public       boolean       callerTransactionActiveOnEntry;
  public       long          startedOn;
  public       long          lastLapOn;
  public       long          lastParsed;
  public       long          lastDocuments;
  public       long          lastVertices;
  public       long          lastEdges;
  public       long          lastLinkedEdges;

  /**
   * Whether the transaction the row loop about to run will use belongs to the import, as opposed to predating it -
   * and therefore whether that loop may commit it, roll it back, and correct its own counters against it.
   * <p>
   * The single place this decision is made, so the five row loops that gate on it cannot each answer it slightly
   * differently. The last time the answer was hand-applied per loop, one of the four operations in
   * {@code RDFImporterFormat.load()} came apart from its siblings and committed the caller's transaction
   * (issues #7272, #7328).
   * <p>
   * Two conditions, both meaning "not the caller's":
   * <ul>
   *   <li>{@link #callerTransactionActiveOnEntry} is false - nothing predating the import was ever there;</li>
   *   <li>or it is true but no transaction is active any more. A caller transaction that has since been resolved
   *   is not a caller transaction: the loop's own {@code begin()} pushes a fresh one, and calling that fresh one
   *   the caller's would leave it on the stack with nothing allowed to resolve it - not the loop's gates, and not
   *   {@code Importer.load()}'s own cleanup, which is gated on the same flag.</li>
   * </ul>
   * Call it once, immediately before the loop's {@code begin()}, and keep the answer in a local: after that
   * {@code begin()} a transaction is always active, so asking again would always say "the caller's".
   */
  public boolean importOwnsTransaction(final Database database) {
    return !callerTransactionActiveOnEntry || !database.isTransactionActive();
  }

  /**
   * Closes the phase that has just finished and opens the next one: the phase's row count is folded into the
   * import-wide total and the per-phase counter zeroed, so the format about to run measures {@code -parsingLimitEntries}
   * and its own commit cadence against its own rows and reports its own count.
   * <p>
   * Called by {@link Importer#loadFromSource} immediately before handing the source to the format, rather than by
   * each format on entry to its own {@code load()}: a format could forget, two of them had, and a format added
   * later inherits the reset instead of having to know about it (issue #7342).
   * <p>
   * {@code lastParsed} - the value {@code FormatImporter#printProgress} subtracts to turn the counter into a rate -
   * goes with it. Left behind, it made the first progress line of every phase after the first report a NEGATIVE
   * rate, which is the visible artefact of the per-phase reset and was already true for the nine formats that used
   * to reset the counter themselves.
   */
  public void beginPhase() {
    parsedInPreviousPhases.addAndGet(parsed.getAndSet(0));
    lastParsed = 0;
    lastLapOn = System.currentTimeMillis();
  }

  /**
   * The rows this IMPORT parsed: the phases already finished plus the one still running. This is what
   * {@code parsedRecords} reports (issue #7342).
   */
  public long getParsedTotal() {
    return parsedInPreviousPhases.get() + parsed.get();
  }

  public Map<String, Object> toMap() {
    final LinkedHashMap<String, Object> map = new LinkedHashMap<>();

    final long parsedTotal = getParsedTotal();
    if (parsedTotal > 0)
      map.put("parsedRecords", parsedTotal);
    if (errors.get() > 0)
      map.put("errors", errors.get());
    if (warnings.get() > 0)
      map.put("warnings", warnings.get());
    if (createdDocuments.get() > 0)
      map.put("createdDocuments", createdDocuments.get());
    if (createdVertices.get() > 0)
      map.put("createdVertices", createdVertices.get());
    if (createdEdges.get() > 0)
      map.put("createdEdges", createdEdges.get());
    if (createdTimeSeriesSamples.get() > 0)
      map.put("createdTimeSeriesSamples", createdTimeSeriesSamples.get());

    return map;
  }
}
