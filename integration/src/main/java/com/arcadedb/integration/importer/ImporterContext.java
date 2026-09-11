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
   * Rows parsed by the phase currently running, and only by it. One ImporterContext serves every phase of an
   * import - {@link Importer#load()} calls {@code loadFromSource()} for the url, documents, vertices and edges
   * sources against the same context - so this counter is zeroed at each phase boundary by
   * {@link #beginParsingPhase()}. Several formats take a decision off it (a commit boundary, a parse limit), and
   * an inherited offset moved those boundaries to somewhere inside the phase's first batch (issues #7288, #7313).
   * Use {@link #totalParsed} for the whole run.
   */
  public final AtomicLong parsed                     = new AtomicLong();
  /**
   * Rows parsed by every phase that has already finished. Monotonic for the life of the import, and the companion
   * the per-phase {@link #parsed} counter needs so that "the rows this import parsed" remains answerable once the
   * reset is hoisted (issue #7342). It excludes the phase currently running, which no boundary has folded in yet -
   * {@link #totalParsedRecords()} is the sum a reader wants.
   */
  public final AtomicLong totalParsed                = new AtomicLong();
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
  /**
   * The value of {@link #parsed} at the previous progress line, subtracted from the current one by
   * {@code FormatImporter#printProgress} to print a rate. Zeroed by {@link #beginParsingPhase()} together with the
   * counter it is a high-water mark of: left behind, it made the first progress line after a phase boundary
   * subtract the previous phase's total from the new phase's handful of rows and print a negative rate (#7342).
   */
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
   * Opens a parsing phase: rolls the rows the phase that just finished parsed into {@link #totalParsed}, then
   * zeroes {@link #parsed} and the {@link #lastParsed} high-water mark that is read against it.
   * <p>
   * Called from {@link Importer#loadFromSource} immediately before {@code FormatImporter.load()}, which is the one
   * place every phase of every import passes through, so a format cannot forget it and a format added later
   * inherits it (issue #7342). The nine formats that used to zero the counter themselves call this instead of
   * assigning the field, so there is a single definition of what a phase reset is and the two cannot drift; they
   * keep doing it because {@code FormatImporter.load()} is public API and is called directly, not only through
   * {@code loadFromSource()}. Calling it twice for the same phase is harmless: the second call folds zero.
   * <p>
   * Not to be confused with {@code OrientDBImporter.parseRecords()}'s own {@code parsed.set(0)}, which is not a
   * phase boundary: {@code run()} parses the same input up to three times (ANALYZE, CREATE_SCHEMA, CREATE_EDGES)
   * and every pass re-counts the records, so that one deliberately discards the previous pass's rows instead of
   * accumulating them.
   */
  public void beginParsingPhase() {
    // getAndSet rather than a read followed by set(0): the fold is then correct whichever thread is incrementing
    // `parsed` at the time, rather than only because all eleven increment sites happen to run on the thread inside
    // FormatImporter.load() today. Same cost, and a future off-thread writer cannot lose a row to it.
    totalParsed.addAndGet(parsed.getAndSet(0L));
    // lastLapOn is deliberately NOT reset with it. printProgress divides every delta on the line by the same
    // window, and the createdDocuments/Vertices/Edges counters it also prints are import-wide and never reset -
    // restarting the clock here would leave their deltas measured over a window shorter than the rows they cover
    // and overstate those rates. A window that opened slightly before this phase did understates the parse rate of
    // the first line after a boundary by the same slight amount; that is the cheaper of the two errors.
    lastParsed = 0L;
  }

  /**
   * Rows parsed by this import so far: every finished phase plus the one currently running.
   * <p>
   * {@link #toMap()} deliberately does not call this - it needs both published keys built from one reading of
   * {@link #parsed}, which a second call here would re-read. The caller this exists for is a progress reader that
   * wants a figure which does not jump backwards at a phase boundary, which is what the server's once-a-second
   * import counter needs and does not yet use (issue #7483).
   */
  public long totalParsedRecords() {
    return totalParsed.get() + parsed.get();
  }

  /**
   * The report {@link Importer#load()} returns.
   * <p>
   * {@code parsedRecords} is the row count of the <b>last phase that ran</b>, which is what nine of the ten
   * {@code FormatImporter} implementations already reported and what callers of this map read today.
   * {@code totalParsedRecords} is the whole run. On a single-phase import - every {@code IMPORT DATABASE}
   * without a {@code documents}/{@code vertices}/{@code edges} setting, and the whole HTTP/gRPC
   * {@code import database} command - the two are equal.
   */
  public Map<String, Object> toMap() {
    final LinkedHashMap<String, Object> map = new LinkedHashMap<>();

    // Read once each, and summed here rather than through totalParsedRecords(): the two keys must agree, so the
    // run total has to be built from the same reading of `parsed` that the phase key was, and neither key may be
    // tested against one value and published with another.
    final long phaseParsed = parsed.get();
    if (phaseParsed > 0)
      map.put("parsedRecords", phaseParsed);
    final long runParsed = totalParsed.get() + phaseParsed;
    if (runParsed > 0)
      map.put("totalParsedRecords", runParsed);
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
