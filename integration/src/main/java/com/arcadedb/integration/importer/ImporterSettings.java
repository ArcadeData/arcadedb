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

import com.arcadedb.utility.FileUtils;

import java.util.HashMap;
import java.util.Map;

public class ImporterSettings {
  public String  database     = "./databases/imported";
  public String  url          = null;
  public boolean wal          = false;
  public int     verboseLevel = 2;
  public boolean probeOnly    = false;

  public String documents;
  public String documentsFileType;
  public String documentsDelimiter;
  public String documentsHeader;
  public Long   documentsSkipEntries      = null;
  public String documentTypeName          = "Document";
  public String documentPropertiesInclude = "*";
  public String vertices;
  public String verticesFileType;
  public String verticesDelimiter;
  public String verticesHeader;
  public Long   verticesSkipEntries       = null;
  public String vertexTypeName            = "Node";
  public String vertexPropertiesInclude   = "*";
  public long   expectedVertices          = 0l;

  public String  edges;
  public String  edgesFileType;
  public String  edgesDelimiter;
  public String  edgesHeader;
  public Long    edgesSkipEntries       = null;
  public String  edgeTypeName           = "Relationship";
  public String  edgePropertiesInclude  = "*";
  public long    expectedEdges          = 0l;
  public long    maxRAMIncomingEdges    = 256 * 1024 * 1024; // 256MB
  public String  edgeFromField          = null;
  public String  edgeToField            = null;
  public boolean edgeBidirectional      = true;
  public String  typeIdProperty         = null;
  public boolean typeIdPropertyIsUnique = false;
  public String  typeIdType             = "String";
  public int     parallel               = 1;
  public boolean forceDatabaseCreate    = false;
  public boolean trimText               = true;
  public long    analysisLimitBytes     = 100000;
  public long    analysisLimitEntries   = 10000;
  public long    parsingLimitBytes;
  public long    parsingLimitEntries;
  public int     commitEvery            = 5000;
  public String  mapping                = null;
  /**
   * Governs document/vertex rows only (see {@link #isSkipOnRowError()}); CSV edges always skip-and-log
   * unconditionally because an unresolved from/to vertex reference is an expected outcome during graph import, not a
   * data error. JSON edges are unaffected by that CSV-specific rule: they're created via the same per-record path as
   * JSON documents/vertices, so this setting still governs them the same way it governs those.
   */
  public String  onRowError             = "abort";

  public final Map<String, Object> options = new HashMap<>();

  public ImporterSettings() {
    parallel = Runtime.getRuntime().availableProcessors() / 2 - 1;
    if (parallel < 1)
      parallel = 1;
  }

  /**
   * Tells whether a single malformed/out-of-range row or property should be skipped and logged instead of aborting the
   * whole import job. Opt-in via {@code -onRowError skip}; defaults to {@code abort} for backward compatibility.
   * <p>
   * For CSV vertex imports, which normally persist records asynchronously via {@code database.async()} in batches,
   * enabling this switches to a synchronous save per vertex instead (the same way document imports already work): a
   * batched async commit rolling back on a persist-time failure (mandatory property, unique index, ...) would otherwise
   * take down every other vertex queued in the same uncommitted batch, not just the failing one.
   * <p>
   * Both CSV document and vertex imports also commit each row in its own transaction while this is enabled, rather
   * than the single whole-file transaction (documents) / {@code commitEvery}-sized async batches (vertices) used in
   * the default {@code abort} mode: a failure that only surfaces at index time (e.g. a duplicate key) already has a
   * bucket write by then, and only rolling back that one row's own transaction can undo it without also discarding
   * an unrelated, already-committed row. In short, "skip" trades import throughput - effectively a batch size of 1
   * for the whole run, not just for the failing row - for the guarantee that a bad row can never take a good one
   * down with it, nor leave a partially-written "ghost" record behind. For vertices specifically, the cost is
   * larger than just batching down to 1: dropping {@code database.async()} entirely also gives up its worker-thread
   * parallelism, so a vertex import under this setting is fully synchronous and single-threaded regardless of
   * {@code -commitEvery}/{@code -parallel}, which are silently inapplicable there while this is enabled.
   * <p>
   * Because of that per-row transaction ownership, "skip" requires exclusive control of the transaction and cannot
   * be used while one is already active on the target {@code Database} - e.g. inside an embedding caller's own
   * transaction, or a server HTTP command executed with the default atomic/{@code autoCommit} behavior, which wraps
   * the whole command in one transaction. Both CSV and JSON reject this combination eagerly (see
   * {@link #newExclusiveTransactionRequiredException()}) rather than silently committing/discarding whatever the
   * caller had pending.
   */
  public boolean isSkipOnRowError() {
    return "skip".equalsIgnoreCase(onRowError);
  }

  /**
   * Shared exception for the constraint documented on {@link #isSkipOnRowError()}: {@code -onRowError skip} commits
   * or rolls back per row, so it must own the transaction outright. Used by both {@code CSVImporterFormat} and
   * {@code JSONImporterFormat} so the message can't drift between them.
   */
  public static IllegalStateException newExclusiveTransactionRequiredException() {
    return new IllegalStateException(
        "-onRowError skip requires exclusive control of the transaction and cannot be used while a transaction is "
            + "already active (e.g. inside a caller-managed transaction, or a server HTTP command executed with the "
            + "default atomic/autoCommit behavior - retry with autoCommit=false or from a session)");
  }

  public <T> T getValue(final String name, final T defaultValue) {
    final Object v = options.get(name);
    return v != null ? (T) v : defaultValue;
  }

  public int getIntValue(final String name, final int defaultValue) {
    final Object v = options.get(name);
    if (v != null) {
      if (v instanceof Number number)
        return number.intValue();
      else
        return Integer.parseInt(v.toString());
    }
    return defaultValue;
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; i += 2) {
        final char begin = args[i].charAt(0);
        if (begin != '-')
          throw new IllegalArgumentException("Arguments must begin with '-'");
        parseParameter(args[i].substring(1), args[i + 1]);
      }
  }

  public void parseParameter(final String name, String value) {
    value = FileUtils.getStringContent(value);

    switch (name) {
    case "database" -> database = value;
    case "url" -> url = value;
    case "forceDatabaseCreate" -> forceDatabaseCreate = Boolean.parseBoolean(value);
    case "wal" -> wal = Boolean.parseBoolean(value);
    case "commitEvery" -> commitEvery = Integer.parseInt(value);
    case "parallel" -> parallel = Integer.parseInt(value);
    case "typeIdProperty" -> typeIdProperty = value;
    case "typeIdUnique" -> typeIdPropertyIsUnique = Boolean.parseBoolean(value);
    case "typeIdType" -> typeIdType = value;
    case "trimText" -> trimText = Boolean.parseBoolean(value);
    case "analysisLimitBytes" -> analysisLimitBytes = FileUtils.getSizeAsNumber(value);
    case "analysisLimitEntries" -> analysisLimitEntries = Long.parseLong(value);
    case "parsingLimitBytes" -> parsingLimitBytes = FileUtils.getSizeAsNumber(value);
    case "parsingLimitEntries" -> parsingLimitEntries = Long.parseLong(value);
    case "mapping" -> mapping = value;
    case "probeOnly" -> probeOnly = Boolean.parseBoolean(value);
    case "onRowError" -> {
      if (!"abort".equalsIgnoreCase(value) && !"skip".equalsIgnoreCase(value))
        throw new IllegalArgumentException("Invalid value '" + value + "' for -onRowError. Supported values are 'abort' and 'skip'");
      onRowError = value;
    }
    // DOCUMENT SETTINGS
    case "documents" -> documents = value;
    case "documentsFileType" -> documentsFileType = value;
    case "documentsDelimiter" -> documentsDelimiter = value;
    case "documentsHeader" -> documentsHeader = value;
    case "documentsSkipEntries" -> documentsSkipEntries = Long.parseLong(value);
    case "documentPropertiesInclude" -> documentPropertiesInclude = value;
    case "documentType" -> documentTypeName = value;
    // VERTICES SETTINGS
    case "vertices" -> vertices = value;
    case "verticesFileType" -> verticesFileType = value;
    case "verticesDelimiter" -> verticesDelimiter = value;
    case "verticesHeader" -> verticesHeader = value;
    case "verticesSkipEntries" -> verticesSkipEntries = Long.parseLong(value);
    case "expectedVertices" -> expectedVertices = Integer.parseInt(value);
    case "vertexType" -> vertexTypeName = value;
    case "vertexPropertiesInclude" -> vertexPropertiesInclude = value;
    // EDGES SETTINGS
    case "edges" -> edges = value;
    case "edgesFileType" -> edgesFileType = value;
    case "edgesDelimiter" -> edgesDelimiter = value;
    case "edgesHeader" -> edgesHeader = value;
    case "edgesSkipEntries" -> edgesSkipEntries = Long.parseLong(value);
    case "expectedEdges" -> expectedEdges = Integer.parseInt(value);
    case "maxRAMIncomingEdges" -> maxRAMIncomingEdges = Long.parseLong(value);
    case "edgeType" -> edgeTypeName = value;
    case "edgePropertiesInclude" -> edgePropertiesInclude = value;
    case "edgeFromField" -> edgeFromField = value;
    case "edgeToField" -> edgeToField = value;
    case "edgeBidirectional" -> edgeBidirectional = Boolean.parseBoolean(value);
    }

    // SAVE THE SETTING IN THE OPTIONS
    options.put(name, value);
  }
}
