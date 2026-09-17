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
import com.arcadedb.database.DatabaseInternal;

import java.io.IOException;
import java.util.Map;

public class Importer extends AbstractImporter {
  /**
   * The source URL currently being read, recorded BEFORE the read rather than after it succeeds, so a failure while
   * reading can name it. {@link AbstractImporter#source} cannot: it is assigned only once the sniff has succeeded.
   */
  private String loadingUrl;

  public Importer(final String[] args) {
    super(args);
  }

  /**
   * Embeds the importer into an already-open {@code database}, which the caller keeps owning (and may keep using)
   * after {@link #load()} returns. Note: a CSV vertex import in default "abort" mode registers its own
   * {@code database.async().onError(...)} handler for the duration of the import and does not restore whatever
   * handler {@code database} had before - there is no getter on {@code DatabaseAsyncExecutor} to read it back (see
   * {@code CSVImporterFormat.loadVertices}). If the caller had a custom handler of their own registered before
   * calling {@link #load()}, it is gone afterward.
   */
  public Importer(final Database database, final String url) {
    super((DatabaseInternal) database);
    settings.url = url;
  }

  public static void main(final String[] args) {
    new Importer(args).load();
    System.exit(0);
  }

  /**
   * Overrides {@link com.arcadedb.GlobalConfiguration#SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS} for this import.
   * The server command handler calls this reflectively with the value it already validated the URL against, so the
   * deep fetch inside {@link SourceDiscovery} cannot land on a stricter-or-looser answer than the pre-check that
   * accepted the command (issue #6474, mirroring the restore-side fix in #6381/#6449).
   */
  public Importer setAllowLocalUrls(final boolean allowLocalUrls) {
    settings.allowLocalUrls = allowLocalUrls;
    return this;
  }

  public Map<String, Object> load() {
    source = null;
    loadingUrl = null;

    try {
      final int cfgValue = settings.getIntValue("maxValueSampling", 100);
      final AnalyzedSchema analyzedSchema = new AnalyzedSchema(cfgValue);

      openDatabase();

      startImporting();

      // Determine entity type for the main URL: if vertexType or edgeType is explicitly set, route accordingly
      AnalyzedEntity.EntityType urlEntityType = AnalyzedEntity.EntityType.DATABASE;
      if (settings.options.containsKey("vertexType") && settings.vertices == null)
        urlEntityType = AnalyzedEntity.EntityType.VERTEX;
      else if (settings.options.containsKey("edgeType") && settings.edges == null)
        urlEntityType = AnalyzedEntity.EntityType.EDGE;

      loadFromSource(settings.url, urlEntityType, analyzedSchema);
      loadFromSource(settings.documents, AnalyzedEntity.EntityType.DOCUMENT, analyzedSchema);
      loadFromSource(settings.vertices, AnalyzedEntity.EntityType.VERTEX, analyzedSchema);
      loadFromSource(settings.edges, AnalyzedEntity.EntityType.EDGE, analyzedSchema);

      if (settings.probeOnly)
        return null;

      // Each loadFromSource() call above already commits its own transaction when it owns it (see
      // CSVImporterFormat's ownsTransaction / JSONImporterFormat's own nesting), so a transaction still active here
      // for an externally-managed database that genuinely had one active before this import began (see
      // ImporterContext#callerTransactionActiveOnEntry) means a format deliberately left it open because it belongs
      // to the caller - committing it here would undo that on the success path the same way an unconditional commit
      // would on failure.
      if (!context.callerTransactionActiveOnEntry && database.isTransactionActive())
        database.commit();

    } catch (final Exception e) {
      if (settings.probeOnly)
        throw new IllegalArgumentException(e);
      else
        throw new ImportException(importFailureMessage(e), e);
    } finally {
      stopImporting();
      if (database != null) {
        closeDatabase();
      }
      closeInputFile();
    }

    return context.toMap();
  }

  /**
   * The message an import failure is reported with.
   * <p>
   * It used to be {@code "Error on parsing source '" + source + "'"} and nothing else, which failed the caller twice
   * over: {@code source} is only assigned once the sniff has SUCCEEDED, so anything that goes wrong while reading the
   * source - the case a remote import most often hits - named the source {@code null}; and the cause's own message,
   * the only part that says WHAT went wrong, appeared nowhere in the text a client is shown. A read timeout on a
   * stalled remote source then reached the operator as {@code "Error on parsing source 'null'"}, naming neither the
   * timeout nor the setting that relaxes it (issues #7500, #7346, #7461).
   * <p>
   * The URL is the one being read when the failure happened, which is known from the start, and the cause's message
   * is appended. In production mode the server conceals this whole string before it reaches a client - see
   * {@code ArcadeDBServer.isProductionMode()} - so naming the cause here costs nothing there and everything is still
   * in the server log.
   */
  private String importFailureMessage(final Exception e) {
    final String where = loadingUrl != null ? loadingUrl : source != null ? source.toString() : settings.url;
    final String why = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
    return "Error on parsing source '" + where + "': " + why;
  }

  protected void loadFromSource(final String url, AnalyzedEntity.EntityType entityType, final AnalyzedSchema analyzedSchema)
      throws IOException {
    if (url == null)
      // SKIP IT
      return;

    // THE SOURCE BEING READ, RECORDED BEFORE THE READ RATHER THAN AFTER IT SUCCEEDS, SO A FAILURE CAN NAME IT
    loadingUrl = url;

    final SourceDiscovery sourceDiscovery = new SourceDiscovery(url, settings.allowLocalUrls);

    if (settings.probeOnly) {
      sourceDiscovery.getSource();
      return;
    }

    final SourceSchema sourceSchema = sourceDiscovery.getSchema(settings, entityType, analyzedSchema, logger);
    if (sourceSchema == null) {
      //LogManager.instance().log(this, Level.WARNING, "XML importing aborted because unable to determine the schema");
      return;
    }

    updateDatabaseSchema(sourceSchema.getSchema());

    source = sourceDiscovery.getSource();
    parser = new Parser(source, 0);
    parser.reset();

    format = sourceSchema.getContentImporter();

    // ONE ImporterContext SERVES EVERY PHASE OF AN IMPORT, SO THE ROW COUNTER ARRIVES CARRYING WHATEVER THE PREVIOUS
    // PHASE LEFT IN IT. ZEROED HERE, ONCE, RATHER THAN BY EACH FORMAT ON ENTRY TO ITS OWN load(): TWO OF THE ELEVEN
    // FORMATS DID NOT, AND THE TWO THAT TOOK A DECISION OFF THE VALUE - THE -parsingLimitEntries CHECK AND THE COMMIT
    // CADENCE - TRUNCATED OR SKIPPED A WHOLE PHASE WHEN THEY INHERITED A NON-ZERO ONE (ISSUES #7288, #7313). A FORMAT
    // ADDED LATER NOW INHERITS THE RESET INSTEAD OF HAVING TO REMEMBER IT (ISSUE #7342)
    context.beginPhase();

    format.load(sourceSchema, entityType, parser, database, context, settings);
  }

  protected void closeDatabase() {
    if (!databaseCreatedDuringImporting)
      return;

    if (database != null) {
      if (database.isTransactionActive())
        database.commit();
      database.close();
    }
  }
}
