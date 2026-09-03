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
package com.arcadedb.integration.importer.format;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.Record;
import com.arcadedb.database.RID;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.index.CompressedRID2RIDIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.integration.importer.AnalyzedEntity.EntityType;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeFullTextIndexBuilder;
import com.arcadedb.schema.TypeLSMSparseVectorIndexBuilder;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class JsonlImporterFormat extends AbstractImporterFormat {

  private ConsoleLogger          logger;
  private CompressedRID2RIDIndex ridIndex;

  // Issue #6460: LINK / LIST-of-LINK / MAP-of-LINK property values are remapped through ridIndex as soon as they are
  // loaded (loadProperties() below). A value that cannot be resolved yet - it references a record appearing LATER in
  // the source stream - is a forward reference: the referenced record's old-to-new RID mapping does not exist yet.
  // Rather than leaving it silently wrong (the original bug) or failing the whole record, the record's own new RID
  // and, per still-unresolved property, the OLD/source RID value(s) that failed to resolve are remembered here and
  // revisited in a reconciliation pass once the entire file has been read and every RID mapping is known (see
  // reconcileUnresolvedLinks()).
  //
  // Issue #6795: the value used to be just the set of unresolved PROPERTY NAMES, so reconciliation re-applied
  // ridIndex to every RID element of a LIST/MAP property, including ones pass 1 already resolved. On a normal
  // same-schema restore the source and target RID spaces overlap, so an already-correct element (now holding its
  // NEW target RID) could equal another record's OLD source RID and get silently re-pointed a second time.
  // Recording the specific old-source RIDs left unresolved lets reconciliation touch only those elements.
  private final Map<RID, Map<String, Set<RID>>> pendingLinkReconciliation = new HashMap<>();

  /**
   * Records - or TIMESERIES samples - accumulated before the periodic commit in {@link #load} fires.
   */
  private static final int COMMIT_EVERY = 1_000;

  /**
   * TIMESERIES samples appended since the last commit. One {@code "ts"} line carries a whole chunk of samples
   * (up to {@code JsonlExporterFormat}'s own chunk size), so counting LINES the way {@code context.parsed} does
   * would let a thousand such lines - a million samples - pile into a single transaction's WAL and dirty pages
   * before it commits. Counted apart, so a chunk weighs what it actually costs.
   */
  private int timeSeriesSamplesSinceCommit;

  @Override
  public void load(SourceSchema sourceSchema,
      EntityType entityType,
      Parser parser,
      DatabaseInternal database,
      ImporterContext context,
      ImporterSettings settings) throws IOException {

    logger = new ConsoleLogger(settings.verboseLevel);

    logger.logLine(2, "Start importing... ");

    // Governs the commit granularity below (see the loop): skip mode needs one record per transaction so a
    // failed record's rollback can never discard an earlier, already-successful one riding in the same batch.
    final boolean skipOnRowError = settings.isSkipOnRowError();

    // "skip" mode commits/rolls back per record, so it must own the transaction outright - exactly like
    // CSVImporterFormat/JSONImporterFormat (see ImporterSettings#isSkipOnRowError() and
    // ImporterSettings#newExclusiveTransactionRequiredException()). Checked eagerly, before touching the database
    // at all, so a caller-managed transaction's pending work is never at risk (issue #6561).
    if (skipOnRowError && context.callerTransactionActiveOnEntry)
      throw ImporterSettings.newExclusiveTransactionRequiredException();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
      context.startedOn = System.currentTimeMillis();
      final BufferedReader reader = new BufferedReader(inputFileReader);

      ridIndex = new CompressedRID2RIDIndex(database, 1000, 1000);
      pendingLinkReconciliation.clear();
      timeSeriesSamplesSinceCommit = 0;

      if (!database.isTransactionActive())
        database.begin();

      String line;
      while ((line = reader.readLine()) != null) {

        var jsonLine = new JSONObject(line);
        final String recordType = jsonLine.getString("t");

        try {
          switch (recordType) {
          case "schema" -> loadSchema(database, context, settings, jsonLine.getJSONObject("c"));
          case "d" -> loadDocument(database, context, jsonLine.getJSONObject("c"));
          case "v" -> loadVertex(database, context, jsonLine.getJSONObject("c"));
          case "e" -> loadEdge(database, context, jsonLine.getJSONObject("c"));
          case "ts" -> loadTimeSeriesSamples(database, context, jsonLine.getJSONObject("c"));
          }
        } catch (final Exception e) {
          // Every per-record failure (document/vertex/edge, and a broken schema entry too) funnels through here,
          // issue #6468: logging, error counting and the abort/skip decision live in ONE place instead of being
          // duplicated at each call site, which is also what keeps them from drifting out of sync with each other.
          LogManager.instance().log(this, Level.SEVERE, "Error on importing '%s' record: %s", e, recordType, line);
          context.errors.incrementAndGet();
          if (!skipOnRowError)
            throw new ImportException("Error on importing '" + recordType + "' record", e);

          // -onRowError skip: a failed record must leave no partial write behind for a later periodic commit to
          // durably persist - e.g. a vertex whose save() succeeded but whose RID-map bookkeeping then failed would
          // otherwise sit in the database uncounted and unreachable by ridIndex, silently cascading into every edge
          // that references it (the exact mechanism issue #6468 describes). Rolling back only this record's own
          // uncommitted work - never the batch - is what makes that guarantee hold.
          if (database.isTransactionActive())
            database.rollback();
          database.begin();
          timeSeriesSamplesSinceCommit = 0;
          continue;
        }

        context.parsed.incrementAndGet();

        // Skip mode commits every record individually (see above and the field comment on skipOnRowError); the
        // default "abort" mode keeps the coarser periodic commit for throughput, since any failure there aborts
        // and rolls back the whole in-flight batch anyway (see the ImportException catch below). Neither runs at
        // all when the caller already owns the active transaction (skip mode never reaches here - see the guard
        // above): a caller-managed transaction is never ours to commit piecemeal, only to accumulate into and hand
        // back to whoever owns it (issue #6561).
        if (!context.callerTransactionActiveOnEntry && (skipOnRowError || context.parsed.get() % COMMIT_EVERY == 0
            || timeSeriesSamplesSinceCommit >= COMMIT_EVERY)) {
          database.commit();
          database.begin();
          timeSeriesSamplesSinceCommit = 0;
        }
      }

      // Issue #6460: resolve any LINK / LIST-of-LINK / MAP-of-LINK property values that were still forward
      // references when their owning record was loaded (see the field comment on pendingLinkReconciliation).
      // Every RID mapping is known by now, so this is the only point where they can be reliably fixed up.
      reconcileUnresolvedLinks(database, context, skipOnRowError);
    } catch (ImportException e) {
      // A per-record failure in default "abort" mode must fail the whole import loudly (issue #6468): rolling
      // back the in-flight batch here - instead of committing it below - is what keeps a partial import from
      // masquerading as a successful one, and the callerTransactionActiveOnEntry guard mirrors
      // JSONImporterFormat's contract of never discarding a caller's own transaction.
      if (!context.callerTransactionActiveOnEntry && database.isTransactionActive())
        database.rollback();
      throw e;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      // Gated on callerTransactionActiveOnEntry exactly like the ImportException catch above (issue #6561): a
      // transaction that predates this import is never ours to commit, on success or on failure - it's left open
      // for whoever owns it to decide.
      if (!context.callerTransactionActiveOnEntry && database.isTransactionActive())
        database.commit();
    }
    context.lastLapOn = System.currentTimeMillis();
  }

  private void loadSchema(DatabaseInternal database,
      ImporterContext context,
      ImporterSettings settings,
      JSONObject importedSchema) {

    logger.logLine(2, "Loading schema... ");
    var databaseSchema = database.getSchema();
    var importedSettings = importedSchema.getJSONObject("settings");
    databaseSchema.setDateFormat(importedSettings.getString("dateFormat"));
    databaseSchema.setDateTimeFormat(importedSettings.getString("dateTimeFormat"));
    databaseSchema.setZoneId(ZoneId.of(importedSettings.getString("zoneId")));

    var types = importedSchema.getJSONObject("types");

    //create types
    types.keySet()
        .forEach(typeName -> {

          var type = types.getJSONObject(typeName);
          var typeType = type.getString("type");

          var docType = switch (typeType) {
            case "v" -> databaseSchema.createVertexType(typeName);
            // An edge type's declarations are part of the schema, not decoration: recreating it with
            // createEdgeType() alone silently turned a unidirectional type bidirectional, and would turn a
            // LIGHTWEIGHT type into a record-backed one, changing the storage shape of every imported edge.
            // UNIQUE is deliberately NOT set here - see the pass after the indexes below.
            case "e" -> databaseSchema.buildEdgeType().withName(typeName)
                .withBidirectional(!type.has("bidirectional") || type.getBoolean("bidirectional"))
                .withLightweight(type.getBoolean("lightweight", false))
                .create();
            case "d" -> databaseSchema.createDocumentType(typeName);
            // A TIMESERIES type is exported as "t" and had no case here at all, so the switch's default threw and
            // the whole import aborted at the schema line - not a partial restore, no restore (issue #7032).
            case "t" -> createTimeSeriesType(databaseSchema, typeName, type);
            default -> throw new IllegalStateException("Unexpected value: " + typeType);
          };

        });
    // add properties
    types.keySet()
        .forEach(typeName -> {

          var type = types.getJSONObject(typeName);
          var docType = databaseSchema.getType(typeName);

          var properties = type.getJSONObject("properties");
          properties.keySet()
              .forEach(propertyName -> {
                var property = properties.getJSONObject(propertyName);
                if (docType.existsProperty(propertyName)) {
                  // A TIMESERIES type's columns are registered as properties by its own builder, so they already
                  // exist by the time this pass runs and createProperty() would throw. Their CUSTOM metadata is
                  // still the export's to restore - it is not something the builder can know.
                  if (property.has("custom")) {
                    final JSONObject custom = property.getJSONObject("custom");
                    for (final String key : custom.keySet())
                      docType.getProperty(propertyName).setCustomValue(key, custom.get(key));
                  }
                  return;
                }
                docType.createProperty(propertyName, property);
              });

        });

    // Add super types
    types.keySet()
        .forEach(typeName -> {
          var type = types.getJSONObject(typeName);
          var docType = databaseSchema.getType(typeName);

          var parents = type.getJSONArray("parents");
          parents
              .toList()
              .stream()
              .map(Object::toString)
              .forEach(docType::addSuperType);
        });

    // Add indexes
    types.keySet()
        .forEach(typeName -> {
          var type = types.getJSONObject(typeName);
          var docType = databaseSchema.getType(typeName);

          var indexes = type.getJSONObject("indexes");
          indexes.keySet()
              .forEach(index -> {
                var idx = indexes.getJSONObject(index);
                var idxType = Schema.INDEX_TYPE.valueOf(idx.getString("type"));
                var idxFields = idx.getJSONArray("properties").toList().stream().map(Object::toString).toArray(String[]::new);

                // LSM_VECTOR indexes carry their own metadata (dimensions, similarity, ...) and, unlike the other
                // index types, do not serialise the "unique"/"nullStrategy" fields (issue #5069). Rebuild them
                // through the dedicated vector builder so the exported metadata is restored instead of crashing.
                if (idxType == Schema.INDEX_TYPE.LSM_VECTOR) {
                  loadVectorIndex(databaseSchema, typeName, idxFields, idx);
                  return;
                }

                // FULL_TEXT (analyzer, BM25 tuning, per-field boosts) and LSM_SPARSE_VECTOR (dimensions, modifier,
                // weightQuantization) keep their real configuration entirely in their metadata, same as LSM_VECTOR
                // above. getOrCreateTypeIndex() carries no metadata, so routing them through it silently dropped
                // every one of those settings on restore even though the export carries them (issue #5650).
                if (idxType == Schema.INDEX_TYPE.FULL_TEXT) {
                  loadFullTextIndex(databaseSchema, typeName, idxFields, idx);
                  return;
                }
                if (idxType == Schema.INDEX_TYPE.LSM_SPARSE_VECTOR) {
                  loadSparseVectorIndex(databaseSchema, typeName, idxFields, idx);
                  return;
                }

                var typeIndex = docType.getOrCreateTypeIndex(idxType, idx.getBoolean("unique"), idxFields);
                typeIndex.setNullStrategy(NULL_STRATEGY.valueOf(idx.getString("nullStrategy")));
              });

        });

    // Restore the UNIQUE declaration on edge types, last.
    //
    // On a regular edge type the constraint is materialised as the (@out, @in) index, which the export already
    // carries and the loop above has just recreated. Declaring UNIQUE at type-creation time would have built a
    // second one and collided with it - and would also have created the two endpoint properties before the
    // property loop tried to. On a LIGHTWEIGHT type there is no index at all and the flag is the whole constraint.
    types.keySet()
        .forEach(typeName -> {
          final JSONObject type = types.getJSONObject(typeName);
          if ("e".equals(type.getString("type")) && type.getBoolean("unique", false)
              && databaseSchema.getType(typeName) instanceof LocalEdgeType edgeType)
            edgeType.setUnique(true);
        });

    // Type-level ALIASES and CUSTOM metadata (issue #7032). The export has always carried both, and the import
    // restored neither, while property-level CUSTOM - handled a few lines up, by the same JSON, in adjacent code -
    // was restored. A restored type silently lost every alias queries referred to it by and every custom key an
    // application read back.
    types.keySet()
        .forEach(typeName -> {
          final JSONObject type = types.getJSONObject(typeName);
          final DocumentType docType = databaseSchema.getType(typeName);

          if (!type.isNull("aliases")) {
            final Set<String> aliases = new HashSet<>(type.getJSONArray("aliases").toListOfStrings());
            if (!aliases.isEmpty())
              try {
                docType.setAliases(aliases);
              } catch (final Exception e) {
                // setAliases() refuses an alias another type already answers to, which an import into a
                // NON-EMPTY target reaches without anything being wrong with the export. Same trade-off as the
                // strategy restore below: the type keeps its own name, the import keeps going, and the log says
                // what was dropped - aborting here would discard every record the file has not reached yet.
                context.warnings.incrementAndGet();
                LogManager.instance().log(this, Level.WARNING,
                    "Cannot restore the aliases %s on type '%s': %s. The type is reachable by its own name only",
                    e, aliases, typeName, e.getMessage() != null ? e.getMessage() : e.toString());
              }
          }

          if (type.has("custom")) {
            final JSONObject custom = type.getJSONObject("custom");
            for (final String key : custom.keySet())
              docType.setCustomValue(key, custom.get(key));
          }
        });

    // The bucket-selection strategy goes LAST, for the reason LocalSchema's own loader puts it last: a
    // PARTITIONED strategy binds to an index, so it can only be restored once the indexes above exist. Losing it
    // is not cosmetic - the strategy is what spreads writes across buckets, and `thread` is the documented remedy
    // for the single-bucket write contention the server warns about (issue #7032).
    types.keySet()
        .forEach(typeName -> {
          final JSONObject type = types.getJSONObject(typeName);
          if (!type.has("bucketSelectionStrategy"))
            return;

          final JSONObject strategy = type.getJSONObject("bucketSelectionStrategy");
          final Object[] properties = strategy.has("properties") ?
              strategy.getJSONArray("properties").toList().toArray() :
              new Object[0];
          try {
            databaseSchema.getType(typeName).setBucketSelectionStrategy(strategy.getString("name"), properties);
          } catch (final Exception e) {
            // One type declining its strategy must not abort an import that has already recreated the schema and
            // is about to load the records - same trade-off LocalSchema makes on open. The type falls back to
            // round-robin, which costs the partition pruning and keeps the data.
            context.warnings.incrementAndGet();
            LogManager.instance().log(this, Level.WARNING,
                "Cannot restore the '%s' bucket selection strategy on type '%s': %s. The type falls back to round-robin",
                e, strategy.getString("name"), typeName, e.getMessage() != null ? e.getMessage() : e.toString());
          }
        });

    // final report
    databaseSchema.getTypes()
        .forEach(type -> logger.logLine(2, " - Created type %s: %s", type.getName(), type.toJSON()));
  }

  /**
   * Recreates a TIMESERIES type from its exported schema definition (issue #7032).
   * <p>
   * Everything the type's identity depends on comes out of the export: the column list WITH the per-column codec
   * (which is not re-derivable - see {@link TimeSeriesTypeBuilder#withColumn}), the shard count, the retention and
   * compaction settings, and the downsampling tiers. What is deliberately NOT carried over is the format versions:
   * this build writes its own current formats into a type it is creating now, and the exported
   * {@code sealedFormatVersion}/{@code mutableFormatVersion} describe the pages of the SOURCE database, which the
   * target does not have.
   */
  private DocumentType createTimeSeriesType(final Schema databaseSchema, final String typeName, final JSONObject type) {
    final TimeSeriesTypeBuilder builder = databaseSchema.buildTimeSeriesType().withName(typeName);

    final JSONArray columns = type.getJSONArray("tsColumns", null);
    if (columns == null || columns.length() == 0)
      throw new ImportException("TIMESERIES type '" + typeName + "' carries no column definition in the export");

    for (int i = 0; i < columns.length(); i++) {
      final JSONObject column = columns.getJSONObject(i);
      final String columnName = column.getString("name");
      // The three enums are parsed inside one try: a hand-edited or foreign-tool-generated export reaches them
      // with a value none of them knows, and the raw IllegalArgumentException they raise names the bad token and
      // nothing else - not the column it came from, not the type, not even that this was a schema line.
      try {
        final Type dataType = Type.getTypeByName(column.getString("dataType"));
        final ColumnDefinition.ColumnRole role = ColumnDefinition.ColumnRole.valueOf(column.getString("role"));
        final String compression = column.getString("compression", null);
        builder.withColumn(compression != null ?
            new ColumnDefinition(columnName, dataType, role, TimeSeriesCodec.valueOf(compression)) :
            new ColumnDefinition(columnName, dataType, role));
      } catch (final IllegalArgumentException | NullPointerException e) {
        throw new ImportException(
            "Column '" + columnName + "' of TIMESERIES type '" + typeName + "' declares a data type, role or "
                + "compression this build does not know: " + e.getMessage(), e);
      }
    }

    if (type.has("precision"))
      builder.withPrecision(type.getString("precision"));
    // 0 is the builder's own "use the default" (it resolves to ASYNC_WORKER_THREADS), which is the right answer
    // for an export written before the count was persisted.
    builder.withShards(type.getInt("shardCount", 0));
    builder.withRetention(type.getLong("retentionMs", 0L));
    builder.withCompactionBucketInterval(type.getLong("compactionBucketIntervalMs", 0L));

    final JSONArray tiers = type.getJSONArray("downsamplingTiers", null);
    if (tiers != null) {
      final List<DownsamplingTier> parsed = new ArrayList<>(tiers.length());
      for (int i = 0; i < tiers.length(); i++) {
        final JSONObject tier = tiers.getJSONObject(i);
        parsed.add(new DownsamplingTier(tier.getLong("afterMs"), tier.getLong("granularityMs")));
      }
      builder.withDownsamplingTiers(parsed);
    }

    return builder.create();
  }

  /**
   * Appends one chunk of TIMESERIES samples, as written by {@code JsonlExporterFormat.exportTimeSeries} (issue
   * #7032). Each sample is the type's columns in schema order, timestamp first - a TimeSeries row has no RID, so
   * there is nothing to remap and nothing to record in {@code ridIndex}.
   */
  private void loadTimeSeriesSamples(final DatabaseInternal database, final ImporterContext context,
      final JSONObject chunk) throws IOException {
    final String typeName = chunk.getString("t");
    if (!(database.getSchema().getType(typeName) instanceof LocalTimeSeriesType tsType))
      throw new ImportException("Type '" + typeName + "' is not a TIMESERIES type, cannot import its samples");

    final TimeSeriesEngine engine = tsType.getEngine();
    if (engine == null)
      throw new ImportException("TimeSeries engine for type '" + typeName + "' is not initialized");

    final List<ColumnDefinition> columns = tsType.getTsColumns();
    int timestampIdx = 0;
    for (int i = 0; i < columns.size(); i++)
      if (columns.get(i).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP) {
        timestampIdx = i;
        break;
      }

    final JSONArray samples = chunk.getJSONArray("s");
    final int rows = samples.length();

    // Arity is checked for the WHOLE chunk before a single value is decoded, so a malformed chunk is refused by
    // the caller's row-error policy (abort or skip) rather than half-applied. A sample SHORTER than the schema
    // used to fail mid-decode, and a LONGER one was silently truncated to the columns this type declares - which
    // is the worse of the two, because it discards data without saying so.
    for (int r = 0; r < rows; r++) {
      final int arity = samples.getJSONArray(r).length();
      if (arity != columns.size())
        throw new ImportException("TIMESERIES sample " + r + " of type '" + typeName + "' has " + arity
            + " value(s) but the type declares " + columns.size() + " column(s)");
    }

    final long[] timestamps = new long[rows];
    // appendBatch indexes the value columns, skipping the timestamp - see ObjectColumnsRowSource.
    final Object[][] values = new Object[columns.size() - 1][rows];

    for (int r = 0; r < rows; r++) {
      final JSONArray sample = samples.getJSONArray(r);
      timestamps[r] = ((Number) sample.get(timestampIdx)).longValue();
      int valueIdx = 0;
      for (int c = 0; c < columns.size(); c++) {
        if (c == timestampIdx)
          continue;
        values[valueIdx][r] = decodeSampleValue(sample.get(c), columns.get(c).getDataType());
        valueIdx++;
      }
    }

    engine.appendBatch(timestamps, values);
    context.createdTimeSeriesSamples.addAndGet(rows);
    timeSeriesSamplesSinceCommit += rows;
  }

  /**
   * Reverses {@code JsonlExporterFormat.encodeSampleValue}: the non-finite doubles travel as string markers
   * because {@code JSONArray.put(Number)} would otherwise rewrite them to 0, turning "no measurement" into a
   * measurement of zero. Every other value passes through - {@code ObjectColumnsRowSource} accepts any
   * {@link Number} for a numeric column, so JSON's widening does no harm.
   */
  private static Object decodeSampleValue(final Object value, final Type dataType) {
    if (value instanceof String text && (dataType == Type.DOUBLE || dataType == Type.FLOAT)) {
      switch (text) {
      case "NaN":
        return Double.NaN;
      case "PosInfinity":
        return Double.POSITIVE_INFINITY;
      case "NegInfinity":
        return Double.NEGATIVE_INFINITY;
      default:
        // Not reachable from an export this build wrote - encodeSampleValue only ever emits the three markers
        // above. Kept as a tolerant parse for a hand-written or foreign-tool file that quoted a plain number,
        // which is otherwise a perfectly well-formed sample; a genuinely unparseable token still fails loudly,
        // through the same per-line row-error policy as anything else on the line.
        return Double.parseDouble(text);
      }
    }
    return value;
  }

  /**
   * Recreates an {@code LSM_VECTOR} index from its exported schema definition (issue #5069). Vector indexes need the
   * full metadata (dimensions, similarity function, HNSW parameters, ...) that {@code getOrCreateTypeIndex} cannot
   * carry, so they are rebuilt through the dedicated {@link TypeLSMVectorIndexBuilder}. Records imported afterwards
   * populate the graph incrementally through the index {@code put} hook.
   */
  private void loadVectorIndex(final Schema databaseSchema, final String typeName, final String[] fields, final JSONObject idx) {
    // withPersistedMetadata, not withMetadata: the exported definition is what LSMVectorIndex.toJSON() wrote, so it
    // carries structural keys (type, bucket, indexName, version, ...) that the METADATA-clause reader rejects as typos,
    // and it names the metric "similarityFunction" rather than the clause's "similarity" (issue #5639).
    final TypeLSMVectorIndexBuilder builder = databaseSchema.buildTypeIndex(typeName, fields)
        .withType(Schema.INDEX_TYPE.LSM_VECTOR)
        .withLSMVectorType();
    builder.withPersistedMetadata(idx);
    builder.withIgnoreIfExists(true);
    builder.create();
  }

  /**
   * Recreates a {@code FULL_TEXT} index from its exported schema definition (issue #5650), mirroring
   * {@link #loadVectorIndex}. Unlike LSM_VECTOR, the persisted definition of a FULL_TEXT index does carry
   * {@code unique}/{@code nullStrategy} (see {@code LSMTreeFullTextIndex.toJSON()}), so those are still applied
   * explicitly; everything else (analyzers, BM25 tuning, per-field boosts) comes back through the dedicated builder.
   * <p>
   * {@code withFreshCorpusCounters()} is required here and only here: this index is created empty and then
   * repopulated by replaying every document afterwards ({@link #loadDocument}/{@link #loadVertex}), so the BM25
   * corpus counters {@code withPersistedMetadata} just restored - describing the SOURCE database's already-indexed
   * corpus - must NOT carry through, or every replayed document would double-count on top of them.
   */
  private void loadFullTextIndex(final Schema databaseSchema, final String typeName, final String[] fields, final JSONObject idx) {
    final TypeFullTextIndexBuilder builder = databaseSchema.buildTypeIndex(typeName, fields)
        .withType(Schema.INDEX_TYPE.FULL_TEXT)
        .withFullTextType();
    builder.withUnique(idx.getBoolean("unique"));
    builder.withNullStrategy(NULL_STRATEGY.valueOf(idx.getString("nullStrategy")));
    builder.withPersistedMetadata(idx);
    builder.withFreshCorpusCounters();
    builder.withIgnoreIfExists(true);
    builder.create();
  }

  /**
   * Recreates an {@code LSM_SPARSE_VECTOR} index from its exported schema definition (issue #5650), mirroring
   * {@link #loadVectorIndex}. Unlike LSM_VECTOR, the persisted definition of a sparse index does carry
   * {@code unique}/{@code nullStrategy} (see {@code LSMSparseVectorIndex.toJSON()}), so those are still applied
   * explicitly; everything else (dimensions, modifier, weightQuantization) comes back through the dedicated builder.
   */
  private void loadSparseVectorIndex(final Schema databaseSchema, final String typeName, final String[] fields, final JSONObject idx) {
    final TypeLSMSparseVectorIndexBuilder builder = databaseSchema.buildTypeIndex(typeName, fields)
        .withType(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR)
        .withSparseVectorType();
    builder.withUnique(idx.getBoolean("unique"));
    builder.withNullStrategy(NULL_STRATEGY.valueOf(idx.getString("nullStrategy")));
    builder.withPersistedMetadata(idx);
    builder.withIgnoreIfExists(true);
    builder.create();
  }

  /**
   * Errors are intentionally NOT caught here (issue #6468): the RID is parsed and validated before {@code save()}
   * so that a malformed "r" field never leaves an orphaned, ridIndex-less record behind for the caller's per-record
   * rollback to have to clean up - and any other failure (a bad property value, a missing type, ...) is left to
   * propagate to {@link #load}'s single, centralized catch, which owns logging, error counting, and the abort/skip
   * decision for every record kind.
   */
  private void loadDocument(DatabaseInternal database,
      ImporterContext context,
      JSONObject document) {
    var properties = document.getJSONObject("p");
    var oldRid = new RID(document.getString("r"));
    var imported = database.newDocument(document.getString("t"));
    var unresolvedLinks = loadProperties(database, imported, properties);
    imported.save();
    ridIndex.put(oldRid, imported.getIdentity());
    if (!unresolvedLinks.isEmpty())
      pendingLinkReconciliation.put(imported.getIdentity(), unresolvedLinks);
    context.createdDocuments.incrementAndGet();
  }

  /**
   * See {@link #loadDocument}: same reasoning, same RID-before-save ordering.
   */
  private void loadVertex(DatabaseInternal database,
      ImporterContext context,
      JSONObject vertex) {
    var properties = vertex.getJSONObject("p");
    var oldRid = new RID(vertex.getString("r"));
    var imported = database.newVertex(vertex.getString("t"));
    var unresolvedLinks = loadProperties(database, imported, properties);
    imported.save();
    ridIndex.put(oldRid, imported.getIdentity());
    if (!unresolvedLinks.isEmpty())
      pendingLinkReconciliation.put(imported.getIdentity(), unresolvedLinks);
    context.createdVertices.incrementAndGet();
  }

  /**
   * See {@link #loadDocument}: errors, including an unresolved out/in vertex, propagate to {@link #load}'s
   * centralized catch rather than being handled here.
   */
  private void loadEdge(DatabaseInternal database, ImporterContext context, JSONObject edge) {
    var properties = edge.getJSONObject("p");
    var edgeType = edge.getString("t");

    var out = new RID(edge.getString("o"));
    var newOut = ridIndex.get(out);
    if (newOut == null)
      throw new ImportException("Out vertex not found: " + out);

    var in = new RID(edge.getString("i"));
    var newIn = ridIndex.get(in);
    if (newIn == null)
      throw new ImportException("In vertex not found: " + in);

    var sourceVertex = (Vertex) database.lookupByRID(newOut, false);

    MutableEdge imported = sourceVertex.newEdge(edgeType, newIn);
    if (!(imported instanceof LightEdge)) {
      // A lightweight edge has no record: it is already connected by newEdge, carries no properties, and
      // rejects fromMap()/save() by design.
      var unresolvedLinks = loadProperties(database, imported, properties);
      imported.save();
      if (!unresolvedLinks.isEmpty())
        pendingLinkReconciliation.put(imported.getIdentity(), unresolvedLinks);
    }

    context.createdEdges.incrementAndGet();
  }

  @Override
  public SourceSchema analyze(EntityType entityType,
      Parser parser,
      ImporterSettings settings,
      AnalyzedSchema analyzedSchema) throws IOException {

    return new SourceSchema(this, parser.getSource(), analyzedSchema);

  }

  @Override
  public String getFormat() {
    return "JSONL";
  }

  // utility methods from JsonSerializer

  /**
   * Loads a record's properties and, before they are written, remaps any LINK / LIST-of-LINK / MAP-of-LINK value
   * from the source RID the exporter emitted to the RID the referenced record was actually recreated at (issue
   * #6460). Without this, edges are healed through {@code ridIndex} on the endpoint paths ({@link #loadEdge}) but a
   * regular LINK-typed property was passed through untouched, so after import it silently pointed at the source
   * database's RID - now a different, unrelated record, or none.
   *
   * @return per still-unresolved property, the OLD/source RID value(s) that could not be resolved yet because they
   * reference a record that has not been imported so far (a forward reference); empty when every LINK value
   * resolved immediately. The caller is expected to remember these against the record's own (new) identity so
   * {@link #reconcileUnresolvedLinks} can revisit them once every RID mapping is known.
   */
  private Map<String, Set<RID>> loadProperties(DatabaseInternal database, MutableDocument imported, JSONObject properties) {
    Map<String, Object> json2map = json2map(database, properties);
    final Map<String, Set<RID>> unresolved = remapLinkProperties(imported.getType(), json2map);
    imported.fromMap(json2map);
    return unresolved;
  }

  /**
   * Remaps every LINK-typed value in {@code properties} in place, consulting {@code ridIndex} for the schema-declared
   * LINK properties of {@code type} (scalar LINK, and LIST/MAP declared {@code OF LINK}). A value that {@code
   * ridIndex} does not (yet) know about is left untouched and its OLD/source RID is added to the returned map,
   * keyed by property name.
   * <p>
   * Only schema-declared properties are inspected: without a declared type there is no reliable way to tell a LINK
   * string ("#12:34") apart from an ordinary string that merely looks like one.
   */
  private Map<String, Set<RID>> remapLinkProperties(final DocumentType type, final Map<String, Object> properties) {
    if (type == null || properties.isEmpty())
      return Map.of();

    Map<String, Set<RID>> unresolved = null;

    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final Object value = entry.getValue();
      if (value == null)
        continue;

      final Property property = type.getPolymorphicPropertyIfExists(entry.getKey());
      if (property == null)
        continue;

      final Type propertyType = property.getType();

      final Set<RID> propertyUnresolved;
      if (propertyType == Type.LINK) {
        propertyUnresolved = new HashSet<>();
        entry.setValue(remapLinkValue(value, propertyUnresolved));
      } else if (propertyType == Type.LIST && "LINK".equalsIgnoreCase(property.getOfType()) && value instanceof List<?> list) {
        propertyUnresolved = new HashSet<>();
        final List<Object> remapped = new ArrayList<>(list.size());
        for (final Object item : list)
          remapped.add(remapLinkValue(item, propertyUnresolved));
        entry.setValue(remapped);
      } else if (propertyType == Type.MAP && "LINK".equalsIgnoreCase(property.getOfType()) && value instanceof Map<?, ?> valueMap) {
        propertyUnresolved = new HashSet<>();
        final Map<Object, Object> remapped = new HashMap<>();
        for (final Map.Entry<?, ?> mapEntry : valueMap.entrySet())
          remapped.put(mapEntry.getKey(), remapLinkValue(mapEntry.getValue(), propertyUnresolved));
        entry.setValue(remapped);
      } else
        continue;

      if (!propertyUnresolved.isEmpty()) {
        if (unresolved == null)
          unresolved = new HashMap<>();
        unresolved.put(entry.getKey(), propertyUnresolved);
      }
    }

    return unresolved == null ? Map.of() : unresolved;
  }

  /**
   * Resolves a single LINK value (the exported source RID, as a String) to the RID the referenced record was
   * actually recreated at. If {@code ridIndex} has no mapping yet for it - the referenced record has not been
   * imported so far - its OLD/source RID is recorded into {@code unresolved} and the original value is returned
   * unchanged, to be revisited by {@link #reconcileUnresolvedLinks}.
   */
  private Object remapLinkValue(final Object value, final Set<RID> unresolved) {
    if (!(value instanceof String string))
      return value;

    final RID oldRid;
    try {
      oldRid = new RID(string);
    } catch (final Exception e) {
      // Not a RID-shaped string. Schema says this is a LINK, but the data disagrees - leave it for the normal
      // property conversion/validation path to deal with rather than silently swallowing it here.
      return value;
    }

    final RID newRid = ridIndex.get(oldRid);
    if (newRid != null)
      return newRid.toString();

    unresolved.add(oldRid);
    return value;
  }

  /**
   * Revisits every record that {@link #loadProperties} could not fully resolve LINK values for on first pass (a
   * forward reference to a record imported later in the source stream), now that the whole file has been read and
   * every old-to-new RID mapping is known. A property still unresolved after this (the referenced record was never
   * imported at all - excluded from the export, or a genuinely dangling link in the source database) is left as-is,
   * matching the pre-fix behavior for that case, and is not treated as an import error.
   * <p>
   * <b>Known limitation:</b> until this method runs, an unresolved forward-reference LINK value sits in the record
   * as the raw <i>source</i>-database RID (see {@link #remapLinkValue}) rather than a null or otherwise-neutral
   * placeholder. If that property backs a UNIQUE index, this transient value can coincidentally collide with
   * another record's already-resolved value in the <i>target</i> database, raising a {@code DuplicateKeyException}
   * for data that would otherwise import cleanly. Accepted as a known limitation for now rather than fixed, since
   * closing it properly (a null placeholder, a pre-scan of forward references, or documenting it permanently) is a
   * design choice, not a mechanical patch - see PR #6654's "Review follow-ups" for the options considered.
   * <p>
   * A failure reconciling one record (including the {@code DuplicateKeyException} above) is handled exactly like a
   * per-record failure in the main import loop (issue #6468, {@link #load}): logged, counted into
   * {@code context.errors}, and either aborted via {@link ImportException} or skipped per {@code skipOnRowError},
   * rather than propagating raw and uncounted past this method.
   */
  private void reconcileUnresolvedLinks(final DatabaseInternal database, final ImporterContext context, final boolean skipOnRowError) {
    if (pendingLinkReconciliation.isEmpty())
      return;

    int reconciled = 0;
    for (final Map.Entry<RID, Map<String, Set<RID>>> entry : pendingLinkReconciliation.entrySet()) {
      try {
        final Record record = database.lookupByRID(entry.getKey(), true);
        if (!(record instanceof Document document))
          continue;

        final DocumentType type = document.getType();
        final MutableDocument mutable = document.modify();
        boolean changed = false;

        for (final Map.Entry<String, Set<RID>> propertyEntry : entry.getValue().entrySet()) {
          final String propertyName = propertyEntry.getKey();
          // Issue #6795: only an element whose CURRENT value is one of the OLD/source RIDs pass 1 actually left
          // unresolved for this property is a candidate for remapping - an element pass 1 already resolved (now
          // holding its NEW target RID) is left untouched even if that value happens to also be a key in
          // ridIndex, which a normal same-schema restore (overlapping source/target RID spaces) makes routine.
          final Set<RID> stillUnresolved = propertyEntry.getValue();

          final Property property = type.getPolymorphicPropertyIfExists(propertyName);
          if (property == null)
            continue;

          final Object currentValue = mutable.get(propertyName);
          if (currentValue == null)
            continue;

          if (property.getType() == Type.LINK && currentValue instanceof RID currentRid) {
            if (stillUnresolved.contains(currentRid)) {
              final RID newRid = ridIndex.get(currentRid);
              if (newRid != null) {
                mutable.set(propertyName, newRid);
                changed = true;
              }
            }
          } else if (currentValue instanceof List<?> list) {
            final List<Object> updated = new ArrayList<>(list.size());
            boolean listChanged = false;
            for (final Object item : list) {
              if (item instanceof RID itemRid && stillUnresolved.contains(itemRid)) {
                final RID newRid = ridIndex.get(itemRid);
                if (newRid != null) {
                  updated.add(newRid);
                  listChanged = true;
                  continue;
                }
              }
              updated.add(item);
            }
            if (listChanged) {
              mutable.set(propertyName, updated);
              changed = true;
            }
          } else if (currentValue instanceof Map<?, ?> valueMap) {
            final Map<Object, Object> updated = new HashMap<>();
            boolean mapChanged = false;
            for (final Map.Entry<?, ?> mapEntry : valueMap.entrySet()) {
              Object mapValue = mapEntry.getValue();
              if (mapValue instanceof RID itemRid && stillUnresolved.contains(itemRid)) {
                final RID newRid = ridIndex.get(itemRid);
                if (newRid != null) {
                  mapValue = newRid;
                  mapChanged = true;
                }
              }
              updated.put(mapEntry.getKey(), mapValue);
            }
            if (mapChanged) {
              mutable.set(propertyName, updated);
              changed = true;
            }
          }
        }

        if (changed)
          mutable.save();
      } catch (final Exception e) {
        // Same per-record failure handling as the main import loop (JsonlImporterFormat.java:113-138, issue
        // #6468): logged, counted, and either aborts the whole import or is skipped per -onRowError, exactly like
        // any other record failure, instead of propagating raw and uncounted.
        LogManager.instance().log(this, Level.SEVERE, "Error on reconciling forward-referenced LINK propert(y/ies) for record '%s'", e, entry.getKey());
        context.errors.incrementAndGet();
        if (!skipOnRowError)
          throw new ImportException("Error on reconciling forward-referenced LINK properties for record " + entry.getKey(), e);

        if (database.isTransactionActive())
          database.rollback();
        database.begin();
        continue;
      }

      // Mirrors the periodic commit granularity of the main import loop (see load()): without this, every record
      // touched here rides in the single transaction still open when the main loop finished, on top of the batches
      // already committed for the initial load, producing one very large WAL transaction for a restore with many
      // forward-referencing LINK properties. Skip mode commits every record individually, same reasoning as the
      // main loop: a failed record's rollback must never discard an earlier, already-reconciled one riding in the
      // same batch. Same callerTransactionActiveOnEntry gate as load()'s own periodic commit, and for the same
      // reason (issue #6561): skip mode never reaches here when the caller owns the transaction (rejected eagerly
      // in load()), and the default mode must not commit a transaction it doesn't own either.
      if (!context.callerTransactionActiveOnEntry && (skipOnRowError || ++reconciled % 1000 == 0)) {
        database.commit();
        database.begin();
      }
    }

    pendingLinkReconciliation.clear();
  }

  private Map<String, Object> json2map(DatabaseInternal database, final JSONObject json) {
    final Map<String, Object> map = new HashMap<>();
    for (final String k : json.keySet()) {
      final Object value = convertFromJSONType(database, json.get(k));
      map.put(k, value);
    }
    return map;
  }

  private Object convertFromJSONType(DatabaseInternal database, Object value) {
    if (value instanceof JSONObject json) {
      if (json.has("t")) {
        String embeddedTypeName = json.getString("t");

        final DocumentType type = database.getSchema().getType(embeddedTypeName);

        if (type instanceof LocalVertexType) {
          final MutableVertex v = database.newVertex(embeddedTypeName);
          v.fromJSON((JSONObject) value);
          value = v;
        } else if (type != null) {
          final MutableEmbeddedDocument embeddedDocument = ((DatabaseInternal) database).newEmbeddedDocument(null,
              embeddedTypeName);
          embeddedDocument.fromJSON(((JSONObject) value).getJSONObject("p"));
          value = embeddedDocument;
        }
      } else {
        final Map<String, Object> map = new HashMap<>();
        for (final String k : json.keySet()) {
          final Object v = convertFromJSONType(database, json.get(k));
          map.put(k, v);
        }
        value = map;
      }
    } else if (value instanceof JSONArray array) {
      final List<Object> list = new ArrayList<>();
      for (int i = 0; i < array.length(); ++i)
        list.add(convertFromJSONType(database, array.get(i)));
      value = list;
    }

    return value;
  }

}
