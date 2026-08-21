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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
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
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Schema;
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
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class JsonlImporterFormat extends AbstractImporterFormat {

  private ConsoleLogger          logger;
  private CompressedRID2RIDIndex ridIndex;

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

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
      context.startedOn = System.currentTimeMillis();
      final BufferedReader reader = new BufferedReader(inputFileReader);

      ridIndex = new CompressedRID2RIDIndex(database, 1000, 1000);

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
          continue;
        }

        context.parsed.incrementAndGet();

        // Skip mode commits every record individually (see above and the field comment on skipOnRowError); the
        // default "abort" mode keeps the coarser periodic commit for throughput, since any failure there aborts
        // and rolls back the whole in-flight batch anyway (see the ImportException catch below).
        if (skipOnRowError || context.parsed.get() % 1000 == 0) {
          database.commit();
          database.begin();
        }
      }
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
      if (database.isTransactionActive())
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

    // final report
    databaseSchema.getTypes()
        .forEach(type -> logger.logLine(2, " - Created type %s: %s", type.getName(), type.toJSON()));
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
    loadProperties(database, imported, properties);
    imported.save();
    ridIndex.put(oldRid, imported.getIdentity());
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
    loadProperties(database, imported, properties);
    imported.save();
    ridIndex.put(oldRid, imported.getIdentity());
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
      loadProperties(database, imported, properties);
      imported.save();
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
  private void loadProperties(DatabaseInternal database, MutableDocument imported, JSONObject properties) {
    Map<String, Object> json2map = json2map(database, properties);
    imported.fromMap(json2map);
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
