package com.arcadedb.integration.importer.format;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.CompressedRID2RIDIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.integration.importer.AnalyzedEntity.ENTITY_TYPE;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Schema;
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

public class JsonlImporterFormat extends AbstractImporterFormat {

  private ConsoleLogger          logger;
  private CompressedRID2RIDIndex ridIndex;

  @Override
  public void load(SourceSchema sourceSchema,
      ENTITY_TYPE entityType,
      Parser parser,
      DatabaseInternal database,
      ImporterContext context,
      ImporterSettings settings) throws IOException {

    logger = new ConsoleLogger(settings.verboseLevel);

    logger.logLine(2, "Start importing... ");

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

        switch (jsonLine.getString("t")) {
        case "schema" -> loadSchema(database, context, settings, jsonLine.getJSONObject("c"));
        case "d" -> loadDocument(database, context, settings, jsonLine.getJSONObject("c"));
        case "v" -> loadVertex(database, context, settings, jsonLine.getJSONObject("c"));
        case "e" -> loadEdge(database, context, settings, jsonLine.getJSONObject("c"));
        }

        context.parsed.incrementAndGet();
        if (context.parsed.get() % 1000 == 0) {
          database.commit();
          database.begin();
        }
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
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
            case "e" -> databaseSchema.createEdgeType(typeName);
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
                var typeIndex = docType.getOrCreateTypeIndex(idxType, idx.getBoolean("unique"), idxFields);
                typeIndex.setNullStrategy(NULL_STRATEGY.valueOf(idx.getString("nullStrategy")));
              });

        });

    // final report
    databaseSchema.getTypes()
        .forEach(type -> logger.logLine(2, " - Created type %s: %s",type.getName(), type.toJSON()));
  }

  private void loadDocument(DatabaseInternal database,
      ImporterContext context,
      ImporterSettings settings,
      JSONObject document) {
    var properties = document.getJSONObject("p");
    var imported = database.newDocument(document.getString("t"));
    loadProperties(database, imported, properties);
    imported.save();
    var oldRid = new RID(database, document.getString("r"));
    ridIndex.put(oldRid, imported.getIdentity());
    context.createdDocuments.incrementAndGet();
  }

  private void loadVertex(DatabaseInternal database,
      ImporterContext context,
      ImporterSettings settings,
      JSONObject vertex) {

    var properties = vertex.getJSONObject("p");
    var imported = database.newVertex(vertex.getString("t"));
    loadProperties(database, imported, properties);
    imported.save();
    var oldRid = new RID(database, vertex.getString("r"));
    ridIndex.put(oldRid, imported.getIdentity());
    context.createdVertices.incrementAndGet();

  }

  private void loadEdge(DatabaseInternal database, ImporterContext context,
      ImporterSettings settings,
      JSONObject edge) {
    var properties = edge.getJSONObject("p");
    var edgeType = edge.getString("t");

    var out = new RID(database, edge.getString("o"));
    var newOut = ridIndex.get(out);

    var in = new RID(database, edge.getString("i"));
    var newIn = ridIndex.get(in);
    var sourceVertex = (Vertex) database.lookupByRID(newOut, false);

    MutableEdge imported = sourceVertex.newEdge(edgeType, newIn, true);
    loadProperties(database, imported, properties);
    imported.save();

    context.createdEdges.incrementAndGet();
  }

  @Override
  public SourceSchema analyze(ENTITY_TYPE entityType,
      Parser parser,
      ImporterSettings settings,
      AnalyzedSchema analyzedSchema) throws IOException {

    return new SourceSchema(this, parser.getSource(), analyzedSchema);

  }

  @Override
  public String getFormat() {
    return "JSONL";
  }

  // utility methods from JSONSerializer
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
