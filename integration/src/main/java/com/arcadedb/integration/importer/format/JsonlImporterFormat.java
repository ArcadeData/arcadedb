package com.arcadedb.integration.importer.format;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.CompressedRID2RIDIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.ZoneId;
import java.util.logging.Level;

public class JsonlImporterFormat extends AbstractImporterFormat {

  private CompressedRID2RIDIndex ridIndex;

  @Override
  public void load(SourceSchema sourceSchema,
      AnalyzedEntity.ENTITY_TYPE entityType,
      Parser parser,
      DatabaseInternal database,
      ImporterContext context,
      ImporterSettings settings) throws IOException {

    LogManager.instance().log(this, Level.INFO, "Start loading... ");
    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {

      final BufferedReader reader = new BufferedReader(inputFileReader);

      try {
        ridIndex = new CompressedRID2RIDIndex(database, 1000, 1000);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      String line;

      while ((line = reader.readLine()) != null) {

        JSONObject jsonLine = new JSONObject(line);

        String type = jsonLine.getString("t");

        switch (type) {
        case "schema" -> loadSchema(database, context, settings, jsonLine.getJSONObject("c"));
//        case "d" -> System.out.println("document = " + jsonObject);
        case "v" -> loadVertices(database, context, settings, jsonLine.getJSONObject("c"));
        case "e" -> loadEdges(database, context, settings, jsonLine.getJSONObject("c"));
        }
      }
    }

  }

  private void loadSchema(DatabaseInternal database,
      ImporterContext context,
      ImporterSettings settings,
      JSONObject importedSchema) {

    var databaseSchema = database.getSchema();
    var importedSettings = importedSchema.getJSONObject("settings");
    databaseSchema.setDateFormat(importedSettings.getString("dateFormat"));
    databaseSchema.setDateTimeFormat(importedSettings.getString("dateTimeFormat"));
    databaseSchema.setZoneId(ZoneId.of(importedSettings.getString("zoneId")));

    var types = importedSchema.getJSONObject("types");
    types.keySet().forEach(typeName -> {
      var type = types.getJSONObject(typeName);
      var typeType = type.getString("type");
      var docType = switch (typeType) {
        case "v" -> databaseSchema.createVertexType(typeName);
        case "e" -> databaseSchema.createEdgeType(typeName);
        case "d" -> databaseSchema.createDocumentType(typeName);
        default -> throw new IllegalStateException("Unexpected value: " + typeType);
      };

      var properties = type.getJSONObject("properties");
      properties.keySet().forEach(propertyName -> {
        var property = properties.getJSONObject(propertyName);
        var propType = property.getString("type");
        docType.createProperty(propertyName, propType);
      });

      var indexes = type.getJSONObject("indexes");
      indexes.keySet().forEach(index -> {
        var idx = indexes.getJSONObject(index);
        Schema.INDEX_TYPE idxType = Schema.INDEX_TYPE.valueOf(idx.getString("type"));
        String[] idxFields = idx.getString("properties").split(",");
        var typeIndex = docType.createTypeIndex(idxType, true, idxFields);
        typeIndex.setNullStrategy(NULL_STRATEGY.valueOf(idx.getString("nullStrategy")));
      });
    });

  }

  private void loadVertices(DatabaseInternal database, ImporterContext context, ImporterSettings settings, JSONObject vertex) {

    var properties = vertex.getJSONObject("p");
    var imported = database.newVertex(vertex.getString("t"))
        .fromJSON(properties)
        .save();
    var r = new RID(database, vertex.getString("r"));
    ridIndex.put(r, imported.getIdentity());

  }

  private void loadEdges(DatabaseInternal database, ImporterContext context, ImporterSettings settings, JSONObject edge) {
    var properties = edge.getJSONObject("p");
    var edgeType = edge.getString("t");

    var out = new RID(database, edge.getString("o"));
    var newOut = ridIndex.get(out);

    var in = new RID(database, edge.getString("i"));
    var newIn = ridIndex.get(in);
    var sourceVertex = (Vertex) database.lookupByRID(newOut, false);

    sourceVertex.newEdge(edgeType, newIn, true).fromJSON(properties).save();

  }

  @Override
  public SourceSchema analyze(AnalyzedEntity.ENTITY_TYPE entityType,
      Parser parser,
      ImporterSettings settings,
      AnalyzedSchema analyzedSchema) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Start Analyze... ");

    return new SourceSchema(this, parser.getSource(), analyzedSchema);

  }

  @Override
  public String getFormat() {
    return "JSONL";
  }
}
