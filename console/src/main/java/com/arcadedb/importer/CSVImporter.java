/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.database.async.NewRecordCallback;
import com.arcadedb.importer.graph.EdgeLinkedCallback;
import com.arcadedb.importer.graph.GraphImporter;
import com.arcadedb.index.CompressedAny2RIDIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.logging.Level;

public class CSVImporter extends AbstractContentImporter {
  private static final Object[] NO_PARAMS = new Object[] {};
  public static final  int      _32MB     = 32 * 1024 * 1024;

  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser,
      final DatabaseInternal database, final ImporterContext context, final ImporterSettings settings) throws ImportException {

    context.parsed.set(0);

    switch (entityType) {
    case DOCUMENT:
      loadDocuments(sourceSchema, parser, database, context, settings);
      break;

    case VERTEX:
      loadVertices(sourceSchema, parser, database, context, settings);
      break;

    case EDGE:
      loadEdges(sourceSchema, parser, database, context, settings);
      break;
    }
  }

  private void loadDocuments(final SourceSchema sourceSchema, final Parser parser, final Database database, final ImporterContext context,
      final ImporterSettings settings) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    LogManager.instance().log(this, Level.INFO, "Started importing documents from CSV source", null);

    final long beginTime = System.currentTimeMillis();

    long skipEntries = settings.documentsSkipEntries != null ? settings.documentsSkipEntries.longValue() : 0;
    if (settings.documentsHeader == null && settings.documentsSkipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      skipEntries = 1l;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();

      final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.vertexTypeName);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.documentPropertiesInclude.equalsIgnoreCase("*")) {
        final String[] includes = settings.documentPropertiesInclude.split(",");
        final Set<String> propertiesSet = new HashSet<>();

        for (String i : includes)
          propertiesSet.add(i);

        for (AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else {
        // INCLUDE ALL THE PROPERTIES
        for (AnalyzedProperty p : entity.getProperties())
          properties.add(p);
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following document properties: %s", null, properties);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          // SKIP IT
          continue;

        final MutableDocument document = database.newDocument(settings.documentTypeName);

        for (int p = 0; p < properties.size(); ++p) {
          final AnalyzedProperty prop = properties.get(p);
          document.set(prop.getName(), row[prop.getIndex()]);
        }

        database.async().createRecord(document, new NewRecordCallback() {
          @Override
          public void call(final Record newDocument) {
            context.createdDocuments.incrementAndGet();
          }
        });
      }

      database.commit();
      database.async().waitCompletion();

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      LogManager.instance()
          .log(this, Level.INFO, "Importing of documents from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdDocuments.get() / elapsedInSecs : context.createdDocuments.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total documents: %d", null, context.createdDocuments.get());

      csvParser.stopParsing();
    }
  }

  private void loadVertices(final SourceSchema sourceSchema, final Parser parser, final Database database, final ImporterContext context,
      final ImporterSettings settings) throws ImportException {

    final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.vertexTypeName);
    final AnalyzedProperty id = entity.getProperty(settings.typeIdProperty);

    if (id == null) {
      LogManager.instance().log(this, Level.INFO, "Property Id '%s.%s' is null. Importing is aborted", null, settings.vertexTypeName,
          settings.typeIdProperty);
      throw new IllegalArgumentException(
          "Property Id '" + settings.vertexTypeName + "." + settings.typeIdProperty + "' is null. Importing is aborted");
    }

    long expectedVertices = settings.expectedVertices;
    if (expectedVertices <= 0 && entity != null)
      expectedVertices = (int) (sourceSchema.getSource().totalSize / entity.getAverageRowLength());
    if (expectedVertices <= 0)
      expectedVertices = 1000000;
    else if (expectedVertices > Integer.MAX_VALUE)
      expectedVertices = Integer.MAX_VALUE;

    context.graphImporter = new GraphImporter((DatabaseInternal) database, (int) expectedVertices, (int) settings.expectedEdges);

    final AbstractParser csvParser = createCSVParser(settings, ",");

    LogManager.instance().log(this, Level.INFO, "Started importing vertices from CSV source", null);

    final long beginTime = System.currentTimeMillis();

    long skipEntries = settings.verticesSkipEntries != null ? settings.verticesSkipEntries.longValue() : 0;
    if (settings.verticesSkipEntries == null && settings.verticesSkipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      skipEntries = 1l;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      final int idIndex = id.getIndex();

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.vertexPropertiesInclude.isEmpty() && !settings.vertexPropertiesInclude.equalsIgnoreCase("*")) {
        final String[] includes = settings.vertexPropertiesInclude.split(",");
        final Set<String> propertiesSet = new HashSet<>();

        for (String i : includes)
          propertiesSet.add(i);

        for (AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else {
        // INCLUDE ALL THE PROPERTIES
        for (AnalyzedProperty p : entity.getProperties())
          properties.add(p);
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following vertex properties: %s", null, properties);

      String[] row;
      final Object[] vertexProperties = new Object[properties.size() * 2];

      final CompressedAny2RIDIndex<Object> verticesIndex = context.graphImporter.getVerticesIndex();

      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          // SKIP IT
          continue;

        if (idIndex >= row.length) {
          LogManager.instance()
              .log(this, Level.INFO, "Property Id is configured on property %d but cannot be found on current record. Skip it", null,
                  idIndex);
          continue;
        }

        final long vertexId = Long.parseLong(row[idIndex]);

        for (int p = 0; p < properties.size(); ++p) {
          final AnalyzedProperty prop = properties.get(p);
          vertexProperties[p * 2] = prop.getName();
          vertexProperties[p * 2 + 1] = row[prop.getIndex()];
        }

        context.graphImporter.createVertex(settings.vertexTypeName, vertexId, vertexProperties);

        context.createdVertices.incrementAndGet();

        if (line > 0 && line % 10000000 == 0) {
          LogManager.instance().log(this, Level.INFO, "Map chunkSize=%s chunkAllocated=%s size=%d totalUsedSlots=%d", null,
              FileUtils.getSizeAsString(verticesIndex.getChunkSize()), FileUtils.getSizeAsString(verticesIndex.getChunkAllocated()),
              verticesIndex.size(), verticesIndex.getTotalUsedSlots());
        }
      }

      database.async().waitCompletion();

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      LogManager.instance()
          .log(this, Level.INFO, "Importing of vertices from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdVertices.get() / elapsedInSecs : context.createdVertices.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total vertices.: %d", null, context.createdVertices.get());

      csvParser.stopParsing();

      context.graphImporter.startImportingEdges();
    }
  }

  private void loadEdges(final SourceSchema sourceSchema, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    final long beginTime = System.currentTimeMillis();

    final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.edgeTypeName);
    final AnalyzedProperty from = entity.getProperty(settings.edgeFromField);
    final AnalyzedProperty to = entity.getProperty(settings.edgeToField);

    long expectedEdges = settings.expectedEdges;
    if (expectedEdges <= 0 && entity != null)
      expectedEdges = (int) (sourceSchema.getSource().totalSize / entity.getAverageRowLength());

    if (expectedEdges <= 0 || expectedEdges > _32MB)
      // USE CHUNKS OF 16MB EACH
      expectedEdges = _32MB;

    long expectedVertices = settings.expectedVertices;
    if (expectedVertices <= 0)
      expectedVertices = expectedEdges / 2;

    LogManager.instance()
        .log(this, Level.INFO, "Started importing edges from CSV source (expectedVertices=%d expectedEdges=%d)", null, expectedVertices,
            expectedEdges);

    long skipEntries = settings.edgesSkipEntries != null ? settings.edgesSkipEntries.longValue() : 0;
    if (settings.edgesSkipEntries == null && settings.edgesSkipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      skipEntries = 1l;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.edgePropertiesInclude.isEmpty() && !settings.edgePropertiesInclude.equalsIgnoreCase("*")) {
        final String[] includes = settings.edgePropertiesInclude.split(",");
        final Set<String> propertiesSet = new HashSet<>();

        for (String i : includes)
          propertiesSet.add(i);

        for (AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else {
        // INCLUDE ALL THE PROPERTIES
        for (AnalyzedProperty p : entity.getProperties())
          properties.add(p);
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following edge properties: %s", null, properties);

      // TODO: LET THE EDGE NAME TO BE HERE ON A SINGLE CONNECTION
      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          // SKIP IT
          continue;

        try {
          createEdgeFromRow(row, properties, from, to, context, settings);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on parsing line %d", e, line);
        }
      }

      context.graphImporter.close(new EdgeLinkedCallback() {
        @Override
        public void onLinked(long linked) {
          context.linkedEdges.addAndGet(linked);
        }
      });

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      LogManager.instance()
          .log(this, Level.INFO, "Importing of edges from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdEdges.get() / elapsedInSecs : context.createdEdges.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines......: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total edges.......: %d", null, context.createdEdges.get());
      LogManager.instance().log(this, Level.INFO, "- Total linked Edges: %d", null, context.linkedEdges.get());
      LogManager.instance().log(this, Level.INFO, "- Skipped edges.....: %d", null, context.skippedEdges.get());

      csvParser.stopParsing();
    }
  }

  public void createEdgeFromRow(final String[] row, final List<AnalyzedProperty> properties, final AnalyzedProperty from,
      final AnalyzedProperty to, final ImporterContext context, final ImporterSettings settings) {

    if (from.getIndex() >= row.length || to.getIndex() >= row.length) {
      context.skippedEdges.incrementAndGet();
      return;
    }

    final String fromValue = row[from.getIndex()];
    final String toValue = row[to.getIndex()];

    if (fromValue == null || toValue == null) {
      context.skippedEdges.incrementAndGet();
      return;
    }

    final long sourceVertexKey = Long.parseLong(fromValue);
    final long destinationVertexKey = Long.parseLong(toValue);

    final Object[] params;
    if (row.length > 2) {
      params = new Object[properties.size() * 2];
      for (int i = 0; i < properties.size(); ++i) {
        final AnalyzedProperty property = properties.get(i);
        params[i * 2] = property.getName();
        params[i * 2 + 1] = row[property.getIndex()];
      }
    } else {
      params = NO_PARAMS;
    }

    context.graphImporter.createEdge(sourceVertexKey, settings.edgeTypeName, destinationVertexKey, params, context, settings);
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) throws IOException {
    parser.reset();

    String delimiter = ",";

    if (settings.options.containsKey("delimiter"))
      delimiter = settings.options.get("delimiter");

    CsvParserSettings csvParserSettings;
    TsvParserSettings tsvParserSettings;
    AbstractParser csvParser;

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      tsvParserSettings = new TsvParserSettings();
      csvParser = new TsvParser(tsvParserSettings);
    } else {
      csvParserSettings = new CsvParserSettings();
      csvParser = new CsvParser(csvParserSettings);
      csvParserSettings.setDelimiterDetectionEnabled(false);
      if (delimiter != null) {
        csvParserSettings.detectFormatAutomatically(delimiter.charAt(0));
        csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
      }
    }

    final List<String> fieldNames = new ArrayList<>();

    final String entityName = entityType == AnalyzedEntity.ENTITY_TYPE.DOCUMENT ?
        settings.documentTypeName :
        entityType == AnalyzedEntity.ENTITY_TYPE.VERTEX ? settings.vertexTypeName : settings.edgeTypeName;

    long skipEntries = 0;
    final String header;
    switch (entityType) {
    case VERTEX:
      header = settings.verticesHeader;
      skipEntries = settings.verticesSkipEntries != null ? settings.verticesSkipEntries.longValue() : 0;
      if (settings.verticesSkipEntries == null && settings.verticesSkipEntries == null)
        // BY DEFAULT SKIP THE FIRST LINE AS HEADER
        skipEntries = 1l;
      break;

    case EDGE:
      header = settings.edgesHeader;
      skipEntries = settings.edgesSkipEntries != null ? settings.edgesSkipEntries.longValue() : 0;
      if (settings.edgesSkipEntries == null && settings.edgesSkipEntries == null)
        // BY DEFAULT SKIP THE FIRST LINE AS HEADER
        skipEntries = 1l;
      break;

    case DOCUMENT:
      header = settings.documentsHeader;
      skipEntries = settings.documentsSkipEntries != null ? settings.documentsSkipEntries.longValue() : 0;
      if (settings.documentsSkipEntries == null && settings.documentsSkipEntries == null)
        // BY DEFAULT SKIP THE FIRST LINE AS HEADER
        skipEntries = 1l;
      break;

    default:
      header = null;
    }

    if (header != null) {
      if (delimiter == null)
        fieldNames.add(header);
      else {
        final String[] headerColumns = header.split(",");
        for (String column : headerColumns)
          fieldNames.add(column);
      }
      LogManager.instance().log(this, Level.INFO, "Parsing with custom header: %s", null, fieldNames);
    }

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        if (skipEntries > 0 && line < skipEntries)
          continue;

        if (settings.analysisLimitBytes > 0 && csvParser.getContext().currentChar() > settings.analysisLimitBytes)
          break;

        if (settings.analysisLimitEntries > 0 && line > settings.analysisLimitEntries)
          break;

        if (line == 0 && header == null) {
          // READ THE HEADER FROM FILE
          for (String cell : row)
            fieldNames.add(cell);
          LogManager.instance().log(this, Level.INFO, "Reading header from 1st line in data file: %s", null, Arrays.toString(row));
        } else {
          // DATA LINE
          final AnalyzedEntity entity = analyzedSchema.getOrCreateEntity(entityName, entityType);

          entity.setRowSize(row);
          for (int i = 0; i < row.length; ++i) {
            entity.getOrCreateProperty(fieldNames.get(i), row[i]);
          }
        }
      }

    } catch (EOFException e) {
      // REACHED THE LIMIT
    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    }

    // END OF PARSING. THIS DETERMINES THE TYPE
    analyzedSchema.endParsing();

    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public String getFormat() {
    return "CSV";
  }

  protected AbstractParser createCSVParser(final ImporterSettings settings, String delimiter) {
    if (settings.options.containsKey("delimiter"))
      delimiter = settings.options.get("delimiter");

    CsvParserSettings csvParserSettings;
    TsvParserSettings tsvParserSettings;
    AbstractParser csvParser;

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      tsvParserSettings = new TsvParserSettings();
      csvParser = new TsvParser(tsvParserSettings);
    } else {
      csvParserSettings = new CsvParserSettings();
      csvParser = new CsvParser(csvParserSettings);
      if (delimiter != null)
        csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
    }
    return csvParser;
  }
}
