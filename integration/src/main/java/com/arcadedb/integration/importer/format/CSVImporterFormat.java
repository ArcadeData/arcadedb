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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedProperty;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.common.CommonParserSettings;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class CSVImporterFormat extends AbstractImporterFormat {
  private static final Object[] NO_PARAMS = new Object[] {};
  public static final  int      _32MB     = 32 * 1024 * 1024;

  @Override
  public void load(final SourceSchema sourceSchema,
      final AnalyzedEntity.EntityType entityType, final Parser parser,
      final DatabaseInternal database,
      final ImporterContext context,
      final ImporterSettings settings) throws ImportException {

    context.parsed.set(0);

    switch (entityType) {
    case DOCUMENT, DATABASE -> loadDocuments(sourceSchema, parser, database, context, settings);
    case VERTEX -> loadVertices(sourceSchema, parser, database, context, settings);
    case EDGE -> loadEdges(sourceSchema, parser, database, context, settings);
    }
  }

  private void loadDocuments(final SourceSchema sourceSchema, final Parser parser, final Database database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
    final AbstractParser<?> csvParser = createCSVParser(settings);

    LogManager.instance().log(this, Level.INFO, "Started importing documents from CSV source");

    final long beginTime = System.currentTimeMillis();

    // IN "skip" MODE, EACH ROW GETS ITS OWN commit()/rollback() INSTEAD OF SHARING ONE TRANSACTION FOR THE WHOLE FILE
    // (WHY: SEE ImporterSettings#isSkipOnRowError()). THE OTHER THROWING CALL IN THIS METHOD, csvParser.parseNext(),
    // ONLY RUNS AT THE TOP OF THE NEXT LOOP ITERATION - RIGHT AFTER THE PRIOR ROW'S TRANSACTION WAS ALREADY COMMITTED
    // OR ROLLED BACK AND A FRESH, EMPTY ONE BEGUN - SO AN IOException FROM IT CAN NEVER LEAVE A ROW'S PARTIAL WRITE
    // SITTING IN AN OPEN TRANSACTION.
    //
    // parseNext() IS DELIBERATELY LEFT OUTSIDE THE PER-ROW try/catch BELOW, UNLIKE THE ROW-PROCESSING LOGIC: A
    // univocity TextParsingException (E.G. A VALUE EXCEEDING maxCharsPerColumn/maxColumns, A MALFORMED QUOTED FIELD)
    // MEANS THE UNDERLYING PARSER'S OWN POSITION TRACKING IS COMPROMISED. TESTED EMPIRICALLY: CATCHING SUCH AN
    // EXCEPTION AND CALLING parseNext() AGAIN DOES NOT CLEANLY RESUME AT THE NEXT LINE - IT CAN RETURN A TRUNCATED,
    // WRONG ROW AND SILENTLY DROP THE FOLLOWING ONE ENTIRELY, WITH NO ERROR RAISED FOR THE LOST ROW. THAT IS WORSE
    // THAN ABORTING, SO A CSV-SYNTAX-LEVEL PARSE FAILURE STILL ABORTS THE IMPORT EVEN IN "skip" MODE; ONLY
    // ROW-CONTENT VALIDATION FAILURES (OUT-OF-RANGE VALUES, MISSING MANDATORY PROPERTIES, DUPLICATE KEYS, ...) ARE
    // SKIPPABLE.
    final boolean skipOnError = settings.isSkipOnRowError();

    long skipEntries = settings.documentsSkipEntries != null ? settings.documentsSkipEntries : 0;
    if (settings.documentsHeader == null && settings.documentsSkipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      skipEntries = 1l;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();

      final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.documentTypeName);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!"*".equalsIgnoreCase(settings.documentPropertiesInclude)) {
        final String[] includes = settings.documentPropertiesInclude.split(",");

        final Set<String> propertiesSet = new HashSet<>(Arrays.asList(includes));

        for (final AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else if (entity != null) {
        // INCLUDE ALL THE PROPERTIES
        properties.addAll(entity.getProperties());
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following document properties: %s", null, properties);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          // SKIP IT
          continue;

        try {
          final MutableDocument document = database.newDocument(settings.documentTypeName);

          for (final AnalyzedProperty prop : properties) {
            final String value = row[prop.getIndex()];
            if (value != null && !value.isEmpty())
              document.set(prop.getName(), value);
          }

          document.save();

          if (skipOnError) {
            // COUNT ONLY AFTER commit() SUCCEEDS: A DUPLICATE-KEY VIOLATION IS ONLY DETECTED AT COMMIT TIME (SEE THE
            // COMMENT ABOVE), SO INCREMENTING RIGHT AFTER save() WOULD OVERCOUNT A ROW THAT commit() THEN ROLLS BACK.
            database.commit();
            context.createdDocuments.incrementAndGet();
            database.begin();
          } else
            context.createdDocuments.incrementAndGet();
        } catch (final RuntimeException e) {
          if (!skipOnError)
            throw e;
          if (database.isTransactionActive())
            database.rollback();
          LogManager.instance()
              .log(this, Level.WARNING, "Error on importing document at line %d, skipping it (reason: %s)", null, line,
                  e.getMessage());
          LogManager.instance().log(this, Level.FINE, "Full error on importing document at line %d", e, line);
          context.errors.incrementAndGet();
          database.begin();
        }
      }

      database.commit();

    } catch (final IOException e) {
      throw new ImportException("Error on importing CSV", e);
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      LogManager.instance()
          .log(this, Level.INFO, "Importing of documents from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdDocuments.get() / elapsedInSecs : context.createdDocuments.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total documents: %d", null, context.createdDocuments.get());
      LogManager.instance().log(this, Level.INFO, "- Skipped rows...: %d", null, context.errors.get());

      csvParser.stopParsing();
    }
  }

  private void loadVertices(final SourceSchema sourceSchema, final Parser parser, final Database database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {

    final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.vertexTypeName);
    if (entity == null) {
      LogManager.instance().log(this, Level.INFO, "Vertex type '%s' not defined", null, settings.vertexTypeName);
      return;
    }

    int idIndex = -1;
    if (settings.typeIdProperty != null) {
      final AnalyzedProperty id = entity.getProperty(settings.typeIdProperty);

      if (id == null) {
        LogManager.instance()
            .log(this, Level.INFO, "Property Id '%s.%s' is null. Importing is aborted", null, settings.vertexTypeName,
                settings.typeIdProperty);
        throw new IllegalArgumentException(
            "Property Id '" + settings.vertexTypeName + "." + settings.typeIdProperty + "' is null. Importing is aborted");
      }

      idIndex = id.getIndex();

      // Ensure the typeIdProperty has a unique index for edge resolution
      if (!database.getSchema().getType(settings.vertexTypeName).existsProperty(settings.typeIdProperty))
        database.transaction(
            () -> database.getSchema().getType(settings.vertexTypeName).createProperty(settings.typeIdProperty, Type.STRING));
      if (database.getSchema().getType(settings.vertexTypeName).getIndexesByProperties(settings.typeIdProperty).isEmpty())
        database.transaction(
            () -> database.getSchema().getType(settings.vertexTypeName).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, settings.typeIdProperty));
    }

    final AbstractParser<?> csvParser = createCSVParser(settings);

    LogManager.instance().log(this, Level.INFO, "Started importing vertices from CSV source");

    final long beginTime = System.currentTimeMillis();

    // "skip" MODE SAVES VERTICES SYNCHRONOUSLY INSTEAD OF VIA database.async() - SEE ImporterSettings#isSkipOnRowError()
    // FOR WHY (AN ASYNC BATCH ROLLBACK ON A PERSIST-TIME FAILURE WOULD TAKE DOWN EVERY OTHER VERTEX QUEUED IN THE SAME
    // UNCOMMITTED BATCH, NOT JUST THE FAILING ONE).
    final boolean skipOnError = settings.isSkipOnRowError();

    final AtomicReference<Throwable> firstAsyncError = new AtomicReference<>();
    if (!skipOnError)
      database.async().onError(exception -> {
        LogManager.instance().log(this, Level.SEVERE, "Error on inserting vertices", exception);
        context.errors.incrementAndGet();
        firstAsyncError.compareAndSet(null, exception);
      });

    long skipEntries = settings.verticesSkipEntries != null ? settings.verticesSkipEntries : 0;
    if (settings.verticesSkipEntries == null)
      skipEntries = 1L;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
      csvParser.beginParsing(inputFileReader);

      // BEGUN ONLY AFTER THE SOURCE IS SUCCESSFULLY OPENED: IF beginParsing()/THE READER ITSELF THROWS IOException, NO
      // TRANSACTION IS LEFT DANGLING FOR THE CALLER TO RECONCILE.
      if (skipOnError && !database.isTransactionActive())
        database.begin();

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.vertexPropertiesInclude.isEmpty() && !"*".equalsIgnoreCase(settings.vertexPropertiesInclude)) {
        final String[] includes = settings.vertexPropertiesInclude.split(",");
        final Set<String> propertiesSet = new HashSet<>(Arrays.asList(includes));
        for (final AnalyzedProperty p : entity.getProperties())
          if (propertiesSet.contains(p.getName()))
            properties.add(p);
      } else {
        properties.addAll(entity.getProperties());
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following vertex properties: %s", null, properties);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          continue;

        if (idIndex >= 0 && idIndex >= row.length) {
          LogManager.instance()
              .log(this, Level.INFO, "Property Id is configured on property %d but cannot be found on current record. Skip it",
                  null, idIndex);
          continue;
        }

        try {
          final MutableVertex v = database.newVertex(settings.vertexTypeName);
          if (idIndex >= 0)
            v.set(settings.typeIdProperty, row[idIndex]);
          for (int p = 0; p < properties.size(); ++p) {
            final AnalyzedProperty prop = properties.get(p);
            final String value = row[prop.getIndex()];
            if (value != null && !value.isEmpty())
              v.set(prop.getName(), value);
          }

          if (skipOnError) {
            // EACH VERTEX COMMITS IN ITS OWN TRANSACTION; COUNT ONLY AFTER commit() SUCCEEDS (SEE loadDocuments()).
            v.save();
            database.commit();
            context.createdVertices.incrementAndGet();
            database.begin();
          } else
            database.async().createRecord(v, doc -> context.createdVertices.incrementAndGet());
        } catch (final RuntimeException e) {
          if (!skipOnError)
            throw e;
          if (database.isTransactionActive())
            database.rollback();
          LogManager.instance()
              .log(this, Level.WARNING, "Error on importing vertex at line %d, skipping it (reason: %s)", null, line,
                  e.getMessage());
          LogManager.instance().log(this, Level.FINE, "Full error on importing vertex at line %d", e, line);
          context.errors.incrementAndGet();
          database.begin();
        }
      }

      if (skipOnError)
        database.commit();
      else {
        database.async().waitCompletion();

        // A VERTEX CAN ALSO FAIL AT PERSIST TIME ON THE ASYNC WORKER THREAD (MANDATORY PROPERTY, UNIQUE INDEX, ...), OUTSIDE
        // THE PER-ROW try/catch ABOVE: IN "abort" MODE (THE DEFAULT) THAT MUST STILL FAIL THE IMPORT INSTEAD OF SILENTLY
        // LOGGING AND CONTINUING.
        if (firstAsyncError.get() != null)
          throw new ImportException("Error on inserting vertices", firstAsyncError.get());
      }

    } catch (final IOException e) {
      throw new ImportException("Error on importing CSV", e);
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      LogManager.instance()
          .log(this, Level.INFO, "Importing of vertices from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdVertices.get() / elapsedInSecs : context.createdVertices.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total vertices.: %d", null, context.createdVertices.get());
      LogManager.instance().log(this, Level.INFO, "- Skipped rows...: %d", null, context.errors.get());

      csvParser.stopParsing();
    }
  }

  private void loadEdges(final SourceSchema sourceSchema, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
    final AbstractParser csvParser = createCSVParser(settings);

    final long beginTime = System.currentTimeMillis();

    final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.edgeTypeName);
    if (entity == null) {
      LogManager.instance().log(this, Level.INFO, "Edge type '%s' not defined", null, settings.edgeTypeName);
      return;
    }

    final AnalyzedProperty from = entity.getProperty(settings.edgeFromField);
    if (from == null)
      throw new IllegalArgumentException("Specify -edgeFromField <from-field-name>");

    final AnalyzedProperty to = entity.getProperty(settings.edgeToField);
    if (to == null)
      throw new IllegalArgumentException("Specify -edgeToField <from-field-name>");

    long expectedEdges = settings.expectedEdges;
    if (expectedEdges <= 0)
      expectedEdges = (int) (sourceSchema.getSource().totalSize / entity.getAverageRowLength());

    if (expectedEdges <= 0 || expectedEdges > _32MB)
      // USE CHUNKS OF 16MB EACH
      expectedEdges = _32MB;

    long expectedVertices = settings.expectedVertices;
    if (expectedVertices <= 0)
      expectedVertices = expectedEdges / 2;

    LogManager.instance()
        .log(this, Level.INFO, "Started importing edges from CSV source (expectedVertices=%d expectedEdges=%d)", null,
            expectedVertices, expectedEdges);

    database.async().onError(exception -> LogManager.instance().log(this, Level.SEVERE, "Error on inserting edges", exception));

    long skipEntries = settings.edgesSkipEntries != null ? settings.edgesSkipEntries : 0;
    if (settings.edgesSkipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      skipEntries = 1l;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
      csvParser.beginParsing(inputFileReader);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.edgePropertiesInclude.isEmpty() && !"*".equalsIgnoreCase(settings.edgePropertiesInclude)) {
        final String[] includes = settings.edgePropertiesInclude.split(",");

        final Set<String> propertiesSet = new HashSet<>(Arrays.asList(includes));

        for (final AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else {
        // INCLUDE ALL THE PROPERTIES
        properties.addAll(entity.getProperties());
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following edge properties: %s", null, properties);

      String[] row;
      database.begin();
      int txCount = 0;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (skipEntries > 0 && line < skipEntries)
          continue;

        try {
          createEdgeFromRow(database, row, properties, from, to, context, settings);
          txCount++;
          if (txCount >= settings.commitEvery) {
            database.commit();
            database.begin();
            txCount = 0;
          }
        } catch (final Exception e) {
          // UNLIKE loadDocuments/loadVertices, EDGE ROWS ARE ALWAYS SKIPPED-AND-LOGGED REGARDLESS OF -onRowError: A "BAD"
          // EDGE ROW HERE IS TYPICALLY JUST AN UNRESOLVED from/to VERTEX REFERENCE (ALREADY TRACKED VIA context.skippedEdges
          // IN createEdgeFromRow), WHICH IS EXPECTED DURING GRAPH IMPORTS RATHER THAN A DATA-CORRUPTION CASE.
          LogManager.instance().log(this, Level.SEVERE, "Error on parsing line %d", e, line);
        }
      }
      database.commit();

    } catch (final IOException e) {
      throw new ImportException("Error on importing CSV", e);
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      try {
        Thread.sleep(300);
      } catch (final InterruptedException e) {
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

  public void createEdgeFromRow(final Database database, final String[] row, final List<AnalyzedProperty> properties,
      final AnalyzedProperty from, final AnalyzedProperty to, final ImporterContext context, final ImporterSettings settings) {

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

    // Parse vertex keys based on typeIdType setting (fixes GitHub issue #1552)
    final Object sourceVertexKey = parseVertexKey(fromValue, settings.typeIdType);
    final Object destinationVertexKey = parseVertexKey(toValue, settings.typeIdType);

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

    // Look up source and destination vertices by key across all vertex types that have the typeIdProperty indexed
    final Vertex srcVertex = findVertexByKey(database, settings.typeIdProperty, sourceVertexKey);
    if (srcVertex == null) {
      context.skippedEdges.incrementAndGet();
      return;
    }
    final Vertex dstVertex = findVertexByKey(database, settings.typeIdProperty, destinationVertexKey);
    if (dstVertex == null) {
      context.skippedEdges.incrementAndGet();
      return;
    }
    srcVertex.newEdge(settings.edgeTypeName, dstVertex.getIdentity(), settings.edgeBidirectional, params);
    context.createdEdges.incrementAndGet();
  }

  /**
   * Parses a vertex key string based on the configured type.
   * This supports any ID type (String, Long, Integer, etc.) based on typeIdType setting.
   * Added to fix GitHub issue #1552.
   */
  /**
   * Searches all vertex types for a vertex matching the given key property value.
   * Needed because edges can connect different vertex types.
   */
  private Vertex findVertexByKey(final Database database, final String keyProperty, final Object keyValue) {
    for (final DocumentType type : database.getSchema().getTypes()) {
      if (!(type instanceof VertexType))
        continue;
      if (!type.existsProperty(keyProperty))
        continue;
      if (type.getIndexesByProperties(keyProperty).isEmpty())
        continue;
      final IndexCursor cursor = lookupRecord(database, type.getName(), keyProperty, keyValue);
      if (cursor.hasNext())
        return cursor.next().asVertex();
    }
    return null;
  }

  private Object parseVertexKey(final String value, final String typeIdType) {
    if (value == null)
      return null;

    return switch (typeIdType.toUpperCase(Locale.ENGLISH)) {
      case "LONG" -> Long.parseLong(value);
      case "INTEGER", "INT" -> Integer.parseInt(value);
      case "SHORT" -> Short.parseShort(value);
      case "DOUBLE" -> Double.parseDouble(value);
      case "FLOAT" -> Float.parseFloat(value);
      default -> value; // String is the default
    };
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.EntityType entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) throws IOException {
    parser.reset();

    final String delimiter = settings.getValue("delimiter", ",");

    final CsvParserSettings csvParserSettings;
    final TsvParserSettings tsvParserSettings;
    final AbstractParser csvParser;
    final CommonParserSettings parserSettings;

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      parserSettings = tsvParserSettings = new TsvParserSettings();
    } else {
      parserSettings = csvParserSettings = new CsvParserSettings();
      csvParserSettings.setDelimiterDetectionEnabled(false);
      if (delimiter != null) {
        csvParserSettings.detectFormatAutomatically(delimiter.charAt(0));
        csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
      }
    }

    parserSettings.setReadInputOnSeparateThread(false);

    final int maxProperties = settings.getIntValue("maxProperties", 0);
    if (maxProperties > 0)
      parserSettings.setMaxColumns(maxProperties);

    final int maxPropertySize = settings.getIntValue("maxPropertySize", 0);
    if (maxPropertySize != 0) {
      parserSettings.setAutoConfigurationEnabled(false);
      parserSettings.setMaxCharsPerColumn(maxPropertySize);
    }

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      csvParser = new TsvParser((TsvParserSettings) parserSettings);
    } else {
      csvParser = new CsvParser((CsvParserSettings) parserSettings);
    }

    final List<String> fieldNames = new ArrayList<>();

    final String entityName = entityType == AnalyzedEntity.EntityType.VERTEX ?
        settings.vertexTypeName :
        entityType == AnalyzedEntity.EntityType.EDGE ? settings.edgeTypeName : settings.documentTypeName;

    long skipEntries = 0;
    final String header;

    switch (entityType) {
    case VERTEX:
      header = settings.verticesHeader;
      skipEntries = settings.verticesSkipEntries != null ? settings.verticesSkipEntries : 0;
      if (settings.verticesSkipEntries == null)
        // BY DEFAULT SKIP THE FIRST LINE AS HEADER
        skipEntries = 1l;
      break;

    case EDGE:
      header = settings.edgesHeader;
      skipEntries = settings.edgesSkipEntries != null ? settings.edgesSkipEntries : 0;
      if (settings.edgesSkipEntries == null)
        // BY DEFAULT SKIP THE FIRST LINE AS HEADER
        skipEntries = 1l;
      break;

    case DOCUMENT:
      header = settings.documentsHeader;
      skipEntries = settings.documentsSkipEntries != null ? settings.documentsSkipEntries : 0;
      if (settings.documentsSkipEntries == null)
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
        fieldNames.addAll(Arrays.asList(headerColumns));
      }
      LogManager.instance().log(this, Level.INFO, "Parsing with custom header: %s", null, fieldNames);
    }

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream(),
        DatabaseFactory.getDefaultCharset())) {
      csvParser.beginParsing(inputFileReader);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        if (skipEntries > 0 && line < skipEntries && !fieldNames.isEmpty())
          continue;

        if (settings.analysisLimitBytes > 0 && csvParser.getContext().currentChar() > settings.analysisLimitBytes)
          break;

        if (settings.analysisLimitEntries > 0 && line > settings.analysisLimitEntries)
          break;

        if (line == 0 && header == null) {
          // READ THE HEADER FROM FILE
          fieldNames.addAll(Arrays.asList(row));
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

    } catch (final EOFException e) {
      // REACHED THE LIMIT
    } catch (final IOException e) {
      throw new ImportException("Error on importing CSV", e);
    }

    // END OF PARSING. THIS DETERMINES THE TYPE
    analyzedSchema.endParsing();

    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public String getFormat() {
    return "CSV";
  }

  protected AbstractParser createCSVParser(final ImporterSettings settings) {
    String delimiter = ",";
    if (settings.options.containsKey("delimiter"))
      delimiter = settings.getValue("delimiter", ",");

    if ("\t".equals(delimiter) || "\\t".equals(delimiter)) {
      final TsvParserSettings tsvParserSettings = new TsvParserSettings();
      tsvParserSettings.setMaxColumns(settings.getIntValue("maxProperties", tsvParserSettings.getMaxColumns()));
      tsvParserSettings.setMaxCharsPerColumn(settings.getIntValue("maxPropertySize", tsvParserSettings.getMaxCharsPerColumn()));
      return new TsvParser(tsvParserSettings);
    } else {
      final CsvParserSettings csvParserSettings = new CsvParserSettings();
      csvParserSettings.getFormat().setDelimiter(delimiter);
      csvParserSettings.setMaxColumns(settings.getIntValue("maxProperties", csvParserSettings.getMaxColumns()));
      csvParserSettings.setMaxCharsPerColumn(settings.getIntValue("maxPropertySize", csvParserSettings.getMaxCharsPerColumn()));
      return new CsvParser(csvParserSettings);
    }

  }
}
