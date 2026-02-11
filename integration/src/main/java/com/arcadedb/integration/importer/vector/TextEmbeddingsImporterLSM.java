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
package com.arcadedb.integration.importer.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.DateUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

/**
 * Imports Embeddings using LSMVector index instead of HNSW.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TextEmbeddingsImporterLSM {
  private final    InputStream      inputStream;
  private final    ImporterSettings settings;
  private final    ConsoleLogger    logger;
  private          boolean          normalizeVectors  = false;
  private          String           databasePath;
  private          boolean          overwriteDatabase = false;
  private          long             errors            = 0L;
  private          long             warnings          = 0L;
  private          DatabaseFactory  factory;
  private          Database         database;
  private          long             beginTime;
  private          boolean          error             = false;
  private          ImporterContext  context           = new ImporterContext();
  private          String           vectorTypeName;
  private          String           similarityFunctionName;
  private          String           vectorPropertyName;
  private          String           idPropertyName    = "name";
  private          int              maxConnections    = 16;
  private          int              beamWidth         = 100;
  private volatile long             embeddingsParsed  = 0L;
  private volatile long             verticesCreated   = 0L;

  public TextEmbeddingsImporterLSM(final DatabaseInternal database, final InputStream inputStream,
      final ImporterSettings settings) {
    this.settings = settings;
    this.database = database;
    this.databasePath = database.getDatabasePath();
    this.inputStream = inputStream;
    this.logger = new ConsoleLogger(settings.verboseLevel);

    similarityFunctionName = settings.getValue("distanceFunction", "Cosine");
    similarityFunctionName =
        Character.toUpperCase(similarityFunctionName.charAt(0)) + similarityFunctionName.substring(1).toLowerCase(Locale.ENGLISH);

    vectorTypeName = settings.getValue("vectorType", "Float");
    // USE CAMEL CASE FOR THE VECTOR TYPE
    vectorTypeName = Character.toUpperCase(vectorTypeName.charAt(0)) + vectorTypeName.substring(1).toLowerCase(Locale.ENGLISH);

    if (settings.options.containsKey("vectorProperty"))
      this.vectorPropertyName = settings.getValue("vectorProperty", null);

    if (settings.options.containsKey("idProperty"))
      this.idPropertyName = settings.getValue("idProperty", null);

    this.maxConnections = settings.getIntValue("m", 16);
    this.beamWidth = settings.getIntValue("beamWidth", 100);

    if (settings.options.containsKey("normalizeVectors"))
      this.normalizeVectors = Boolean.parseBoolean(settings.getValue("normalizeVectors", null));
  }

  public Database run() throws IOException, ClassNotFoundException, InterruptedException {
    if (!createDatabase())
      return null;

    beginTime = System.currentTimeMillis();

    final List<TextFloatsEmbedding> texts = loadFromFile();

    if (settings.documentsSkipEntries != null) {
      for (int i = 0; i < settings.documentsSkipEntries; i++)
        texts.remove(0);
    }

    if (!texts.isEmpty()) {
      final int dimensions = texts.get(1).dimensions();

      logger.logLine(2, "- Parsed %,d embeddings with %,d dimensions in RAM", texts.size(), dimensions);

      // Normalize similarity function name for SQL
      final String normalizedSimilarity = switch (similarityFunctionName) {
        case "Cosine" -> "COSINE";
        case "Euclidean" -> "EUCLIDEAN";
        case "Dot_product", "Dotproduct" -> "DOT_PRODUCT";
        default -> similarityFunctionName.toUpperCase(Locale.ENGLISH);
      };

      // Create vertex type and LSM_VECTOR index
      // Check if type exists first
      if (!database.getSchema().existsType(settings.vertexTypeName)) {
        database.command("sql", "CREATE VERTEX TYPE " + settings.vertexTypeName);
      }

      // Create properties
      final var type = database.getSchema().getType(settings.vertexTypeName);
      if (!type.existsProperty(idPropertyName)) {
        database.command("sql", "CREATE PROPERTY " + settings.vertexTypeName + "." + idPropertyName + " STRING");
      }
      if (!type.existsProperty(vectorPropertyName)) {
        database.command("sql", "CREATE PROPERTY " + settings.vertexTypeName + "." + vectorPropertyName + " ARRAY_OF_FLOATS");
      }

      // Create LSM_VECTOR index
      final String indexName = settings.vertexTypeName + "[" + vectorPropertyName + "]";
      try {
        database.getSchema().getIndexByName(indexName);
        // Index already exists
      } catch (final Exception e) {
        // Index doesn't exist, create it
        database.command("sql", "CREATE INDEX ON " + settings.vertexTypeName + " (" + vectorPropertyName + ") LSM_VECTOR " +
            "METADATA {dimensions: " + dimensions + ", similarity: '" + normalizedSimilarity + "', " +
            "maxConnections: " + maxConnections + ", beamWidth: " + beamWidth + ", idPropertyName: '" + idPropertyName + "'}");
      }

      logger.logLine(2, "- Created schema and LSM_VECTOR index");

      // Insert all embeddings
      final int batchSize = 1000;
      for (int i = 0; i < texts.size(); i += batchSize) {
        final int start = i;
        final int end = Math.min(i + batchSize, texts.size());

        database.transaction(() -> {
          for (int j = start; j < end; j++) {
            final TextFloatsEmbedding embedding = texts.get(j);
            final MutableDocument vertex = database.newVertex(settings.vertexTypeName);
            vertex.set(idPropertyName, embedding.id());
            vertex.set(vectorPropertyName, embedding.vector());
            vertex.save();
            ++verticesCreated;
          }
        });

        if ((i / batchSize) % 10 == 0) {
          logger.logLine(2, "- Inserted %,d / %,d vertices", verticesCreated, texts.size());
        }
      }

      logger.logLine(2, "- Inserted all %,d vertices with vector index", verticesCreated);
    }

    logger.logLine(1, "***************************************************************************************************");
    logger.logLine(1, "Import of Text Embeddings database completed in %s with %,d errors and %,d warnings.",
        DateUtils.formatElapsed((System.currentTimeMillis() - beginTime)), errors, warnings);
    logger.logLine(1, "\nSUMMARY\n");
    logger.logLine(1, "- Embeddings.................................: %,d", texts.size());
    logger.logLine(1, "- Vertices created...........................: %,d", verticesCreated);
    logger.logLine(1, "***************************************************************************************************");
    logger.logLine(1, "");

    if (database != null) {
      logger.logLine(1, "NOTES:");
      logger.logLine(1, "- you can find your new ArcadeDB database in '" + database.getDatabasePath() + "'");
    }

    return database;
  }

  public void printProgress() {
    float progressPerc = 0F;
    if (verticesCreated > 0 && embeddingsParsed > 0)
      progressPerc = (verticesCreated * 100F / embeddingsParsed);

    String result = "- %.2f%%".formatted(progressPerc);

    if (embeddingsParsed > 0)
      result += " - %,d embeddings parsed".formatted(embeddingsParsed);
    if (verticesCreated > 0)
      result += " - %,d vertices created".formatted(verticesCreated);

    result += " (elapsed " + DateUtils.formatElapsed(System.currentTimeMillis() - beginTime) + ")";

    logger.logLine(2, result);
  }

  private boolean createDatabase() {
    if (database == null) {
      factory = new DatabaseFactory(databasePath);
      if (factory.exists()) {
        if (!overwriteDatabase) {
          logger.errorLine("Database already exists on path '%s'", databasePath);
          ++errors;
          return false;
        } else {
          database = factory.open();
          logger.errorLine("Found existent database at '%s', dropping it and recreate a new one", databasePath);
          database.drop();
        }
      }

      // CREATE THE DATABASE
      database = factory.create();
    }
    return true;
  }

  public boolean isError() {
    return error;
  }

  public ImporterContext getContext() {
    return context;
  }

  public TextEmbeddingsImporterLSM setContext(final ImporterContext context) {
    this.context = context;
    return this;
  }

  private List<TextFloatsEmbedding> loadFromFile() throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      Stream<String> parser = reader.lines();

      if (settings.parsingLimitEntries > 0)
        parser = parser.limit(settings.parsingLimitEntries);

      final AtomicInteger vectorSize = new AtomicInteger(301);

      return parser.map(line -> {
        ++embeddingsParsed;

        final List<String> tokens = CodeUtils.split(line, ' ', -1, vectorSize.get());

        String word = tokens.get(0);

        float[] vector = new float[tokens.size() - 1];
        for (int i = 1; i < tokens.size(); i++)
          vector[i - 1] = Float.parseFloat(tokens.get(i));

        vectorSize.set(vector.length);

        if (normalizeVectors)
          // FOR INNER PRODUCT SEARCH NORMALIZE VECTORS
          vector = VectorUtils.normalize(vector);

        return new TextFloatsEmbedding(word, vector);
      }).collect(Collectors.toList());
    }
  }
}
