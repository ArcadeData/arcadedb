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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.lsm.LSMVectorIndex;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.schema.LSMVectorIndexBuilder;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.DateUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Imports Embeddings in arbitrary dimensions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TextEmbeddingsImporter {
  private final    InputStream      inputStream;
  private final    ImporterSettings settings;
  private final    ConsoleLogger    logger;
  private          int              maxConnections;
  private          int              beamWidth;
  private          float            alpha;
  private          boolean          normalizeVectors    = false;
  private          String           databasePath;
  private          boolean          overwriteDatabase   = false;
  private          long             errors              = 0L;
  private          long             warnings            = 0L;
  private          DatabaseFactory  factory;
  private          Database         database;
  private          long             beginTime;
  private          boolean          error               = false;
  private          ImporterContext  context             = new ImporterContext();
  private          String           vectorTypeName;
  private          String           distanceFunctionName;
  private          String           vectorPropertyName;
  private          String           idPropertyName      = "name";
  private          String           deletedPropertyName = "deleted";
  private volatile long             embeddingsParsed    = 0L;
  private volatile long             indexedEmbedding    = 0L;
  private volatile long             verticesCreated     = 0L;
  private volatile long             verticesConnected   = 0L;

  public TextEmbeddingsImporter(final DatabaseInternal database, final InputStream inputStream, final ImporterSettings settings)
      throws ClassNotFoundException {
    this.settings = settings;
    this.database = database;
    this.databasePath = database.getDatabasePath();
    this.inputStream = inputStream;
    this.logger = new ConsoleLogger(settings.verboseLevel);

    distanceFunctionName = settings.getValue("distanceFunction", "InnerProduct");
    distanceFunctionName =
        Character.toUpperCase(distanceFunctionName.charAt(0)) + distanceFunctionName.substring(1).toLowerCase(Locale.ENGLISH);

    vectorTypeName = settings.getValue("vectorType", "Float");
    // USE CAMEL CASE FOR THE VECTOR TYPE
    vectorTypeName = Character.toUpperCase(vectorTypeName.charAt(0)) + vectorTypeName.substring(1).toLowerCase(Locale.ENGLISH);

    if (settings.options.containsKey("vectorProperty"))
      this.vectorPropertyName = settings.getValue("vectorProperty", null);

    if (settings.options.containsKey("idProperty"))
      this.idPropertyName = settings.getValue("idProperty", null);

    if (settings.options.containsKey("deletedProperty"))
      this.deletedPropertyName = settings.getValue("deletedProperty", null);

    // LSM Vector Index parameters with backward compatibility for old HNSW names
    // maxConnections: LSM name, m: old HNSW name (default: 16)
    this.maxConnections = settings.getIntValue("maxConnections",
        settings.getIntValue("m", 16));

    // beamWidth: LSM name (no direct HNSW equivalent, use max of ef/efConstruction or default 100)
    // Take the maximum of ef and efConstruction for better beam width
    int ef = settings.getIntValue("ef", 100);
    int efConstruction = settings.getIntValue("efConstruction", 100);
    int defaultBeamWidth = Math.max(ef, efConstruction);
    this.beamWidth = settings.getIntValue("beamWidth", defaultBeamWidth);

    // alpha: Diversity parameter (LSM specific, default: 1.2)
    // Parse as int and convert to float (e.g., 120 -> 1.2)
    this.alpha = settings.getIntValue("alpha", 120) / 100f;

    if (settings.options.containsKey("normalizeVectors"))
      this.normalizeVectors = Boolean.parseBoolean(settings.getValue("normalizeVectors", null));
  }

  public Database run() throws IOException, ClassNotFoundException, InterruptedException {
    if (!createDatabase())
      return null;

    beginTime = System.currentTimeMillis();

    // Load all embeddings from file
    final List<TextFloatsEmbedding> allTexts = loadFromFile();

    if (allTexts.isEmpty()) {
      logger.logLine(1, "No embeddings found in input file");
      return database;
    }

    if (settings.documentsSkipEntries != null && settings.documentsSkipEntries > 0) {
      for (int i = 0; i < settings.documentsSkipEntries && !allTexts.isEmpty(); i++)
        allTexts.removeFirst();
    }

    if (allTexts.isEmpty()) {
      logger.logLine(1, "No embeddings left after skipping entries");
      return database;
    }

    final int dimensions = allTexts.get(0).dimensions();

    logger.logLine(2, "- Parsed %,d embeddings with %,d dimensions", allTexts.size(), dimensions);

    // Create vertex type within a transaction if it doesn't exist
    LocalSchema schema = database.getSchema().getEmbedded();
    if (!schema.existsType(settings.vertexTypeName)) {
      database.transaction(() -> {
        final LocalSchema txSchema = database.getSchema().getEmbedded();
        if (!txSchema.existsType(settings.vertexTypeName)) {
          txSchema.createVertexType(settings.vertexTypeName);
        }
      });
      // Get fresh schema reference after transaction completes
      schema = database.getSchema().getEmbedded();
    }

    // Verify vector property name is not null, use default if needed
    final String vectorProp = vectorPropertyName != null ? vectorPropertyName : "vector";

    // Create LSMVectorIndex using builder
    final LSMVectorIndexBuilder builder = schema.buildLSMVectorIndex(settings.vertexTypeName, vectorProp);
    builder.withDimensions(dimensions)
        .withSimilarity(distanceFunctionName)
        .withMaxConnections(maxConnections)
        .withBeamWidth(beamWidth)
        .withAlpha(alpha);

    final LSMVectorIndex lsmVectorIndex = builder.create();

    logger.logLine(2, "- Created LSMVectorIndex %s  with %,d dimensions", lsmVectorIndex.getName(), dimensions);

    // Process embeddings and create vertices
    long embeddingsProcessed = 0;
    database.begin();
    for (final TextFloatsEmbedding embedding : allTexts) {
      // Create vertex with properties and save it
      MutableVertex vertex = database.newVertex(settings.vertexTypeName)
          .set(idPropertyName, embedding.id())
          .set(vectorProp, embedding.vector());

      if (deletedPropertyName != null && !deletedPropertyName.isEmpty()) {
        vertex = vertex.set(deletedPropertyName, false);
      }

      vertex = vertex.save();
      ++verticesCreated;

      // Index the vector
      float[] vector = embedding.vector();
      if (normalizeVectors) {
        vector = VectorUtils.normalize(vector);
      }

      lsmVectorIndex.put(new Object[] { vector }, new RID[] { vertex.getIdentity() });
      ++indexedEmbedding;

      ++embeddingsProcessed;
      if (embeddingsProcessed % 1000 == 0) {
        database.commit();
        database.begin();
        logger.logLine(2, "- Processed %,d embeddings", embeddingsProcessed);
      }
    }

    lsmVectorIndex.compact();
    logger.logLine(1, "***************************************************************************************************");
    logger.logLine(1, "Import of Text Embeddings database completed in %s with %,d errors and %,d warnings.",
        DateUtils.formatElapsed((System.currentTimeMillis() - beginTime)), errors, warnings);
    logger.logLine(1, "\nSUMMARY\n");
    logger.logLine(1, "- Embeddings.................................: %,d", allTexts.size());
    logger.logLine(1, "- Vertices Created...........................: %,d", verticesCreated);
    logger.logLine(1, "- Embeddings Indexed..........................: %,d", indexedEmbedding);
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
    if (verticesConnected > 0)
      progressPerc = 40F + (verticesConnected * 60F / embeddingsParsed); // 60% OF THE TOTAL PROCESS
    else if (verticesCreated > 0)
      progressPerc = 10F + (verticesCreated * 30F / embeddingsParsed); // 30% OF THE TOTAL PROCESS
    else if (indexedEmbedding > 0)
      progressPerc = indexedEmbedding * 10F / embeddingsParsed; // 10% OF THE TOTAL PROCESS

    String result = "- %.2f%%".formatted(progressPerc);

    if (embeddingsParsed > 0)
      result += " - %,d embeddings parsed".formatted(embeddingsParsed);
    if (indexedEmbedding > 0)
      result += " - %,d embeddings indexed".formatted(indexedEmbedding);
    if (verticesCreated > 0)
      result += " - %,d vertices created".formatted(verticesCreated);
    if (verticesConnected > 0)
      result += " - %,d vertices connected".formatted(verticesConnected);

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

  public TextEmbeddingsImporter setContext(final ImporterContext context) {
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

        String word = tokens.getFirst();

        float[] vector = new float[tokens.size() - 1];
        for (int i = 1; i < tokens.size() - 1; i++)
          vector[i] = Float.parseFloat(tokens.get(i));

        vectorSize.set(vector.length);

        if (normalizeVectors)
          // FOR INNER PRODUCT SEARCH NORMALIZE VECTORS
          vector = VectorUtils.normalize(vector);

        return new TextFloatsEmbedding(word, vector);
      }).collect(Collectors.toList());
    }
  }
}
