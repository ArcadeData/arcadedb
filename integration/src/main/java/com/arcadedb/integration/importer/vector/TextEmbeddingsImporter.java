/*
 * Copyright 2023 Arcade Data Ltd
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
package com.arcadedb.integration.importer.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.vector.HnswVectorIndexRAM;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.index.vector.distance.DistanceFunctionFactory;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.DateUtils;
import com.github.jelmerk.knn.DistanceFunction;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

/**
 * Imports Embeddings in arbitrary dimensions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TextEmbeddingsImporter {
  private final    InputStream      inputStream;
  private final    ImporterSettings settings;
  private final    ConsoleLogger    logger;
  private          int              m;
  private          int              ef;
  private          int              efConstruction;
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

    this.m = settings.getIntValue("m", 16);
    this.ef = settings.getIntValue("ef", 256);
    this.efConstruction = settings.getIntValue("efConstruction", 256);

    if (settings.options.containsKey("normalizeVectors"))
      this.normalizeVectors = Boolean.parseBoolean(settings.getValue("normalizeVectors", null));
  }

  public Database run() throws IOException, ClassNotFoundException, InterruptedException {
    if (!createDatabase())
      return null;

    final DistanceFunction distanceFunction = DistanceFunctionFactory.getImplementationByName(
        vectorTypeName + distanceFunctionName);

    beginTime = System.currentTimeMillis();

    final List<TextFloatsEmbedding> texts = loadFromFile();

    if (settings.documentsSkipEntries != null) {
      for (int i = 0; i < settings.documentsSkipEntries; i++)
        texts.remove(0);
    }

    if (!texts.isEmpty()) {
      final int dimensions = texts.get(1).dimensions();

      logger.logLine(2, "- Parsed %,d embeddings with %,d dimensions in RAM", texts.size(), dimensions);

      final HnswVectorIndexRAM<String, float[], TextFloatsEmbedding, Float> hnswIndex = HnswVectorIndexRAM.newBuilder(dimensions,
          distanceFunction,
          texts.size()).withM(m).withEf(ef).withEfConstruction(efConstruction).build();

      hnswIndex.addAll(texts, Runtime.getRuntime().availableProcessors(), (workDone, max) -> ++indexedEmbedding, 1);

      Type vectorPropertyType = switch (vectorTypeName) {
        case "Short" -> Type.ARRAY_OF_SHORTS;
        case "Integer" -> Type.ARRAY_OF_INTEGERS;
        case "Long" -> Type.ARRAY_OF_LONGS;
        case "Float" -> Type.ARRAY_OF_FLOATS;
        case "Double" -> Type.ARRAY_OF_DOUBLES;
        default -> throw new IllegalArgumentException("Type '" + vectorTypeName + "' not supported");
      };

      hnswIndex.createPersistentIndex(database)//
          .withVertexType(settings.vertexTypeName).withEdgeType(settings.edgeTypeName)
          .withVectorProperty(vectorPropertyName, vectorPropertyType)
          .withIdProperty(idPropertyName)//
          .withDeletedProperty(deletedPropertyName)//
          .withVertexCreationCallback((record, item, total) -> ++verticesCreated)//
          .withCallback((record, total) -> ++verticesConnected)//
          .withBatchSize(1000).create();
    }

    logger.logLine(1, "***************************************************************************************************");
    logger.logLine(1, "Import of Text Embeddings database completed in %s with %,d errors and %,d warnings.",
        DateUtils.formatElapsed((System.currentTimeMillis() - beginTime)), errors, warnings);
    logger.logLine(1, "\nSUMMARY\n");
    logger.logLine(1, "- Embeddings.................................: %,d", texts.size());
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
      final Stream<String> parser = reader.lines();

      if (settings.parsingLimitEntries > 0)
        parser.limit(settings.parsingLimitEntries);

      final AtomicInteger vectorSize = new AtomicInteger(301);

      return parser.map(line -> {
        ++embeddingsParsed;

        final List<String> tokens = CodeUtils.split(line, ' ', -1, vectorSize.get());

        String word = tokens.get(0);

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
