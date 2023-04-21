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

package com.arcadedb.vector;

import com.arcadedb.database.Database;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.Pair;
import com.arcadedb.vector.algorithm.CosineDistance;
import com.arcadedb.vector.parser.VectorParser;
import com.arcadedb.vector.parser.VectorParserCallback;
import com.arcadedb.vector.universe.VectorUniverse;

import java.io.*;
import java.util.*;
import java.util.logging.*;

/**
 * Main entrypoint to parse a vector file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorParserMain {
  private static final int PRINT_STATUS_EVERY = 100_000;
  private static final int MAX_WORDS          = 0;//200_000; // SET 0 FOR NO LIMITS

  public static void main(final String[] args) throws IOException {
    final VectorUniverse<String> universe = readWord2Vec(args[0]);

    //final SortedVectorUniverse<?> sortedUniverse = new SortedVectorUniverse<>(universe).readFromFile(new File("sortedDimensions.bin.gz"));

    final Map<Float, Integer> valueDistribution = universe.getValueDistribution();
    LogManager.instance().log(null, Level.INFO, "Value Distribution (%d):", valueDistribution.size());
    int i = 0;
    for (Map.Entry<Float, Integer> entry : valueDistribution.entrySet())
      LogManager.instance().log(null, Level.INFO, "- %d -> %f: %d", i++, entry.getKey(), entry.getValue());

    // QUANTIZE BASED ON THE RANGE FOUND
//    final float[] boundaries = universe.calculateBoundariesOfValues();
//    for (int i = 0; i < universe.size(); i++) {
//      final IndexableVector<String> w = universe.get(i);
//      w.quantize(boundaries[0], boundaries[1]);
//    }

//    final SortedVectorUniverse<?> sortedUniverse = new SortedVectorUniverse<>(universe).computeAndWriteToFile(new File("sortedDimensions.bin.gz"));

    final VectorSimilarity similarity = new VectorSimilarity().setUniverse(universe).setMax(20).setAlgorithm(new CosineDistance()).setMinDistance(0.5F);

//    try (DatabaseFactory factory = new DatabaseFactory("vector")) {
//      if (factory.exists())
//        factory.open().drop();
//
//      try (final Database database = factory.create()) {
//        database.getSchema().createVertexType("Word").createProperty("name", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
//        database.getSchema().createEdgeType("SimilarTo").createProperty("distance", Type.FLOAT);

    for (int sourceIndex = 0; sourceIndex < universe.size(); ++sourceIndex) {
      final IndexableVector<String> sourceWord = universe.get(sourceIndex);
      //final List<Pair<Comparable, Float>> similar = similarity.calculateTopSimilarUsingIndex(sortedUniverse, sourceWord, 0.3F, 200);
      final List<Pair<Comparable, Float>> similar = similarity.calculateTopSimilar(sourceWord, universe.getEntries());

      LogManager.instance().log(null, Level.INFO, "%d/%d: %s-> %s", sourceIndex, universe.size(), sourceWord.getSubject(), similar);

      //writeToGraph(database, sourceWord, similar);
    }
//      }
//    }
  }

  private static void writeToGraph(Database database, IndexableVector<String> sourceWord, List<Pair<Comparable, Float>> similar) {
    database.transaction(() -> {
      final Vertex sourceWordVertex = getOrCreateWord(database, sourceWord.subject);

      for (int i = 0; i < similar.size(); i++) {
        final Pair<Comparable, Float> entry = similar.get(i);

        final Vertex destWordVertex = getOrCreateWord(database, (String) entry.getFirst());
        sourceWordVertex.newEdge("SimilarTo", destWordVertex, true, "distance", entry.getSecond()).save();
      }
    });
  }

  private static VectorUniverse<String> readWord2Vec(final String fileName) throws IOException {
    return new VectorParser().loadFromBinaryFile(new File(fileName), false, new VectorParserCallback() {
      @Override
      public boolean onWord(final int parsedWordCounter, final int totalWords, final String word, final float[] vector) {
        if (parsedWordCounter % PRINT_STATUS_EVERY == 0)
          LogManager.instance().log(this, Level.INFO, "%d/%d - Parsed word %s", parsedWordCounter, totalWords, word);

        if (MAX_WORDS > 0 && parsedWordCounter > MAX_WORDS)
          return false;
        return true;
      }
    });
  }

  private static Vertex getOrCreateWord(final Database database, final String word) {
    final Vertex wordVertex;
    final IndexCursor cursor = database.lookupByKey("Word", "name", word);
    if (cursor.hasNext())
      wordVertex = cursor.next().asVertex();
    else
      wordVertex = database.newVertex("Word").set("name", word).save();
    return wordVertex;
  }
}
