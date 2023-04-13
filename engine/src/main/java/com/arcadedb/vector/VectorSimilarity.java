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

import com.arcadedb.utility.Pair;
import com.arcadedb.vector.algorithm.CosineDistance;
import com.arcadedb.vector.algorithm.VectorDistanceAlgorithm;

import java.util.*;

/**
 * Main entrypoint to parse a vector file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorSimilarity {
  private static final int                             MAX_SIMILAR = 20;
  private              List<? extends IndexableVector> universe;
  private              VectorDistanceAlgorithm         algorithm   = new CosineDistance();
  private              int                             max         = MAX_SIMILAR;
  private              float                           minDistance = Float.MIN_VALUE;

  public List<Pair<Comparable, Float>> calculateTopSimilar(final IndexableVector element) {
    final LinkedList<Pair<Comparable, Float>> similar = new LinkedList<>();

    final int totalElements = universe.size();
    for (int destIndex = 0; destIndex < totalElements; ++destIndex) {
      final IndexableVector currentElement = universe.get(destIndex);
      if (element.getSubject().equals(currentElement.getSubject()))
        continue;

      final float distance = algorithm.calculate(element.getVector(), currentElement.getVector());
      if (minDistance > Float.MIN_VALUE && distance < minDistance)
        // TOO DISTANT: SKIP IT
        continue;

      final int totalSimilar = similar.size();
      if (totalSimilar > 0) {
        final Float minSimilarDistance = similar.getLast().getSecond();
        if (distance <= minSimilarDistance)
          continue;

        final Float maxSimilarDistance = similar.getFirst().getSecond();
        if (distance >= maxSimilarDistance)
          // APPEND AT THE BEGINNING
          similar.addFirst(new Pair<>(currentElement.getSubject(), distance));
        else {
          // INSERT IN THE RIGHT ORDER
          for (int i = 0; i < totalSimilar; ++i) {
            float currDistance = similar.get(i).getSecond();
            if (distance >= currDistance) {
              similar.add(i, new Pair<>(currentElement.getSubject(), distance));
              break;
            }
          }
        }

        if (totalSimilar + 1 > max)
          // REMOVE THE MINIMUM DISTANCE (LAST ELEMENT)
          similar.removeLast();

      } else
        // FIRST ELEMENT
        similar.add(new Pair<>(currentElement.getSubject(), distance));
    }
    return similar;
  }

  public int getMax() {
    return max;
  }

  public VectorSimilarity setMax(final int max) {
    this.max = max;
    return this;
  }

  public VectorDistanceAlgorithm getAlgorithm() {
    return algorithm;
  }

  public VectorSimilarity setAlgorithm(final VectorDistanceAlgorithm algorithm) {
    this.algorithm = algorithm;
    return this;
  }

  public List<? extends IndexableVector> getUniverse() {
    return universe;
  }

  public VectorSimilarity setUniverse(final List<? extends IndexableVector> universe) {
    this.universe = universe;
    return this;
  }

  public float getMinDistance() {
    return minDistance;
  }

  public VectorSimilarity setMinDistance(final float minDistance) {
    this.minDistance = minDistance;
    return this;
  }
}
