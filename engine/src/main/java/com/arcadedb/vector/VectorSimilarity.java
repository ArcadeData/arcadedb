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
import com.arcadedb.vector.universe.SortedVectorUniverse;
import com.arcadedb.vector.universe.VectorUniverse;

import java.util.*;

/**
 * Main entrypoint to parse a vector file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorSimilarity {
  private static final int                     MAX_SIMILAR = 20;
  private              VectorUniverse<?>       universe;
  private              VectorDistanceAlgorithm algorithm   = new CosineDistance();
  private              int                     max         = MAX_SIMILAR;
  private              float                   minDistance = Float.MIN_VALUE;

  public List<Pair<Comparable, Float>> calculateTopSimilar(final IndexableVector element) {
    return calculateTopSimilar(element, universe.getEntries());
  }

  public List<Pair<Comparable, Float>> calculateTopSimilar(final IndexableVector element, final Collection<? extends IndexableVector> candidates) {
    final LinkedList<Pair<Comparable, Float>> similar = new LinkedList<>();

    for (IndexableVector currentElement : candidates) {
      if (element.getSubject().equals(currentElement.getSubject()))
        continue;

      final float distance = algorithm.calculate(element, currentElement);
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

  public List<Pair<Comparable, Float>> calculateTopSimilarUsingIndex(final SortedVectorUniverse index, final IndexableVector element, final float maxDistance,
      final int maxAdjacentElements) {
    final Set<Integer> candidatePositions = new HashSet<>();

    final float[] elementArray = element.getVector();
    final int dimensions = universe.dimensions();
    final int entries = universe.size();

    for (int dimension = 0; dimension < dimensions; dimension++) {
      final SortedVectorUniverse.SortedDimension sortedDimension = index.getSortedDimensions(dimension);
      final float valueToFind = elementArray[dimension];
      int pos = sortedDimension.searchFirstValue(valueToFind);
      if (pos < 0)
        // NOT FOUND, CONVERT INSERTION POINT IN THE CLOSEST POINT
        pos = (pos + 1) * -1;

      boolean searchRight = true;
      boolean searchLeft = true;
      int searchRightIndex = pos;
      int searchLeftIndex = pos;
      int candidateFound = 0;

      while (candidateFound < maxAdjacentElements) {
        if (collectInResult(maxDistance, candidatePositions, sortedDimension, valueToFind, pos))
          ++candidateFound;

        if (candidateFound < maxAdjacentElements) {
          if (searchRight && searchRightIndex < entries) {
            if (!collectInResult(maxDistance, candidatePositions, sortedDimension, valueToFind, ++searchRightIndex))
              searchRight = false;
            else
              ++candidateFound;
          }

          if (searchLeft && searchLeftIndex > 0) {
            if (!collectInResult(maxDistance, candidatePositions, sortedDimension, valueToFind, --searchLeftIndex))
              searchLeft = false;
            else
              ++candidateFound;
          }
        }
      }
    }

    final List<IndexableVector<?>> candidateElements = new ArrayList<>(candidatePositions.size());
    for (Integer pos : candidatePositions)
      candidateElements.add(universe.get(pos));

    return calculateTopSimilar(element, candidateElements);
  }

  private boolean collectInResult(final float maxDistance, final Set<Integer> candidates, final SortedVectorUniverse.SortedDimension sortedDimension,
      final float valueToFind, final int pos) {
    if (pos >= sortedDimension.size())
      return false;

    final float value = sortedDimension.getValue(pos);
    final float distance = Math.abs(value - valueToFind);
    if (distance <= maxDistance) {
      candidates.add(pos);
      return true;
    }
    return false;
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

  public VectorUniverse<?> getUniverse() {
    return universe;
  }

  public VectorSimilarity setUniverse(final VectorUniverse<?> universe) {
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
