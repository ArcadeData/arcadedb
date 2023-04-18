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

package com.arcadedb.vector.algorithm;

import com.arcadedb.vector.IndexableVector;

/**
 * Uses the Euclidean algorithm to compute the distance between two vectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EuclideanDistance implements VectorDistanceAlgorithm {
  public float calculate(final IndexableVector a, final IndexableVector b) {
    final float[] aVector = a.getVector();
    final float[] bVector = b.getVector();
    if (aVector.length != bVector.length)
      throw new IllegalArgumentException("vectors must have the same length (" + aVector.length + " <> " + bVector.length + ")");

    double diffSquareSum = 0.0F;
    for (int i = 0; i < aVector.length; i++)
      diffSquareSum += (aVector[i] - bVector[i]) * (aVector[i] - bVector[i]);
    return (float) Math.sqrt(diffSquareSum);
  }
}
