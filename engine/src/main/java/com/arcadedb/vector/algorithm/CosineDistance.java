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
 * Uses the Cosine algorithm to compute the distance between two vectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CosineDistance implements VectorDistanceAlgorithm {
  public float calculate(final IndexableVector a, final IndexableVector b) {
    final float[] aVector = a.getVector();
    final float[] bVector = b.getVector();

    if (aVector.length != bVector.length)
      throw new IllegalArgumentException("vectors must have the same length (" + aVector.length + " <> " + bVector.length + ")");

    float normA = a.getNorm();
    float normB = b.getNorm();

    float dot0 = 0F;
    float dot1 = 0F;
    float dot2 = 0F;
    float dot3 = 0F;
    float dot4 = 0F;
    float dot5 = 0F;
    float dot6 = 0F;
    float dot7 = 0F;

    for (int i = 0; i < aVector.length; i += 8) {
      dot0 = Math.fma(aVector[i], bVector[i], dot0);
      dot1 = Math.fma(aVector[i + 1], bVector[i + 1], dot1);
      dot2 = Math.fma(aVector[i + 2], bVector[i + 2], dot2);
      dot3 = Math.fma(aVector[i + 3], bVector[i + 3], dot3);
      dot4 = Math.fma(aVector[i + 4], bVector[i + 4], dot4);
      dot5 = Math.fma(aVector[i + 5], bVector[i + 5], dot5);
      dot6 = Math.fma(aVector[i + 6], bVector[i + 6], dot6);
      dot7 = Math.fma(aVector[i + 7], bVector[i + 7], dot7);
    }

    final float dotProduct = dot0 + dot1 + dot2 + dot3 + dot4 + dot5 + dot6 + dot7;

    return dotProduct / (normA * normB);
  }
}
