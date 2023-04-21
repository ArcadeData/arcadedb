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

package com.arcadedb.vector.universe;/*
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
 */

import com.arcadedb.log.LogManager;
import com.arcadedb.vector.IndexableVector;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class VectorUniverse<T extends Comparable> {
  private final List<? extends IndexableVector<T>> entries;

  public VectorUniverse(final List<? extends IndexableVector<T>> entries) {
    this.entries = entries;
  }

  public int size() {
    return entries.size();
  }

  public List<? extends IndexableVector<T>> getEntries() {
    return entries;
  }

  public IndexableVector<T> get(final int i) {
    return entries.get(i);
  }

  public float[] calculateBoundariesOfValues() {
    // SOME STATS
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;

    final int tot = size();
    for (int i = 0; i < tot; i++) {
      final IndexableVector<?> w = get(i);
      float[] floats = w.getVector();
      for (int j = 0; j < floats.length; j++) {
        final float f = floats[j];
        if (f > max)
          max = f;
        if (f < min)
          min = f;
      }
    }
    LogManager.instance().log(null, Level.INFO, "min=%f max=%f", min, max);
    return new float[] { min, max };
  }

  public Map<Float, Integer> getValueDistribution() {
    final Map<Float, AtomicInteger> map = new HashMap<>();
    final int tot = size();
    for (int i = 0; i < tot; i++) {
      final IndexableVector<?> w = get(i);
      float[] floats = w.getVector();
      for (int j = 0; j < floats.length; j++) {
        AtomicInteger value = map.get(floats[j]);
        if (value == null) {
          value = new AtomicInteger(0);
          map.put(floats[j], value);
        }
        value.incrementAndGet();
      }
    }

    final TreeMap<Integer, Float> ordered = new TreeMap<>();
    for (Map.Entry<Float, AtomicInteger> entry : map.entrySet())
      ordered.put(entry.getValue().get(), entry.getKey());

    final LinkedHashMap<Float, Integer> orderedMapByOccurrences = new LinkedHashMap<>();
    for (Map.Entry<Integer, Float> entry : ordered.descendingMap().entrySet())
      orderedMapByOccurrences.put(entry.getValue(), entry.getKey());

    return orderedMapByOccurrences;
  }

  public int dimensions() {
    return entries.get(0).getVector().length;
  }
}
