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

package com.arcadedb.vector;/*
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

import java.util.*;
import java.util.logging.*;

public class VectorUniverse<T extends Comparable> {
  private final List<? extends IndexableVector<T>> list;

  public VectorUniverse(final List<? extends IndexableVector<T>> list) {
    this.list = list;
  }

  public int size() {
    return list.size();
  }

  public IndexableVector<T> get(final int i) {
    return list.get(i);
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

  public int dimensions() {
    return list.get(0).getVector().length;
  }
}
