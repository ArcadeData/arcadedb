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

import java.util.*;

/**
 * Contains a Quantized list of vectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QuantizedVectors {
  private final List<QuantizedVector<?>> items;
  private final float                    min;
  private final float                    max;

  public QuantizedVectors(final List<IndexableVector<?>> list, final float min, final float max) {
    this.items = new ArrayList<>(list.size());
    this.min = min;
    this.max = max;

    final int tot = list.size();
    for (int i = 0; i < tot; i++) {
      final IndexableVector<?> vector = list.get(i);
      items.add(vector.quantize(min, max));
    }
  }

  public List<QuantizedVector<?>> getItems() {
    return items;
  }
}
