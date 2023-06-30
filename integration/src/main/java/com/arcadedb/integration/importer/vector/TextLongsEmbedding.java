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

import com.github.jelmerk.knn.Item;

import java.util.*;

public class TextLongsEmbedding implements Item<String, long[]> {
  private final String id;
  private final long[] vector;

  public TextLongsEmbedding(String id, long[] vector) {
    this.id = id;
    this.vector = vector;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public long[] vector() {
    return vector;
  }

  @Override
  public int dimensions() {
    return vector.length;
  }

  @Override
  public String toString() {
    return "IndexedText{" + "id='" + id + '\'' + ", vector=" + Arrays.toString(vector) + '}';
  }
}
