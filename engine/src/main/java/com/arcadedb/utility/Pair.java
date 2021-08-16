/*
 * Copyright 2021 Arcade Data Ltd
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
package com.arcadedb.utility;

import java.util.Map;

/**
 * Container for pair of non null objects.
 */
public class Pair<V1, V2> {
  private final V1 first;
  private final V2 second;

  public Pair(final V1 first, final V2 second) {
    this.first = first;
    this.second = second;
  }

  public Pair(final Map.Entry<V1, V2> entry) {
    this.first = entry.getKey();
    this.second = entry.getValue();
  }

  public V1 getFirst() {
    return first;
  }

  public V2 getSecond() {
    return second;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final Pair<?, ?> oRawPair = (Pair<?, ?>) o;

    if (!first.equals(oRawPair.first))
      return false;
    return second.equals(oRawPair.second);
  }

  @Override
  public int hashCode() {
    int result = first.hashCode();
    result = 31 * result + second.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "<" + first + "," + second + ">";
  }
}
