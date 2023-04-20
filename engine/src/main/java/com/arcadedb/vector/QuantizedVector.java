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

/**
 * Base class to represent an indexable vertex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QuantizedVector<T extends Comparable> implements Comparable<QuantizedVector<T>> {
  protected     T     subject;
  private final int[] vector;
  private final int   norm;

  public QuantizedVector(final T subject, final int[] vector) {
    this.subject = subject;
    this.vector = vector;

    // COMPUTE NORM
    float total = 0.0F;
    for (int i = 0; i < vector.length; i++)
      total = Math.fma(vector[i], vector[i], total);
    norm = (int) Math.sqrt(total);
  }

  public T getSubject() {
    return subject;
  }

  public int[] getVector() {
    return vector;
  }

  @Override
  public int compareTo(final QuantizedVector<T> o) {
    return subject.compareTo(o.subject);
  }

  public int getNorm() {
    return norm;
  }
}
