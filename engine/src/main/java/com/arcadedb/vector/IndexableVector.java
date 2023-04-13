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
 * base class to represent an indexable vertex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class IndexableVector<T extends Comparable> implements Comparable<IndexableVector<T>> {
  protected     T       subject;
  private final float[] vector;

  public IndexableVector(final T subject, final float[] vector) {
    this.subject = subject;
    this.vector = vector;
  }

  public T getSubject() {
    return subject;
  }

  public float[] getVector() {
    return vector;
  }

  @Override
  public int compareTo(final IndexableVector<T> o) {
    return subject.compareTo(o.subject);
  }
}
