/*
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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.select;

import com.arcadedb.database.Document;

/**
 * Lightweight wrapper for vector k-NN search results, holding the matched document and its distance from the query vector.
 *
 * @param <T> the document type (Document, Vertex, etc.)
 */
public class SelectVectorResult<T extends Document> {
  private final T     document;
  private final float distance;

  public SelectVectorResult(final T document, final float distance) {
    this.document = document;
    this.distance = distance;
  }

  public T getDocument() {
    return document;
  }

  public float getDistance() {
    return distance;
  }

  @Override
  public String toString() {
    return document + " (distance=" + distance + ")";
  }
}
