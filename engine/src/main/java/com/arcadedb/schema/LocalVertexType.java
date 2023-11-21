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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;

import java.util.*;

public class LocalVertexType extends LocalDocumentType implements VertexType {
  private List<Bucket> additionalBuckets = new ArrayList<>();

  public LocalVertexType(final LocalSchema schema, final String name) {
    super(schema, name);
  }

  @Override
  public MutableVertex newRecord() {
    return schema.getDatabase().newVertex(name);
  }

  @Override
  public List<Bucket> getInvolvedBuckets() {
    final ArrayList<Bucket> result = new ArrayList<>(super.getInvolvedBuckets());
    result.addAll(additionalBuckets);
    return result;
  }

  @Override
  protected void addBucketInternal(final Bucket bucket) {
    super.addBucketInternal(bucket);
    additionalBuckets.addAll(
        ((DatabaseInternal) schema.getDatabase()).getGraphEngine().createVertexAdditionalBuckets((LocalBucket) bucket));
  }
}
