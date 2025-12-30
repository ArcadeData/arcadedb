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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;

import java.io.*;
import java.util.*;

public class LocalVertexType extends LocalDocumentType implements VertexType {
  private final List<Bucket> additionalBuckets = new ArrayList<>();

  public LocalVertexType(final LocalSchema schema, final String name) {
    super(schema, name);
  }

  @Override
  public MutableVertex newRecord() {
    return schema.getDatabase().newVertex(name);
  }

  @Override
  public void rename(final String newName) {
    final String oldName = name;

    super.rename(newName);

    final List<Bucket> removedBuckets = new ArrayList<>();

    try {
      for (Bucket bucket : additionalBuckets) {
        final String oldBucketName = bucket.getName();

        String newBucketName;
        if (oldBucketName.endsWith(GraphEngine.OUT_EDGES_SUFFIX)) {
          newBucketName = oldBucketName.substring(0, oldBucketName.length() - GraphEngine.OUT_EDGES_SUFFIX.length());
        } else if (oldBucketName.endsWith(GraphEngine.IN_EDGES_SUFFIX)) {
          newBucketName = oldBucketName.substring(0, oldBucketName.length() - GraphEngine.IN_EDGES_SUFFIX.length());
        } else
          throw new SchemaException(
              "Cannot rename bucket '" + oldBucketName + "' because it does not follow the naming convention");

        newBucketName = newName + newBucketName.substring(newBucketName.lastIndexOf("_"));

        ((LocalBucket) bucket).rename(newBucketName);

        removedBuckets.add(bucket);

        schema.bucketMap.remove(oldBucketName);
        schema.bucketMap.put(bucket.getName(), (LocalBucket) bucket);
      }

    } catch (IOException e) {
      super.rename(oldName);

      boolean corrupted = false;
      for (Bucket bucket : removedBuckets) {
        try {
          final String newBucketName = bucket.getName();
          final String oldBucketName = oldName + newBucketName.substring(newBucketName.lastIndexOf("_"));
          ((LocalBucket) bucket).rename(oldBucketName);
        } catch (IOException ex) {
          corrupted = true;
        }
      }

      if (corrupted)
        throw new SchemaException("Error on renaming type '" + oldName + "' in '" + newName
            + "'. The database schema is corrupted, check single file names for buckets " + removedBuckets, e);

      throw new SchemaException("Error on renaming type '" + oldName + "' in '" + newName + "'", e);
    }
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
